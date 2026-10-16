"""Opt-in fenced local delivery state; no destination schema or network access."""

from alembic import op

revision = "sc0002"
down_revision = "sc0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(SQL)


def downgrade() -> None:
    raise RuntimeError("delivery state must be reconciled before an explicitly reviewed rollback")


SQL = r"""
CREATE TABLE source_sync_owner (
    singleton BOOLEAN PRIMARY KEY CHECK(singleton),
    generation BIGINT NOT NULL DEFAULT 0 CHECK(generation>=0),
    token UUID, lock_pid INTEGER, lock_started TIMESTAMPTZ,
    identity JSONB, state JSONB NOT NULL DEFAULT '{}'
);
INSERT INTO source_sync_owner(singleton) VALUES(TRUE);
CREATE TABLE source_sync_batches (
    batch_id UUID PRIMARY KEY,
    payload BYTEA NOT NULL CHECK(octet_length(payload)<=2097152),
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64),
    stream TEXT NOT NULL CHECK(stream IN ('candles','quotes','coverage','status')),
    bookkeeping JSONB NOT NULL,
    receipt JSONB,
    phase TEXT NOT NULL DEFAULT 'staged'
        CHECK(phase IN ('staged','received','acknowledged','replaced','quarantined')),
    attempts INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX source_sync_pending ON source_sync_batches(stream,created_at)
    WHERE phase IN ('staged','received');
CREATE TABLE source_sync_archive (
    batch_id UUID PRIMARY KEY, payload_sha256 TEXT NOT NULL,
    receipt JSONB, completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE source_sync_guard(singleton BOOLEAN PRIMARY KEY CHECK(singleton),stamp TIMESTAMPTZ);
INSERT INTO source_sync_guard VALUES(TRUE,NULL);

CREATE FUNCTION source_sync_lock_alive(p INTEGER, started TIMESTAMPTZ) RETURNS BOOLEAN
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
    SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
        WHERE l.locktype='advisory' AND l.granted AND l.pid=p AND a.backend_start=started
        AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
        AND l.classid=((hashtextextended('marketdata:source-sync',0)>>32)&4294967295)::oid
        AND l.objid=(hashtextextended('marketdata:source-sync',0)&4294967295)::oid
        AND l.objsubid=1)
$$;
CREATE FUNCTION source_sync_claim(t UUID) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE started TIMESTAMPTZ; g BIGINT;
BEGIN
    SELECT backend_start INTO started FROM pg_stat_activity WHERE pid=pg_backend_pid();
    IF NOT public.source_sync_lock_alive(pg_backend_pid(),started) THEN
        RAISE EXCEPTION 'worker lock required' USING ERRCODE='55000';
    END IF;
    UPDATE public.source_sync_owner SET generation=generation+1,token=t,
        lock_pid=pg_backend_pid(),lock_started=started WHERE singleton RETURNING generation INTO g;
    RETURN g;
END $$;
CREATE FUNCTION source_sync_enter(t UUID,g BIGINT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE owner public.source_sync_owner;
BEGIN
    SELECT * INTO owner FROM public.source_sync_owner WHERE singleton FOR SHARE;
    IF owner.token IS DISTINCT FROM t OR owner.generation<>g OR NOT
        public.source_sync_lock_alive(owner.lock_pid,owner.lock_started) THEN
        RAISE EXCEPTION 'worker fenced' USING ERRCODE='55000';
    END IF;
    IF owner.identity IS NOT NULL AND (owner.identity->>'source_epoch')::uuid <>
        (SELECT source_epoch FROM public.source_capture_clock WHERE singleton) THEN
        RAISE EXCEPTION 'source epoch changed' USING ERRCODE='55000';
    END IF;
    PERFORM set_config('marketdata.sync_token',t::text,true);
    PERFORM set_config('marketdata.sync_generation',g::text,true);
END $$;
CREATE FUNCTION source_sync_assert() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
    PERFORM public.source_sync_enter(
        nullif(current_setting('marketdata.sync_token',true),'')::uuid,
        nullif(current_setting('marketdata.sync_generation',true),'')::bigint);
    IF nullif(current_setting('marketdata.sync_token',true),'') IS NULL THEN
        RAISE EXCEPTION 'worker fence missing' USING ERRCODE='55000';
    END IF;
END $$;
CREATE FUNCTION source_sync_commit_fence() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
    PERFORM public.source_sync_assert(); RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER source_sync_owner_commit AFTER UPDATE ON source_sync_owner
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.state IS DISTINCT FROM OLD.state OR NEW.identity IS DISTINCT FROM OLD.identity)
    EXECUTE FUNCTION source_sync_commit_fence();
CREATE CONSTRAINT TRIGGER source_sync_batch_commit AFTER INSERT OR UPDATE OR DELETE ON
            source_sync_batches
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source_sync_commit_fence();
CREATE CONSTRAINT TRIGGER source_sync_capture_commit AFTER UPDATE ON source_sync_guard
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION source_sync_commit_fence();

CREATE FUNCTION source_sync_state(operation TEXT, data JSONB) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE previous public.source_sync_batches; raw BYTEA; id UUID; n BIGINT;
BEGIN
    PERFORM public.source_sync_assert();
    IF operation='initialize' THEN
        IF (SELECT identity FROM public.source_sync_owner WHERE singleton) IS NOT NULL THEN
            IF (SELECT identity FROM public.source_sync_owner WHERE singleton)<>data->'identity'
            THEN
                RAISE EXCEPTION 'identity mismatch' USING ERRCODE='23514';
            END IF;
        ELSE
            IF (data->'identity'->>'source_epoch')::uuid <>
                (SELECT source_epoch FROM public.source_capture_clock) THEN
                RAISE EXCEPTION 'epoch mismatch' USING ERRCODE='23514';
            END IF;
            UPDATE public.source_sync_owner SET identity=data->'identity',state=data->'state'
                WHERE singleton;
        END IF;
    ELSIF operation='state' THEN
        UPDATE public.source_sync_owner SET state=state||data WHERE singleton;
    ELSIF operation='stage' THEN
        raw=decode(data->>'payload','base64'); id=(data->>'batch_id')::uuid;
        IF encode(sha256(raw),'hex')<>data->>'payload_sha256' OR octet_length(raw)>2097152 THEN
            RAISE EXCEPTION 'immutable payload hash mismatch' USING ERRCODE='23514';
        END IF;
        IF EXISTS(SELECT 1 FROM public.source_sync_batches WHERE batch_id=id) THEN
            IF NOT EXISTS(SELECT 1 FROM public.source_sync_batches WHERE batch_id=id AND
            payload=raw) THEN
                RAISE EXCEPTION 'immutable batch conflict' USING ERRCODE='23514';
            END IF;
        ELSE
            SELECT coalesce(sum(octet_length(payload)),0) INTO n FROM public.source_sync_batches;
            IF n+octet_length(raw)>67108864 OR
                (SELECT count(*) FROM public.source_sync_batches)>=10000 THEN
                RAISE EXCEPTION 'staged capacity exhausted' USING ERRCODE='54000';
            END IF;
            INSERT INTO
            public.source_sync_batches(batch_id,payload,payload_sha256,stream,bookkeeping)
                VALUES(id,raw,data->>'payload_sha256',data->>'stream',data->'bookkeeping');
        END IF;
    ELSIF operation IN ('receipt','attempt','replace','quarantine','retry') THEN
        id=(data->>'batch_id')::uuid;
        SELECT * INTO previous FROM public.source_sync_batches WHERE batch_id=id FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'unknown batch' USING ERRCODE='23514'; END IF;
        IF operation='retry' THEN
            UPDATE public.source_sync_batches SET phase='staged',error_code=NULL,
                next_attempt=clock_timestamp() WHERE batch_id=id AND phase='quarantined';
        ELSIF operation='receipt' THEN
            IF data->'receipt'->>'state'<>'committed' OR
                data->'receipt'->>'payload_sha256'<>previous.payload_sha256 OR
                (data->'receipt'->>'batch_id')::uuid<>id THEN
                RAISE EXCEPTION 'receipt mismatch' USING ERRCODE='23514';
            END IF;
            IF previous.receipt IS NOT NULL AND previous.receipt<>data->'receipt' THEN
                RAISE EXCEPTION 'receipt changed' USING ERRCODE='23514';
            END IF;
            UPDATE public.source_sync_batches SET receipt=data->'receipt',phase='received',
                error_code=NULL WHERE batch_id=id AND phase IN ('staged','received');
        ELSIF operation='attempt' THEN
            UPDATE public.source_sync_batches SET attempts=attempts+1,
                error_code=left(data->>'code',100), next_attempt=clock_timestamp()+
                make_interval(secs=>least(300,greatest(0,(data->>'delay')::double precision)))
                WHERE batch_id=id AND phase='staged';
        ELSE
            UPDATE public.source_sync_batches SET phase=CASE operation WHEN 'replace' THEN
                'replaced' ELSE 'quarantined' END,error_code=left(data->>'code',100)
                WHERE batch_id=id AND phase='staged';
        END IF;
    ELSIF operation='prune' THEN
        WITH removed AS (DELETE FROM public.source_sync_batches WHERE batch_id IN
            (SELECT batch_id FROM public.source_sync_batches WHERE phase='acknowledged'
                ORDER BY created_at LIMIT 500) RETURNING *)
        INSERT INTO public.source_sync_archive(batch_id,payload_sha256,receipt)
            SELECT batch_id,payload_sha256,receipt-'records' FROM removed ON CONFLICT DO NOTHING;
        -- Archive receipts remain durable; monitor their smaller, unbounded audit growth.
    ELSE
        RAISE EXCEPTION 'unsupported operation' USING ERRCODE='23514';
    END IF;
    RETURN (SELECT jsonb_build_object('identity',identity,'state',state)
        FROM public.source_sync_owner WHERE singleton);
END $$;
CREATE FUNCTION source_sync_storage() RETURNS JSONB
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
    SELECT jsonb_build_object('cluster_wal_bytes',coalesce(sum(size),0)) FROM pg_ls_waldir()
$$;

CREATE FUNCTION source_sync_capture(operation TEXT,data JSONB) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE s public.source_capture_clock; b public.source_sync_batches;
    r JSONB; actual JSONB; o JSONB; f JSONB; timed BOOLEAN;
    field_time TIMESTAMPTZ; field_ns BIGINT;
    pos BIGINT; n BIGINT; bytes BIGINT; name TEXT; counts JSONB;
BEGIN
    PERFORM public.source_sync_assert();
    PERFORM set_config('synchronous_commit','on',true);
    SELECT * INTO s FROM public.source_capture_clock WHERE singleton FOR UPDATE;
    IF operation='clock' THEN RETURN to_jsonb(s); END IF;
    UPDATE public.source_sync_guard SET stamp=clock_timestamp() WHERE singleton;
    IF operation='metadata_position' THEN
        UPDATE public.source_capture_clock SET head=head+1 WHERE singleton RETURNING head INTO pos;
        RETURN to_jsonb(pos);
    END IF;
    IF operation IN ('ack_candles','ack_quote') THEN
        SELECT * INTO b FROM public.source_sync_batches WHERE batch_id=(data->>'batch_id')::uuid;
        IF b.receipt IS NULL OR b.phase NOT IN ('received','acknowledged') OR
            (data->>'epoch')::uuid<>s.source_epoch THEN
            RAISE EXCEPTION 'verified batch required' USING ERRCODE='23514';
        END IF;
        IF operation='ack_candles' THEN
            IF b.stream<>'candles' OR b.bookkeeping->>'after'<>data->>'after' OR
                b.bookkeeping->>'through'<>data->>'through' OR
                (data->>'through')::bigint>s.head OR s.candle_ack NOT IN
                    ((data->>'after')::bigint,(data->>'through')::bigint) THEN
                RAISE EXCEPTION 'cursor mismatch' USING ERRCODE='23514';
            END IF;
            IF s.candle_ack<>(data->>'through')::bigint AND EXISTS(
                SELECT 1 FROM public.source_candle_outbox o WHERE
                    o.position>(data->>'after')::bigint AND o.position<=(data->>'through')::bigint
                    AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(
                        convert_from(b.payload,'UTF8')::jsonb->'records') x WHERE x=o.record)
            ) THEN RAISE EXCEPTION 'uncovered candle range' USING ERRCODE='23514'; END IF;
            UPDATE public.source_capture_clock SET candle_ack=(data->>'through')::bigint WHERE
            singleton;
        ELSE
            IF b.stream<>'quotes' OR NOT EXISTS(SELECT 1 FROM
                jsonb_array_elements(convert_from(b.payload,'UTF8')::jsonb->'records') x
                WHERE x->>'source_position'=data->>'version' AND
                x->'observation'->>'instrument_id'=data->>'instrument' AND
                x->'observation'->>'provider'=data->>'provider') THEN
                RAISE EXCEPTION 'quote version mismatch' USING ERRCODE='23514';
            END IF;
            UPDATE public.source_quote_state SET
            acked_version=greatest(acked_version,(data->>'version')::bigint)
                WHERE instrument_id=(data->>'instrument')::uuid AND provider=data->>'provider'
                AND version>=(data->>'version')::bigint;
            IF NOT FOUND THEN RAISE EXCEPTION 'unknown quote version' USING ERRCODE='23514'; END IF;
        END IF;
        RETURN 'true'::jsonb;
    ELSIF operation='ack_batch' THEN
        SELECT * INTO b FROM public.source_sync_batches WHERE batch_id=(data->>'batch_id')::uuid
            AND phase='received' AND receipt IS NOT NULL FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'receipt required' USING ERRCODE='23514'; END IF;
        UPDATE public.source_sync_batches SET phase='acknowledged'
            WHERE batch_id=(data->>'batch_id')::uuid AND phase='received' AND receipt IS NOT NULL;
        IF NOT FOUND THEN RAISE EXCEPTION 'receipt required' USING ERRCODE='23514'; END IF;
        SELECT coalesce(state->'delivery_counts','{}') INTO counts
            FROM public.source_sync_owner WHERE singleton;
        IF b.stream='candles' THEN
            FOREACH name IN ARRAY ARRAY['candle_completed','candle_corrected','history_seed'] LOOP
                SELECT count(*) INTO n FROM jsonb_each_text(b.bookkeeping->'event_kinds') x
                    WHERE x.value=name;
                counts=jsonb_set(counts,ARRAY[name],to_jsonb(coalesce((counts->>name)::bigint,0)+n));
            END LOOP;
        ELSE
            n=jsonb_array_length(convert_from(b.payload,'UTF8')::jsonb->'records');
            counts=jsonb_set(counts,ARRAY[b.stream],
                to_jsonb(coalesce((counts->>b.stream)::bigint,0)+n));
        END IF;
        UPDATE public.source_sync_owner SET state=state||jsonb_build_object(
            'delivery_counts',counts,'last_ack',jsonb_build_object('batch_id',b.batch_id,
                'stream',b.stream,'after',b.bookkeeping->'after','through',b.bookkeeping->'through'))
            WHERE singleton;
    ELSIF operation='empty_ack' THEN
        IF (data->>'epoch')::uuid<>s.source_epoch OR (data->>'after')::bigint<>s.candle_ack OR
            (data->>'through')::bigint<s.candle_ack OR
            (data->>'through')::bigint>s.head OR EXISTS(SELECT 1 FROM public.source_candle_outbox
                WHERE position>s.candle_ack AND position<=(data->>'through')::bigint) THEN
            RAISE EXCEPTION 'nonempty cursor range' USING ERRCODE='23514';
        END IF;
        UPDATE public.source_capture_clock SET candle_ack=(data->>'through')::bigint WHERE
            singleton;
    ELSIF operation='cleanup' THEN
        WITH removed AS (DELETE FROM public.source_candle_outbox WHERE position IN
            (SELECT position FROM public.source_candle_outbox WHERE position<=s.candle_ack
                ORDER BY position LIMIT least(10000,greatest(1,(data->>'limit')::int))) RETURNING
            byte_count)
        SELECT count(*),coalesce(sum(byte_count),0) INTO n,bytes FROM removed;
        UPDATE public.source_capture_clock SET
            pending_rows=pending_rows-n,pending_bytes=pending_bytes-bytes
            WHERE singleton;
        RETURN to_jsonb(n);
    ELSIF operation='seed_candles' THEN
        n=jsonb_array_length(data->'rows'); bytes=0;
        IF n>500 OR (data->>'head')::bigint<>s.head THEN
            RAISE EXCEPTION 'seed bound changed' USING ERRCODE='23514'; END IF;
        FOR r IN SELECT * FROM jsonb_array_elements(data->'rows') LOOP
            o=r->'record'->'observation'; pos=(r->'record'->>'source_position')::bigint;
            SELECT payload INTO actual FROM public.market_candles WHERE
                instrument_id=(o->>'instrument_id')::uuid AND provider=o->>'provider'
                AND bucket_start=(o->>'bucket_start')::timestamptz AND interval='1m' AND
            price_basis='native';
            IF actual IS NULL OR (actual-ARRAY['open','high','low','close','volume'])<>
                (o-ARRAY['open','high','low','close','volume']) THEN
                RAISE EXCEPTION 'seed must match stored candle' USING ERRCODE='23514'; END IF;
            FOREACH name IN ARRAY ARRAY['open','high','low','close','volume'] LOOP
                IF (actual->>name)::numeric IS DISTINCT FROM (o->>name)::numeric THEN
                    RAISE EXCEPTION 'seed price mismatch' USING ERRCODE='23514'; END IF;
            END LOOP;
            IF pos<>s.head+1 OR octet_length(r->>'canonical')>65536 OR
                (r->>'canonical')::jsonb<>r->'record' THEN
                RAISE EXCEPTION 'seed record mismatch' USING ERRCODE='23514'; END IF;
            IF EXISTS(SELECT 1 FROM public.source_candle_exports WHERE
                instrument_id=(o->>'instrument_id')::uuid AND provider=o->>'provider'
                AND bucket_start=(o->>'bucket_start')::timestamptz) AND
                coalesce(data->>'repair','false')<>'true' THEN
                RAISE EXCEPTION 'already enrolled' USING ERRCODE='23514'; END IF;
            bytes=bytes+octet_length(r->>'canonical'); s.head=pos;
            INSERT INTO public.source_candle_outbox
            VALUES(pos,(r->'record'->>'observation_id')::uuid,
(o->>'instrument_id')::uuid,o->>'provider',r->'record',octet_length(r->>'canonical'),
                'history_seed',clock_timestamp());
            INSERT INTO public.source_candle_exports
            VALUES((o->>'instrument_id')::uuid,o->>'provider',
                (o->>'bucket_start')::timestamptz,pos) ON
            CONFLICT(instrument_id,provider,bucket_start)
                DO UPDATE SET position=EXCLUDED.position;
        END LOOP;
        IF s.pending_rows+n>least(1000000,(data->>'max_rows')::bigint) OR
            s.pending_bytes+bytes>least(536870912,(data->>'max_bytes')::bigint) THEN
            RAISE EXCEPTION 'capture capacity exhausted' USING ERRCODE='54000'; END IF;
        UPDATE public.source_capture_clock SET head=s.head,pending_rows=pending_rows+n,
            pending_bytes=pending_bytes+bytes WHERE singleton;
    ELSIF operation='resnapshot_quote' THEN
        r=data->'record'; o=r->'observation';
        SELECT record->'observation' INTO actual FROM public.source_quote_state WHERE
            instrument_id=(o->>'instrument_id')::uuid AND provider=o->>'provider';
        IF actual IS NULL OR (actual-'tick_id')<>(o-'tick_id') OR
            (r->>'source_position')::bigint<>s.head+1 THEN
            RAISE EXCEPTION 'resnapshot must preserve observed fields' USING ERRCODE='23514';
        END IF;
        UPDATE public.source_quote_state SET version=s.head+1,record=r
            WHERE instrument_id=(o->>'instrument_id')::uuid AND provider=o->>'provider';
        UPDATE public.source_capture_clock SET head=head+1 WHERE singleton;
    ELSIF operation='seed_quote' THEN
        r=data->'record'; o=r->'observation';
        SELECT payload INTO actual FROM public.latest_prices WHERE
            instrument_id=(o->>'instrument_id')::uuid AND provider=o->>'provider';
        IF actual IS NULL OR EXISTS(SELECT 1 FROM public.source_quote_state WHERE
            instrument_id=(o->>'instrument_id')::uuid AND provider=o->>'provider') OR
            (r->>'source_position')::bigint<>s.head+1 THEN
            RAISE EXCEPTION 'quote seed changed' USING ERRCODE='23514'; END IF;
        FOREACH name IN ARRAY ARRAY['instrument_id','provider','provider_symbol','canonical_symbol',
            'asset_class','instrument_type','exchange',
            'received_timestamp','realtime_status'] LOOP
            IF actual->name IS DISTINCT FROM o->name THEN
                RAISE EXCEPTION 'quote seed identity mismatch' USING ERRCODE='23514'; END IF;
        END LOOP;
        FOREACH name IN ARRAY ARRAY['last','bid','ask','last_size','bid_size','ask_size','volume']
            LOOP
            IF (actual->>name)::numeric IS DISTINCT FROM (o->>name)::numeric THEN
                RAISE EXCEPTION 'quote seed value mismatch' USING ERRCODE='23514'; END IF;
            IF actual->>name IS NOT NULL THEN
                f=o->'metadata'->'field_observations'->name;
                timed=NOT(actual->'metadata' ? 'timestamped_fields') OR
                    (actual->'metadata'->'timestamped_fields') ? name;
                field_time=NULL; field_ns=NULL;
                IF timed THEN
                    field_time=(actual->>'event_timestamp')::timestamptz;
                    field_ns=(actual->>'event_timestamp_ns')::bigint;
                END IF;
                IF f IS NULL OR f->>'observation_id' IS DISTINCT FROM actual->>'tick_id' OR
                    (f->>'value')::numeric IS DISTINCT FROM (actual->>name)::numeric OR
                    (f->>'received_timestamp')::timestamptz IS DISTINCT FROM
                        (actual->>'received_timestamp')::timestamptz OR
                    f->'realtime_status' IS DISTINCT FROM actual->'realtime_status' OR
                    f->'metadata' IS DISTINCT FROM actual->'metadata' OR
                    f->'sequence_number' IS DISTINCT FROM actual->'sequence_number' OR
                    (f->>'event_timestamp')::timestamptz IS DISTINCT FROM field_time OR
                    (f->>'event_timestamp_ns')::bigint IS DISTINCT FROM field_ns THEN
                    RAISE EXCEPTION 'quote seed provenance mismatch' USING ERRCODE='23514';
                END IF;
            END IF;
        END LOOP;
        name=CASE WHEN o->>'last' IS NOT NULL THEN 'last' WHEN o->>'bid' IS NOT NULL
            THEN 'bid' ELSE 'ask' END;
        f=o->'metadata'->'field_observations'->name;
        IF (o->>'event_timestamp')::timestamptz IS DISTINCT FROM
            (f->>'event_timestamp')::timestamptz OR (o->>'event_timestamp_ns')::bigint
            IS DISTINCT FROM (f->>'event_timestamp_ns')::bigint THEN
            RAISE EXCEPTION 'quote seed primary mismatch' USING ERRCODE='23514';
        END IF;
        INSERT INTO public.source_quote_state(instrument_id,provider,version,fields,record)
            VALUES((o->>'instrument_id')::uuid,o->>'provider',(r->>'source_position')::bigint,
                   o->'metadata'->'field_observations',r);
        UPDATE public.source_capture_clock SET head=head+1 WHERE singleton;
    ELSE RAISE EXCEPTION 'unsupported capture operation' USING ERRCODE='23514';
    END IF;
    RETURN 'true'::jsonb;
END $$;
REVOKE ALL ON FUNCTION source_sync_lock_alive(INTEGER,TIMESTAMPTZ),source_sync_claim(UUID),
    source_sync_enter(UUID,BIGINT),source_sync_assert(),source_sync_commit_fence(),
    source_sync_state(TEXT,JSONB),source_sync_capture(TEXT,JSONB),source_sync_storage() FROM PUBLIC;
"""
