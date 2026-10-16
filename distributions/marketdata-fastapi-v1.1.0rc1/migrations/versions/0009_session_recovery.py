"""Atomic, idempotent recovery of an expired, unchanged producer session."""

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ingest_session_recoveries (
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            recovery_id UUID NOT NULL,
            credential_key_id TEXT NOT NULL,
            request_sha256 BYTEA NOT NULL CHECK (octet_length(request_sha256)=32),
            request JSONB NOT NULL,
            previous_session_id UUID NOT NULL,
            previous_session_generation BIGINT NOT NULL CHECK (previous_session_generation>0),
            session_id UUID NOT NULL,
            session_generation BIGINT NOT NULL CHECK (
                session_generation>previous_session_generation AND
                session_generation-previous_session_generation=1),
            receipt JSONB NOT NULL CHECK (receipt->>'state'='committed'),
            server_recovered_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (producer_id,recovery_id),
            UNIQUE (producer_id,previous_session_id,previous_session_generation)
        );
        CREATE FUNCTION ingest_recover_session(p_producer UUID,p_key TEXT,p_request JSONB)
            RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
            SET search_path=pg_catalog,public AS $$
        DECLARE
            producer public.ingest_producers%ROWTYPE;
            prior public.ingest_session_recoveries%ROWTYPE;
            recovery UUID; previous_session UUID; previous_generation BIGINT;
            manifest JSONB; normalized JSONB; digest BYTEA;
            new_session UUID; recovered_at TIMESTAMPTZ; lease TIMESTAMPTZ; result JSONB;
        BEGIN
            -- Validate even direct SQL calls; runtime has no table DML or policy UPDATE.
            IF p_request IS NULL OR jsonb_typeof(p_request)<>'object' OR
                octet_length(p_request::text)>262144 OR
                NOT p_request ?& ARRAY['protocol_version','recovery_id','previous_session_id',
                    'previous_session_generation','streams'] OR
                (SELECT count(*) FROM jsonb_object_keys(p_request))<>5 OR
                p_request->'protocol_version'<>'1'::jsonb OR
                jsonb_typeof(p_request->'previous_session_generation')<>'number' OR
                NOT (p_request->>'previous_session_generation' ~ '^[1-9][0-9]{0,18}$') OR
                jsonb_typeof(p_request->'streams')<>'array' THEN
                RETURN jsonb_build_object('error','invalid_recovery');
            END IF;
            IF jsonb_array_length(p_request->'streams') NOT BETWEEN 1 AND 1000 THEN
                RETURN jsonb_build_object('error','invalid_recovery');
            END IF;
            BEGIN
                recovery := (p_request->>'recovery_id')::uuid;
                previous_session := (p_request->>'previous_session_id')::uuid;
                previous_generation := (p_request->>'previous_session_generation')::bigint;
                IF recovery IS NULL OR previous_session IS NULL THEN
                    RETURN jsonb_build_object('error','invalid_recovery');
                END IF;
                IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_request->'streams') s WHERE
                    jsonb_typeof(s)<>'object' OR
                    NOT s ?& ARRAY['instrument_id','provider','stream','source_epoch',
                        'ownership_generation'] OR
                    (SELECT count(*) FROM jsonb_object_keys(s))<>5 OR
                    jsonb_typeof(s->'ownership_generation')<>'number' OR
                    NOT (s->>'ownership_generation' ~ '^[1-9][0-9]{0,18}$')) THEN
                    RETURN jsonb_build_object('error','invalid_recovery');
                END IF;
                IF EXISTS(SELECT 1 FROM jsonb_to_recordset(p_request->'streams') AS s(
                    instrument_id UUID,provider TEXT,stream TEXT,source_epoch UUID,
                    ownership_generation BIGINT) WHERE instrument_id IS NULL OR
                    provider IS NULL OR provider !~ '^[a-z][a-z0-9_]{0,99}$' OR
                    stream IS NULL OR stream NOT IN ('candles','quotes','coverage','status') OR
                    source_epoch IS NULL OR ownership_generation IS NULL OR
                    ownership_generation<1) THEN
                    RETURN jsonb_build_object('error','invalid_recovery');
                END IF;
                SELECT jsonb_agg(jsonb_build_object('instrument_id',s.instrument_id,
                    'provider',s.provider,'stream',s.stream,'source_epoch',s.source_epoch,
                    'ownership_generation',s.ownership_generation)
                    ORDER BY s.instrument_id,s.provider,s.stream) INTO manifest
                    FROM jsonb_to_recordset(p_request->'streams') AS s(instrument_id UUID,
                        provider TEXT,stream TEXT,source_epoch UUID,ownership_generation BIGINT);
            EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                RETURN jsonb_build_object('error','invalid_recovery');
            END;
            IF (SELECT count(DISTINCT (s->>'instrument_id',s->>'provider',s->>'stream'))
                FROM jsonb_array_elements(manifest) s)<>jsonb_array_length(manifest) THEN
                RETURN jsonb_build_object('error','invalid_recovery');
            END IF;
            normalized := jsonb_build_object('protocol_version',1,'recovery_id',recovery,
                'previous_session_id',previous_session,
                'previous_session_generation',previous_generation,'streams',manifest);
            digest := sha256(convert_to(normalized::text,'UTF8'));
            SELECT * INTO producer FROM public.ingest_producers
                WHERE producer_id=p_producer FOR UPDATE;
            IF NOT FOUND OR NOT producer.enabled OR producer.credential_key_id<>p_key
                OR p_key IS NULL THEN
                RETURN jsonb_build_object('error','producer_not_authorized');
            END IF;
            SELECT * INTO prior FROM public.ingest_session_recoveries
                WHERE producer_id=p_producer AND recovery_id=recovery;
            IF FOUND AND prior.request_sha256<>digest THEN
                RETURN jsonb_build_object('error','recovery_payload_conflict');
            END IF;
            -- Producer first, then sorted stream rows: same order as existing uploads.
            -- Include rows now owned by someone else, so a racing transfer is observed.
            PERFORM 1 FROM public.ingest_source_owners o JOIN jsonb_to_recordset(manifest)
                AS s(instrument_id UUID,provider TEXT,stream TEXT,source_epoch UUID,
                    ownership_generation BIGINT)
                ON (o.instrument_id,o.provider,o.stream)=(s.instrument_id,s.provider,s.stream)
                ORDER BY o.instrument_id,o.provider,o.stream FOR SHARE OF o;
            IF (SELECT count(*) FROM public.ingest_source_owners WHERE producer_id=p_producer)
                    <>jsonb_array_length(manifest) OR
                EXISTS(SELECT 1 FROM jsonb_to_recordset(manifest) AS s(instrument_id UUID,
                    provider TEXT,stream TEXT,source_epoch UUID,ownership_generation BIGINT)
                    LEFT JOIN public.ingest_source_owners o ON
                        (o.instrument_id,o.provider,o.stream)=(s.instrument_id,s.provider,s.stream)
                    WHERE (o.producer_id,o.source_epoch,o.ownership_generation) IS DISTINCT FROM
                        (p_producer,s.source_epoch,s.ownership_generation)) THEN
                RETURN jsonb_build_object('error','recovery_ownership_changed');
            END IF;
            IF prior.recovery_id IS NOT NULL THEN
                IF (producer.session_id,producer.session_generation) IS DISTINCT FROM
                    (prior.session_id,prior.session_generation) THEN
                    RETURN jsonb_build_object('error','recovery_obsolete');
                END IF;
                -- Safe lost-response replay, even if that lease has since expired.
                -- Never extend it or reveal any subsequent replacement session.
                RETURN prior.receipt;
            END IF;
            IF (producer.session_id,producer.session_generation) IS DISTINCT FROM
                (previous_session,previous_generation) THEN
                RETURN jsonb_build_object('error','producer_session_fenced');
            END IF;
            IF producer.session_expires_at>clock_timestamp() THEN
                RETURN jsonb_build_object('error','producer_session_active');
            END IF;
            IF previous_generation=9223372036854775807 THEN
                RETURN jsonb_build_object('error','session_generation_exhausted');
            END IF;
            new_session := gen_random_uuid();
            recovered_at := clock_timestamp(); lease := recovered_at+interval '120 seconds';
            UPDATE public.ingest_producers SET session_id=new_session,
                session_generation=previous_generation+1,session_expires_at=lease
                WHERE producer_id=p_producer;
            result := jsonb_build_object('protocol_version',1,'producer_id',p_producer,
                'recovery_id',recovery,'previous_session_id',previous_session,
                'previous_session_generation',previous_generation,'session_id',new_session,
                'session_generation',previous_generation+1,'expires_at',lease,
                'server_recovered_at',recovered_at,'state','committed','streams',manifest);
            INSERT INTO public.ingest_session_recoveries VALUES (p_producer,recovery,p_key,digest,
                normalized,previous_session,previous_generation,new_session,previous_generation+1,
                result,recovered_at);
            RETURN result;
        END $$;
        REVOKE ALL ON FUNCTION ingest_recover_session(UUID,TEXT,JSONB) FROM PUBLIC;
    """)


def downgrade() -> None:
    op.execute("""
        DROP FUNCTION ingest_recover_session(UUID,TEXT,JSONB);
        DROP TABLE ingest_session_recoveries;
    """)
