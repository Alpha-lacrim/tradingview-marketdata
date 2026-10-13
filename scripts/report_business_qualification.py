"""Publish per-source qualification and honest category denominators; no network I/O."""

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from observe_ordinary_collector import distribution


def measure(candidate, raw):
    result = dict(candidate)
    passes = raw.get("passes", [])
    quote = raw.get("quote") or {}
    observations = quote.get("observations", [])
    supported = [p for p in passes if p["request_success"]]
    classification = next(
        (p["classifications"] for p in reversed(passes) if p.get("session_info")), {}
    )
    expected = set(classification.get("EXPECTED", []))
    closed = set(classification.get("NOT_EXPECTED", []))
    unknown = set(classification.get("UNKNOWN", [])) if classification else None
    first = passes[0] if passes else {}
    last = passes[-1] if passes else {}
    immediate = (
        set(first.get("stored_buckets", [])) & set(first.get("native_buckets", [])) & expected
        if first.get("commit_acknowledged_at") and first.get("exact_native_db_readback")
        else set()
    )
    eventual = set(last.get("stored_buckets", [])) & expected
    absent = [
        dict(attempt=p["attempt"], buckets=sorted(expected - set(p["native_buckets"])))
        for p in supported
    ]
    errors = [p for p in passes if not p["request_success"]]
    source_errors = [p for p in errors if p["error_class"] == "PermanentProviderError"]
    transports = [p for p in errors if p not in source_errors]
    info = next((p.get("source_info", {}) for p in reversed(passes) if p.get("source_info")), {})
    volume_type = info.get("volume_type")
    volume_present = any(v is not None for p in supported for v in p["native_volumes"])
    fields = {
        key: any(o.get(key) is not None for o in observations)
        for key in ("last", "bid", "ask", "event_timestamp")
    }
    ages = [
        o["last_source_age_seconds"]
        for o in observations
        if o.get("last_source_age_seconds") is not None
    ]
    first_acks = {}
    for p in passes:
        if p.get("commit_acknowledged_at") and p.get("exact_native_db_readback"):
            for bucket in set(p["native_buckets"]) & set(p["stored_buckets"]):
                first_acks.setdefault(
                    bucket,
                    (
                        datetime.fromisoformat(p["commit_acknowledged_at"])
                        - datetime.fromisoformat(bucket)
                        - timedelta(minutes=1)
                    ).total_seconds(),
                )
    native_support = any(p["native_buckets"] for p in supported)
    status = quote.get("source_metadata", {}).get("update_mode") or next(
        (p.get("source_status") for p in reversed(passes) if p.get("source_status")), None
    )
    percentage = 100 * len(eventual) / len(expected) if expected else None
    clean = all(p.get("exact_native_db_readback") and not p.get("db_error") for p in passes)
    fresh = (
        fields["last"]
        and ages
        and max(ages) <= 30
        and status in ("streaming", "realtime")
        and not quote.get("rejection")
    )
    bid_ask_receipts = [
        datetime.fromisoformat(o["received_timestamp"])
        for o in observations
        if o.get("bid") is not None and o.get("ask") is not None
    ]
    bid_ask_stream = bool(
        len(bid_ask_receipts) >= 2
        and (max(bid_ask_receipts) - min(bid_ask_receipts)).total_seconds() >= 1
        and status in ("streaming", "realtime")
        and not quote.get("rejection")
    )
    healthy = bool(
        native_support
        and expected
        and unknown == set()
        and percentage >= 99.9
        and fresh
        and not errors
        and clean
    )
    error_text = " ".join(p.get("source_error") or "" for p in errors)
    if not passes:
        qualification = "NOT_TESTED"
    elif "unavailable" in error_text and not native_support:
        qualification = "UNAVAILABLE"
    elif status and "delayed" in status:
        qualification = "DELAYED"
    elif "access_rejected" in error_text:
        qualification = "AUTHORIZATION_BLOCKED"
    elif expected - eventual or source_errors:
        qualification = "SOURCE_LIMITED"
    elif healthy:
        qualification = "PARTIALLY VERIFIED"
    else:
        qualification = "INCONCLUSIVE"
    result.update(
        native_1m_ohlc_support=True if native_support else None,
        latest_price_field_availability=fields,
        current_price_source_available=bool(fresh or bid_ask_stream),
        source_timed_last_price_fresh=bool(fresh),
        delivered_bid_ask_stream_observed=bid_ask_stream,
        bid_ask_source_timestamp_available=False,
        bid_ask_freshness_semantics="local receipt evidence; upstream bid/ask time unavailable",
        quote_observations=len(observations),
        quote_source_age_seconds=distribution(ages),
        quote_rejection=quote.get("rejection"),
        current_price_ui_implemented=False,
        volume_availability=volume_present if supported else None,
        volume_type=volume_type,
        volume_units="source tick count per native minute; not traded currency/metal volume"
        if volume_type == "tick"
        else "base asset units per native minute"
        if volume_type == "base" and candidate["category"] == "Binance"
        else "source base units; exact physical/contract unit unverified"
        if volume_type == "base"
        else None,
        quote_volume_semantics=(
            "upstream quote volume window unverified; never combined with candle volume"
        ),
        data_status="realtime (upstream-reported, not independent exchange latency verification)"
        if status in ("streaming", "realtime")
        else status or "unknown",
        expected_market_session=info,
        session_classification_evidence=classification,
        expected_test_minutes=len(expected) if classification else None,
        not_expected_market_closed_minutes=len(closed),
        unknown_session_minutes=len(unknown) if unknown is not None else 60,
        immediate_candle_commits=len(immediate),
        recovered_candle_count=len(eventual - immediate),
        eventual_candle_commits=len(eventual),
        unresolved_expected_gaps=len(expected - eventual),
        unresolved_expected_buckets=sorted(expected - eventual),
        source_absent_observations=sum(len(p["buckets"]) for p in absent),
        source_absent_observation_detail=absent,
        transport_failures=len(transports),
        source_validation_failures=len(source_errors),
        db_persistence_failures=sum(p.get("db_error") is not None for p in passes),
        immediate_completeness_percentage=100 * len(immediate) / len(expected)
        if expected
        else None,
        eventual_completeness_percentage=percentage,
        source_request_successes=len(supported),
        source_requests_observed=len(passes),
        close_to_commit_latency=dict(
            measurement="historical acknowledgement age; not scheduled current-minute latency",
            **distribution(list(first_acks.values())),
        ),
        source_request_latency_seconds=distribution(
            [p["request_seconds"] for p in passes if p.get("request_seconds") is not None]
        ),
        exact_native_db_readback=clean,
        healthy_source_snapshot=healthy,
        qualification_status=qualification,
        qualification_scope=(
            "two historical captures + bounded quote probe; ordinary business-universe "
            "scheduled soak and UI streaming NOT TESTED"
        ),
    )
    return result


def publish(
    proposal_path,
    checkpoint_path,
    output,
    csv_output,
    quote_correction=None,
    volume_provenance=None,
):
    proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
    raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    correction = (
        json.loads(quote_correction.read_text(encoding="utf-8")) if quote_correction else {}
    )
    provenance = (
        json.loads(volume_provenance.read_text(encoding="utf-8")) if volume_provenance else {}
    )
    volume_capture = {
        **correction.get("volume_capture", {}),
        **provenance.get("volume_capture", {}),
    }
    rows = []
    for candidate in proposal["candidates"]:
        original = raw["sources"].get(candidate["canonical_symbol"], {})
        corrected = correction.get("sources", {}).get(candidate["canonical_symbol"])
        source = dict(original, quote=corrected) if corrected else original
        result = measure(candidate, source)
        result["initial_quote_rejection"] = (original.get("quote") or {}).get("rejection")
        result["quote_identity_fix_verified"] = bool(corrected and corrected["observations"])
        result["normalized_volume_available"] = result["volume_availability"]
        capture = volume_capture.get(candidate["tradingview_symbol"], {})
        result["raw_native_volume_capture_samples"] = len(capture.get("raw_volume_samples", []))
        result["raw_native_volume_available"] = (
            any(v["numeric"] for v in capture.get("raw_volume_samples", [])) if capture else None
        )
        if result["volume_type"] is None:
            result["volume_availability"] = result["raw_native_volume_available"]
            result["volume_type"] = (
                "unknown (numeric source values observed)"
                if result["raw_native_volume_available"]
                else "unknown"
            )
        rows.append(result)
    categories = {}
    for category in ("Binance", "OANDA", "FXCM", "Precious metals", "Energy"):
        selected = [r for r in rows if r["category"] == category]
        expected = sum(r["expected_test_minutes"] or 0 for r in selected)
        immediate = sum(r["immediate_candle_commits"] for r in selected)
        eventual = sum(r["eventual_candle_commits"] for r in selected)
        categories[category] = dict(
            candidate_count=len(selected),
            healthy_source_snapshot_count=sum(r["healthy_source_snapshot"] for r in selected),
            unhealthy_or_unverified_count=sum(not r["healthy_source_snapshot"] for r in selected),
            expected_test_minutes=expected,
            unknown_session_minutes=sum(r["unknown_session_minutes"] for r in selected),
            not_expected_market_closed_minutes=sum(
                r["not_expected_market_closed_minutes"] for r in selected
            ),
            immediate_candle_commits=immediate,
            recovered_candle_count=sum(r["recovered_candle_count"] for r in selected),
            eventual_candle_commits=eventual,
            unresolved_expected_gaps=expected - eventual,
            source_absent_observations=sum(r["source_absent_observations"] for r in selected),
            transport_failures=sum(r["transport_failures"] for r in selected),
            source_validation_failures=sum(r["source_validation_failures"] for r in selected),
            db_persistence_failures=sum(r["db_persistence_failures"] for r in selected),
            immediate_completeness_percentage=100 * immediate / expected if expected else None,
            eventual_completeness_percentage=100 * eventual / expected if expected else None,
            status_counts=dict(Counter(r["qualification_status"] for r in selected)),
            latency_scope=(
                "historical acknowledgement ages retained per instrument; "
                "scheduled business-universe close-to-commit NOT TESTED"
            ),
        )
    preferred = []
    for pair in proposal["majors"] + proposal["crosses"]:
        sources = [
            r
            for r in rows
            if r["category"] in ("OANDA", "FXCM") and r["tradingview_symbol"].endswith(":" + pair)
        ]
        usable = [
            r
            for r in sources
            if r["native_1m_ohlc_support"]
            and r["current_price_source_available"]
            and r["eventual_completeness_percentage"] is not None
        ]
        usable.sort(
            key=lambda r: (
                -r["eventual_completeness_percentage"],
                r["transport_failures"],
                not bool(r["normalized_volume_available"]),
                r["quote_source_age_seconds"].get("max", float("inf")),
                r["source_request_latency_seconds"].get("p50", float("inf")),
            )
        )
        preferred.append(
            dict(
                pair=pair,
                preferred_ui_source=usable[0]["tradingview_symbol"] if usable else None,
                decision_status="PROVISIONAL_SHORT_CAPTURE_NOT_EMPLOYER_APPROVED",
                both_sources_retained=True,
                automatic_broker_failover=False,
                selection_basis=(
                    "native completeness, errors, classified volume, quote freshness, latency; "
                    "never merge feeds"
                ),
                comparison=[
                    dict(
                        source=r["tradingview_symbol"],
                        eventual_completeness=r["eventual_completeness_percentage"],
                        quote_fresh=r["current_price_source_available"],
                        volume_type=r["volume_type"],
                        qualification=r["qualification_status"],
                    )
                    for r in sources
                ],
            )
        )
    data = dict(
        status="PROVISIONAL_NOT_DEPLOYED",
        generated_from_sha256=hashlib.sha256(checkpoint_path.read_bytes()).hexdigest(),
        quote_correction_sha256=hashlib.sha256(quote_correction.read_bytes()).hexdigest()
        if quote_correction
        else None,
        unclassified_raw_volume_capture=volume_capture,
        volume_provenance_sha256=hashlib.sha256(volume_provenance.read_bytes()).hexdigest()
        if volume_provenance
        else None,
        isolated_volume_persistence=provenance.get("isolated_volume_persistence"),
        started_at=raw["started_at"],
        finished_at=raw.get("finished_at"),
        stopped_reason=raw.get("stopped_reason"),
        window_start=raw["window_start"],
        window_end=raw["window_end"],
        ordinary_scheduled_cycles=raw["ordinary_scheduled_cycles"],
        test_database=raw["test_database"],
        configuration=raw["configuration"],
        production_unchanged=raw.get("production_unchanged"),
        production_running=raw.get("production_running"),
        categories=categories,
        candidates=rows,
        futures_alternatives=proposal["futures_alternatives"],
        preferred_ui_sources=preferred,
    )
    output.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    fields = [k for k, v in rows[0].items() if not isinstance(v, (dict, list))]
    fields += [
        "latest_price_field_availability",
        "expected_market_session",
        "close_to_commit_latency",
        "source_request_latency_seconds",
        "quote_source_age_seconds",
    ]
    with csv_output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        futures_rows = [
            {
                **{k: None for k in fields},
                **{k: v for k, v in f.items() if k in fields},
                "category": "Futures alternatives",
                "business_priority": "SEPARATELY_LABELLED_ALTERNATIVE",
                "canonical_symbol": None,
                "candidate_approval_status": "PROVISIONAL_ALTERNATIVE_ONLY",
                "qualification_status": "NOT_TESTED",
                "qualification_scope": "unsupported alias; contract/entitlement unresolved",
            }
            for f in proposal["futures_alternatives"]
        ]
        for row in rows + futures_rows:
            writer.writerow(
                {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()}
            )
    print(json.dumps(categories, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--proposal", type=Path, default=Path("docs/business-universe-proposal.json")
    )
    parser.add_argument(
        "--checkpoint", type=Path, default=Path("artifacts/business-source-qualification.json")
    )
    parser.add_argument(
        "--output", type=Path, default=Path("docs/business-source-qualification.json")
    )
    parser.add_argument("--csv", type=Path, default=Path("docs/business-source-qualification.csv"))
    parser.add_argument("--quote-correction", type=Path)
    parser.add_argument("--volume-provenance", type=Path)
    args = parser.parse_args()
    publish(
        args.proposal,
        args.checkpoint,
        args.output,
        args.csv,
        args.quote_correction,
        args.volume_provenance,
    )
