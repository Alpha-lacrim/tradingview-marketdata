"""Assemble the M5 public sidecar from existing local evidence, never private credentials."""

import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads((ROOT / path).read_text("utf-8"))


def suite(path):
    tree = ET.parse(ROOT / path)
    cases = tree.findall(".//testcase")
    failures = sum(c.find("failure") is not None for c in cases)
    errors = sum(c.find("error") is not None for c in cases)
    skips = [
        dict(
            test=c.get("classname", "") + "." + c.get("name", ""),
            reason=c.find("skipped").get("message", ""),
        )
        for c in cases
        if c.find("skipped") is not None
    ]
    return dict(
        total=len(cases),
        passed=len(cases) - failures - errors - len(skips),
        failed=failures,
        errors=errors,
        skipped=len(skips),
        skip_details=skips,
        seconds=sum(float(s.get("time", "0")) for s in tree.findall(".//testsuite")),
        junit=path,
        sha256=digest(ROOT / path),
    )


def main():
    baseline = read("artifacts/v11-m5-baseline/manifest.json")
    changed = [p for p, d in baseline.items() if not (ROOT / p).exists() or digest(ROOT / p) != d]
    assert not any(
        p.startswith(("migrations/", "source_migrations/", "scripts/sql/")) for p in changed
    )
    frozen = ROOT / "artifacts/marketdata-fastapi-v1.0.zip"
    assert digest(frozen) == "3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a"
    with zipfile.ZipFile(frozen) as z:
        entries = [n for n in z.namelist() if not n.endswith("/")]
        assert all((ROOT / "distributions" / n).read_bytes() == z.read(n) for n in entries)
    result = dict(
        milestone="5",
        version="1.1.0rc1",
        candidate=True,
        generated_at=datetime.now(UTC).isoformat(),
        source_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT)
        .decode()
        .strip(),
        uncommitted_changes_preserved=True,
        destination_head="0010",
        source_base="0006",
        source_head="sc0002",
        migrations_changed=False,
        artifacts=[],
        components={},
        suites={},
        exported_installs={},
        pipeline={},
        modified_baseline_files=changed,
        frozen=dict(
            path=frozen.relative_to(ROOT).as_posix(),
            sha256=digest(frozen),
            matching_payload_entries=len(entries),
        ),
    )
    for component, name in [
        ("company", "marketdata-fastapi-v1.1.0rc1"),
        ("collector", "marketdata-collector-sync-v1.1.0rc1"),
    ]:
        package = ROOT / "distributions" / name
        result["components"][component] = dict(
            release=read(str(package.relative_to(ROOT) / "RELEASE.json")),
            migrations=read(str(package.relative_to(ROOT) / "MIGRATIONS.json")),
            manifest_sha256=digest(package / "SHA256SUMS"),
            clean_build=read(f"artifacts/v11-m5-{component}-qualified-build.json"),
            package_tests=suite(f"artifacts/v11-m5-{component}-qualified-package.xml"),
        )
        assert result["components"][component]["clean_build"]["exit_code"] == 0
        for line in (package / "SHA256SUMS").read_text().splitlines():
            checksum, relative = line.split("  ", 1)
            assert digest(package / relative) == checksum
        archive = ROOT / "artifacts" / (name + ".zip")
        with zipfile.ZipFile(archive) as z:
            assert z.testzip() is None
        result["artifacts"].append(
            dict(
                path=archive.relative_to(ROOT).as_posix(),
                sha256=digest(archive),
                bytes=archive.stat().st_size,
            )
        )
    for wheel in sorted((ROOT / "artifacts/v11-m5-wheels").glob("*.whl")):
        result["artifacts"].append(
            dict(
                path=wheel.relative_to(ROOT).as_posix(),
                sha256=digest(wheel),
                bytes=wheel.stat().st_size,
            )
        )
    for profile in ("pg", "ts"):
        s = suite(f"artifacts/v11-m5-{profile}-final-suite.xml")
        assert s["failed"] == s["errors"] == 0
        result["suites"][profile] = s
        install = "final-install" if profile == "pg" else "installed"
        result["exported_installs"][profile] = read(
            f"artifacts/v11-m5-{profile}-{install}/verification.json"
        )
        pipeline = read(f"artifacts/v11-m5-{profile}-pipeline/report.json")
        assert "failure_category" not in pipeline
        assert all(c["passed"] for c in pipeline["checks"])
        comparison = read(f"artifacts/v11-m5-{profile}-pipeline/database-comparison.json")
        assert comparison["candles"]["equal"]
        assert comparison["quotes"]["all_fields_and_ordering_equal"]
        assert comparison["coverage"]["count"] == comparison["coverage"]["equal_count"]
        assert all(r["equal"] for r in comparison["revision_manifests"])
        assert comparison["archived_receipt_hash_mismatches"] == 0
        assert comparison["remaining_source_candles"] == comparison["remaining_source_quotes"] == 0
        result["pipeline"][profile] = dict(
            checks=pipeline["checks"],
            completed_at=pipeline["completed_at"],
            websocket_events=pipeline["websocket_events"],
            container_security_boundaries=pipeline["container_security_boundaries"],
            database_comparison=comparison,
        )
    result["tools"] = read("artifacts/v11-m5-tools/verification.json")
    result["images"] = read("artifacts/v11-m5-final-images.json")
    result["shared_modules"] = read("artifacts/v11-m5-shared-modules.json")
    result["powershell_examples"] = read("artifacts/v11-m5-powershell-syntax.json")
    result["zip_reproducibility"] = read("artifacts/v11-m5-reproducibility.json")
    result["isolated_runtime_cleanup"] = read("artifacts/v11-m5-runtime/cleanup-completed.json")
    result["post_cleanup"] = read("artifacts/v11-m5-runtime/post-cleanup.json")
    result["static_checks"] = {
        p: dict(
            path="artifacts/v11-m5-" + p + ".log",
            sha256=digest(ROOT / ("artifacts/v11-m5-" + p + ".log")),
        )
        for p in ("ruff", "format", "mypy")
    }
    result["prior_failed_attempts"] = {
        "initial_full_pg": suite("artifacts/v11-m5-pg-full.xml"),
        "concurrent_build_regression_pg": suite("artifacts/v11-m5-pg-release-regression.xml"),
        "concurrent_build_regression_ts": suite("artifacts/v11-m5-ts-release-regression.xml"),
        "api_timeout_rechecks": {
            p: suite(f"artifacts/v11-m5-{p}-timeout-recheck.xml") for p in ("pg", "ts")
        },
        "notes": [
            "Initial full run exposed version-expectation and lock-cleanup issues; "
            "fixed and retested.",
            "Build overlap produced API/query timeouts; API rechecks and final full suites pass.",
            "Timescale install checks passed but DB drop raced an internal background session. "
            "Final harness retains generated DBs until labelled task-runtime cleanup.",
            "Default PyPI DNS/download attempts failed; cold builds use verified HTTPS mirror "
            "and unchanged requirement hashes. Resolver failures before builds are retained.",
            "First exact-image pipeline exposed an unnecessary source import on destination "
            "snapshot persistence. Conditional import and real-SQL regression fix it; failed "
            "run evidence is retained under v11-m5-pg-pipeline-import-failure.",
            "The first TS image run's read-only test observer retained an HTTP keep-alive "
            "connection across READ restart. It now reconnects after the intentional restart; "
            "production worker transport retries are unchanged. Failed observer evidence retained.",
        ],
    }
    result["readiness"] = dict(
        source_packages_built=True,
        fresh_mirror_images_built=True,
        default_pypi_fresh_build_verified=False,
        isolated_migrations_verified=True,
        isolated_integration_verified=True,
        genuine_independent_acquisition="M4 scoped brief BTC only; not rerun in M5",
        injected_99_workload="M4 bounded PG/TS qualification; injected rates only",
        full_genuine_99_qualified=False,
        company_network_tls_credentials_provisioned=False,
        company_deployed=False,
        legacy_collector_transition_completed=False,
        verdict="Release candidate prepared and isolated installation verified; production/site "
        "and genuine full-universe acceptance remain outstanding",
    )
    result["not_executed"] = [
        "Company deployment or company production database access",
        "Legacy collector migration/activation or real producer ownership transfer",
        "New genuine TradingView acquisition in M5/full genuine99 qualification",
        "Company-approved gateway/firewall/WAN/public-PKI installation",
        "Physical Windows sleep/reboot/Task Scheduler or Linux systemd boot acceptance",
        "Site off-host scheduled backup/RPO/RTO and long dedicated production soak",
    ]
    result["new_task_files"] = sorted(
        p.relative_to(ROOT).as_posix()
        for directory in ("release", "src", "scripts", "tests", "docs", "distributions")
        for p in (ROOT / directory).rglob("*")
        if p.is_file()
        and p.relative_to(ROOT).as_posix() not in baseline
        and not any(
            part in {".git", ".venv", ".pytest_cache", ".ruff_cache", ".mypy_cache", "__pycache__"}
            for part in p.parts
        )
    ) + ["compose.v11-m5-test.yaml"]
    output = ROOT / "docs/fastapi-v1.1-milestone5-verification.json"
    output.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    lines = [
        f"{a['sha256']}  {a['path'].removeprefix('artifacts/')}\n" for a in result["artifacts"]
    ]
    (ROOT / "artifacts/marketdata-v1.1.0rc1-SHA256SUMS").write_text(
        "".join(lines), encoding="utf-8"
    )
    print(result["readiness"]["verdict"])


if __name__ == "__main__":
    main()
