"""Safety boundaries for the opt-in full-pipeline qualification harness."""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location(
    "v11_qualification", SCRIPTS / "qualify_v11_pipeline.py"
)
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://user:secret@company.example:55995/m4_fixture_test",
        "postgresql://user:secret@127.0.0.1:5432/m4_fixture_test",
        "postgresql://user:secret@127.0.0.1:55995/marketdata",
        "postgresql://user:secret@127.0.0.1:55995/unrelated_test",
        "postgresql://user:secret@localhost:55995/m4_fixture_test",
    ],
)
def test_qualification_rejects_non_task_databases(dsn):
    with pytest.raises(ValueError, match="task-owned"):
        qualification.task_database(dsn)


def test_qualification_preserves_all_business_identities_and_simulation_labels():
    original = qualification.business_registry(99)
    assert len(original) == 99
    names = {r["instrument"]["canonical_symbol"] for r in original}
    assert "FX:OANDA:EURUSD" in names and "FX:FXCM:EURUSD" in names
    assert len(names) == 99
    assert all(m["realtime_status"] == "simulated" for r in original for m in r["mappings"])
    assert sum(r["instrument"]["asset_class"] == "crypto" for r in original) == 50
    assert sum(r["instrument"]["asset_class"] == "forex" for r in original) == 42
    assert sum(r["instrument"]["asset_class"] == "metals" for r in original) == 4
    assert sum(r["instrument"]["asset_class"] == "energy" for r in original) == 3


def test_genuine_profile_is_explicitly_bounded_to_one_feed():
    assert (
        qualification.business_registry(1, True)[0]["instrument"]["canonical_symbol"]
        == "CRYPTO:BTCUSDT"
    )
    with pytest.raises(AssertionError):
        qualification.business_registry(99, True)


@pytest.mark.parametrize(
    "extra",
    [
        ["--feeds", "0"],
        ["--quote-hz", "0"],
        ["--burst-hz", "11"],
        ["--soak-seconds", "3601"],
        ["--quote-hz", "10", "--soak-seconds", "3600"],
    ],
)
def test_qualification_rejects_unbounded_workloads(monkeypatch, extra):
    monkeypatch.setattr(
        sys, "argv", ["qualify", "--database-file", "private.json", "--output", "new", *extra]
    )
    with pytest.raises(SystemExit):
        qualification.arguments()
