from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DRIVER = ROOT / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("claims_mode_campaign", DRIVER)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


def package(path: Path, **files: str) -> Path:
    path.mkdir(parents=True)
    for name, content in {"__init__.py": "", **files}.items():
        (path / name).write_text(content)
    return path


def entry(path: Path) -> dict:
    return {"path": str(path), "source_hashes": campaign._hash_package(path)}


def test_v3_mode_defaults_to_indexed_and_lookup_only_is_parsed(tmp_path):
    runtime = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    e = entry(runtime)
    default_path = tmp_path / "default.json"
    default_path.write_text(
        json.dumps(
            {"version": 3, "baseline": e, "candidates": {n: e for n in campaign.V3_CANDIDATES}}
        )
    )
    assert campaign._load_manifest(default_path)["_claims_mode"] == "indexed"

    lookup_path = tmp_path / "lookup.json"
    lookup_path.write_text(
        json.dumps(
            {
                "version": 3,
                "claims_mode": "lookup_only",
                "baseline": e,
                "candidates": {n: e for n in campaign.V3_CANDIDATES},
            }
        )
    )
    assert campaign._load_manifest(lookup_path)["_claims_mode"] == "lookup_only"


@pytest.mark.parametrize("value", [None, True, False, "other", 1])
def test_v3_rejects_invalid_claims_mode(value, tmp_path):
    runtime = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 3,
                "claims_mode": value,
                "baseline": entry(runtime),
                "candidates": {n: entry(runtime) for n in campaign.V3_CANDIDATES},
            }
        )
    )
    with pytest.raises(ValueError, match="claims_mode"):
        campaign._load_manifest(manifest)


def test_legacy_and_v2_manifest_reject_claims_mode(tmp_path):
    runtime = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    e = entry(runtime)
    legacy = tmp_path / "legacy.json"
    legacy.write_text(json.dumps({"baseline": e, "binding": e, "claims_mode": "lookup_only"}))
    with pytest.raises(ValueError):
        campaign._load_manifest(legacy)
    v2 = tmp_path / "v2.json"
    v2.write_text(
        json.dumps(
            {"version": 2, "baseline": e, "candidates": {"queue": e}, "claims_mode": "lookup_only"}
        )
    )
    with pytest.raises(ValueError):
        campaign._load_manifest(v2)


def test_preflight_excludes_claims_mode_metadata(tmp_path):
    runtime = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    e = entry(runtime)
    manifest = {
        "_version": 3,
        "_claims_mode": "lookup_only",
        "baseline": e,
        **{n: e for n in campaign.V3_CANDIDATES},
    }
    paths = campaign._preflight_paths(manifest, tmp_path / "manifest.json", tmp_path / "out")
    assert set(paths) == {"baseline", *campaign.V3_CANDIDATES}


def test_run_records_claims_mode_in_report_and_input_provenance(tmp_path, monkeypatch):
    runtime = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    e = entry(runtime)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "version": 3,
                "claims_mode": "lookup_only",
                "baseline": e,
                "candidates": {name: e for name in campaign.V3_CANDIDATES},
            }
        )
    )
    package_paths = {
        "baseline": runtime,
        **{name: runtime for name in campaign.V3_CANDIDATES},
    }
    source_provenance = {
        name: {
            "path": str(runtime),
            "source_hashes": e["source_hashes"],
            "eligible": False,
            "deferred_reason": "test fixture candidate is unchanged",
        }
        for name in package_paths
    }
    expected_distribution = campaign.bench.build_dataset(*campaign.bench.PROFILES["perf"])[2]
    monkeypatch.setattr(campaign, "_campaign_provenance", lambda _budget: {"git_revision": "test"})
    monkeypatch.setattr(campaign, "_preflight", lambda _paths, _budget: {})
    monkeypatch.setattr(
        campaign,
        "_freeze_v3_packages",
        lambda *_args, **_kwargs: {"packages": package_paths, "provenance": source_provenance},
    )
    monkeypatch.setattr(
        campaign.bench,
        "supervised_prepare",
        lambda *_args, **_kwargs: {
            "status": "prepared",
            "source_hashes": e["source_hashes"],
            "distribution": expected_distribution,
            "seed_sha256": "seed",
        },
    )
    monkeypatch.setattr(
        campaign, "_run_v3", lambda *args, **_kwargs: args[4].update(status="qualified")
    )
    monkeypatch.setattr(campaign, "_integrity_check", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)

    args = campaign.parser().parse_args(
        ["--manifest", str(manifest_path), "--output", str(tmp_path / "evidence")]
    )
    assert campaign.run(args, budget=campaign.bench.Budget(limit_s=10800)) == 0
    report = json.loads((tmp_path / "evidence" / "report.json").read_text())
    assert report["claims_mode"] == "lookup_only"
    assert report["inputs"]["_claims_mode"] == {"value": "lookup_only", "source": "manifest"}


def test_lookup_only_claim_ownership_is_narrow_and_combination_uses_union(tmp_path):
    assert campaign._v3_allowed_ownership("claims", "lookup_only") == {"current_store.py"}
    baseline = package(
        tmp_path / "baseline",
        **{
            "current_store.py": "STORE = 1\n",
            "current_config.py": "CONFIG = 1\n",
            "current_runtime.py": "RUNTIME = 1\n",
            "cli.py": "CLI = 1\n",
            "current_claim_indexes.py": "INDEX = 1\n",
            "current_reconcile.py": "RECONCILE = 1\n",
            "current_sync.py": "SYNC = 1\n",
            "current_keyset.py": "KEYSET = 1\n",
        },
    )
    claims = package(
        tmp_path / "claims", **{p.name: p.read_text() for p in baseline.iterdir() if p.is_file()}
    )
    (claims / "current_store.py").write_text("STORE = 2\n")
    queue = package(
        tmp_path / "queue", **{p.name: p.read_text() for p in baseline.iterdir() if p.is_file()}
    )
    (queue / "current_reconcile.py").write_text("RECONCILE = 2\n")
    combo = package(
        tmp_path / "combo", **{p.name: p.read_text() for p in baseline.iterdir() if p.is_file()}
    )
    (combo / "current_store.py").write_text("STORE = 2\n")
    (combo / "current_reconcile.py").write_text("RECONCILE = 2\n")
    resolved = {
        "baseline": (baseline, campaign._hash_package(baseline)),
        "claims": (claims, campaign._hash_package(claims)),
        "queue": (queue, campaign._hash_package(queue)),
        "_combination:claims+queue": (combo, campaign._hash_package(combo)),
    }
    frozen = campaign._freeze_v3_packages(
        resolved, tmp_path / "frozen", campaign.bench.Budget(limit_s=10800), "lookup_only"
    )
    assert frozen["provenance"]["claims"]["eligible"]
    assert frozen["provenance"]["_combination:claims+queue"]["eligible"]


def test_lookup_only_skips_index_gate_and_reports_unindexed_claim_lookup(tmp_path, monkeypatch):
    baseline = package(tmp_path / "baseline", **{"__init__.py": ""})
    claims = package(tmp_path / "claims", **{"__init__.py": "", "current_store.py": "LOOKUP = 1\n"})
    output = tmp_path / "evidence"
    output.mkdir()
    report = {"deferred": {}, "stages": {}}
    monkeypatch.setattr(campaign.bench, "_save", lambda *_args: None)
    monkeypatch.setattr(
        campaign,
        "_stage_sample",
        lambda *_args, workload=None, **_kwargs: {"wall_s": 1.0, "workload": workload},
    )
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(
        campaign, "_compare_stage", lambda *args, **kwargs: {"status": "qualified", "saved_s": 2.0}
    )
    monkeypatch.setattr(
        campaign.bench,
        "claim_resource_diagnostics",
        lambda *_args: pytest.fail("lookup_only must skip index diagnostics"),
        raising=False,
    )
    campaign._run_v3(
        tmp_path / "seed",
        output,
        campaign.bench.Budget(limit_s=10800),
        set(),
        report,
        {"baseline": baseline, "claims": claims},
        {"claims": {"eligible": True}},
        "seed-hash",
        {},
        "lookup_only",
    )
    assert report["retained"] == ["claims"]
    assert report["candidate_descriptions"]["claims"] == "Selective lookup without indexes"
    assert "claim_resource_diagnostics" not in report
