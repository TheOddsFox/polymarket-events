"""V4 cannot retain a fast runtime whose final evidence is incomplete or changed."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("campaign_v4_adversarial", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
benchmark = campaign.bench


def _manifest(root):
    entries = {}
    for name in ("baseline", "outcome_grouping", "claim_reverse_lookup", "combined"):
        package = root / name
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "current_store.py").write_text(f"VARIANT = {name!r}\n")
        (package / "support.py").write_text("UNCHANGED = True\n")
        entries[name] = {
            "path": str(package),
            "source_hashes": benchmark.source_hashes(package),
        }
    path = root / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "version": 4,
                "baseline": entries["baseline"],
                "candidates": {name: entries[name] for name in campaign.V4_CANDIDATES},
                "combined": entries["combined"],
            }
        )
    )
    return path, entries


@pytest.mark.parametrize("mutation", [None, "original", "frozen", "seed", "harness", "deadline"])
def test_final_real_integrity_rejects_changed_evidence_or_expired_budget(
    tmp_path, monkeypatch, mutation
):
    path, entries = _manifest(tmp_path)
    output = tmp_path / "evidence"
    args = campaign.parser().parse_args(["--manifest", str(path), "--output", str(output)])
    clock = {"now": 0.0, "harness": "unchanged"}
    monkeypatch.setattr(campaign.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(campaign, "_machine_provenance", lambda _path: {})
    monkeypatch.setattr(
        campaign, "_campaign_provenance", lambda _budget: {"source": clock["harness"]}
    )
    distribution = {"test_fixture": "bounded integrity-only seed"}
    monkeypatch.setattr(benchmark, "build_dataset", lambda *_args: ([], [], distribution))

    def prepare(*_args, **_kwargs):
        seed = output / "seed"
        seed.mkdir()
        database = seed / "catalogue.duckdb"
        database.write_bytes(b"immutable synthetic seed")
        return {
            "status": "prepared",
            "source_hashes": entries["baseline"]["source_hashes"],
            "distribution": distribution,
            "seed_sha256": benchmark.digest(database),
        }

    def qualify(seed, _output, _budget, _warmed, report, packages, provenance, _harness):
        report.update(status="qualified", retained=["outcome_grouping"])
        if mutation == "original":
            target = Path(provenance["outcome_grouping"]["path"]) / "current_store.py"
            target.write_text("CHANGED = True\n")
        elif mutation == "frozen":
            (packages["outcome_grouping"] / "current_store.py").write_text("CHANGED = True\n")
        elif mutation == "seed":
            (seed / "catalogue.duckdb").write_bytes(b"changed seed")
        elif mutation == "harness":
            clock["harness"] = "changed"
        elif mutation == "deadline":
            clock["now"] = 10800 - benchmark.SHUTDOWN_RESERVE_S

    monkeypatch.setattr(benchmark, "supervised_prepare", prepare)
    monkeypatch.setattr(campaign, "_run_v4", qualify)
    result = campaign.run(args, budget=benchmark.Budget(limit_s=10800))
    report = json.loads((output / "report.json").read_text())
    if mutation is None:
        assert result == 0
        assert report["retained"] == ["outcome_grouping"]
        assert report["integrity_validation"] == "passed"
    else:
        assert report["retained"] == []
        assert report["integrity_validation"] == (
            "incomplete" if mutation == "deadline" else "failed"
        )
        assert report["status"] == ("insufficient_evidence" if mutation == "deadline" else "failed")


@pytest.mark.parametrize("name", ["outcome_grouping", "combined"])
def test_valid_inventory_cannot_hide_out_of_scope_shared_package_changes(tmp_path, name):
    _path, entries = _manifest(tmp_path)
    package = Path(entries[name]["path"])
    (package / "support.py").write_text("CHANGED = True\n")
    entries[name]["source_hashes"] = benchmark.source_hashes(package)
    resolved = {
        key: (Path(entry["path"]), entry["source_hashes"]) for key, entry in entries.items()
    }
    with pytest.raises(ValueError, match="outside current_store.py"):
        campaign._freeze_v4_packages(resolved, tmp_path / "frozen", benchmark.Budget(limit_s=10800))


def _samples(runtime):
    return [
        {
            "pair": pair,
            "platform": "darwin",
            "measurement_version": 2,
            "semantic_hash": "complete-public-private-and-publication-oracle",
            "cleanup_deferred": False,
            "acquire_apply_s": runtime,
            "wall_s": runtime + 5,
            "peak_rss_bytes": 1000,
            "worker_peak_before_oracle_bytes": 900,
            "http_attempts": 10,
            "downloaded_bytes": 1000,
            "runtime_reserved_attempts": 20,
            "runtime_downloaded_bytes": 900,
        }
        for pair in (1, 2, 3)
    ]


@pytest.mark.parametrize(
    "counter",
    ["http_attempts", "downloaded_bytes", "runtime_reserved_attempts", "runtime_downloaded_bytes"],
)
def test_one_pair_with_more_http_load_cannot_hide_behind_large_speedup(counter):
    baseline, candidate = _samples(100), _samples(50)
    assert benchmark.comparison(baseline, candidate, "perf")["status"] == "qualified"
    candidate[1][counter] += 1
    assert benchmark.comparison(baseline, candidate, "perf")["status"] == "deferred"


@pytest.mark.parametrize("pairs", [(1, 2), (1, 1, 3)])
def test_missing_or_duplicate_pair_is_insufficient_even_when_every_sample_is_fast(pairs):
    baseline, candidate = _samples(100), _samples(50)
    candidate = [{**candidate[index], "pair": pair} for index, pair in enumerate(pairs)]
    assert benchmark.comparison(baseline, candidate, "perf")["status"] == "insufficient_evidence"


def test_combination_timeout_preserves_stronger_single_for_final_integrity(tmp_path, monkeypatch):
    _path, entries = _manifest(tmp_path)
    packages = {name: Path(entry["path"]) for name, entry in entries.items()}
    provenance = {name: {"eligible": True} for name in packages}
    report = {"stages": {}, "deferred": {}}
    output = tmp_path / "evidence"
    output.mkdir()
    clock = {"now": 0.0}
    monkeypatch.setattr(campaign.time, "monotonic", lambda: clock["now"])

    def warm(name, _package, _seed, _output, _budget, _warmed, current_report):
        current_report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1.0}}

    def compare(stage, _left_name, _left, right_name, _right, *_args):
        clock["now"] += 1
        if right_name == "combined":
            raise TimeoutError("combined worker did not complete")
        value = {"status": "qualified", "saved_s": 3 if right_name == "claim_reverse_lookup" else 2}
        report["stages"][stage] = {"status": "qualified", "comparison": value}
        return value

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(campaign, "_run_v4_diagnostics", lambda *_args: None)
    budget = benchmark.Budget(limit_s=10800)
    campaign._run_v4(tmp_path / "seed", output, budget, set(), report, packages, provenance, {})
    assert report["retained"] == ["claim_reverse_lookup"]
    assert report["status"] == "qualified_individual_combination_deferred"
    assert report["final_combination"]["status"] == "insufficient_evidence"
    assert budget.remaining > 1
    assert "integrity_validation" not in report
