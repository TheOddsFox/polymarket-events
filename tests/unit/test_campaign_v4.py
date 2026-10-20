from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("optimization_campaign_v4", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
benchmark = campaign.bench


def package(path: Path, store: str, extra: str = "same") -> Path:
    path.mkdir(parents=True)
    (path / "__init__.py").write_text("")
    (path / "current_store.py").write_text(store)
    (path / "support.py").write_text(extra)
    return path


def entry(path: Path) -> dict:
    return {"path": str(path), "source_hashes": benchmark.source_hashes(path)}


def manifest(path: Path, *, version=4) -> Path:
    baseline = package(path / "baseline", "BASE = 1\n")
    grouping = package(path / "grouping", "BASE = 1\nGROUP = 1\n")
    reverse = package(path / "reverse", "BASE = 1\nREVERSE = 1\n")
    combined = package(path / "combined", "BASE = 1\nGROUP = 1\nREVERSE = 1\n")
    value = {
        "version": version,
        "baseline": entry(baseline),
        "candidates": {
            "outcome_grouping": entry(grouping),
            "claim_reverse_lookup": entry(reverse),
        },
        "combined": entry(combined),
    }
    manifest_path = path / "manifest.json"
    manifest_path.write_text(json.dumps(value))
    return manifest_path


def test_v4_manifest_requires_exact_candidates_and_combined_entry(tmp_path):
    path = manifest(tmp_path)
    loaded = campaign._load_manifest(path)
    assert loaded["_version"] == 4
    assert set(loaded) == {
        "_version",
        "baseline",
        "outcome_grouping",
        "claim_reverse_lookup",
        "combined",
    }
    value = json.loads(path.read_text())
    value["candidates"].pop("claim_reverse_lookup")
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="requires outcome_grouping and claim_reverse_lookup"):
        campaign._load_manifest(path)


@pytest.mark.parametrize("mutation", ["unknown", "executable", "missing_combined"])
def test_v4_manifest_rejects_unknown_or_executable_configuration(tmp_path, mutation):
    path = manifest(tmp_path)
    value = json.loads(path.read_text())
    if mutation == "unknown":
        value["shell"] = "run this"
    elif mutation == "executable":
        value["combined"]["command"] = ["python", "untrusted.py"]
    else:
        del value["combined"]
    path.write_text(json.dumps(value))
    if mutation == "executable":
        loaded = campaign._load_manifest(path)
        with pytest.raises(ValueError, match="only path and source_hashes"):
            campaign._resolve_entry(loaded["combined"], path)
    else:
        with pytest.raises(ValueError):
            campaign._load_manifest(path)


def test_v4_rejects_candidate_outside_current_store_ownership(tmp_path):
    baseline = package(tmp_path / "base", "BASE = 1\n")
    changed = package(tmp_path / "changed", "BASE = 1\n", extra="changed")
    resolved = {
        "baseline": (baseline, benchmark.source_hashes(baseline)),
        "outcome_grouping": (changed, benchmark.source_hashes(changed)),
        "claim_reverse_lookup": (baseline, benchmark.source_hashes(baseline)),
        "combined": (baseline, benchmark.source_hashes(baseline)),
    }
    with pytest.raises(ValueError, match="outside current_store.py"):
        campaign._freeze_v4_packages(resolved, tmp_path / "frozen", benchmark.Budget(limit_s=10800))


def test_v4_freezes_all_four_packages_without_assembling_shared_file(tmp_path):
    path = manifest(tmp_path)
    loaded = campaign._load_manifest(path)
    paths = campaign._preflight_paths(loaded, path, tmp_path / "evidence")
    resolved = campaign._preflight(paths, benchmark.Budget(limit_s=10800))
    frozen = campaign._freeze_v4_packages(
        resolved, tmp_path / "evidence", benchmark.Budget(limit_s=10800)
    )
    assert set(frozen["packages"]) == {
        "baseline",
        "outcome_grouping",
        "claim_reverse_lookup",
        "combined",
    }
    assert frozen["provenance"]["combined"]["eligible"] is True
    assert (frozen["packages"]["combined"] / "current_store.py").read_text().count("=") == 3


def test_v4_best_candidate_uses_absolute_savings_and_grouping_tie_break():
    report = {
        "qualified_candidates": {
            "outcome_grouping": {"comparison": {"saved_s": 5.0}},
            "claim_reverse_lookup": {"comparison": {"saved_s": 8.0}},
        }
    }
    assert campaign._best_v4_candidate(report) == "claim_reverse_lookup"
    report["qualified_candidates"]["outcome_grouping"]["comparison"]["saved_s"] = 8.0
    assert campaign._best_v4_candidate(report) == "outcome_grouping"


def test_v4_compares_in_required_order_and_falls_back_when_combination_fails(tmp_path, monkeypatch):
    base = package(tmp_path / "base", "BASE = 1\n")
    grouping = package(tmp_path / "grouping", "BASE = 1\nGROUP = 1\n")
    reverse = package(tmp_path / "reverse", "BASE = 1\nREVERSE = 1\n")
    combined = package(tmp_path / "combined", "BASE = 1\nGROUP = 1\nREVERSE = 1\n")
    packages = {
        "baseline": base,
        "outcome_grouping": grouping,
        "claim_reverse_lookup": reverse,
        "combined": combined,
    }
    provenance = {
        name: {
            "eligible": True,
            "source_hashes": benchmark.source_hashes(path),
            "path": str(path),
        }
        for name, path in packages.items()
    }
    report = {"retained": [], "qualified_candidates": {}, "deferred": {}, "stages": {}}
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    calls = []
    admission_holdbacks = {}

    class BoundaryBudget:
        limit_s = 10800

        def __init__(self):
            self.holdback_s = 0.0
            self._reserved_candidate_admissions = 0

        @property
        def remaining(self):
            if self.holdback_s == 15.0:
                return 7.5
            self._reserved_candidate_admissions += 1
            # The first 16.25s holdback check admits the second independent
            # comparison. The same check would reject the combination unless
            # the protected window is released before its admission.
            return 7.5 if self._reserved_candidate_admissions == 1 else 7.49

        def admit(self, *_args):
            return None

    def warm(name, _package, _seed, _output, budget, warmed, current_report):
        key = (name, None)
        warmed.add(key)
        admission_holdbacks[name] = budget.holdback_s
        current_report.setdefault("warmups", {})[name] = {
            "sample": {"wall_s": 1.0, "acquire_apply_s": 1.0}
        }

    def compare(stage, left_name, _left, right_name, _right, *_args, **_kwargs):
        calls.append((stage, left_name, right_name))
        value = {
            "status": "qualified",
            "saved_s": 7.0 if right_name == "outcome_grouping" else 9.0,
        }
        if right_name == "combined":
            value = {"status": "deferred", "reason": "regression"}
        stage_samples = [
            {"category": category, "pair": pair}
            for category in ("baseline", right_name)
            for pair in (1, 2, 3)
        ]
        report["stages"][stage] = {
            "status": value["status"],
            "comparison": value,
            "samples": stage_samples,
        }
        return value

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_run_v4_diagnostics", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monotonic_values = iter((0.0, 1.0, 10.0, 12.0))
    monkeypatch.setattr(campaign.time, "monotonic", lambda: next(monotonic_values))
    campaign._run_v4(
        tmp_path / "seed",
        evidence,
        BoundaryBudget(),
        set(),
        report,
        packages,
        provenance,
        {},
    )
    assert calls == [
        ("outcome_grouping_vs_baseline", "baseline", "outcome_grouping"),
        ("claim_reverse_lookup_vs_baseline", "baseline", "claim_reverse_lookup"),
        ("combined_vs_baseline", "baseline", "combined"),
    ]
    assert report["retained"] == ["claim_reverse_lookup"]
    assert report["status"] == "qualified_individual_combination_deferred"
    assert admission_holdbacks["claim_reverse_lookup"] > 15.0
    assert admission_holdbacks["combined"] == 15.0


def test_v4_machine_provenance_labels_usb_speed_as_unavailable(tmp_path):
    provenance = campaign._machine_provenance(tmp_path)
    assert provenance["python"]
    assert provenance["duckdb"]
    assert provenance["usb_storage_link"] == "not exposed by campaign host API"


def test_v4_machine_provenance_handles_platforms_without_statvfs(tmp_path, monkeypatch):
    monkeypatch.delattr(campaign.os, "statvfs", raising=False)
    provenance = campaign._machine_provenance(tmp_path)
    assert provenance["filesystem_error"] == "unsupported_on_this_platform"


def test_v4_final_integrity_failure_clears_provisional_retention(tmp_path, monkeypatch):
    path = manifest(tmp_path)
    args = campaign.parser().parse_args(
        ["--manifest", str(path), "--output", str(tmp_path / "evidence")]
    )
    baseline = campaign._load_manifest(path)["baseline"]
    _baseline_package, baseline_hashes = campaign._entry_path(baseline, path)
    distribution = benchmark.build_dataset(*benchmark.PROFILES["perf"])[2]
    monkeypatch.setattr(campaign, "_machine_provenance", lambda _path: {"test": True})
    monkeypatch.setattr(campaign, "_campaign_provenance", lambda _budget: {})
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(
        benchmark,
        "supervised_prepare",
        lambda *_args, **_kwargs: {
            "status": "prepared",
            "source_hashes": baseline_hashes,
            "distribution": distribution,
            "seed_sha256": "0" * 64,
        },
    )

    def qualify(*_args, **_kwargs):
        report = _args[4]
        report["retained"] = ["claim_reverse_lookup"]
        report["status"] = "qualified"

    monkeypatch.setattr(campaign, "_run_v4", qualify)

    def fail_integrity(*_args, **_kwargs):
        raise campaign.CampaignIntegrityError("seed changed")

    monkeypatch.setattr(campaign, "_integrity_check", fail_integrity)
    result = campaign.run(args, budget=benchmark.Budget(limit_s=10800))
    report = json.loads((tmp_path / "evidence" / "report.json").read_text())
    assert result == 1
    assert report["status"] == "failed"
    assert report["retained"] == []
    assert report["integrity_validation"] == "failed"


def test_v4_requires_the_shared_10800_second_budget(tmp_path):
    path = manifest(tmp_path)
    args = campaign.parser().parse_args(
        ["--manifest", str(path), "--output", str(tmp_path / "evidence")]
    )
    with pytest.raises(ValueError, match="10,800-second budget"):
        campaign.run(args, budget=benchmark.Budget(limit_s=1800))


def test_v4_diagnostics_use_extra_shutdown_holdback_and_restore_integrity_reserve(
    tmp_path, monkeypatch
):
    seen = {}

    def diagnostics(_package, _output, budget, *, extended):
        seen["holdback"] = budget.holdback_s
        seen["extended"] = extended
        return {"status": "complete"}

    monkeypatch.setattr(benchmark, "claim_resource_diagnostics", diagnostics, raising=False)
    report = {}
    budget = benchmark.Budget(limit_s=10800)
    campaign._run_v4_diagnostics(
        tmp_path,
        budget,
        report,
        {"claim_reverse_lookup": tmp_path / "runtime"},
    )
    assert seen == {"holdback": 30.0, "extended": True}
    assert budget.holdback_s == 15.0
    assert report["claim_resource_diagnostics"] == {"status": "complete"}


def test_v4_diagnostic_timeout_is_recorded_without_escaping(tmp_path, monkeypatch):
    def diagnostics(*_args, **_kwargs):
        raise TimeoutError("shared budget reserve reached")

    monkeypatch.setattr(benchmark, "claim_resource_diagnostics", diagnostics, raising=False)
    report = {}
    budget = benchmark.Budget(limit_s=10800)
    campaign._run_v4_diagnostics(
        tmp_path,
        budget,
        report,
        {"claim_reverse_lookup": tmp_path / "runtime"},
    )
    assert report["claim_resource_diagnostics"]["status"] == "insufficient_evidence"
    assert budget.holdback_s == 15.0


def _v4_test_runtime_packages(tmp_path):
    base = package(tmp_path / "base", "BASE = 1\n")
    grouping = package(tmp_path / "grouping", "BASE = 1\nGROUP = 1\n")
    reverse = package(tmp_path / "reverse", "BASE = 1\nREVERSE = 1\n")
    combined = package(tmp_path / "combined", "BASE = 1\nGROUP = 1\nREVERSE = 1\n")
    packages = {
        "baseline": base,
        "outcome_grouping": grouping,
        "claim_reverse_lookup": reverse,
        "combined": combined,
    }
    provenance = {
        name: {"eligible": True, "path": str(path), "source_hashes": benchmark.source_hashes(path)}
        for name, path in packages.items()
    }
    return packages, provenance


def _mock_v4_orchestration(monkeypatch, output, packages, provenance, results):
    report = {"retained": [], "qualified_candidates": {}, "deferred": {}, "stages": {}}
    output.mkdir(exist_ok=True)

    def warm(name, _package, _seed, _output, _budget, warmed, current_report):
        warmed.add((name, None))
        current_report.setdefault("warmups", {})[name] = {
            "sample": {"wall_s": 1.0, "acquire_apply_s": 1.0}
        }

    def compare(stage, left_name, _left, right_name, _right, *_args, **_kwargs):
        result = results.get(right_name, {"status": "deferred", "reason": "performance gate"})
        report["stages"][stage] = {
            "status": result["status"],
            "comparison": result,
            "samples": [
                {"category": category, "pair": pair}
                for category in (left_name, right_name)
                for pair in (1, 2, 3)
            ],
        }
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_run_v4_diagnostics", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    campaign._run_v4(
        output / "seed",
        output,
        benchmark.Budget(limit_s=10800),
        set(),
        report,
        packages,
        provenance,
        {},
    )
    return report


def test_v4_all_completed_performance_deferrals_are_deferred_not_insufficient(
    tmp_path, monkeypatch
):
    packages, provenance = _v4_test_runtime_packages(tmp_path)
    report = _mock_v4_orchestration(
        monkeypatch,
        tmp_path / "evidence",
        packages,
        provenance,
        {
            "outcome_grouping": {"status": "deferred", "reason": "under threshold"},
            "claim_reverse_lookup": {"status": "deferred", "reason": "under threshold"},
        },
    )
    assert report["status"] == "deferred"
    assert report["retained"] == []


def test_v4_mixed_completed_and_partial_comparison_is_insufficient(tmp_path, monkeypatch):
    packages, provenance = _v4_test_runtime_packages(tmp_path)
    report = {"retained": [], "qualified_candidates": {}, "deferred": {}, "stages": {}}
    evidence = tmp_path / "evidence"
    evidence.mkdir()

    def warm(name, _package, _seed, _output, _budget, warmed, current_report):
        warmed.add((name, None))
        current_report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1.0}}

    calls = []

    def compare(stage, left_name, _left, right_name, _right, *_args, **_kwargs):
        calls.append(right_name)
        if right_name == "claim_reverse_lookup":
            raise TimeoutError("pair set did not finish")
        result = {"status": "deferred", "reason": "under threshold"}
        report["stages"][stage] = {
            "status": "deferred",
            "comparison": result,
            "samples": [
                {"category": category, "pair": pair}
                for category in (left_name, right_name)
                for pair in (1, 2, 3)
            ],
        }
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_run_v4_diagnostics", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    campaign._run_v4(
        evidence / "seed",
        evidence,
        benchmark.Budget(limit_s=10800),
        set(),
        report,
        packages,
        provenance,
        {},
    )
    assert calls == ["outcome_grouping", "claim_reverse_lookup"]
    assert report["status"] == "insufficient_evidence"


def test_v4_invalid_candidates_preserve_invalid_status_and_allow_verified_fallback(
    tmp_path, monkeypatch
):
    packages, provenance = _v4_test_runtime_packages(tmp_path)
    report = {"retained": [], "qualified_candidates": {}, "deferred": {}, "stages": {}}
    evidence = tmp_path / "evidence"
    evidence.mkdir()

    def warm(name, _package, _seed, _output, _budget, warmed, current_report):
        warmed.add((name, None))
        current_report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1.0}}

    def compare(stage, left_name, _left, right_name, _right, *_args, **_kwargs):
        result = (
            {"status": "qualified", "saved_s": 3.0}
            if right_name == "outcome_grouping"
            else {"status": "invalid", "reason": "oracle mismatch"}
        )
        report["stages"][stage] = {
            "status": result["status"],
            "comparison": result,
            "samples": [
                {"category": category, "pair": pair}
                for category in (left_name, right_name)
                for pair in (1, 2, 3)
            ],
        }
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_run_v4_diagnostics", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    campaign._run_v4(
        evidence / "seed",
        evidence,
        benchmark.Budget(limit_s=10800),
        set(),
        report,
        packages,
        provenance,
        {},
    )
    assert report["invalid_candidates"] == ["claim_reverse_lookup"]
    assert report["retained"] == ["outcome_grouping"]
    assert report["status"] == "invalid_with_qualified_fallback"


def test_v4_all_invalid_comparisons_are_invalid_with_no_retention(tmp_path, monkeypatch):
    packages, provenance = _v4_test_runtime_packages(tmp_path)
    report = _mock_v4_orchestration(
        monkeypatch,
        tmp_path / "evidence",
        packages,
        provenance,
        {
            "outcome_grouping": {"status": "invalid", "reason": "oracle mismatch"},
            "claim_reverse_lookup": {"status": "invalid", "reason": "oracle mismatch"},
        },
    )
    assert report["status"] == "invalid"
    assert report["invalid_candidates"] == ["outcome_grouping", "claim_reverse_lookup"]
    assert report["retained"] == []


def test_v4_invalid_combination_is_recorded_once_with_best_qualified_fallback(
    tmp_path, monkeypatch
):
    packages, provenance = _v4_test_runtime_packages(tmp_path)
    report = _mock_v4_orchestration(
        monkeypatch,
        tmp_path / "evidence",
        packages,
        provenance,
        {
            "outcome_grouping": {"status": "qualified", "saved_s": 7.0},
            "claim_reverse_lookup": {"status": "qualified", "saved_s": 9.0},
            "combined": {"status": "invalid", "reason": "oracle mismatch"},
        },
    )
    assert report["status"] == "invalid_with_qualified_fallback"
    assert report["invalid_candidates"] == ["combined"]
    assert report["retained"] == ["claim_reverse_lookup"]


def test_v4_all_invalid_candidates_exit_nonzero(tmp_path, monkeypatch):
    path = manifest(tmp_path)
    args = campaign.parser().parse_args(
        ["--manifest", str(path), "--output", str(tmp_path / "evidence")]
    )
    baseline = campaign._load_manifest(path)["baseline"]
    _baseline_package, baseline_hashes = campaign._entry_path(baseline, path)
    distribution = benchmark.build_dataset(*benchmark.PROFILES["perf"])[2]
    monkeypatch.setattr(campaign, "_machine_provenance", lambda _path: {})
    monkeypatch.setattr(campaign, "_campaign_provenance", lambda _budget: {})
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(
        benchmark,
        "supervised_prepare",
        lambda *_args, **_kwargs: {
            "status": "prepared",
            "source_hashes": baseline_hashes,
            "distribution": distribution,
            "seed_sha256": "0" * 64,
        },
    )

    def invalid(*_args, **_kwargs):
        report = _args[4]
        report.update(status="invalid", retained=[], invalid_candidates=["both candidates"])

    monkeypatch.setattr(campaign, "_run_v4", invalid)
    monkeypatch.setattr(campaign, "_integrity_check", lambda *_args: None)
    assert campaign.run(args, budget=benchmark.Budget(limit_s=10800)) == 1
    result = json.loads((tmp_path / "evidence/report.json").read_text())
    assert result["status"] == "invalid"
    assert result["retained"] == []
