from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("optimization_campaign_v5", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
benchmark = campaign.bench


def package(path: Path, store: str, helper: str = "SAME = 1\n") -> Path:
    path.mkdir(parents=True)
    (path / "__init__.py").write_text("")
    (path / "current_store.py").write_text(store)
    (path / "helper.py").write_text(helper)
    return path


def entry(path: Path) -> dict:
    return {"path": str(path), "source_hashes": benchmark.source_hashes(path)}


def create_manifest(root: Path, *, combinations=True) -> Path:
    baseline = package(root / "baseline", "BASE = 1\n")
    candidates = {
        name: package(root / name, f"BASE = 1\nFEATURE = {name!r}\n")
        for name in campaign.V5_CANDIDATES
    }
    combined = package(root / "all", "BASE = 1\nFEATURES = ('all',)\n")
    data = {
        "version": 5,
        "baseline": entry(baseline),
        "candidates": {name: entry(path) for name, path in candidates.items()},
    }
    if combinations:
        data["combinations"] = {"+".join(sorted(campaign.V5_CANDIDATES)): entry(combined)}
    path = root / "manifest.json"
    path.write_text(json.dumps(data))
    return path


def test_v5_manifest_accepts_optional_combinations_and_requires_fixed_candidates(tmp_path):
    path = create_manifest(tmp_path, combinations=False)
    loaded = campaign._load_manifest(path)
    assert loaded["_version"] == 5
    assert set(loaded) == {"_version", "baseline", *campaign.V5_CANDIDATES}

    data = json.loads(path.read_text())
    data["candidates"].pop("typed_keys")
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="requires receipt_factoring"):
        campaign._load_manifest(path)


@pytest.mark.parametrize(
    "key",
    [
        "typed_keys+receipt_factoring",
        "receipt_factoring+receipt_factoring",
        "receipt_factoring+queue",
        "receipt_factoring",
    ],
)
def test_v5_manifest_rejects_noncanonical_or_unknown_combination_keys(tmp_path, key):
    path = create_manifest(tmp_path, combinations=False)
    data = json.loads(path.read_text())
    data["combinations"] = {key: data["baseline"]}
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="sorted feature names"):
        campaign._load_manifest(path)


def test_v5_manifest_rejects_unknown_fields_and_executable_package_settings(tmp_path):
    path = create_manifest(tmp_path, combinations=False)
    data = json.loads(path.read_text())
    data["command"] = ["python", "run.py"]
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="requires only version"):
        campaign._load_manifest(path)

    data.pop("command")
    data["candidates"]["typed_keys"]["command"] = ["python", "run.py"]
    path.write_text(json.dumps(data))
    loaded = campaign._load_manifest(path)
    with pytest.raises(ValueError, match="only path and source_hashes"):
        campaign._resolve_entry(loaded["typed_keys"], path)


def test_v5_freezes_all_required_packages_and_supplied_combo_without_composition(tmp_path):
    manifest_path = create_manifest(tmp_path)
    manifest = campaign._load_manifest(manifest_path)
    output = tmp_path / "evidence"
    paths = campaign._preflight_paths(manifest, manifest_path, output)
    resolved = campaign._preflight(paths, benchmark.Budget(limit_s=10800))
    frozen = campaign._freeze_v5_packages(resolved, output, benchmark.Budget(limit_s=10800))
    combo = "combination:" + "+".join(sorted(campaign.V5_CANDIDATES))
    assert set(frozen["packages"]) == {"baseline", *campaign.V5_CANDIDATES, combo}
    assert frozen["provenance"][combo]["eligible"] is True
    assert frozen["packages"][combo] != frozen["packages"]["receipt_factoring"]


def test_v5_rejects_changes_outside_current_store(tmp_path):
    base = package(tmp_path / "base", "BASE = 1\n")
    changed = package(tmp_path / "changed", "BASE = 1\n", helper="CHANGED = 1\n")
    resolved = {"baseline": (base, benchmark.source_hashes(base))}
    resolved.update(
        {
            name: (
                changed if name == "receipt_factoring" else base,
                benchmark.source_hashes(changed if name == "receipt_factoring" else base),
            )
            for name in campaign.V5_CANDIDATES
        }
    )
    with pytest.raises(ValueError, match="outside current_store.py"):
        campaign._freeze_v5_packages(resolved, tmp_path / "frozen", benchmark.Budget(limit_s=10800))


def test_v5_best_single_uses_savings_then_declared_priority():
    report = {
        "qualified_candidates": {
            "receipt_factoring": {"comparison": {"saved_s": 5.0}},
            "read_consolidation": {"comparison": {"saved_s": 8.0}},
            "typed_keys": {"comparison": {"saved_s": 8.0}},
        }
    }
    assert campaign._best_v5_candidate(report) == "read_consolidation"
    report["qualified_candidates"]["receipt_factoring"]["comparison"]["saved_s"] = 8.0
    assert campaign._best_v5_candidate(report) == "receipt_factoring"


def test_v5_only_compares_exact_qualified_feature_set_and_falls_back(tmp_path, monkeypatch):
    packages = {
        name: package(tmp_path / name, f"VALUE = {name!r}\n")
        for name in ("baseline", *campaign.V5_CANDIDATES)
    }
    provenance = {
        name: {"eligible": True, "source_hashes": benchmark.source_hashes(path), "path": str(path)}
        for name, path in packages.items()
    }
    report = {"stages": {}, "warmups": {}, "deferred": {}}
    output = tmp_path / "out"
    output.mkdir()
    warmed = set()
    budget = benchmark.Budget(limit_s=10800)
    compared = []

    def warm(name, _package, _seed, _output, _budget, _warmed, current, attribution=False):
        assert attribution is True
        current.setdefault("warmups", {})[f"{name}:attribution-v1"] = {
            "sample": {"wall_s": 1.0, "warmup_diagnostics": {"version": 1}}
        }

    def compare(stage, _left_name, _left, right_name, _right, *_args, **kwargs):
        compared.append(right_name)
        result = {"status": "qualified", "saved_s": 7.0}
        if right_name.startswith("combination:"):
            result = {"status": "deferred", "saved_s": 0.0, "reason": "combination gate"}
        report["stages"][stage] = {"comparison": result, "samples": []}
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_: None)
    monkeypatch.setattr(campaign, "_candidate_stage_estimate", lambda *_: 1.0)
    campaign._run_v5(
        tmp_path / "seed",
        output,
        budget,
        warmed,
        report,
        packages,
        provenance,
        {},
    )
    assert compared == list(campaign.V5_CANDIDATES)
    assert report["retained"] == ["receipt_factoring"]
    assert report["final_combination"]["status"] == "not_supplied"


def test_v5_uses_combination_reserve_after_first_qualified_comparison(tmp_path, monkeypatch):
    packages = {
        name: package(tmp_path / name, f"VALUE = {name!r}\n")
        for name in ("baseline", *campaign.V5_CANDIDATES)
    }
    provenance = {
        name: {"eligible": True, "source_hashes": benchmark.source_hashes(path), "path": str(path)}
        for name, path in packages.items()
    }
    report = {"stages": {}, "warmups": {}, "deferred": {}}
    output = tmp_path / "out"
    output.mkdir()

    class BoundaryBudget:
        holdback_s = 0.0
        remaining = 1000.0

        def admit(self, _estimate=0):
            pass

    budget = BoundaryBudget()
    holds = []

    def warm(name, _package, _seed, _output, _budget, _warmed, current, attribution=False):
        current.setdefault("warmups", {})[f"{name}:attribution-v1"] = {
            "sample": {"wall_s": 1.0, "warmup_diagnostics": {"version": 1}}
        }

    def compare(stage, _left_name, _left, right_name, _right, *_args, **_kwargs):
        holds.append(budget.holdback_s)
        result = {"status": "qualified", "saved_s": 2.0}
        report["stages"][stage] = {"comparison": result, "samples": []}
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_: None)
    monkeypatch.setattr(campaign, "_candidate_stage_estimate", lambda *_: 1.0)
    campaign._run_v5(tmp_path / "seed", output, budget, set(), report, packages, provenance, {})
    assert holds[0] == 15.0
    assert holds[1] >= 15.0  # Plus 125% of the first completed qualified comparison.


def test_v5_combination_runtime_failure_falls_back_to_strongest_single(tmp_path, monkeypatch):
    names = (
        "baseline",
        *campaign.V5_CANDIDATES,
        "combination:" + "+".join(sorted(campaign.V5_CANDIDATES)),
    )
    packages = {name: package(tmp_path / name, f"VALUE = {name!r}\n") for name in names}
    provenance = {
        name: {
            "eligible": True,
            "source_hashes": benchmark.source_hashes(path),
            "path": str(path),
        }
        for name, path in packages.items()
    }
    report = {"stages": {}, "warmups": {}, "deferred": {}}
    output = tmp_path / "out"
    output.mkdir()

    def warm(name, _package, _seed, _output, _budget, _warmed, current, attribution=False):
        assert attribution is True
        current.setdefault("warmups", {})[f"{name}:attribution-v1"] = {
            "sample": {"wall_s": 1.0, "warmup_diagnostics": {"version": 1}}
        }

    def compare(stage, _left_name, _left, right_name, _right, *_args, **_kwargs):
        if right_name.startswith("combination:"):
            raise ValueError("candidate worker failed")
        saved = {"receipt_factoring": 1.0, "read_consolidation": 5.0, "typed_keys": 3.0}[right_name]
        result = {"status": "qualified", "saved_s": saved}
        report["stages"][stage] = {"comparison": result, "samples": []}
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_: None)
    monkeypatch.setattr(campaign, "_candidate_stage_estimate", lambda *_: 1.0)
    campaign._run_v5(
        tmp_path / "seed",
        output,
        benchmark.Budget(limit_s=10800),
        set(),
        report,
        packages,
        provenance,
        {},
    )

    assert report["retained"] == ["read_consolidation"]
    assert report["final_combination"]["status"] == "failed"
    assert report["deferred"]["combination:read_consolidation+receipt_factoring+typed_keys"] == (
        "candidate worker failed"
    )


def test_v5_combination_integrity_failure_is_not_downgraded(tmp_path, monkeypatch):
    names = (
        "baseline",
        *campaign.V5_CANDIDATES,
        "combination:" + "+".join(sorted(campaign.V5_CANDIDATES)),
    )
    packages = {name: package(tmp_path / name, f"VALUE = {name!r}\n") for name in names}
    provenance = {
        name: {
            "eligible": True,
            "source_hashes": benchmark.source_hashes(path),
            "path": str(path),
        }
        for name, path in packages.items()
    }
    report = {"stages": {}, "warmups": {}, "deferred": {}}
    output = tmp_path / "out"
    output.mkdir()

    def warm(name, _package, _seed, _output, _budget, _warmed, current, attribution=False):
        current.setdefault("warmups", {})[f"{name}:attribution-v1"] = {
            "sample": {"wall_s": 1.0, "warmup_diagnostics": {"version": 1}}
        }

    def compare(stage, _left_name, _left, right_name, _right, *_args, **_kwargs):
        if right_name.startswith("combination:"):
            raise campaign.CampaignIntegrityError("measured package was modified")
        result = {"status": "qualified", "saved_s": 1.0}
        report["stages"][stage] = {"comparison": result, "samples": []}
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_: None)
    monkeypatch.setattr(campaign, "_candidate_stage_estimate", lambda *_: 1.0)
    with pytest.raises(campaign.CampaignIntegrityError, match="modified"):
        campaign._run_v5(
            tmp_path / "seed",
            output,
            benchmark.Budget(limit_s=10800),
            set(),
            report,
            packages,
            provenance,
            {},
        )


def test_v5_scheduler_diagnostics_are_supervised_and_keep_integrity_reserve(tmp_path, monkeypatch):
    class Budget:
        holdback_s = 0.0
        remaining = 20.0

    report = {}
    calls = []
    monkeypatch.setattr(
        benchmark,
        "supervised",
        lambda command, *_args, **_kwargs: (
            calls.append(command) or {"status": "measured", "scales": []}
        ),
    )
    campaign._run_v5_scheduler_diagnostics(
        tmp_path / "seed", tmp_path, tmp_path / "baseline", Budget(), report
    )
    assert report["scheduler_diagnostics"]["status"] == "measured"
    assert calls and "--_scheduler_diagnostics" in calls[0]

    class ExhaustedBudget:
        holdback_s = 0.0
        remaining = 4.99

    campaign._run_v5_scheduler_diagnostics(
        tmp_path / "seed", tmp_path, tmp_path / "baseline", ExhaustedBudget(), report
    )
    assert report["scheduler_diagnostics"]["status"] == "not_run"
    assert len(calls) == 1
