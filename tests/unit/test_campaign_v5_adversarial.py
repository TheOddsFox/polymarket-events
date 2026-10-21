"""V5 must retain only complete evidence for the exact measured feature set."""

from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("campaign_v5_adversarial", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
benchmark = campaign.bench
NAMES = ("receipt_factoring", "read_consolidation", "typed_keys")


def _entry(root, name):
    package = root / name
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "current_store.py").write_text(f"VARIANT = {name!r}\n")
    (package / "support.py").write_text("UNCHANGED = True\n")
    return {"path": str(package), "source_hashes": benchmark.source_hashes(package)}


def _manifest(root, combinations=()):
    entries = {name: _entry(root, name) for name in ("baseline", *NAMES)}
    combos = {key: _entry(root, f"combo-{index}") for index, key in enumerate(combinations)}
    value = {
        "version": 5,
        "baseline": entries["baseline"],
        "candidates": {name: entries[name] for name in NAMES},
        "combinations": combos,
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(value))
    return path, value, entries


@pytest.mark.parametrize(
    "key",
    [
        "receipt_factoring+read_consolidation",
        "receipt_factoring+receipt_factoring",
        "receipt_factoring",
        "read_consolidation+unknown",
        "read_consolidation++typed_keys",
    ],
)
def test_combination_keys_require_exact_unique_sorted_features(tmp_path, key):
    path, _, _ = _manifest(tmp_path, combinations=[key])
    with pytest.raises(ValueError, match="sorted feature names"):
        campaign._load_manifest(path)


@pytest.mark.parametrize("mutation", ["unknown", "missing", "extra", "executable"])
def test_v5_manifest_rejects_unknown_missing_and_executable_inputs(tmp_path, mutation):
    path, value, _ = _manifest(tmp_path)
    if mutation == "unknown":
        value["timeout"] = 1
    elif mutation == "missing":
        del value["candidates"]["typed_keys"]
    elif mutation == "extra":
        value["candidates"]["queue"] = value["baseline"]
    else:
        value["baseline"]["command"] = ["python", "untrusted.py"]
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        loaded = campaign._load_manifest(path)
        campaign._resolve_entry(loaded["baseline"], path)


def test_v5_duplicate_json_fields_fail_before_package_access(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text('{"version":5,"version":4}')
    with pytest.raises(ValueError, match="duplicate key"):
        campaign._load_manifest(path)


@pytest.mark.parametrize(
    "target", ["receipt_factoring", "_combination:read_consolidation+typed_keys"]
)
def test_complete_inventory_cannot_authorize_changes_outside_store(tmp_path, target):
    path, _, _ = _manifest(tmp_path, combinations=["read_consolidation+typed_keys"])
    loaded = campaign._load_manifest(path)
    entry = loaded[target]
    package = Path(entry["path"])
    (package / "support.py").write_text("CHANGED = True\n")
    entry["source_hashes"] = benchmark.source_hashes(package)
    resolved = {
        name: (Path(entry["path"]), entry["source_hashes"])
        for name, entry in loaded.items()
        if name != "_version"
    }
    with pytest.raises(ValueError, match="outside current_store.py"):
        campaign._freeze_v5_packages(resolved, tmp_path / "frozen", benchmark.Budget(limit_s=10800))


def _run_fixture(tmp_path, monkeypatch, statuses, *, combos=(), durations=None):
    path, _, entries = _manifest(tmp_path, combinations=combos)
    loaded = campaign._load_manifest(path)
    packages = {name: Path(entry["path"]) for name, entry in entries.items()}
    for name, entry in loaded.items():
        if name.startswith("_combination:"):
            packages[name.removeprefix("_")] = Path(entry["path"])
    provenance = {name: {"eligible": True} for name in packages}
    report = {"stages": {}, "deferred": {}}
    output = tmp_path / "evidence"
    output.mkdir()
    clock = {"now": 0.0}
    calls, holdbacks = [], {}
    monkeypatch.setattr(campaign.time, "monotonic", lambda: clock["now"])

    def warm(name, _package, _seed, _output, budget, _warmed, current_report, **kwargs):
        assert kwargs.get("attribution") is True
        holdbacks[name] = budget.holdback_s
        clock["now"] += 1
        current_report.setdefault("warmups", {})[f"{name}:attribution-v1"] = {
            "sample": {
                "wall_s": 1.0,
                "warmup_diagnostics": {"version": 1, "attribution_mode": "direct_unprofiled"},
            }
        }

    def compare(stage, _left_name, _left, right_name, _right, *_args, **kwargs):
        assert kwargs.get("attribution") is True
        calls.append(right_name)
        clock["now"] += (durations or {}).get(right_name, 1)
        status, saving = statuses.get(right_name, ("deferred", 0))
        if status == "timeout":
            report["stages"][stage] = {
                "samples": [{"category": "baseline", "pair": 1}],
                "status": "running",
            }
            raise TimeoutError("injected unfinished comparison")
        result = {"status": status, "saved_s": saving}
        report["stages"][stage] = {
            "status": status,
            "comparison": result,
            "samples": [
                {"category": side, "pair": pair}
                for side in ("baseline", right_name)
                for pair in (1, 2, 3)
            ],
        }
        return result

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    budget = benchmark.Budget(limit_s=10800)
    campaign._run_v5(tmp_path / "seed", output, budget, set(), report, packages, provenance, {})
    return report, calls, holdbacks, budget


def test_missing_exact_pair_never_searches_or_uses_supplied_triple(tmp_path, monkeypatch):
    triple = "+".join(sorted(NAMES))
    report, calls, _, _ = _run_fixture(
        tmp_path,
        monkeypatch,
        {"receipt_factoring": ("qualified", 5), "read_consolidation": ("qualified", 9)},
        combos=[triple],
    )
    assert calls == list(NAMES)
    assert report["retained"] == ["read_consolidation"]
    assert report["final_combination"]["status"] == "not_supplied"
    assert set(report["final_combination"]["features"]) == set(NAMES[:2])


@pytest.mark.parametrize(
    "qualifiers,expected",
    [
        (NAMES[:2], "receipt_factoring"),
        (NAMES[1:], "read_consolidation"),
        (NAMES, "receipt_factoring"),
    ],
)
def test_missing_combination_savings_ties_use_declared_candidate_priority(
    tmp_path, monkeypatch, qualifiers, expected
):
    report, calls, _, _ = _run_fixture(
        tmp_path, monkeypatch, {name: ("qualified", 5) for name in qualifiers}
    )
    assert calls == list(NAMES)
    assert report["retained"] == [expected]


@pytest.mark.parametrize("status", ["deferred", "invalid", "timeout"])
def test_failed_exact_combination_retains_strongest_complete_single(tmp_path, monkeypatch, status):
    features = "+".join(sorted(NAMES))
    combo_name = f"combination:{features}"
    report, calls, _, _ = _run_fixture(
        tmp_path,
        monkeypatch,
        {
            "receipt_factoring": ("qualified", 8),
            "read_consolidation": ("qualified", 9),
            "typed_keys": ("qualified", 6),
            combo_name: (status, 20),
        },
        combos=[features],
    )
    assert calls == [*NAMES, combo_name]
    assert report["retained"] == ["read_consolidation"]
    assert report["final_combination"]["status"] == (
        "insufficient_evidence" if status == "timeout" else status
    )
    assert "integrity_validation" not in report


def test_combination_reserve_grows_with_largest_qualified_comparison(tmp_path, monkeypatch):
    features = "+".join(sorted(NAMES))
    combo_name = f"combination:{features}"
    report, calls, holdbacks, _ = _run_fixture(
        tmp_path,
        monkeypatch,
        {name: ("qualified", 5) for name in (*NAMES, combo_name)},
        combos=[features],
        durations={"receipt_factoring": 10, "read_consolidation": 30, "typed_keys": 2},
    )
    assert holdbacks["receipt_factoring"] == 15
    assert holdbacks["read_consolidation"] == 15 + 1.25 * 11
    assert holdbacks["typed_keys"] == 15 + 1.25 * 31
    assert holdbacks[combo_name] == 15
    assert calls == [*NAMES, combo_name]
    assert report["retained"] == list(NAMES)


def test_partial_first_comparison_is_not_a_complete_deferral(tmp_path, monkeypatch):
    report, calls, _, _ = _run_fixture(tmp_path, monkeypatch, {"receipt_factoring": ("timeout", 0)})
    assert calls == ["receipt_factoring"]
    assert report["retained"] == []
    assert report["status"] == "insufficient_evidence"
    assert report["stages"]["receipt_factoring_vs_baseline"]["status"] == "incomplete"


def _score_rows(runtime):
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
def test_single_http_regression_rejects_an_otherwise_fast_v5_candidate(counter):
    baseline, candidate = _score_rows(100), _score_rows(50)
    candidate[1][counter] += 1
    assert benchmark.comparison(baseline, candidate, "perf")["status"] == "deferred"


@pytest.mark.parametrize("mutation", ["original", "frozen", "seed", "harness", "deadline"])
def test_final_integrity_failure_clears_otherwise_qualified_v5_retention(
    tmp_path, monkeypatch, mutation
):
    path, _, entries = _manifest(tmp_path)
    output = tmp_path / "evidence"
    args = campaign.parser().parse_args(["--manifest", str(path), "--output", str(output)])
    clock = {"now": 0.0, "harness": "unchanged"}
    monkeypatch.setattr(campaign.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(campaign, "_machine_provenance", lambda _path: {})
    monkeypatch.setattr(
        campaign, "_campaign_provenance", lambda _budget: {"source": clock["harness"]}
    )
    distribution = {"test_fixture": "bounded V5 final-integrity seed"}
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
        report.update(status="qualified", retained=["receipt_factoring"])
        if mutation == "original":
            target = Path(provenance["receipt_factoring"]["path"]) / "current_store.py"
            target.write_text("CHANGED = True\n")
        elif mutation == "frozen":
            (packages["receipt_factoring"] / "current_store.py").write_text("CHANGED = True\n")
        elif mutation == "seed":
            (seed / "catalogue.duckdb").write_bytes(b"changed seed")
        elif mutation == "harness":
            clock["harness"] = "changed"
        else:
            clock["now"] = 10800 - benchmark.SHUTDOWN_RESERVE_S

    monkeypatch.setattr(benchmark, "supervised_prepare", prepare)
    monkeypatch.setattr(campaign, "_run_v5", qualify)
    result = campaign.run(args, budget=benchmark.Budget(limit_s=10800))
    report = json.loads((output / "report.json").read_text())
    assert report["retained"] == []
    assert report["integrity_validation"] == ("incomplete" if mutation == "deadline" else "failed")
    assert report["status"] == ("insufficient_evidence" if mutation == "deadline" else "failed")
    assert result == (0 if mutation == "deadline" else 1)


@pytest.mark.parametrize("remaining", [7.5, math.nextafter(7.5, 0)])
def test_whole_comparison_admission_includes_headroom_at_exact_boundary(
    tmp_path, monkeypatch, remaining
):
    _, _, entries = _manifest(tmp_path)
    packages = {name: Path(entry["path"]) for name, entry in entries.items()}
    provenance = {name: {"eligible": name == "receipt_factoring"} for name in packages}
    report = {"stages": {}, "deferred": {}}
    output = tmp_path / "evidence"
    output.mkdir()
    monkeypatch.setattr(campaign.time, "monotonic", lambda: 0.0)

    def warm(name, _package, _seed, _output, _budget, _warmed, current_report, **_kwargs):
        current_report.setdefault("warmups", {})[f"{name}:attribution-v1"] = {
            "sample": {"wall_s": 1.0}
        }

    comparisons = []

    def compare(stage, *_args, **_kwargs):
        comparisons.append(stage)
        value = {"status": "qualified", "saved_s": 2}
        report["stages"][stage] = {"comparison": value}
        return value

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)

    class BoundaryBudget:
        limit_s = 10800
        holdback_s = 0.0

        @property
        def remaining(self):
            return remaining

        def admit(self, *_args):
            return None

    budget = BoundaryBudget()
    campaign._run_v5(tmp_path / "seed", output, budget, set(), report, packages, provenance, {})
    assert bool(comparisons) == (remaining == 7.5)
    assert report["retained"] == (["receipt_factoring"] if remaining == 7.5 else [])
