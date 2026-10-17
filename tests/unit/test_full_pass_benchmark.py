from __future__ import annotations

import importlib.util
import json
import math
import os
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-full-pass.py"
SPEC = importlib.util.spec_from_file_location("benchmark_full_pass", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def row(
    pair,
    acquire=20.0,
    wall=30.0,
    rss=1000,
    attempts=10,
    semantic="same",
    runtime_bytes=100,
    offered_bytes=100,
    reserved_attempts=None,
):
    return {
        "pair": pair,
        "acquire_apply_s": acquire,
        "publish_verify_s": 1.0,
        "wall_s": wall,
        "peak_rss_bytes": rss,
        "http_attempts": attempts,
        "downloaded_bytes": offered_bytes,
        "runtime_reserved_attempts": attempts if reserved_attempts is None else reserved_attempts,
        "runtime_downloaded_bytes": runtime_bytes,
        "semantic_hash": semantic,
    }


def test_dataset_is_deterministic_with_explicit_cohorts_and_orphans():
    first = benchmark.build_dataset(250, 1010)
    second = benchmark.build_dataset(250, 1010)
    events, markets, distribution = first
    assert first == second
    assert distribution == {
        "events": 250,
        "markets": 1010,
        "owned_markets": 1000,
        "orphan_markets": 10,
        "closed_events": 225,
        "urgent_active_events": 2,
        "ordinary_active_events": 23,
        "membership_complete": True,
        "faults_enabled": False,
        "delay_s": 0,
    }
    assert all(len(event["markets"]) == 4 for event in events)
    assert all(event["markets"][0]["conditionId"] for event in events)
    assert sum(event["_benchmark_receipt"] == "2026-10-13T01:00:00Z" for event in events) == 2
    assert len(markets) == 1010


def test_normalization_removes_only_declared_volatile_fields():
    value = {
        "capture_id": "x",
        "path": "/tmp/a",
        "elapsed_s": 4,
        "cursor": 99,
        "release_id": "r",
        "source": "fixture",
        "nested": {"response_id": "y", "remaining": 3},
    }
    assert benchmark.normalize(value) == {
        "cursor": 99,
        "nested": {"remaining": 3},
        "source": "fixture",
    }


def test_comparison_requires_three_semantically_equivalent_pairs_and_gate():
    baseline = [row(pair) for pair in (1, 2, 3)]
    candidate = [row(pair, acquire=18.0, wall=30.0, rss=1000, attempts=10) for pair in (1, 2, 3)]
    assert (
        benchmark.comparison(baseline[:2], candidate, "perf")["status"] == "insufficient_evidence"
    )
    assert benchmark.comparison(baseline, candidate, "smoke")["status"] == "deferred"
    assert benchmark.comparison(baseline, candidate, "perf")["status"] == "qualified"
    assert (
        benchmark.comparison(baseline, [*candidate[:2], row(3, semantic="different")], "perf")[
            "status"
        ]
        == "invalid"
    )
    assert (
        benchmark.comparison(baseline, [row(1), row(2), row(3, attempts=11)], "perf")["status"]
        == "deferred"
    )
    consumed_baseline = [row(pair, runtime_bytes=90, offered_bytes=110) for pair in (1, 2, 3)]
    consumed_candidate = [
        row(pair, acquire=18, runtime_bytes=91, offered_bytes=110) for pair in (1, 2, 3)
    ]
    assert (
        benchmark.comparison(consumed_baseline, consumed_candidate, "perf")["status"] == "deferred"
    )


def test_measurement_versions_cannot_be_compared_without_a_fresh_baseline():
    baseline = [row(pair) for pair in (1, 2, 3)]
    current = [
        {
            **row(pair, acquire=18),
            "measurement_version": 2,
            "worker_peak_before_oracle_bytes": 900,
        }
        for pair in (1, 2, 3)
    ]
    result = benchmark.comparison(baseline, current, "perf")
    assert result["status"] == "invalid"
    assert "fresh baseline" in result["reason"]


def test_phase_attribution_uses_disjoint_worker_boundaries():
    assert benchmark.phase_attribution(2, 5, 11, 13, 17) == {
        "worker_setup_wall_s": 3,
        "runtime_sequence_wall_s": 6,
        "oracle_wall_s": 4,
    }
    with pytest.raises(ValueError, match="overlap"):
        benchmark.phase_attribution(2, 5, 11, 10, 17)


def test_cleanup_warnings_are_attributed_to_each_runtime_invocation():
    result = {
        "publication": {
            "cleanup_warning": "release cleanup deferred",
            "retention_warning": "raw cleanup deferred",
        }
    }
    assert benchmark.publication_cleanup_warnings(result, 0) == [
        {"step": 0, "kind": "cleanup_warning", "message": "release cleanup deferred"},
        {"step": 0, "kind": "retention_warning", "message": "raw cleanup deferred"},
    ]


def test_source_accounting_accepts_cancelled_prefetch_partial_body():
    assert benchmark.validate_source_accounting(
        {"faults": 0, "http_attempts": 4, "downloaded_bytes": 110},
        expected_attempts=4,
        expected_bytes=90,
    ) == (4, 110)


@pytest.mark.parametrize("attempts, downloaded", [(True, 10), (1, True), (-1, 10), (1, -1)])
def test_source_accounting_rejects_invalid_counters(attempts, downloaded):
    with pytest.raises(ValueError):
        benchmark.validate_source_accounting(
            {"faults": 0, "http_attempts": attempts, "downloaded_bytes": downloaded},
            expected_attempts=1,
            expected_bytes=10,
        )


@pytest.mark.parametrize("attempts, downloaded", [(True, 10), (1, True), (-1, 10), (1, -1)])
def test_source_accounting_rejects_invalid_runtime_counters(attempts, downloaded):
    with pytest.raises(ValueError):
        benchmark.validate_source_accounting(
            {"faults": 0, "http_attempts": 1, "downloaded_bytes": 10},
            expected_attempts=attempts,
            expected_bytes=downloaded,
        )


@pytest.mark.parametrize("value", [True, math.nan, math.inf, -1, 600.01, 601, "12"])
def test_elapsed_validation_rejects_invalid_or_exhausted_budget(value):
    with pytest.raises(ValueError):
        benchmark.validate_elapsed(value)


def test_elapsed_zero_is_valid_and_reserve_is_preserved():
    assert benchmark.validate_elapsed(0) == 0.0
    budget = benchmark.Budget(100.0)
    assert budget.remaining <= 485.0
    with pytest.raises(TimeoutError):
        benchmark.Budget(585).admit(1)


def test_valid_exhausted_baseline_is_deferred_without_starting_a_worker(tmp_path, monkeypatch):
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    (baseline / "report.json").write_text(
        '{"kind":"synthetic-full-pass","profile":"smoke","status":"baseline_ready","elapsed_seconds":600}'
    )

    def refuse_without_budget(command, output, budget, log):
        budget.admit(1)
        pytest.fail("validation worker must not start without budget")

    monkeypatch.setattr(benchmark, "supervised_prepare", refuse_without_budget)
    output = tmp_path / "output"
    args = benchmark.parser().parse_args(
        ["--profile", "smoke", "--output", str(output), "--compare", str(baseline)]
    )
    assert benchmark.run(args) == 0
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "insufficient_evidence"


def test_source_accounting_rejects_mismatched_runtime_counts_and_faults():
    stats = {"http_attempts": 12, "downloaded_bytes": 120, "faults": 0}
    assert benchmark.validate_source_accounting(stats, 12, 120) == (12, 120)
    with pytest.raises(ValueError, match="exceed"):
        benchmark.validate_source_accounting(stats, 11, 120)
    # A cancelled prefetch can consume fewer bytes than the fixture offered.
    assert benchmark.validate_source_accounting(stats, 12, 119) == (12, 120)
    # A reservation cancelled before the fixture serves it is a valid partial count.
    assert benchmark.validate_source_accounting(stats, 13, 119) == (12, 120)
    with pytest.raises(ValueError, match="bytes exceed"):
        benchmark.validate_source_accounting(stats, 12, 121)
    with pytest.raises(ValueError, match="faults"):
        benchmark.validate_source_accounting({**stats, "faults": 1}, 12, 120)
    with pytest.raises(ValueError, match="exceed"):
        benchmark.validate_source_accounting(stats, 11, 120)


def test_comparison_defers_when_runtime_reservations_increase():
    baseline = [row(pair, reserved_attempts=12, attempts=10) for pair in (1, 2, 3)]
    candidate = [row(pair, acquire=18, reserved_attempts=13, attempts=10) for pair in (1, 2, 3)]
    assert benchmark.comparison(baseline, candidate, "perf")["status"] == "deferred"


def test_run_sample_wall_includes_supervisor_startup_and_cleanup(tmp_path, monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(benchmark.time, "monotonic", lambda: clock[0])
    runtime = tmp_path / "runtime" / "oddsfox_catalogue"
    runtime.mkdir(parents=True)

    def fake_supervised(*args, **kwargs):
        clock[0] += 5.0
        return {
            **row(1, wall=3.0),
            "measurement_version": 2,
            "worker_peak_before_oracle_bytes": 900,
            "worker_wall_s": 3.0,
            "runtime_source": str(runtime),
            "source_stats": {"http_attempts": 10, "downloaded_bytes": 100, "faults": 0},
            "runtime_reserved_attempts": 10,
            "runtime_downloaded_bytes": 100,
            "source_failure": None,
        }

    monkeypatch.setattr(benchmark, "supervised", fake_supervised)
    result = benchmark.run_sample(
        tmp_path / "seed", tmp_path, runtime, benchmark.Budget(), "smoke", "timed"
    )
    assert result["wall_s"] == 5.0
    assert result["worker_wall_s"] == 3.0


def test_baseline_semantic_samples_must_match():
    assert benchmark.validate_semantic_samples([row(pair) for pair in (1, 2, 3)]) == "same"
    with pytest.raises(ValueError, match="semantically inconsistent"):
        benchmark.validate_semantic_samples([row(1), row(2), row(3, semantic="different")])


def test_output_and_baseline_must_be_disjoint():
    root = Path("/tmp/evidence")
    benchmark.validate_disjoint(root / "new", root / "old")
    for output, baseline in ((root, root / "old"), (root / "old", root), (root, root)):
        with pytest.raises(ValueError, match="disjoint"):
            benchmark.validate_disjoint(output, baseline)


def test_baseline_tree_rejects_symlinks_and_special_files(tmp_path):
    tree = tmp_path / "baseline"
    tree.mkdir()
    (tree / "ok.py").write_text("# source\n")
    benchmark.validate_tree(tree, python_only=True)
    link = tree / "link.py"
    link.symlink_to(tree / "ok.py")
    with pytest.raises(ValueError, match="symlink or special"):
        benchmark.validate_tree(tree, python_only=True)
    link.unlink()
    if hasattr(os, "mkfifo"):
        os.mkfifo(tree / "pipe")
        with pytest.raises(ValueError, match="symlink or special"):
            benchmark.validate_tree(tree)


def test_baseline_tree_rejects_symlink_ancestor(tmp_path):
    outside = tmp_path / "outside"
    package = outside / "oddsfox_catalogue"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("# source\n")
    linked_parent = tmp_path / "baseline-runtime"
    linked_parent.symlink_to(outside, target_is_directory=True)
    with pytest.raises(Exception, match="symlink"):
        benchmark.validate_tree(linked_parent / "oddsfox_catalogue", python_only=True)


def test_baseline_metrics_reject_nan_and_wrong_shape():
    benchmark.validate_report_metrics([row(pair) for pair in (1, 2, 3)])
    bad = [row(pair) for pair in (1, 2, 3)]
    bad[0]["wall_s"] = math.nan
    with pytest.raises(ValueError, match="invalid baseline metric"):
        benchmark.validate_report_metrics(bad)
    with pytest.raises(ValueError, match="three complete"):
        benchmark.validate_report_metrics([])


def test_baseline_metrics_require_consistent_offered_and_consumed_byte_counters():
    rows = [row(pair) for pair in (1, 2, 3)]
    benchmark.validate_report_metrics(rows)
    rows[0]["runtime_downloaded_bytes"] = rows[0]["downloaded_bytes"] + 1
    with pytest.raises(ValueError, match="exceeds source"):
        benchmark.validate_report_metrics(rows)
