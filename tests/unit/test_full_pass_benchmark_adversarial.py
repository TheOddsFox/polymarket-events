"""Evidence must fail closed before damaged input or invalid timing can qualify."""

import json
import math
import os
import signal
import sys

import pytest
from test_full_pass_benchmark import benchmark, row


def baseline_fixture(tmp_path):
    baseline = tmp_path / "baseline"
    seed = baseline / "seed"
    runtime = baseline / "runtime" / "oddsfox_catalogue"
    seed.mkdir(parents=True)
    runtime.mkdir(parents=True)
    (seed / "catalogue.duckdb").write_bytes(b"synthetic seed")
    (runtime / "__init__.py").write_text("# frozen runtime\n")
    hashes = benchmark.source_hashes(runtime)
    report = {
        "kind": "synthetic-full-pass",
        "status": "baseline_ready",
        "profile": "smoke",
        "elapsed_seconds": 100.0,
        "samples": [row(pair) for pair in (1, 2, 3)],
        "source_hashes": hashes,
        "runtime_hash": benchmark.canonical_hash(hashes),
        "seed_sha256": benchmark.digest(seed / "catalogue.duckdb"),
        "distribution": benchmark.build_dataset(*benchmark.PROFILES["smoke"])[2],
    }
    (baseline / "report.json").write_text(json.dumps(report))
    return baseline


def args(output, baseline):
    return benchmark.parser().parse_args(
        ["--profile", "smoke", "--output", str(output), "--compare", str(baseline)]
    )


def test_nested_output_rejected_without_mutating_baseline(tmp_path):
    baseline = baseline_fixture(tmp_path)
    before = {path: path.read_bytes() for path in baseline.rglob("*") if path.is_file()}
    output = baseline / "nested" / "new"
    with pytest.raises(ValueError, match="disjoint"):
        benchmark.run(args(output, baseline))
    assert not output.parent.exists()
    assert {path: path.read_bytes() for path in baseline.rglob("*") if path.is_file()} == before


@pytest.mark.parametrize("artifact", ["seed", "runtime"])
@pytest.mark.parametrize("unsafe", ["symlink", "fifo"])
def test_unsafe_baseline_descendants_never_reach_workers(tmp_path, monkeypatch, artifact, unsafe):
    baseline = baseline_fixture(tmp_path)
    tree = baseline / "seed" if artifact == "seed" else baseline / "runtime" / "oddsfox_catalogue"
    outside = tmp_path / "outside.py"
    outside.write_text("preserve me")
    path = tree / "unsafe.py"
    if unsafe == "symlink":
        path.symlink_to(outside)
    else:
        os.mkfifo(path)
    monkeypatch.setattr(benchmark, "run_sample", lambda *a, **k: pytest.fail("unsafe worker"))
    with pytest.raises(ValueError, match="symlink or special"):
        benchmark.validate_baseline(baseline, "smoke", benchmark.REPO / "src" / "oddsfox_catalogue")
    assert outside.read_text() == "preserve me"


@pytest.mark.parametrize("artifact", ["seed", "runtime"])
def test_tampered_baseline_bytes_fail_before_any_worker(tmp_path, monkeypatch, artifact):
    baseline = baseline_fixture(tmp_path)
    target = (
        baseline / "seed" / "catalogue.duckdb"
        if artifact == "seed"
        else baseline / "runtime" / "oddsfox_catalogue" / "__init__.py"
    )
    target.write_bytes(b"tampered")
    monkeypatch.setattr(benchmark, "validate_root", lambda path: path)
    monkeypatch.setattr(benchmark, "run_sample", lambda *a, **k: pytest.fail("tampered worker"))
    with pytest.raises(ValueError, match="damaged"):
        benchmark.validate_baseline(baseline, "smoke", benchmark.REPO / "src" / "oddsfox_catalogue")
    assert target.read_bytes() == b"tampered"


@pytest.mark.parametrize(
    "metric",
    [
        "acquire_apply_s",
        "publish_verify_s",
        "wall_s",
        "peak_rss_bytes",
        "http_attempts",
        "downloaded_bytes",
    ],
)
@pytest.mark.parametrize("invalid", [None, True, "1", -1, math.nan, math.inf, -math.inf])
def test_baseline_rejects_each_invalid_metric(metric, invalid):
    samples = [row(pair) for pair in (1, 2, 3)]
    samples[1][metric] = invalid
    with pytest.raises(ValueError, match="invalid baseline metric"):
        benchmark.validate_report_metrics(samples)


@pytest.mark.parametrize(
    "invalid", [None, False, -math.inf, -0.01, math.nextafter(600.0, math.inf)]
)
def test_elapsed_boundaries_are_finite_nonnegative_and_within_total_budget(invalid):
    with pytest.raises(ValueError):
        benchmark.validate_elapsed(invalid)


def test_elapsed_accepts_fraction_immediately_below_reserve():
    value = math.nextafter(585.0, 0)
    assert benchmark.validate_elapsed(value) == value


@pytest.mark.parametrize("elapsed", [585.0, 599.0, 600.0])
def test_valid_exhausted_metadata_retains_evidence_without_starting_work(
    tmp_path, monkeypatch, elapsed
):
    assert benchmark.validate_elapsed(elapsed) == elapsed
    baseline = baseline_fixture(tmp_path)
    path = baseline / "report.json"
    report = json.loads(path.read_text())
    report["elapsed_seconds"] = elapsed
    path.write_text(json.dumps(report))

    def reject_work_without_budget(command, output, budget, log):
        budget.admit(1)
        pytest.fail("worker started inside reserved shutdown budget")

    monkeypatch.setattr(benchmark, "supervised_prepare", reject_work_without_budget)
    monkeypatch.setattr(benchmark, "run_sample", lambda *a, **k: pytest.fail("sample started"))
    output = tmp_path / "output"
    assert benchmark.run(args(output, baseline)) == 0
    result = json.loads((output / "report.json").read_text())
    assert result["status"] == "insufficient_evidence"
    assert result["samples"] == []


def test_http_and_byte_limits_follow_pair_identity_after_alternating_order():
    baseline = [row(1, attempts=10), row(2, attempts=20), row(3, attempts=30)]
    candidates = [
        row(3, acquire=18, attempts=30),
        row(1, acquire=18),
        row(2, acquire=18, attempts=20),
    ]
    assert benchmark.comparison(baseline, candidates, "perf")["status"] == "qualified"
    candidates[0]["downloaded_bytes"] += 1
    assert benchmark.comparison(baseline, candidates, "perf")["status"] == "deferred"


@pytest.mark.parametrize(
    "metric,value",
    [("wall_s", 31.500001), ("peak_rss_bytes", 1050.000001), ("acquire_apply_s", 18.000001)],
)
def test_scoring_rejects_values_just_outside_gates(metric, value):
    baseline = [row(pair) for pair in (1, 2, 3)]
    candidates = [row(pair, acquire=18, wall=31.5, rss=1050) for pair in (1, 2, 3)]
    assert benchmark.comparison(baseline, candidates, "perf")["status"] == "qualified"
    for sample in candidates:
        sample[metric] = value
    assert benchmark.comparison(baseline, candidates, "perf")["status"] == "deferred"


def test_relative_gain_cannot_qualify_without_one_second_absolute_gain():
    baseline = [row(pair, acquire=5) for pair in (1, 2, 3)]
    candidates = [row(pair, acquire=4.01) for pair in (1, 2, 3)]
    assert benchmark.comparison(baseline, candidates, "perf")["status"] == "deferred"


def test_wrong_loaded_runtime_rejected_before_copying_or_running(tmp_path, monkeypatch):
    monkeypatch.setenv("FULL_PASS_EXPECTED_PACKAGE", str(tmp_path / "wrong-runtime"))
    monkeypatch.setattr(benchmark, "copy_root", lambda *a: pytest.fail("copied before provenance"))
    with pytest.raises(ValueError, match="different runtime"):
        benchmark.worker(tmp_path / "seed", tmp_path / "root", tmp_path, "sample", "smoke", 1, None)


def test_sample_rejects_worker_returning_wrong_runtime(tmp_path, monkeypatch):
    result = {**row(1), "runtime_source": str(tmp_path / "wrong-runtime")}
    monkeypatch.setattr(benchmark, "supervised", lambda *a: result)
    with pytest.raises(ValueError, match="provenance"):
        benchmark.run_sample(
            tmp_path / "seed",
            tmp_path,
            tmp_path / "expected-runtime",
            benchmark.Budget(),
            "smoke",
            "sample",
        )


def test_child_environment_removes_ambient_runtime_and_catalogue_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/untrusted")
    monkeypatch.setenv("PYTHONHOME", "/untrusted")
    monkeypatch.setenv("CATALOGUE_ROOT", "/operator/data")
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "https://live.invalid")
    monkeypatch.setenv("VIRTUAL_ENV", "/untrusted")
    package = tmp_path / "oddsfox_catalogue"
    env = benchmark.child_environment("http://127.0.0.1:12345", package)
    assert env["PYTHONPATH"] == str(tmp_path)
    assert env["FULL_PASS_EXPECTED_PACKAGE"] == str(package)
    assert env["CATALOGUE_GAMMA_BASE_URL"] == "http://127.0.0.1:12345"
    assert env["CATALOGUE_GAMMA_ALLOW_LOOPBACK"] == "1"
    assert not {"PYTHONHOME", "CATALOGUE_ROOT", "VIRTUAL_ENV"} & env.keys()


def test_compare_budget_includes_preparation_before_baseline_validation(tmp_path, monkeypatch):
    baseline = baseline_fixture(tmp_path)
    clock = [0.0]
    monkeypatch.setattr(benchmark.time, "monotonic", lambda: clock[0])
    original_read = type(baseline).read_text

    def read_with_preparation_cost(path, *a, **k):
        if path == baseline / "report.json":
            clock[0] += 7.0
        return original_read(path, *a, **k)

    monkeypatch.setattr(type(baseline), "read_text", read_with_preparation_cost)
    monkeypatch.setattr(
        benchmark,
        "supervised_prepare",
        lambda *a, **k: {
            "status": "validated",
            "distribution": {},
            "runtime_hash": "frozen",
            "source_hashes": {},
            "candidate_source_hashes": {},
            "seed_sha256": "frozen",
        },
    )
    observed = []

    def no_time_to_sample(seed, output, runtime, budget, *a, **k):
        observed.append(budget.elapsed)
        raise TimeoutError("deterministic budget stop")

    monkeypatch.setattr(benchmark, "run_sample", no_time_to_sample)
    output = tmp_path / "output"
    assert benchmark.run(args(output, baseline)) == 0
    assert observed == [107.0]
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "insufficient_evidence"
    assert report["elapsed_seconds"] == 107.0


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM, signal.SIGHUP])
def test_main_restores_handlers_and_retains_interrupted_report(tmp_path, monkeypatch, signum):
    signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    original = {number: object() for number in signals}
    installed = dict(original)
    changes = []

    def register(number, handler):
        previous = installed[number]
        installed[number] = handler
        changes.append((number, handler))
        return previous

    monkeypatch.setattr(benchmark.signal, "signal", register)
    output = tmp_path / "output"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark",
            "--profile",
            "smoke",
            "--output",
            str(output),
        ],
    )
    cleanup = []

    def interrupted_supervisor(*a, **k):
        assert all(callable(installed[number]) for number in signals)
        try:
            installed[signum](signum, None)
        finally:
            cleanup.append("supervised cleanup completed")

    monkeypatch.setattr(benchmark, "supervised_prepare", interrupted_supervisor)
    assert benchmark.main() == 128 + signum
    assert cleanup == ["supervised cleanup completed"]
    assert installed == original
    assert [number for number, _ in changes] == [*signals, *signals]
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "partial"
    assert report["interrupted_exit_code"] == 128 + signum
    assert report["samples"] == []
    assert "retained evidence" in report["error"]
