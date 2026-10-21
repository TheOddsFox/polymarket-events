"""Direct diagnostics must preserve connection behavior and stay outside scores."""

from __future__ import annotations

import importlib.util
import signal
from pathlib import Path

import pytest

from oddsfox_catalogue.signals import Terminated

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("attribution_adversarial_campaign", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
benchmark = campaign.bench


class FakeConnection:
    def __init__(self, clock, *, failure=None):
        self.clock = clock
        self.failure = failure
        self.executed = []
        self.marker = object()

    def execute(self, query, parameters=None):
        self.clock["now"] += 2
        self.executed.append((query, parameters))
        if self.failure == "execute":
            raise RuntimeError("execute failed")
        return self

    def fetchone(self):
        self.clock["now"] += 3
        if self.failure == "fetchone":
            raise RuntimeError("fetchone failed")
        return None

    def fetchall(self):
        self.clock["now"] += 5
        if self.failure == "fetchall":
            raise RuntimeError("fetchall failed")
        return [("exact", None)]

    def fetchmany(self, size=1):
        self.clock["now"] += 7
        if self.failure == "fetchmany":
            raise RuntimeError("fetchmany failed")
        return [(index,) for index in range(size)]


class FakeContext:
    def __init__(self, connection):
        self.connection = connection
        self.entered = False
        self.exit_exception = None

    def __enter__(self):
        self.entered = True
        return self.connection

    def __exit__(self, kind, value, traceback):
        self.exit_exception = (kind, value, traceback)
        return False


def test_proxy_uses_entered_connection_preserves_chaining_and_separates_fetch(monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr(benchmark.time, "perf_counter", lambda: clock["now"])
    raw = FakeConnection(clock)
    context = FakeContext(raw)
    metrics = benchmark.new_attribution_metrics()
    with benchmark.AttributedConnection(context, metrics) as conn:
        assert context.entered
        assert conn.execute("SELECT typed parameters", [None, 7, ["a", "b"], [], ("c",)]) is conn
        assert conn.fetchone() is None
        assert conn.fetchall() == [("exact", None)]
        assert conn.fetchmany(2) == [(0,), (1,)]
        assert conn.marker is raw.marker
    assert raw.executed == [("SELECT typed parameters", [None, 7, ["a", "b"], [], ("c",)])]
    assert context.exit_exception == (None, None, None)
    expected = {
        "statement_count": 1,
        "fetchone_count": 1,
        "fetchall_count": 1,
        "fetchmany_count": 1,
        "parameter_scalar_count": 2,
        "parameter_list_element_count": 3,
        "sql_execute_wall_s": 2,
        "result_materialization_wall_s": 15,
    }
    assert {key: metrics[key] for key in expected} == expected


@pytest.mark.parametrize("failure", ["execute", "fetchone", "fetchall", "fetchmany"])
def test_proxy_propagates_errors_and_records_failed_operation(monkeypatch, failure):
    clock = {"now": 0.0}
    monkeypatch.setattr(benchmark.time, "perf_counter", lambda: clock["now"])
    raw = FakeConnection(clock, failure=failure)
    context = FakeContext(raw)
    metrics = benchmark.new_attribution_metrics()
    with (
        pytest.raises(RuntimeError, match=f"{failure} failed") as raised,
        benchmark.AttributedConnection(context, metrics) as conn,
    ):
        conn.execute("SELECT ?", ["value"])
        if failure != "execute":
            getattr(conn, failure)()
    assert context.exit_exception[0] is RuntimeError
    assert context.exit_exception[1] is raised.value
    assert metrics["statement_count"] == 1
    assert metrics["sql_execute_wall_s"] == 2
    if failure == "execute":
        assert metrics["result_materialization_wall_s"] == 0
    else:
        assert metrics[f"{failure}_count"] == 1
        assert (
            metrics["result_materialization_wall_s"]
            == {
                "fetchone": 3,
                "fetchall": 5,
                "fetchmany": 7,
            }[failure]
        )


@pytest.mark.parametrize("stop", [Terminated(signal.SIGTERM), KeyboardInterrupt("operator stop")])
@pytest.mark.parametrize("body_error", [False, True])
def test_finalization_cannot_suppress_operator_stop(monkeypatch, stop, body_error):
    raw = FakeConnection({"now": 0.0})
    context = FakeContext(raw)
    finalizations = []

    def finish(_self):
        finalizations.append(True)
        raise stop

    monkeypatch.setattr(benchmark.AttributedConnection, "_finish_statement", finish)
    with (
        pytest.raises(type(stop)) as raised,
        benchmark.AttributedConnection(context, benchmark.new_attribution_metrics()),
    ):
        if body_error:
            raise RuntimeError("body failure before the operator stop")
    assert raised.value is stop
    assert finalizations == [True]
    assert context.exit_exception is not None
    assert context.exit_exception[0] is (RuntimeError if body_error else None)


def _sample(runtime, diagnostics=None):
    value = {
        "runtime_source": str(runtime.resolve()),
        "acquire_apply_s": 1.0,
        "publish_verify_s": 0.1,
        "wall_s": 3.0,
        "peak_rss_bytes": 1000,
        "worker_peak_before_oracle_bytes": 900,
        "measurement_version": 2,
        "http_attempts": 2,
        "downloaded_bytes": 100,
        "runtime_reserved_attempts": 2,
        "runtime_downloaded_bytes": 100,
        "source_stats": {"http_attempts": 2, "downloaded_bytes": 100, "faults": 0},
        "source_failure": None,
    }
    if diagnostics is not None:
        value["warmup_diagnostics"] = diagnostics
    return value


def test_v5_warmup_uses_direct_attribution_without_profiler(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    commands = []

    def supervised(command, *_args):
        commands.append(command)
        return _sample(runtime, {"version": 1, "attribution_mode": "direct_unprofiled"})

    monkeypatch.setattr(benchmark, "supervised", supervised)
    monkeypatch.setattr(
        benchmark.cProfile, "Profile", lambda: pytest.fail("V5 direct warm-up enabled cProfile")
    )
    result = benchmark.run_sample(
        tmp_path / "seed",
        tmp_path,
        runtime,
        benchmark.Budget(),
        "smoke",
        "warm-direct",
        warmup=True,
        attribution=True,
    )
    assert "--_attribution" in commands[0]
    assert "--_profile" not in commands[0]
    assert result["profile_files"] == []
    assert result["profile_summary"] is None
    assert result["measurement_version"] == 2


def test_attribution_cannot_be_enabled_for_a_scored_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(
        benchmark, "supervised", lambda *_args: pytest.fail("instrumented scored worker started")
    )
    with pytest.raises(ValueError, match="unscored warm-ups"):
        benchmark.run_sample(
            tmp_path / "seed",
            tmp_path,
            tmp_path / "runtime",
            benchmark.Budget(),
            "smoke",
            "scored",
            warmup=False,
            attribution=True,
        )


@pytest.mark.parametrize("operator_stop", [False, True])
def test_worker_restores_all_wrappers_after_setup_failure(
    tmp_path, monkeypatch, capsys, operator_stop
):
    import oddsfox_catalogue
    from oddsfox_catalogue import current_reconcile, current_runtime, current_store

    runtime_symbols = {
        "GammaClient": current_runtime.GammaClient,
        "datetime": current_runtime.datetime,
        "connect_store": current_runtime.connect_store,
    }
    reconcile_symbols = {"fetch_unit": current_reconcile.fetch_unit, "_now": current_reconcile._now}
    store_symbols = {
        name: getattr(current_store, name)
        for name in (
            "apply",
            "_select_in",
            "_write_values",
            "_write_receipts",
            "_select_claims_by_assets",
            "_select_claims_by_markets",
        )
        if hasattr(current_store, name)
    }
    monkeypatch.setenv(
        "FULL_PASS_EXPECTED_PACKAGE", str(Path(oddsfox_catalogue.__file__).resolve().parent)
    )
    monkeypatch.setattr(benchmark, "copy_root", lambda *_args: None)

    class FailedSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            assert current_runtime.connect_store is not runtime_symbols["connect_store"]
            assert current_store.apply is not store_symbols["apply"]
            if operator_stop:
                raise KeyboardInterrupt("operator stopped setup")
            raise RuntimeError("synthetic source setup failure")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(benchmark, "CapacitySource", FailedSource)
    args = (tmp_path / "seed", tmp_path / "root", tmp_path, "failed-setup", "smoke", 1, None)
    if operator_stop:
        with pytest.raises(KeyboardInterrupt, match="operator stopped setup"):
            benchmark.worker(*args, attribution=True)
    else:
        assert benchmark.worker(*args, attribution=True) == 3
        assert "synthetic source setup failure" in capsys.readouterr().out
    for name, original in runtime_symbols.items():
        assert getattr(current_runtime, name) is original
    for name, original in reconcile_symbols.items():
        assert getattr(current_reconcile, name) is original
    for name, original in store_symbols.items():
        assert getattr(current_store, name) is original


@pytest.mark.parametrize(
    "diagnostics",
    [None, {}, {"version": 2, "attribution_mode": "direct_unprofiled"}, {"version": 1}],
)
def test_v5_warmup_rejects_missing_or_wrong_attribution_payload(tmp_path, monkeypatch, diagnostics):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    monkeypatch.setattr(benchmark, "supervised", lambda *_args: _sample(runtime, diagnostics))
    with pytest.raises(ValueError, match="direct unprofiled attribution"):
        benchmark.run_sample(
            tmp_path / "seed",
            tmp_path,
            runtime,
            benchmark.Budget(),
            "smoke",
            "warm-direct",
            warmup=True,
            attribution=True,
        )


def test_scored_comparison_never_passes_attribution_to_samples(tmp_path, monkeypatch):
    calls = []
    report = {"stages": {}}
    monkeypatch.setattr(campaign, "_warm", lambda *_args, **_kwargs: None)

    def sample(_seed, _output, _package, _budget, label, **kwargs):
        calls.append((label, kwargs))
        return {"semantic_hash": "same"}

    monkeypatch.setattr(campaign, "_stage_sample", sample)
    campaign._compare_stage(
        "receipt_vs_baseline",
        "baseline",
        tmp_path / "base",
        "receipt_factoring",
        tmp_path / "receipt",
        tmp_path / "seed",
        tmp_path,
        benchmark.Budget(),
        set(),
        report,
        attribution=True,
        comparison=lambda *_args: {"status": "qualified"},
    )
    assert len(calls) == 6
    assert all(not kwargs.get("attribution") and not kwargs.get("warmup") for _, kwargs in calls)
    assert [label.rsplit("-", 1)[-1] for label, _ in calls] == [
        "baseline",
        "receipt_factoring",
        "receipt_factoring",
        "baseline",
        "baseline",
        "receipt_factoring",
    ]


@pytest.mark.parametrize(
    "diagnostics",
    [None, {"version": 1}, {"version": 1, "attribution_mode": "cprofile"}],
)
@pytest.mark.parametrize("same_name", [False, True])
def test_unattributed_cached_warmup_is_not_reused(tmp_path, monkeypatch, diagnostics, same_name):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "__init__.py").write_text("")
    source_hash = benchmark.canonical_hash(benchmark.source_hashes(runtime))
    existing_name = "baseline:attribution-v1" if same_name else "old:attribution-v1"
    sample = {"wall_s": 1.0}
    if diagnostics is not None:
        sample["warmup_diagnostics"] = diagnostics
    report = {
        "warmups": {existing_name: {"source_hash": source_hash, "workload": None, "sample": sample}}
    }
    warmed = {(source_hash, None, True)}
    calls = []

    def stage_sample(*_args, **kwargs):
        calls.append(kwargs)
        return {
            "wall_s": 1.0,
            "warmup_diagnostics": {"version": 1, "attribution_mode": "direct_unprofiled"},
        }

    monkeypatch.setattr(campaign, "_stage_sample", stage_sample)
    campaign._warm(
        "baseline",
        runtime,
        tmp_path / "seed",
        tmp_path / "evidence",
        benchmark.Budget(),
        warmed,
        report,
        attribution=True,
    )
    assert len(calls) == 1
    assert calls[0]["warmup"] is True
    assert calls[0]["attribution"] is True
    assert report["warmups"]["baseline:attribution-v1"]["sample"]["warmup_diagnostics"] == {
        "version": 1,
        "attribution_mode": "direct_unprofiled",
    }
