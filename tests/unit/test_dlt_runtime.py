"""dlt loading cannot enable hidden telemetry or use ambient runtime directories."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.configuration.utils import ResolvedValueTrace, TraceLogContext
from dlt.common.runtime import anon_tracker, telemetry
from dlt.extract.concurrency import FuturesPool
from dlt.load import Load
from dlt.normalize import Normalize

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runtime import CatalogueRunContext, confined_runtime
from oddsfox_catalogue.load.source import event_resource, make_pipeline


@pytest.fixture
def resolver_container():
    container = Container()
    # Container.get creates missing defaults; absence is part of the contract under test.
    previous = container[TraceLogContext] if TraceLogContext in container else None  # noqa: SIM401
    if previous is not None:
        del container[TraceLogContext]
    try:
        yield container
    finally:
        if TraceLogContext in container:
            del container[TraceLogContext]
        if previous is not None:
            container[TraceLogContext] = previous


def _populated_resolver_context():
    context = TraceLogContext()
    trace = ResolvedValueTrace(
        "synthetic",
        {"payload": object()},
        None,
        dict,
        ("synthetic",),
        "synthetic-provider",
        RuntimeConfiguration(dlthub_telemetry=False),
    )
    context.log(trace)
    context.log(trace._replace(value=None))
    return context


@pytest.mark.parametrize("prior", ["absent", "enabled", "disabled"])
@pytest.mark.parametrize("error", [None, RuntimeError, KeyboardInterrupt])
def test_confined_runtime_restores_resolver_context_and_history(
    tmp_path, resolver_container, prior, error
):
    previous = None if prior == "absent" else _populated_resolver_context()
    if previous is not None:
        previous.logging_enabled = prior == "enabled"
        resolver_container[TraceLogContext] = previous
        resolved = previous.resolved_traces
        all_traces = previous.all_traces
        history = (tuple(resolved), tuple(all_traces))

    def exercise():
        with confined_runtime(tmp_path / "dlt"):
            active = resolver_container[TraceLogContext]
            assert active is not previous
            assert active.logging_enabled is False
            assert active.resolved_traces == active.all_traces == []
            active.log(_populated_resolver_context().all_traces[0])
            assert active.resolved_traces == active.all_traces == []
            if error is not None:
                raise error("synthetic interruption")

    if error is None:
        exercise()
    else:
        with pytest.raises(error, match="synthetic interruption"):
            exercise()

    if previous is None:
        assert TraceLogContext not in resolver_container
    else:
        assert resolver_container[TraceLogContext] is previous
        assert previous.logging_enabled is (prior == "enabled")
        assert previous.resolved_traces is resolved
        assert previous.all_traces is all_traces
        assert (tuple(resolved), tuple(all_traces)) == history


@pytest.mark.parametrize("initialization_fails", [False, True])
def test_resolver_logging_is_disabled_before_runtime_initialization(
    tmp_path, resolver_container, monkeypatch, initialization_fails
):
    previous = _populated_resolver_context()
    resolver_container[TraceLogContext] = previous
    original = CatalogueRunContext.__init__
    initialized = []

    def initialize(self, *args, **kwargs):
        traces = resolver_container[TraceLogContext]
        assert traces is not previous
        assert traces.logging_enabled is False
        assert traces.resolved_traces == traces.all_traces == []
        initialized.append(traces)
        if initialization_fails:
            raise RuntimeError("synthetic initialization failure")
        original(self, *args, **kwargs)
        assert traces.resolved_traces == traces.all_traces == []

    monkeypatch.setattr(CatalogueRunContext, "__init__", initialize)
    if initialization_fails:
        with (
            pytest.raises(RuntimeError, match="synthetic initialization failure"),
            confined_runtime(tmp_path / "dlt"),
        ):
            pytest.fail("failed initialization must not yield")
    else:
        with confined_runtime(tmp_path / "dlt"):
            assert resolver_container[TraceLogContext] is initialized[0]
    assert len(initialized) == 1
    assert resolver_container[TraceLogContext] is previous
    assert len(previous.resolved_traces) == 1
    assert len(previous.all_traces) == 2


def test_nested_confined_runtime_restores_outer_disabled_context(tmp_path, resolver_container):
    previous = _populated_resolver_context()
    resolver_container[TraceLogContext] = previous
    with confined_runtime(tmp_path / "outer"):
        outer = resolver_container[TraceLogContext]
        with (
            pytest.raises(KeyboardInterrupt, match="synthetic nested interruption"),
            confined_runtime(tmp_path / "outer"),
        ):
            inner = resolver_container[TraceLogContext]
            assert inner is not outer
            assert inner.logging_enabled is False
            assert inner.resolved_traces == inner.all_traces == []
            raise KeyboardInterrupt("synthetic nested interruption")
        assert resolver_container[TraceLogContext] is outer
        assert outer.logging_enabled is False
        assert outer.resolved_traces == outer.all_traces == []
        with confined_runtime(tmp_path / "outer"):
            assert resolver_container[TraceLogContext] is not outer
        assert resolver_container[TraceLogContext] is outer
    assert resolver_container[TraceLogContext] is previous
    assert len(previous.resolved_traces) == 1
    assert len(previous.all_traces) == 2


def test_dlt_named_worker_uses_parent_disabled_resolver_context(tmp_path, resolver_container):
    previous = _populated_resolver_context()
    resolver_container[TraceLogContext] = previous
    trace = previous.all_traces[0]
    with confined_runtime(tmp_path / "dlt"):
        active = resolver_container[TraceLogContext]

        def worker():
            assert Container()[TraceLogContext] is active
            assert active.logging_enabled is False
            for _ in range(100):
                Container()[TraceLogContext].log(trace)

        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=Container.thread_pool_prefix() + "resolver-test"
        ) as executor:
            executor.submit(worker).result(timeout=5)
        assert active.resolved_traces == active.all_traces == []
    assert resolver_container[TraceLogContext] is previous
    assert len(previous.resolved_traces) == 1
    assert len(previous.all_traces) == 2


@pytest.mark.parametrize("entrypoint", ["pipeline", "loader"])
def test_loader_uses_confined_runtime_without_initializing_any_tracker(tmp_path, entrypoint):
    ambient = tmp_path / "ambient"
    ambient.mkdir()
    (ambient / ".dlt").mkdir()
    (ambient / ".dlt" / "config.toml").write_text("[runtime]\ndlthub_telemetry = true\n")
    code = r"""
import sys
from pathlib import Path
import requests
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import DictionaryProvider
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.utils import ResolvedValueTrace, TraceLogContext
from dlt.common.runtime import anon_tracker, telemetry

calls = []
def forbidden(*args, **kwargs):
    calls.append("forbidden")
    raise AssertionError("hidden tracker or network initialized")

telemetry.init_anon_tracker = forbidden
anon_tracker.init_anon_tracker = forbidden
anon_tracker.get_anonymous_id = forbidden
requests.Session.send = forbidden

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.source import make_pipeline, event_resource

root = Path(sys.argv[1])
settings = Settings(root)
caller = TraceLogContext()
caller.log(ResolvedValueTrace("synthetic", object(), None, object, (), "test", None))
Container()[TraceLogContext] = caller
caller_history = (tuple(caller.resolved_traces), tuple(caller.all_traces))
if sys.argv[2] == "pipeline":
    pipeline = make_pipeline(
        settings.warehouse_path, settings.dlt_pipelines_dir, settings.load,
        temp_directory=settings.temporary_dir, max_temp_bytes=16 * 1024**2,
    )
    assert Container()[TraceLogContext] is caller
    assert (tuple(caller.resolved_traces), tuple(caller.all_traces)) == caller_history
    pipeline.run(event_resource([], materialize_only=True))
else:
    import httpx
    from oddsfox_catalogue.capture.ledger import Ledger
    from oddsfox_catalogue.capture.runner import CaptureRuntime, run_capture
    from oddsfox_catalogue.gamma.http import GammaClient
    from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
    from dlt.pipeline.pipeline import Pipeline

    original_run = Pipeline.run
    resource_runs = []
    def checked_run(self, *args, **kwargs):
        traces = Container()[TraceLogContext]
        assert traces is not caller
        assert traces.logging_enabled is False
        assert traces.resolved_traces == traces.all_traces == []
        result = original_run(self, *args, **kwargs)
        assert Container()[TraceLogContext] is traces
        assert traces.resolved_traces == traces.all_traces == []
        resource_runs.append(traces)
        return result
    Pipeline.run = checked_run
    body = {
        "id": "1", "question": "Synthetic selected market", "version": "v1",
        "conditionId": "0x" + "a" * 64, "outcomes": ["Yes", "No"],
        "clobTokenIds": ["100", "101"], "events": [],
    }
    client = GammaClient(
        settings.gamma,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)),
    )
    with Ledger(settings.ledger_path) as ledger:
        try:
            load_runtime = LoadRuntime(settings, ledger)
            batches = []
            for _ in range(3):
                run_capture(CaptureRuntime(settings, client, ledger), "selected", market_ids=["1"])
                result = load_pending(load_runtime)
                assert result.pages_loaded == result.market_rows == 1
                assert len(result.batches_registered) == 1
                batches.extend(result.batches_registered)
                assert Container()[TraceLogContext] is caller
                assert caller.logging_enabled is True
                assert (tuple(caller.resolved_traces), tuple(caller.all_traces)) == caller_history
            assert len(set(batches)) == 3
            assert len(resource_runs) == 12
            assert load_pending(load_runtime).pages_loaded == 0
            assert len(resource_runs) == 12
            assert Container()[TraceLogContext] is caller
            assert (tuple(caller.resolved_traces), tuple(caller.all_traces)) == caller_history
            pipeline = load_runtime.pipeline
        finally:
            client.close()
assert pipeline.config.enable_runtime_trace is False
run_context = Container()[PluggableRunContext]
context = run_context.context
assert len(run_context.providers.providers) == 1
assert isinstance(run_context.providers.providers[0], DictionaryProvider)
for name in ("enable_airflow_secrets", "enable_google_secrets", "enable_aws_secrets"):
    assert run_context.providers.providers[0].get_value(name, bool, None, "providers")[0] is False
assert context.runtime_config.dlthub_telemetry is False
assert context.runtime_config.sentry_dsn is None
assert context.runtime_config.dlthub_dsn is None
for name in ("run_dir", "local_dir", "global_dir", "settings_dir", "data_dir"):
    assert Path(getattr(context, name)).is_relative_to(settings.dlt_pipelines_dir), name
assert not list(root.rglob(".anonymous_id"))
assert anon_tracker._THREAD_POOL is None
assert calls == []
print("confined-runtime-ok")
"""
    env = dict(os.environ)
    env.update(
        {
            "DLT_PROJECT_DIR": str(ambient),
            "DLT_LOCAL_DIR": str(ambient),
            "DLT_DATA_DIR": str(ambient),
            "DLT_CONFIG_FOLDER": ".dlt",
            "RUNTIME__DLTHUB_TELEMETRY": "true",
            "RUNTIME__DLTHUB_DSN": "synthetic-unused-platform",
            "RUNTIME__SENTRY_DSN": "https://synthetic@example.invalid/1",
            "PROVIDERS__ENABLE_AIRFLOW_SECRETS": "true",
            "PROVIDERS__ENABLE_GOOGLE_SECRETS": "true",
            "PROVIDERS__ENABLE_AWS_SECRETS": "true",
        }
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path / "operator"), entrypoint],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "confined-runtime-ok" in result.stdout
    assert sorted(str(path.relative_to(ambient)) for path in ambient.rglob("*")) == [
        ".dlt",
        ".dlt/config.toml",
    ]


def test_confined_runtime_stops_an_already_started_tracker(tmp_path, monkeypatch):
    class Pool:
        stopped = False

        def stop(self, wait):
            self.stopped = wait

    telemetry.stop_telemetry()
    pool = Pool()
    monkeypatch.setattr(anon_tracker, "_THREAD_POOL", pool)
    monkeypatch.setattr(anon_tracker, "_ANON_TRACKER_ENDPOINT", "https://example.invalid")
    monkeypatch.setattr(telemetry, "_TELEMETRY_STARTED", True)
    with confined_runtime(tmp_path / "dlt"):
        assert pool.stopped is True
        assert anon_tracker._THREAD_POOL is None
        assert anon_tracker._ANON_TRACKER_ENDPOINT is None
        assert Container()[PluggableRunContext].context.runtime_config.dlthub_telemetry is False


def test_two_roots_cannot_replace_context_during_an_active_load(tmp_path):
    first_entered = threading.Event()
    second_attempted = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()

    def first():
        with confined_runtime(tmp_path / "first"):
            first_entered.set()
            assert second_attempted.wait(5)
            assert not second_entered.is_set()
            assert release_first.wait(5)
            assert Container()[PluggableRunContext].context.pipelines_dir == tmp_path / "first"

    def second():
        assert first_entered.wait(5)
        second_attempted.set()
        with confined_runtime(tmp_path / "second"):
            second_entered.set()
            assert Container()[PluggableRunContext].context.pipelines_dir == tmp_path / "second"

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first)
        second_future = executor.submit(second)
        assert second_attempted.wait(5)
        assert not second_entered.is_set()
        release_first.set()
        first_future.result(timeout=5)
        second_future.result(timeout=5)
    assert second_entered.is_set()


def test_exercised_dlt_worker_and_queue_config_is_single_worker(tmp_path, monkeypatch):
    effective = {"extract": [], "normalize": [], "load": []}
    original_futures = FuturesPool.__init__
    original_normalize = Normalize.__init__
    original_load = Load.__init__

    def futures(self, *args, **kwargs):
        original_futures(self, *args, **kwargs)
        effective["extract"].append((self.workers, self.max_parallel_items))

    def normalize(self, *args, **kwargs):
        original_normalize(self, *args, **kwargs)
        effective["normalize"].append((self.config.workers, self.config.pool_type))

    def load(self, *args, **kwargs):
        original_load(self, *args, **kwargs)
        effective["load"].append((self.config.workers, self.config.pool_type))

    monkeypatch.setattr(FuturesPool, "__init__", futures)
    monkeypatch.setattr(Normalize, "__init__", normalize)
    monkeypatch.setattr(Load, "__init__", load)
    monkeypatch.setenv("EXTRACT__WORKERS", "99")
    monkeypatch.setenv("EXTRACT__MAX_PARALLEL_ITEMS", "99")
    monkeypatch.setenv("NORMALIZE__WORKERS", "99")
    monkeypatch.setenv("LOAD__WORKERS", "99")
    settings = Settings(tmp_path)
    pipeline = make_pipeline(settings.warehouse_path, settings.dlt_pipelines_dir, settings.load)
    pipeline.run(event_resource([], materialize_only=True))
    assert effective["extract"] and set(effective["extract"]) == {(1, 1)}
    assert effective["normalize"] and set(effective["normalize"]) == {(1, "none")}
    assert effective["load"] and set(effective["load"]) == {(1, "none")}
