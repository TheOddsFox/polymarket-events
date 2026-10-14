"""dlt loading cannot enable hidden telemetry or use ambient runtime directories."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import duckdb
import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.configuration.utils import ResolvedValueTrace, TraceLogContext
from dlt.common.runtime import anon_tracker, telemetry
from dlt.destinations.impl.duckdb.configuration import DuckDbConnectionPool
from dlt.extract.concurrency import FuturesPool
from dlt.load import Load
from dlt.normalize import Normalize

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runtime import CatalogueRunContext, confined_runtime
from oddsfox_catalogue.load.source import event_resource, make_pipeline, market_resource


def spill_pinned_owner(settings):
    """An external connection has the same fixed spill identity as a loader-owned one."""
    from oddsfox_catalogue.limits import remaining_temp_bytes

    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    settings.temporary_dir.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(
        str(settings.warehouse_path),
        config={
            "temp_directory": str(settings.temporary_dir),
            "memory_limit": settings.load.duckdb_memory_limit,
            "threads": settings.load.duckdb_threads,
            "max_temp_directory_size": f"{remaining_temp_bytes(settings)}B",
        },
    )


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
    (ambient / ".dlt" / "config.toml").write_text(
        '[runtime]\ndlthub_telemetry = true\n[data_writer]\ncompression = "gzip"\n'
    )
    code = r"""
import pickle
import sys
from pathlib import Path
from io import BytesIO
import requests
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import DictionaryProvider
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.utils import ResolvedValueTrace, TraceLogContext
from dlt.common.data_writers.writers import ParquetDataWriter
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
    import duckdb

    original_run = Pipeline.run
    resource_runs = []
    resource_pipelines = []
    resource_owners = []
    invocation_ranges = []
    def checked_run(self, *args, **kwargs):
        traces = Container()[TraceLogContext]
        assert traces is not caller
        assert traces.logging_enabled is False
        assert traces.resolved_traces == traces.all_traces == []
        result = original_run(self, *args, **kwargs)
        assert Container()[TraceLogContext] is traces
        assert traces.resolved_traces == traces.all_traces == []
        resource_runs.append(traces)
        resource_pipelines.append(self)
        resource_owners.append(kwargs['destination'].config_params['credentials']._external_conn)
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
                first_run = len(resource_runs)
                run_capture(CaptureRuntime(settings, client, ledger), "selected", market_ids=["1"])
                result = load_pending(load_runtime)
                assert result.pages_loaded == result.market_rows == 1
                assert len(result.batches_registered) == 1
                batches.extend(result.batches_registered)
                assert load_runtime.pipeline is load_runtime.connection is None
                with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
                    assert connection.execute('SELECT count(*) FROM bronze.batch_registry').fetchone()[0] == len(batches)
                assert Container()[TraceLogContext] is caller
                assert caller.logging_enabled is True
                assert (tuple(caller.resolved_traces), tuple(caller.all_traces)) == caller_history
                invocation_ranges.append((first_run, len(resource_runs)))
            assert len(set(batches)) == 3
            assert len(resource_runs) == 10
            assert load_pending(load_runtime).pages_loaded == 0
            assert len(resource_runs) == 10
            assert Container()[TraceLogContext] is caller
            assert (tuple(caller.resolved_traces), tuple(caller.all_traces)) == caller_history
            pipeline = resource_pipelines[-1]
            assert len({id(owner) for owner in resource_owners}) == 3
            assert all(len({id(owner) for owner in resource_owners[start:end]}) == 1 for start,end in invocation_ranges)
            for owner in resource_owners:
                try:
                    owner.execute('SELECT 1')
                except duckdb.ConnectionException:
                    pass
                else:
                    raise AssertionError('owner survived finite invocation')
        finally:
            client.close()
assert pipeline.config.enable_runtime_trace is False
run_context = Container()[PluggableRunContext]
context = run_context.context
assert len(run_context.providers.providers) == 1
assert isinstance(run_context.providers.providers[0], DictionaryProvider)
assert run_context.providers.providers[0].get_value("compression", str, None, "data_writer")[0] == "zstd"
assert ParquetDataWriter(BytesIO()).parquet_format.compression == "zstd"
restored = PluggableRunContext(pickle.loads(pickle.dumps(context)))
with Container().injectable_context(restored, lock_context_on_yield=True):
    assert ParquetDataWriter(BytesIO()).parquet_format.compression == "zstd"
    assert Container()[PluggableRunContext].context.pipelines_dir == settings.dlt_pipelines_dir
assert Container()[PluggableRunContext] is run_context
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
            "DATA_WRITER__COMPRESSION": "snappy",
            "NORMALIZE__DATA_WRITER__COMPRESSION": "gzip",
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


def test_external_dlt_pools_preserve_owner_and_refresh_real_connection_limits(
    tmp_path, monkeypatch
):
    from oddsfox_catalogue.limits import remaining_temp_bytes
    from oddsfox_catalogue.load.runner import LoadRuntime, _run_resource

    settings = Settings(tmp_path)
    settings = replace(
        settings,
        load=replace(settings.load, duckdb_memory_limit="64MiB", duckdb_threads=1),
        capture=replace(settings.capture, max_temp_bytes=16 * 1024**2),
    )
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    owner = spill_pinned_owner(settings)
    pools, observed = [], []
    original_borrow = DuckDbConnectionPool.borrow_conn

    def borrow(self, *args, **kwargs):
        connection = original_borrow(self, *args, **kwargs)
        assert self._conn is owner and self._conn_owner is False
        pools.append(self)
        observed.append(
            (
                dict(self.credentials.global_config),
                owner.execute(
                    "SELECT current_setting('memory_limit'),current_setting('threads'),"
                    "current_setting('max_temp_directory_size')"
                ).fetchone(),
            )
        )
        return connection

    monkeypatch.setattr(DuckDbConnectionPool, "borrow_conn", borrow)
    try:
        with confined_runtime(settings.dlt_pipelines_dir):
            runtime = LoadRuntime(settings, SimpleNamespace(), connection=owner)
            _run_resource(runtime, event_resource([], materialize_only=True))
            pipeline = runtime.pipeline
            first = observed[-1]
            (settings.dlt_pipelines_dir / "retained-working-bytes").write_bytes(b"x" * 1024**2)
            expected = f"{remaining_temp_bytes(settings)}B"
            _run_resource(runtime, market_resource([], materialize_only=True))
            assert runtime.pipeline is pipeline
            second = observed[-1]
            assert second[0]["max_temp_directory_size"] == expected
            assert int(expected[:-1]) < int(first[0]["max_temp_directory_size"][:-1])
            assert second[1][2] != first[1][2]
            assert all(
                config["memory_limit"] == "64MiB" and config["threads"] == 1
                for config, _ in observed
            )
            assert all("temp_directory" not in config for config, _ in observed)
            assert all(actual[:2] == ("64.0 MiB", 1) for _, actual in observed)
            assert all(pool._conn_borrows == 0 for pool in pools)
            assert owner.execute("SELECT 1").fetchone() == (1,)
    finally:
        owner.close()
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM bronze.market_observations").fetchone() == (
            0,
        )


@pytest.mark.parametrize("failure", [None, KeyboardInterrupt])
def test_borrowed_semantic_queries_account_storage_and_close_only_fresh_cursor(
    tmp_path, monkeypatch, failure
):
    from oddsfox_catalogue import semantics
    from oddsfox_catalogue.limits import remaining_temp_bytes

    settings = Settings(tmp_path)
    settings = replace(settings, capture=replace(settings.capture, max_temp_bytes=16 * 1024**2))
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    owner = spill_pinned_owner(settings)
    accounted, cursors = [], []
    actual_enforce = semantics.enforce_storage_limits

    def enforce(selected):
        assert selected is settings
        accounted.append(True)
        actual_enforce(selected)

    monkeypatch.setattr(semantics, "enforce_storage_limits", enforce)
    try:
        with semantics.bounded_connection(settings, connection=owner) as first:
            assert first is not owner
            first_setting = first.execute(
                "SELECT current_setting('max_temp_directory_size')"
            ).fetchone()[0]
        (settings.temporary_dir / "additional-spill").write_bytes(b"x" * 1024**2)
        expected = remaining_temp_bytes(settings)

        def query():
            with semantics.bounded_connection(settings, connection=owner) as second:
                assert second is not owner and second is not first
                cursors.append(second)
                value, timezone = second.execute(
                    "SELECT current_setting('max_temp_directory_size'),current_setting('TimeZone')"
                ).fetchone()
                assert value != first_setting and timezone == "UTC"
                assert expected < settings.capture.max_temp_bytes
                if failure is not None:
                    raise failure("synthetic query interruption")

        if failure is None:
            query()
        else:
            with pytest.raises(failure, match="synthetic query interruption"):
                query()
        assert accounted == [True] * 4
        for cursor in [first, *cursors]:
            with pytest.raises(duckdb.ConnectionException, match="already closed"):
                cursor.execute("SELECT 1")
        assert owner.execute("SELECT 1").fetchone() == (1,)
    finally:
        owner.close()


@pytest.mark.parametrize("phase", ["before", "after"])
def test_borrowed_query_fails_closed_when_retained_allowance_is_crossed(tmp_path, phase):
    from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
    from oddsfox_catalogue.limits import retained_bytes
    from oddsfox_catalogue.semantics import bounded_connection

    settings = Settings(tmp_path)
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    owner = spill_pinned_owner(settings)
    settings = replace(
        settings, capture=replace(settings.capture, max_retained_bytes=retained_bytes(settings) + 2)
    )
    yielded = []
    try:
        if phase == "before":
            (tmp_path / "extra-retained").write_bytes(b"xxx")
        with (
            pytest.raises(RequestBudgetExceeded, match="retained"),
            bounded_connection(settings, connection=owner) as cursor,
        ):
            yielded.append(cursor)
            (tmp_path / "extra-retained").write_bytes(b"xxx")
        assert bool(yielded) is (phase == "after")
        for cursor in yielded:
            with pytest.raises(duckdb.ConnectionException, match="already closed"):
                cursor.execute("SELECT 1")
        assert owner.execute("SELECT 1").fetchone() == (1,)
    finally:
        owner.close()


def test_actual_spill_allows_borrowed_queries_and_next_dlt_resources(tmp_path, monkeypatch):
    from oddsfox_catalogue.limits import remaining_temp_bytes, retained_bytes
    from oddsfox_catalogue.load.runner import LoadRuntime, _run_resource
    from oddsfox_catalogue.semantics import bounded_connection

    def offline(*args, **kwargs):
        pytest.fail("spill regression must remain offline")

    monkeypatch.setattr(socket.socket, "connect", offline)
    monkeypatch.setattr(socket, "create_connection", offline)
    settings = Settings(tmp_path)
    settings = replace(
        settings,
        load=replace(settings.load, duckdb_memory_limit="64MiB", duckdb_threads=1),
        capture=replace(
            settings.capture, max_retained_bytes=64 * 1024**2, max_temp_bytes=48 * 1024**2
        ),
    )
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    observed = []
    original_borrow = DuckDbConnectionPool.borrow_conn

    def borrow(self, *args, **kwargs):
        connection = original_borrow(self, *args, **kwargs)
        assert self._conn_owner is False
        config = dict(self.credentials.global_config)
        assert "temp_directory" not in config
        assert config["memory_limit"] == "16MiB" and config["threads"] == 1
        assert self._conn.execute(
            "SELECT current_setting('memory_limit'),current_setting('threads')"
        ).fetchone() == ("16.0 MiB", 1)
        observed.append(config)
        return connection

    monkeypatch.setattr(DuckDbConnectionPool, "borrow_conn", borrow)
    with bounded_connection(settings, read_only=False) as owner:
        assert owner.execute("SELECT current_setting('temp_directory')").fetchone() == (
            str(settings.temporary_dir),
        )
        owner.execute("SET memory_limit='16MiB'")
        settings = replace(settings, load=replace(settings.load, duckdb_memory_limit="16MiB"))
        with owner.cursor() as sort:
            sort.execute(
                "SELECT i,md5(i::VARCHAR) AS payload FROM range(500000) t(i) ORDER BY payload,i"
            )
            first = sort.fetchone()
            assert first is not None
            spilled_bytes, files = owner.execute(
                "SELECT sum(size),count(*) FROM duckdb_temporary_files()"
            ).fetchone()
            assert spilled_bytes > 0 and files > 0, "the regression requires real temporary files"
            assert retained_bytes(settings) <= settings.capture.max_retained_bytes
            count = 1
            previous = (first[1], first[0])
            while rows := sort.fetchmany(8192):
                assert len(rows) <= 8192
                assert previous <= (rows[0][1], rows[0][0])
                previous = (rows[-1][1], rows[-1][0])
                count += len(rows)
            assert count == 500000
        with bounded_connection(settings, connection=owner) as borrowed:
            assert borrowed.execute("SELECT 1").fetchone() == (1,)
        with confined_runtime(settings.dlt_pipelines_dir):
            runtime = LoadRuntime(settings, SimpleNamespace(), connection=owner)
            _run_resource(runtime, event_resource([], materialize_only=True))
            first_cap = observed[-1]["max_temp_directory_size"]
            (settings.dlt_pipelines_dir / "additional-working-bytes").write_bytes(b"x" * 1024**2)
            expected = f"{remaining_temp_bytes(settings)}B"
            _run_resource(runtime, market_resource([], materialize_only=True))
            assert observed[-1]["max_temp_directory_size"] == expected
            assert int(expected[:-1]) < int(first_cap[:-1])
            with bounded_connection(settings, connection=owner) as borrowed:
                assert borrowed.execute(
                    "SELECT count(*) FROM bronze.market_observations"
                ).fetchone() == (0,)
        assert owner.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.parametrize("entrypoint", ["destination", "query"])
def test_external_spill_directory_mismatch_rejects_before_mutation_or_consumption(
    tmp_path, monkeypatch, entrypoint
):
    from oddsfox_catalogue.load.source import make_destination
    from oddsfox_catalogue.semantics import bounded_connection

    settings = Settings(tmp_path)
    settings = replace(settings, load=replace(settings.load, duckdb_memory_limit="64MiB"))
    owner = spill_pinned_owner(settings)
    mismatch = replace(
        settings,
        paths=replace(settings.paths, state_dir="different-state"),
        load=replace(settings.load, duckdb_memory_limit="16MiB", duckdb_threads=1),
    )
    query = "SELECT current_setting('temp_directory'),current_setting('memory_limit'),current_setting('threads'),current_setting('max_temp_directory_size')"
    before = owner.execute(query).fetchone()
    yielded = []

    def forbidden(*args, **kwargs):
        pytest.fail("an incompatible external connection must not reach the dlt factory")

    monkeypatch.setattr("oddsfox_catalogue.load.source.dlt.destinations.duckdb", forbidden)
    try:
        with pytest.raises(ValueError, match="incompatible spill directory"):
            if entrypoint == "destination":
                make_destination(
                    settings.warehouse_path,
                    mismatch.load,
                    temp_directory=mismatch.temporary_dir,
                    max_temp_bytes=16 * 1024**2,
                    connection=owner,
                )
            else:
                with bounded_connection(mismatch, connection=owner):
                    yielded.append(True)
        assert not yielded
        assert owner.execute(query).fetchone() == before
        assert owner.execute("SELECT 1").fetchone() == (1,)
    finally:
        owner.close()


@pytest.mark.parametrize("entrypoint", ["destination", "query"])
def test_external_spill_directory_accepts_lexically_equivalent_path(tmp_path, entrypoint):
    from oddsfox_catalogue.load.source import make_destination
    from oddsfox_catalogue.semantics import bounded_connection

    settings = Settings(tmp_path)
    settings = replace(settings, load=replace(settings.load, duckdb_memory_limit="64MiB"))
    owner = spill_pinned_owner(settings)
    equivalent = replace(settings, paths=replace(settings.paths, state_dir=".state/alias/.."))
    try:
        initial = make_destination(
            settings.warehouse_path,
            settings.load,
            temp_directory=settings.temporary_dir,
            max_temp_bytes=16 * 1024**2,
        )
        assert initial.config_params["credentials"]["global_config"]["temp_directory"] == str(
            settings.temporary_dir
        )
        if entrypoint == "destination":
            destination = make_destination(
                settings.warehouse_path,
                equivalent.load,
                temp_directory=equivalent.temporary_dir,
                max_temp_bytes=16 * 1024**2,
                connection=owner,
            )
            assert "temp_directory" not in destination.config_params["credentials"].global_config
        else:
            with bounded_connection(equivalent, connection=owner) as borrowed:
                assert borrowed.execute("SELECT 1").fetchone() == (1,)
        assert owner.execute("SELECT current_setting('temp_directory')").fetchone() == (
            str(settings.temporary_dir),
        )
    finally:
        owner.close()
