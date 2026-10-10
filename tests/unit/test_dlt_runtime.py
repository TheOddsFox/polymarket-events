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
from dlt.common.runtime import anon_tracker, telemetry
from dlt.extract.concurrency import FuturesPool
from dlt.load import Load
from dlt.normalize import Normalize

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runtime import confined_runtime
from oddsfox_catalogue.load.source import event_resource, make_pipeline


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
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
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
if sys.argv[2] == "pipeline":
    pipeline = make_pipeline(
        settings.warehouse_path, settings.dlt_pipelines_dir, settings.load,
        temp_directory=settings.temporary_dir, max_temp_bytes=16 * 1024**2,
    )
    pipeline.run(event_resource([], materialize_only=True))
else:
    import httpx
    from oddsfox_catalogue.capture.ledger import Ledger
    from oddsfox_catalogue.capture.runner import CaptureRuntime, run_capture
    from oddsfox_catalogue.gamma.http import GammaClient
    from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
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
            run_capture(CaptureRuntime(settings, client, ledger), "selected", market_ids=["1"])
            load_runtime = LoadRuntime(settings, ledger)
            result = load_pending(load_runtime)
            assert len(result.batches_registered) == 1
            pipeline = load_runtime.pipeline
        finally:
            client.close()
assert pipeline.config.enable_runtime_trace is False
context = Container()[PluggableRunContext].context
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
