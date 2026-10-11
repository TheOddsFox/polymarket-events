"""Load reservation recovery preserves order, failure boundaries and released memory."""

import sys
import weakref
from dataclasses import replace
from types import SimpleNamespace

import duckdb
import pytest

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.load import runner
from oddsfox_catalogue.load.rows import RowSet


def pending_runtime(monkeypatch, tmp_path, count):
    pending = [{"page_id": index} for index in range(count)]
    registered = []
    settings = Settings(tmp_path)
    settings = replace(settings, load=replace(settings.load, max_pages_per_run=count))
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    ledger = SimpleNamespace(
        list_batches=lambda: [],
        pending_load_pages=lambda batch_id: list(pending),
    )
    runtime = runner.LoadRuntime(settings, ledger)
    monkeypatch.setattr(runner, "_reset_load_budget", lambda rt: None)
    monkeypatch.setattr(runner, "_preflight_load_bytes", lambda rt, size: None)
    monkeypatch.setattr(runner, "ensure_warehouse_contract", lambda *args, **kwargs: None)
    monkeypatch.setattr("oddsfox_catalogue.certification.mark_dirty", lambda *args: None)
    monkeypatch.setattr(runner, "enforce_storage_limits", lambda settings: None)
    monkeypatch.setattr(runner, "_register_complete_batches", lambda *args: registered.append(True))
    return runtime, pending, registered


@pytest.mark.parametrize("count", [1, 5, 200])
def test_adaptive_load_is_ordered_linear_and_releases_failed_rows(monkeypatch, tmp_path, count):
    runtime, pending, registered = pending_runtime(monkeypatch, tmp_path, count)
    attempts, completed, failed_rows = [], [], []

    def load_chunk(rt, pages, summary):
        assert sys.exception() is None, "child work must run after the parent handler exits"
        assert all(reference() is None for reference in failed_rows)
        ids = tuple(page["page_id"] for page in pages)
        attempts.append(ids)
        if len(pages) > 1:
            rows = RowSet(events=[{"retained_by_failed_traceback": ids}])
            failed_rows.append(weakref.ref(rows))
            raise runner.LoadReservationExceeded("estimated parent cannot fit")
        completed.extend(ids)
        pending.remove(pages[0])
        summary.pages_loaded += 1

    monkeypatch.setattr(runner, "_load_chunk", load_chunk)
    summary = runner._load_pending(runtime)

    assert completed == list(range(count))
    assert summary.pages_loaded == count and not pending
    assert len(attempts) == 2 * count - 1
    assert len(set(attempts)) == len(attempts)
    assert attempts[0] == tuple(range(count))
    if count > 1:
        assert attempts[1] == tuple(range(count // 2))
    assert all(reference() is None for reference in failed_rows)
    assert registered == [True]


@pytest.mark.parametrize("error_type", [RequestBudgetExceeded, runner.LoadBlocked, ValueError])
def test_adaptive_load_does_not_split_other_failures(monkeypatch, tmp_path, error_type):
    runtime, pending, registered = pending_runtime(monkeypatch, tmp_path, 5)
    calls = []
    failure = error_type("not a recoverable reservation")

    def load_chunk(rt, pages, summary):
        calls.append([page["page_id"] for page in pages])
        raise failure

    monkeypatch.setattr(runner, "_load_chunk", load_chunk)
    with pytest.raises(error_type) as caught:
        runner._load_pending(runtime)
    assert caught.value is failure
    assert calls == [list(range(5))]
    assert len(pending) == 5 and not registered


def test_singleton_reservation_stops_after_committed_prefix_and_resumes(monkeypatch, tmp_path):
    runtime, pending, registered = pending_runtime(monkeypatch, tmp_path, 5)
    committed = []
    failure = runner.LoadReservationExceeded("singleton cannot fit")
    block_singleton = True

    def load_chunk(rt, pages, summary):
        if len(pages) > 1:
            raise runner.LoadReservationExceeded("split parent")
        if pages[0]["page_id"] == 2 and block_singleton:
            raise failure
        committed.append(pages[0]["page_id"])
        pending.remove(pages[0])
        summary.pages_loaded += 1

    monkeypatch.setattr(runner, "_load_chunk", load_chunk)
    with pytest.raises(runner.LoadReservationExceeded) as caught:
        runner._load_pending(runtime)
    assert caught.value is failure
    assert committed == [0, 1]
    assert [page["page_id"] for page in pending] == [2, 3, 4]
    assert not registered
    block_singleton = False
    resumed = runner._load_pending(runtime)
    assert resumed.pages_loaded == 3
    assert committed == [0, 1, 2, 3, 4]
    assert not pending and registered == [True]


@pytest.mark.parametrize("allowance", ["retained", "temporary"])
def test_only_estimated_allowance_failures_are_adaptive(monkeypatch, tmp_path, allowance):
    settings = Settings(tmp_path)
    limit = (
        settings.capture.max_retained_bytes
        if allowance == "retained"
        else settings.capture.max_temp_bytes
    )
    runtime = runner.LoadRuntime(settings, SimpleNamespace())
    runtime.storage_snapshot = (limit, 0) if allowance == "retained" else (0, limit)
    with pytest.raises(runner.LoadReservationExceeded) as estimated:
        runner._preflight_load_bytes(runtime, 1)
    assert type(estimated.value) is runner.LoadReservationExceeded
    monkeypatch.setattr(
        runner, "retained_bytes", lambda settings: limit + 1 if allowance == "retained" else 0
    )
    monkeypatch.setattr(
        runner, "temporary_bytes", lambda settings: limit + 1 if allowance == "temporary" else 0
    )
    with pytest.raises(RequestBudgetExceeded) as actual:
        runner._reset_load_budget(runtime)
    assert type(actual.value) is RequestBudgetExceeded


@pytest.mark.parametrize("failure", [None, RuntimeError, KeyboardInterrupt])
def test_finite_load_closes_owner_and_clears_runtime_on_every_exit(monkeypatch, tmp_path, failure):
    runtime, pending, registered = pending_runtime(monkeypatch, tmp_path, 1)
    owners, order = [], []
    monkeypatch.setattr(
        runner, "ensure_warehouse_contract", lambda *a, **k: order.append("version")
    )
    monkeypatch.setattr(
        "oddsfox_catalogue.certification.mark_dirty", lambda *a: order.append("dirty")
    )

    def load_chunk(rt, pages, summary):
        assert order == ["version", "dirty"]
        assert rt.connection is not None
        owners.append(rt.connection)
        rt.connection.execute("CREATE TABLE committed (id INTEGER)")
        rt.connection.execute("INSERT INTO committed VALUES (1)")
        rt.pipeline = object()
        if failure is not None:
            raise failure("synthetic finite-load exit")
        pending.clear()
        summary.pages_loaded = 1

    monkeypatch.setattr(runner, "_load_chunk", load_chunk)
    if failure is None:
        assert runner._load_pending(runtime).pages_loaded == 1
        assert registered == [True]
    else:
        with pytest.raises(failure, match="synthetic finite-load exit"):
            runner._load_pending(runtime)
        assert not registered
    assert runtime.connection is runtime.pipeline is None
    with pytest.raises(duckdb.ConnectionException, match="already closed"):
        owners[0].execute("SELECT 1")
    with duckdb.connect(str(runtime.settings.warehouse_path), read_only=True) as connection:
        assert connection.execute("SELECT * FROM committed").fetchall() == [(1,)]


def test_failed_version_gate_never_opens_load_owner(monkeypatch, tmp_path):
    runtime, _, _ = pending_runtime(monkeypatch, tmp_path, 1)

    def invalid(*args, **kwargs):
        raise ValueError("incompatible warehouse")

    def forbidden(*args, **kwargs):
        pytest.fail("version failure must precede dirty marking and connection acquisition")

    monkeypatch.setattr(runner, "ensure_warehouse_contract", invalid)
    monkeypatch.setattr("oddsfox_catalogue.certification.mark_dirty", forbidden)
    monkeypatch.setattr(runner, "bounded_connection", forbidden)
    runtime.pipeline = object()
    with pytest.raises(ValueError, match="incompatible warehouse"):
        runner._load_pending(runtime)
    assert runtime.connection is runtime.pipeline is None
