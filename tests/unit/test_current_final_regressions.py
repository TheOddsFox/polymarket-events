"""Final publication and bounded parent-repair failure regressions."""

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import httpx
import pytest

from oddsfox_catalogue import (
    current_backup,
    current_publish,
    current_reconcile,
    current_runtime,
    current_store,
    current_sync,
)
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import GammaClient, RequestBudgetExceeded
from oddsfox_catalogue.signals import SignalHold, Terminated


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(str(settings.store_path))
    current_store.initialize(conn)
    current_store.set_state(
        conn,
        "bulk_policy",
        {
            **current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        },
    )
    yield settings, conn
    conn.close()


def apply(conn, payload, lane="events", day=1):
    return current_store.apply(
        conn,
        project_response(
            payload,
            lane,
            received_at=f"2026-10-{day:02d}T00:00:00Z",
            response_id=str(day),
        ),
    )


def test_three_generations_retain_exactly_current_and_previous(catalogue):
    settings, conn = catalogue
    releases = []
    for day in range(1, 4):
        apply(conn, [{"id": "1", "title": str(day), "markets": []}], day=day)
        releases.append(current_publish.publish(settings, conn))
    pointer = json.loads((settings.published_dir / "current.json").read_text())
    assert pointer["release_id"] == releases[2]["release_id"]
    assert pointer["previous_release_id"] == releases[1]["release_id"]
    assert {p.name for p in (settings.published_dir / "releases").iterdir()} == {
        release["release_id"] for release in releases[1:]
    }
    for release in releases[1:]:
        assert current_publish.verify_release(release["path"])["valid"]
    assert current_publish.publish(settings, conn)["noop"] is True
    assert json.loads((settings.published_dir / "current.json").read_text()) == pointer


@pytest.mark.parametrize("artifact", ["release", "stage"])
@pytest.mark.parametrize("unsafe", ["symlink", "fifo"])
def test_cleanup_rejects_unsafe_descendants_before_deleting(catalogue, artifact, unsafe):
    settings, _ = catalogue
    generated = (
        settings.published_dir / "releases" / ("a" * 32)
        if artifact == "release"
        else settings.published_dir / (".staging-" + "a" * 32)
    )
    nested = generated / "nested"
    nested.mkdir(parents=True)
    sentinel = settings.root.parent / "keep.txt"
    sentinel.write_text("keep")
    dangerous = nested / "unsafe"
    if unsafe == "symlink":
        dangerous.symlink_to(sentinel)
    else:
        os.mkfifo(dangerous)
    with pytest.raises(RequestBudgetExceeded, match="symlink or special file"):
        current_publish._cleanup(settings, set())
    assert generated.is_dir()
    assert dangerous.lstat()
    assert sentinel.read_text() == "keep"


@pytest.mark.parametrize(
    "membership", ["missing", "null", "truncated", "empty", "complete", "omitted"]
)
def test_parent_repair_preserves_unknown_and_applies_explicit_membership(catalogue, membership):
    settings, conn = catalogue
    apply(conn, [{"id": "1", "closed": True, "markets": [{"id": "2"}]}])
    apply(conn, [{"id": "3", "events": [{"id": "1"}]}], "markets", day=2)
    assert conn.execute("SELECT event_id FROM _parent_repairs").fetchall() == [("1",)]
    payload = {
        "missing": [{"id": "1"}],
        "null": [{"id": "1", "markets": None}],
        "truncated": [{"id": "1", "markets": [], "hasMore": True}],
        "empty": [{"id": "1", "markets": []}],
        "complete": [{"id": "1", "markets": [{"id": "3"}]}],
        "omitted": [],
    }[membership]
    requests = []
    with GammaClient(
        settings.gamma,
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(200, json=payload)
        ),
    ) as client:
        result = current_reconcile.repair_pending(
            settings, conn, client, deadline=time.monotonic() + 30
        )
        assert result == {"responses": 1, "requests": 1, "records": len(payload)}
        assert current_reconcile.repair_pending(
            settings, conn, client, deadline=time.monotonic() + 30
        ) == {"responses": 0}
    assert len(requests) == 1
    assert requests[0].url.path == "/events"
    assert requests[0].url.params.get_list("id") == ["1"]
    expected = [] if membership == "empty" else [("3",)] if membership == "complete" else [("2",)]
    assert conn.execute("SELECT market_id FROM event_markets").fetchall() == expected
    assert conn.execute("SELECT market_id FROM markets ORDER BY market_id").fetchall() == [
        ("2",),
        ("3",),
    ]
    resolved = membership in {"empty", "complete"}
    assert conn.execute("SELECT event_id FROM _parent_repairs").fetchall() == (
        [] if resolved else [("1",)]
    )
    if membership != "omitted":
        assert conn.execute("SELECT unresolved FROM _membership").fetchone() == (not resolved,)
    assert current_store.get_state(conn, "fetch_pending") is None


def test_parent_repair_has_one_hundred_id_unit_and_omission_retry_fairness(catalogue):
    settings, conn = catalogue
    apply(conn, [{"id": "999", "events": [{"id": str(i)} for i in range(1, 102)]}], "markets")
    calls = []
    with GammaClient(
        settings.gamma,
        transport=httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(200, json=[])),
    ) as client:
        for expected in (100, 1):
            assert current_reconcile.repair_pending(
                settings, conn, client, deadline=time.monotonic() + 30
            ) == {"responses": 1, "requests": 1, "records": 0}
            assert len(calls[-1].url.params.get_list("id")) == expected
        assert current_reconcile.repair_pending(
            settings, conn, client, deadline=time.monotonic() + 30
        ) == {"responses": 0}
    first, second = (set(call.url.params.get_list("id")) for call in calls)
    assert first.isdisjoint(second)
    assert first | second == {str(i) for i in range(1, 102)}
    assert conn.execute("SELECT count(*) FROM _parent_repairs").fetchone() == (101,)


@pytest.mark.parametrize("previous_seconds,reserve", [(0, 1), (20, 30)])
def test_parent_repair_stops_at_publication_reserve_boundary(
    catalogue, monkeypatch, previous_seconds, reserve
):
    settings, conn = catalogue
    apply(conn, [{"id": "2", "events": [{"id": "1"}]}], "markets")
    current_store.set_state(conn, "previous_publish_seconds", previous_seconds)
    monkeypatch.setattr(current_reconcile.time, "monotonic", lambda: 100)
    assert current_reconcile.repair_pending(settings, conn, None, deadline=100 + reserve) == {
        "responses": 0
    }
    assert conn.execute("SELECT event_id FROM _parent_repairs").fetchall() == [("1",)]
    assert "_reconcile_attempts" not in {row[0] for row in conn.execute("SHOW TABLES").fetchall()}


@pytest.mark.parametrize("complete", [False, True])
@pytest.mark.parametrize("parent_first", [False, True])
def test_same_response_only_complete_events_clear_parent_nominations(complete, parent_first):
    parent = {"id": "1", **({"markets": []} if complete else {})}
    other = {"id": "9", "markets": [{"id": "2", "events": [{"id": "1"}, {"id": "7"}]}]}
    rows = project_response(
        [parent, other] if parent_first else [other, parent],
        "events",
        received_at="2026-10-14T00:00:00Z",
        response_id="parents",
    )
    assert rows.parent_event_ids == ({"7"} if complete else {"1", "7"})


@pytest.mark.parametrize("previous_seconds,expected_reserve", [(0, 1), (20, 30)])
def test_refresh_http_deadline_reserves_publication(
    catalogue, monkeypatch, previous_seconds, expected_reserve
):
    settings, conn = catalogue
    current_store.set_state(conn, "previous_publish_seconds", previous_seconds)
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))
    monkeypatch.setattr("oddsfox_catalogue.current_backup.prune", lambda *a, **kw: {})
    deadline = time.monotonic() + 60
    calls = []

    def client(gamma, **kwargs):
        assert kwargs["deadline"] == deadline - expected_reserve
        return GammaClient(
            gamma,
            **kwargs,
            transport=httpx.MockTransport(
                lambda r: calls.append(r) or httpx.Response(200, json=[])
            ),
        )

    monkeypatch.setattr(current_runtime, "GammaClient", client)
    result = current_runtime.execute(settings, "refresh", {}, deadline)
    assert result["status"] == "published"
    assert len(calls) == 2
    assert current_publish.verify_current(settings)["valid"]


@pytest.mark.parametrize("operation", ["import-release", "publish"])
@pytest.mark.parametrize("noop", [False, True])
def test_initial_publish_routes_record_actual_elapsed(catalogue, monkeypatch, operation, noop):
    settings, conn = catalogue
    clock = [100.0]
    published = {"noop": noop, "release_id": "initial"}
    monkeypatch.setattr(current_runtime.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))

    def publish(given_settings, given_conn, *, deadline):
        assert (given_settings, given_conn, deadline) == (settings, conn, 1000)
        clock[0] += 80
        return published

    def seed(*args, **kwargs):
        current_store.set_state(conn, "seed", {"frontiers": {"events": 0, "markets": 0}})
        return {"valid": True, "complete": True}

    monkeypatch.setattr(current_publish, "publish", publish)
    monkeypatch.setattr("oddsfox_catalogue.current_import.import_release", seed)
    result = current_runtime.execute(settings, operation, {}, 1000)
    assert current_store.get_state(conn, "previous_publish_seconds") == 80
    if operation == "import-release":
        assert result["publication"] is published
        assert result["publish_verify_s"] == 80
    else:
        assert result is published


def test_publish_noop_preserves_changed_cost_and_next_change_updates_it(catalogue, monkeypatch):
    settings, conn = catalogue
    clock = [100.0]
    results = [{"noop": False}, {"noop": True}, {"noop": False}]
    durations = [80, 0.125, 40]
    calls = iter(zip(results, durations, strict=True))
    monkeypatch.setattr(current_runtime.time, "monotonic", lambda: clock[0])

    def publish(given_settings, given_conn, *, deadline):
        assert (given_settings, given_conn, deadline) == (settings, conn, 1000)
        result, duration = next(calls)
        clock[0] += duration
        return result

    monkeypatch.setattr(current_publish, "publish", publish)
    for result, elapsed, recorded in zip(results, durations, [80, 80, 40], strict=True):
        actual, measured = current_runtime._publish(settings, conn, 1000)
        assert actual is result
        assert measured == elapsed
        assert current_store.get_state(conn, "previous_publish_seconds") == recorded


@pytest.mark.parametrize("prior", [None, 80])
def test_failed_publish_preserves_previous_cost(catalogue, monkeypatch, prior):
    settings, conn = catalogue
    if prior is not None:
        current_store.set_state(conn, "previous_publish_seconds", prior)
    clock = [100.0]
    monkeypatch.setattr(current_runtime.time, "monotonic", lambda: clock[0])

    def fail(*args, **kwargs):
        clock[0] += 50
        raise CurrentError("publication failed")

    monkeypatch.setattr(current_publish, "publish", fail)
    with pytest.raises(CurrentError, match="publication failed"):
        current_runtime._publish(settings, conn, 1000)
    assert current_store.get_state(conn, "previous_publish_seconds") == prior


def test_publish_measurement_after_noop_drives_acquisition_reserve(catalogue, monkeypatch):
    settings, conn = catalogue
    clock = [100.0]
    publications = iter([(False, 80), (True, 0.125)])
    monkeypatch.setattr(current_runtime.time, "monotonic", lambda: clock[0])

    def publish(*args, **kwargs):
        noop, duration = next(publications)
        clock[0] += duration
        return {"noop": noop}

    monkeypatch.setattr(current_publish, "publish", publish)
    current_runtime._publish(settings, conn, 1000)
    current_runtime._publish(settings, conn, 1000)
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))
    monkeypatch.setattr(current_backup, "prune", lambda *args, **kwargs: {})

    class AcquisitionStarted(Exception):
        pass

    def client(gamma, **kwargs):
        assert kwargs["deadline"] == 1000 - 1.5 * 80
        raise AcquisitionStarted

    monkeypatch.setattr(current_runtime, "GammaClient", client)
    with pytest.raises(AcquisitionStarted):
        current_runtime.execute(settings, "refresh", {}, 1000)
    assert current_store.get_state(conn, "previous_publish_seconds") == 80


def test_refresh_repairs_existing_parent_discovered_in_market_tail(catalogue, monkeypatch):
    settings, conn = catalogue
    apply(conn, [{"id": "1", "markets": [{"id": "2"}]}])
    current_store.set_state(
        conn,
        "discovery",
        {
            "version": 2,
            "next_lane": 0,
            "pending": None,
            "lanes": {
                lane: {"frontier": frontier, "highwater": None, "phase": "tail"}
                for lane, frontier in (("events", 1), ("markets", 2))
            },
        },
    )
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))
    monkeypatch.setattr("oddsfox_catalogue.current_backup.prune", lambda *a, **kw: {})
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path == "/markets":
            return httpx.Response(200, json=[{"id": "3", "events": [{"id": "1"}]}])
        if "id" not in request.url.params:
            return httpx.Response(200, json=[{"id": "1"}])
        assert request.url.params.get_list("id") == ["1"]
        return httpx.Response(200, json=[{"id": "1", "markets": [{"id": "3"}]}])

    monkeypatch.setattr(
        current_runtime,
        "GammaClient",
        lambda gamma, **kw: GammaClient(gamma, **kw, transport=httpx.MockTransport(handler)),
    )
    result = current_runtime.execute(settings, "refresh", {}, time.monotonic() + 30)
    assert result["parent_repairs"] == {"responses": 1, "requests": 1, "records": 1}
    assert [r.url.path for r in calls] == ["/events", "/markets", "/markets", "/events"]
    assert conn.execute("SELECT market_id FROM event_markets").fetchall() == [("3",)]
    assert conn.execute("SELECT market_id FROM markets ORDER BY market_id").fetchall() == [
        ("2",),
        ("3",),
    ]
    assert conn.execute("SELECT count(*) FROM _parent_repairs").fetchone() == (0,)
    assert current_publish.verify_current(settings)["counts"]["event_markets"] == 1


@pytest.mark.parametrize("operation", ["refresh", "reconcile"])
def test_run_reports_live_parent_queue_when_noop_publication_retains_old_backlog(
    catalogue, monkeypatch, operation
):
    settings, conn = catalogue
    apply(conn, [{"id": "3", "events": [{"id": "1"}, {"id": "7"}]}], "markets")
    publication = {"noop": True, "backlog": {"parent_event_repair_count": 0}}
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))
    monkeypatch.setattr(current_backup, "prune", lambda *args, **kwargs: {})
    monkeypatch.setattr(current_runtime, "_publish", lambda *args: (publication, 0))
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.path in {"/events", "/markets"}
        return httpx.Response(200, json=[])

    monkeypatch.setattr(
        current_runtime,
        "GammaClient",
        lambda gamma, **kwargs: GammaClient(
            gamma, **kwargs, transport=httpx.MockTransport(handler)
        ),
    )
    result = current_runtime.execute(settings, operation, {}, time.monotonic() + 30)
    status = current_runtime.execute(settings, "status", {}, time.monotonic() + 30)

    assert requests
    assert conn.execute("SELECT count(*) FROM _parent_repairs").fetchone() == (2,)
    assert result["parent_event_repair_count"] == 2
    assert result["publication"]["backlog"]["parent_event_repair_count"] == 0
    assert status["parent_event_repair_count"] == 2
    assert status["last_run"]["operation"] == operation
    assert status["last_run"]["parent_event_repair_count"] == 2
    assert status["last_run"]["publication"]["backlog"]["parent_event_repair_count"] == 0


def identity(key, tokens):
    return {
        "id": str(key),
        "version": "v1",
        "outcomes": ["Yes", "No"],
        "conditionId": "0x" + str(key) * 64,
        "clobTokenIds": tokens,
    }


def test_reconcile_natively_repairs_asset_conflicts(catalogue, monkeypatch):
    settings, conn = catalogue
    apply(conn, [identity(2, ["11", "12"]), identity(1, ["21", "22"])], "markets")
    changed = [identity(2, ["21", "22"]), identity(1, ["31", "32"])]
    apply(conn, changed, "markets", day=2)
    assert conn.execute("SELECT DISTINCT identity_error FROM markets").fetchall() == [
        ("asset_ownership_conflict",)
    ]
    monkeypatch.setattr(current_reconcile, "_now", lambda: datetime(2026, 12, 1, tzinfo=UTC))
    native = []

    def handler(request):
        if request.url.path == "/markets":
            return httpx.Response(200, json=changed)
        key = request.url.path.rsplit("/", 1)[-1]
        native.append(key)
        return httpx.Response(200, json=next(raw for raw in changed if raw["id"] == key))

    with GammaClient(settings.gamma, transport=httpx.MockTransport(handler)) as client:
        current_reconcile.reconcile(settings, conn, client, deadline=time.monotonic() + 30)
    assert native == ["1", "2"]
    assert conn.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (2,)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (4,)
    assert current_store.validate_store(conn)["valid"]


@pytest.mark.parametrize("native_status", [200, 404])
def test_invalid_identity_native_retry_is_bounded_and_retried_after_hour(
    catalogue, monkeypatch, native_status
):
    settings, conn = catalogue
    apply(conn, [{"id": "1", "markets": [{"id": "2"}]}])
    clock = [datetime(2026, 10, 14, tzinfo=UTC)]
    monkeypatch.setattr(current_reconcile, "_now", lambda: clock[0])
    native = []

    def handler(request):
        if request.url.path == "/events":
            return httpx.Response(200, json=[{"id": "1"}])
        if request.url.path == "/markets":
            return httpx.Response(200, json=[{"id": "2"}])
        assert request.url.path == "/markets/2"
        native.append(request)
        return httpx.Response(native_status, json={"id": "2"})

    with GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), now=lambda: clock[0]
    ) as client:
        current_reconcile.reconcile(settings, conn, client, deadline=time.monotonic() + 30)
        assert len(native) == 1
        clock[0] += timedelta(minutes=59)
        current_reconcile.reconcile(settings, conn, client, deadline=time.monotonic() + 30)
        assert len(native) == 1
        clock[0] += timedelta(minutes=1)
        current_reconcile.reconcile(settings, conn, client, deadline=time.monotonic() + 30)
        assert len(native) == 2
    assert conn.execute("SELECT market_id,usable FROM markets").fetchone() == ("2", False)
    assert conn.execute("SELECT market_id FROM event_markets").fetchall() == [("2",)]


def _stall_worker(*_args):
    while True:
        time.sleep(60)


@pytest.mark.parametrize("failure", [signal.SIGTERM, signal.SIGHUP, "exception"])
def test_supervisor_launch_interrupt_stops_worker_and_closes_pipes(tmp_path, monkeypatch, failure):
    context = multiprocessing.get_context("spawn")
    held = SignalHold()
    processes, pipes = [], []

    class Context:
        @staticmethod
        def Pipe(**kwargs):
            ends = context.Pipe(**kwargs)
            pipes.extend(ends)
            return ends

        class Process:
            def __init__(self, **kwargs):
                self.process = context.Process(**kwargs)
                processes.append(self.process)

            def start(self):
                self.process.start()
                if failure == "exception":
                    raise RuntimeError("launch failure")
                held.receive(failure)

            def __getattr__(self, name):
                return getattr(self.process, name)

    monkeypatch.setattr(current_runtime, "SIGNALS", held)
    monkeypatch.setattr(current_runtime, "_child", _stall_worker)
    monkeypatch.setattr(current_runtime.multiprocessing, "get_context", lambda _: Context())
    try:
        with pytest.raises(RuntimeError if failure == "exception" else Terminated):
            current_runtime.supervise(CurrentSettings(tmp_path / "root"), "refresh", {}, duration=5)
        assert len(processes) == 1
        assert not processes[0].is_alive()
        assert processes[0].exitcode is not None
        assert all(pipe.closed for pipe in pipes)
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        for pipe in pipes:
            pipe.close()


def test_backup_private_restore_and_old_release_share_remaining_spill_budget(
    catalogue, monkeypatch
):
    settings, conn = catalogue
    apply(conn, [{"id": "1", "title": "old"}])
    current_publish.publish(settings, conn)
    apply(conn, [{"id": "1", "title": "new"}], day=2)
    conn.close()
    backup = current_backup.create_backup(settings)
    validated, connections, borrowed = [], [], []
    validate = current_backup._validate
    connect = current_backup.connect_store
    verify = current_backup._verify_release_on_connection

    def validate_private(private, deadline, **kwargs):
        validated.append(private)
        assert str(kwargs["evidence_root"]) == backup["path"]
        return validate(private, deadline, **kwargs)

    @contextmanager
    def private_connection(private, **kwargs):
        with connect(private, **kwargs) as connection:
            connections.append((private, connection))
            yield connection

    def same_connection(path, connection, **kwargs):
        borrowed.append(connection)
        return verify(path, connection, **kwargs)

    monkeypatch.setattr(current_backup, "_validate", validate_private)
    monkeypatch.setattr(current_backup, "connect_store", private_connection)
    monkeypatch.setattr(current_backup, "_verify_release_on_connection", same_connection)
    monkeypatch.setattr(
        current_publish,
        "memory_connection",
        lambda *args: pytest.fail("old release opened a second spill allowance"),
    )
    verified = current_backup.verify_backup(backup["path"])
    assert verified["valid"]
    assert len(validated) == len(connections) == len(borrowed) == 1
    assert connections[0] == (validated[0], borrowed[0])
    private_bytes = sum(
        verified["files"][name]["bytes"] for name in ("catalogue.duckdb", "catalogue.json")
    )
    assert validated[0].limits.max_temp_bytes + private_bytes == CurrentLimits().max_temp_bytes


@pytest.mark.parametrize("excess", [0, 1])
def test_backup_copy_at_temporary_ceiling_is_rejected_before_copy(tmp_path, monkeypatch, excess):
    monkeypatch.setattr(
        current_backup,
        "_inventory",
        lambda *args: {
            "files": {"catalogue.duckdb": {"bytes": CurrentLimits().max_temp_bytes + excess}}
        },
    )
    monkeypatch.setattr(current_backup, "_copy", lambda *args: pytest.fail("over-budget copy"))
    with pytest.raises(CurrentError, match="temporary allowance exhausted"):
        current_backup.verify_backup(tmp_path)


@pytest.mark.parametrize("age", [None, 0, 86399, 86400, 86401])
def test_status_reports_backup_age_and_daily_recovery_point_boundary(catalogue, monkeypatch, age):
    settings, conn = catalogue
    now = datetime(2026, 10, 14, tzinfo=UTC)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(current_runtime, "datetime", FrozenDatetime)
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))
    if age is not None:
        current_store.set_state(
            conn,
            "last_backup",
            {"valid": True, "created_at": (now - timedelta(seconds=age)).isoformat()},
        )
    result = current_runtime.execute(settings, "status", {}, time.monotonic() + 30)
    assert result["recovery_scope"] == "local_only"
    assert result["disk_failure_protection"] is False
    assert result["backup_age_s"] == age
    assert result["recovery_point_overdue"] is (age is None or age > 86400)


@pytest.mark.parametrize("phase", ["before", "after"])
def test_abrupt_death_commits_rows_and_progress_together(tmp_path, phase):
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    with duckdb.connect(str(settings.store_path)) as conn:
        current_store.initialize(conn)
        apply(conn, [{"id": "1", "title": "old"}])
        current_store.set_state(conn, "checkpoint", "old")
    script = """
import os, sys
import duckdb
from oddsfox_catalogue import current_store
from oddsfox_catalogue.current_rows import project_response
conn = duckdb.connect(sys.argv[1])
original = current_store.set_state
def set_state(conn, key, value):
    original(conn, key, value)
    if key == 'checkpoint' and sys.argv[2] == 'before':
        os._exit(79)
current_store.set_state = set_state
current_store.apply(conn, project_response(
    [{'id': '1', 'title': 'new', 'markets': []}], 'events',
    received_at='2026-10-02T00:00:00Z', response_id='new'), {'checkpoint': 'new'})
os._exit(79)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(settings.store_path), phase], timeout=20
    )
    assert result.returncode == 79
    with duckdb.connect(str(settings.store_path)) as conn:
        expected = "old" if phase == "before" else "new"
        assert conn.execute("SELECT title FROM events").fetchone() == (expected,)
        assert current_store.get_state(conn, "checkpoint") == expected
        assert current_store.generation(conn) == (1 if phase == "before" else 2)


@pytest.mark.parametrize("phase", ["before", "after"])
def test_abrupt_death_at_pointer_promotion_keeps_verifiable_release(tmp_path, phase):
    settings = CurrentSettings(
        tmp_path / "catalogue", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    with duckdb.connect(str(settings.store_path)) as conn:
        current_store.initialize(conn)
        apply(conn, [{"id": "1", "title": "old"}])
        first = current_publish.publish(settings, conn)
        apply(conn, [{"id": "1", "title": "new"}], day=2)
    script = """
import os, sys
from dataclasses import replace
from pathlib import Path
import duckdb
from oddsfox_catalogue import current_publish
from oddsfox_catalogue.current_config import CurrentSettings, CurrentLimits
settings = CurrentSettings(Path(sys.argv[1]), limits=replace(CurrentLimits(), free_reserve_bytes=1))
original = os.replace
def replace_file(source, destination):
    pointer = Path(destination) == settings.published_dir / 'current.json'
    if pointer and sys.argv[2] == 'before':
        os._exit(79)
    original(source, destination)
    if pointer and sys.argv[2] == 'after':
        os._exit(79)
current_publish.os.replace = replace_file
conn = duckdb.connect(str(settings.store_path))
current_publish.publish(settings, conn)
"""
    result = subprocess.run([sys.executable, "-c", script, str(settings.root), phase], timeout=20)
    assert result.returncode == 79
    verified = current_publish.verify_current(settings)
    assert verified["generation"] == (1 if phase == "before" else 2)
    assert current_publish.verify_release(first["path"])["valid"]
    with duckdb.connect(str(settings.store_path)) as conn:
        recovered = current_publish.publish(settings, conn)
        assert recovered["generation"] == 2
    pointer = json.loads((settings.published_dir / "current.json").read_text())
    assert pointer["previous_release_id"] == first["release_id"]
    assert {p.name for p in (settings.published_dir / "releases").iterdir()} == {
        pointer["release_id"],
        first["release_id"],
    }
