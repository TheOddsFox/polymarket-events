"""Forced full pass: gates bypassed until every known entity has a newer receipt."""

import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import httpx
import pytest

from oddsfox_catalogue import current_reconcile as reconcile
from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import CaptureStopped, GammaClient, GammaError

NOW = datetime(2026, 10, 14, tzinfo=UTC)


def event(key, closed=False):
    return {
        "id": str(key),
        "active": not closed,
        "closed": closed,
        "archived": False,
        "markets": [],
    }


@pytest.fixture
def setup(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(":memory:")
    current_store.initialize(conn)
    reconcile._initialize(conn)
    current_store.set_state(
        conn,
        "bulk_policy",
        {
            **current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        },
    )
    monkeypatch.setattr(reconcile, "_now", lambda: NOW)
    yield settings, conn
    conn.close()


def seed(conn, records, age):
    current_store.apply(
        conn,
        project_response(
            records,
            "events",
            received_at=(NOW - timedelta(seconds=age)).isoformat(),
            response_id=str(age),
        ),
    )


def run(settings, conn, handler, *, deadline=None):
    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    return reconcile.reconcile(settings, conn, client, deadline=deadline or time.monotonic() + 30)


def test_start_is_idempotent_and_resumes_from_the_original_start(setup):
    _settings, conn = setup
    seed(conn, [event(1), event(2, closed=True)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    reconcile.start_full_pass(conn, now=NOW + timedelta(hours=5))
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["started_at"] == NOW.isoformat()
    assert status["completed_at"] is None
    assert status["remaining"] == 2


def test_pass_bypasses_cold_and_active_gates_only_while_active(setup):
    _settings, conn = setup
    seed(conn, [event(1), event(2, closed=True)], age=3600)
    assert reconcile._due(conn, "events", True, NOW, 58 * 86400) == []
    assert reconcile._due(conn, "events", False, NOW, 20 * 3600) == []
    reconcile.start_full_pass(conn, now=NOW)
    started = reconcile._pass_started(conn)
    # During a pass every known event is visited, open or closed, on either lane.
    cold = reconcile._due(conn, "events", True, NOW, 58 * 86400, pass_started=started)
    active = reconcile._due(conn, "events", False, NOW, 20 * 3600, pass_started=started)
    assert {row[0] for row in cold} == {"1", "2"}
    assert {row[0] for row in active} == {"1", "2"}


def test_pass_completes_only_after_every_known_event_has_a_newer_receipt(setup):
    settings, conn = setup
    seed(conn, [event(1), event(2, closed=True)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    run(settings, conn, lambda r: httpx.Response(200, json=[]), deadline=time.monotonic())
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is None
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 2
    result = run(
        settings,
        conn,
        lambda r: httpx.Response(200, json=[event(1), event(2, closed=True)]),
    )
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["remaining"] == 0
    assert status["completed_at"] is not None
    assert result["source_complete"] is False
    assert reconcile.freshness(conn, now=NOW)["source_complete"] is False


def test_budget_stop_leaves_the_pass_open_for_the_next_invocation(setup):
    settings, conn = setup
    seed(conn, [event(1)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    started = reconcile._pass_started(conn)
    run(settings, conn, lambda r: pytest.fail("no budget left"), deadline=time.monotonic())
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["started_at"] == started.isoformat()
    assert status["completed_at"] is None
    assert status["remaining"] == 1


def test_completed_pass_does_not_resume_and_a_new_pass_can_start(setup):
    settings, conn = setup
    seed(conn, [event(1)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    run(settings, conn, lambda r: httpx.Response(200, json=[event(1)]))
    assert reconcile._pass_started(conn) is None
    later = NOW + timedelta(days=1)
    reconcile.start_full_pass(conn, now=later)
    status = reconcile.full_pass_status(conn, now=later)
    assert status["started_at"] == later.isoformat()
    assert status["completed_at"] is None


def test_empty_store_pass_completes_on_the_next_reconcile(setup):
    settings, conn = setup
    reconcile.start_full_pass(conn, now=NOW)
    run(settings, conn, lambda r: pytest.fail("nothing known to fetch"))
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None


def market(key):
    return {
        "id": str(key),
        "version": "v1",
        "conditionId": f"0x{int(key):064x}",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": [str(int(key) * 10 + i) for i in (1, 2)],
    }


def bulk_list_omits_event_two(request):
    if request.url.path == "/events":
        return httpx.Response(200, json=[event(1)])
    pytest.fail(f"unexpected request {request.url}")


@pytest.mark.parametrize(
    "native",
    [
        lambda request: httpx.Response(404, json={"error": "not found"}),
        lambda request: httpx.Response(200, json=event(2, closed=True)),
    ],
    ids=["native-404-is-absence", "native-200-applies-record"],
)
def test_bulk_omission_is_settled_by_one_native_lookup_then_not_refetched(setup, native):
    settings, conn = setup
    seed(conn, [event(1), event(2, closed=True)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    seen = []

    def handler(request):
        seen.append(request.url.path)
        if request.url.path == "/events/2":
            return native(request)
        return bulk_list_omits_event_two(request)

    run(settings, conn, handler)
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["remaining"] == 0
    assert status["completed_at"] is not None
    assert seen.count("/events/2") == 1
    # The next invocation must not spend requests on the settled entity.
    run(settings, conn, lambda request: pytest.fail(f"refetched {request.url}"))


def test_interrupted_confirmation_resumes_as_absence(setup):
    settings, conn = setup
    seed(conn, [event(1), event(2, closed=True)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)

    def interrupted(request):
        if request.url.path == "/events/2":
            raise CaptureStopped()
        return bulk_list_omits_event_two(request)

    run(settings, conn, interrupted)
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is None
    assert current_store.get_state(conn, "fetch_pending") is not None

    seen = []

    def resumed(request):
        seen.append(request.url.path)
        return httpx.Response(404, json={"error": "not found"})

    run(settings, conn, resumed)
    assert seen == ["/events/2"]
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None


def server_error(request):
    return httpx.Response(500)


def connect_refused(request):
    raise httpx.ConnectError("refused", request=request)


@pytest.mark.parametrize("failure", [server_error, connect_refused], ids=["5xx", "transport"])
def test_server_or_transport_failure_never_counts_as_absent(setup, failure):
    settings, conn = setup
    # No retries: a retry penalty would wait on the real clock inside this test.
    settings = replace(settings, gamma=replace(settings.gamma, max_retries=0))
    seed(conn, [event(1), event(2, closed=True)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)

    def handler(request):
        if request.url.path == "/events/2":
            return failure(request)
        return bulk_list_omits_event_two(request)

    with pytest.raises(GammaError):
        run(settings, conn, handler)
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["remaining"] == 1
    assert status["completed_at"] is None


def test_omitted_known_market_is_settled_by_native_404(setup):
    settings, conn = setup
    old = (NOW - timedelta(seconds=3600)).isoformat()
    current_store.apply(
        conn, project_response([market("10")], "markets", received_at=old, response_id="old")
    )
    reconcile.start_full_pass(conn, now=NOW)
    seen = []

    def handler(request):
        seen.append(request.url.path)
        if request.url.path == "/markets":
            return httpx.Response(200, json=[])
        if request.url.path == "/markets/10":
            return httpx.Response(404, json={"error": "not found"})
        pytest.fail(f"unexpected request {request.url}")

    run(settings, conn, handler)
    assert seen == ["/markets", "/markets/10"]
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None


def test_native_404_records_absence_only_for_known_entities(setup):
    settings, conn = setup
    seed(conn, [event(2)], age=3600)
    client = GammaClient(
        settings.gamma,
        transport=httpx.MockTransport(lambda request: httpx.Response(404, json={})),
        sleep=lambda _: None,
    )
    current_sync.fetch_apply(settings, conn, client, "events", [2, 3], native=True, absence=True)
    rows = conn.execute("SELECT event_id, receipt FROM _membership ORDER BY event_id").fetchall()
    assert [row[0] for row in rows] == ["2"]
    assert rows[0][1] > NOW - timedelta(seconds=3600)
    assert conn.execute("SELECT count(*) FROM events WHERE event_id='3'").fetchone()[0] == 0


def test_window_gaps_cost_no_native_lookup_outside_a_pass(setup):
    settings, conn = setup
    seed(conn, [event(2)], age=3600)
    seen = []

    def handler(request):
        seen.append(request.url.path)
        return httpx.Response(404, json={})

    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    reconcile._confirm_omitted(settings, conn, client, "events", ["2", "3"])
    assert seen == ["/events/2"]


def test_pass_walks_events_in_order_and_resumes_from_the_cursor(setup):
    settings, conn = setup
    seed(conn, [event(i) for i in range(1, 151)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    calls = []

    def stop_after_one(request):
        ids = [int(value) for value in request.url.params.get_list("id")]
        calls.append(ids)
        if ids[0] > 100:
            raise CaptureStopped()
        return httpx.Response(200, json=[event(value) for value in ids])

    run(settings, conn, stop_after_one)
    assert current_store.get_state(conn, "full_pass")["cursor_after"] == 100
    assert list(range(1, 101)) in calls
    resumed_at = len(calls)

    def resume(request):
        calls.append([int(value) for value in request.url.params.get_list("id")])
        return httpx.Response(200, json=[event(value) for value in calls[-1]])

    run(settings, conn, resume)
    assert calls[resumed_at:] == [list(range(101, 151))]
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None


def test_pass_renews_nested_markets_and_sweeps_orphans(setup):
    settings, conn = setup
    parent = event(1)
    parent["markets"] = [market(10)]
    old = (NOW - timedelta(seconds=3600)).isoformat()
    current_store.apply(
        conn, project_response([parent], "events", received_at=old, response_id="e")
    )
    current_store.apply(
        conn, project_response([market(11)], "markets", received_at=old, response_id="m")
    )
    reconcile.start_full_pass(conn, now=NOW)
    seen = []

    def handler(request):
        ids = request.url.params.get_list("id")
        seen.append((request.url.path, ids))
        if request.url.path == "/events":
            return httpx.Response(200, json=[parent])
        if request.url.path == "/markets":
            return httpx.Response(200, json=[market(11)])
        pytest.fail(request.url.path)

    run(settings, conn, handler)
    assert ("/markets", ["10"]) not in seen
    assert ("/markets", ["11"]) in seen
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 0


def test_pass_skips_id_range_windows(setup):
    settings, conn = setup
    seed(conn, [event(2)], age=3600)
    current_store.set_state(
        conn,
        "discovery",
        {"lanes": {"events": {"frontier": 500}, "markets": {"frontier": 500}}},
    )
    reconcile.start_full_pass(conn, now=NOW)
    seen = []

    def handler(request):
        seen.append(request.url.params.get_list("id"))
        return httpx.Response(200, json=[event(2)])

    run(settings, conn, handler)
    assert seen == [["2"]]


def test_urgent_active_event_takes_the_next_pass_slot(setup):
    settings, conn = setup
    seed(conn, [event(i, closed=True) for i in range(1, 4)], age=3600)
    seed(conn, [event(9)], age=23 * 3600)
    reconcile.start_full_pass(conn, now=NOW)
    seen = []

    def handler(request):
        seen.append(request.url.params.get_list("id"))
        return httpx.Response(
            200, json=[event(int(value), closed=value != "9") for value in seen[-1]]
        )

    run(settings, conn, handler)
    assert seen[0] == ["9"]


def test_event_request_count_is_one_per_hundred(setup):
    settings, conn = setup
    seed(conn, [event(i) for i in range(1, 251)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        ids = request.url.params.get_list("id")
        return httpx.Response(200, json=[event(int(value)) for value in ids])

    run(settings, conn, handler)
    assert calls == ["/events", "/events", "/events"]


def test_prefetch_mismatch_is_discarded_and_recounted(setup):
    settings, conn = setup
    seed(conn, [event(1)], age=3600)
    calls = []

    def handler(request):
        calls.append(request.url.params.get_list("id"))
        return httpx.Response(200, json=[event(1)])

    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    prefetcher.submit({"endpoint": "/events", "params": {"limit": 1, "id": [99]}})
    deadline = time.monotonic() + 2
    while client.stats.requests < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    current_sync.fetch_unit(settings, conn, client, "events", [1], prefetcher=prefetcher)
    prefetcher.close()
    assert prefetcher.discarded >= 1
    assert client.stats.requests >= 2
    assert ["1"] in calls


def test_stop_and_deadline_drain_the_prefetch_worker(setup):
    settings, conn = setup
    entered = threading.Event()

    def handler(request):
        entered.set()
        prefetcher._stop.wait(2)
        return httpx.Response(200, json=[])

    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    prefetcher.submit(current_sync.first_bulk_request(settings, conn, "events", [1]))
    assert entered.wait(1)
    prefetcher.close()
    assert not prefetcher._thread.is_alive()

    late = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() - 1)
    before = client.stats.requests
    late.submit(current_sync.first_bulk_request(settings, conn, "events", [1]))
    time.sleep(0.05)
    late.close()
    assert client.stats.requests == before
    assert not late._thread.is_alive()
