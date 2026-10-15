"""Reconciliation budgets and receipts remain conservative under omissions."""

import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import httpx
import pytest

from oddsfox_catalogue import current_reconcile as reconcile
from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import GammaClient

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


def seed(conn, records, age=21 * 3600):
    current_store.apply(
        conn,
        project_response(
            records,
            "events",
            received_at=(NOW - timedelta(seconds=age)).isoformat(),
            response_id=str(age),
        ),
    )


def run(settings, conn, handler):
    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    return reconcile.reconcile(settings, conn, client, deadline=time.monotonic() + 30)


def test_due_omission_retry_fairness_and_noop(setup):
    settings, conn = setup
    seed(conn, [event(1), event(2)])
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert len(calls) == 1
    assert calls[0].url.params.get_list("id") == ["1", "2"]
    assert reconcile.freshness(conn, now=NOW)["active_count"] == 2
    assert reconcile.freshness(conn, now=NOW)["source_complete"] is False
    run(settings, conn, lambda r: pytest.fail("same-hour retry"))
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 2


def test_missing_membership_keeps_previous_complete_and_unresolved(setup):
    settings, conn = setup
    seed(conn, [event(1)])
    payload = event(1)
    payload.pop("markets")
    run(settings, conn, lambda r: httpx.Response(200, json=[payload]))
    assert conn.execute("SELECT unresolved FROM _membership").fetchone()[0]
    assert reconcile.freshness(conn, now=NOW)["active_overdue_count"] == 1


def test_closed_known_ids_and_historical_windows_revisit(setup):
    settings, conn = setup
    seed(conn, [event(1, True)], age=61 * 86400)
    current_store.set_state(
        conn, "discovery", {"lanes": {"events": {"frontier": 3}, "markets": {"frontier": 2}}}
    )
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            200,
            json=[event(2)]
            if request.url.path == "/events" and "2" in request.url.params.get_list("id")
            else [],
        )

    run(settings, conn, handler)
    assert conn.execute("SELECT event_id FROM events ORDER BY event_id").fetchall() == [
        ("1",),
        ("2",),
    ]
    assert any(r.url.path == "/markets" for r in calls)
    assert all(len(r.url.params.get_list("id")) <= 100 for r in calls)
    state = current_store.get_state(conn, "reconcile")
    assert state["windows"]["events"]["next"] == 1
    assert state["windows"]["events"]["cycle"] == NOW.timestamp()


def test_budget_stop_before_http_and_previous_publish_reserve(setup):
    settings, conn = setup
    seed(conn, [event(1)])
    current_store.set_state(conn, "previous_publish_seconds", 30)
    result = run(settings, conn, lambda r: pytest.fail("publication reserve"))
    assert result["responses"] == 0


def test_errors_propagate_without_attempt_checkpoint(setup):
    settings, conn = setup
    seed(conn, [event(1)])
    with pytest.raises(CurrentError):
        run(settings, conn, lambda r: httpx.Response(200, json=[event(999)]))
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone()[0] == 0


def test_frozen_pending_repair_resumes_before_selection(setup):
    settings, conn = setup
    current_store.set_state(
        conn,
        "fetch_pending",
        {
            "identity": {
                "lane": "events",
                "ids": [7],
                "native": True,
                "progress": {"foreign": True},
                "policy": current_sync.POLICY,
                "certificate": None,
                "passes": [None],
            },
            "remaining": [{"endpoint": "/events/7", "params": {}}],
            "raw_id": "frozen",
        },
    )
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(404))
    assert [r.url.path for r in calls] == ["/events/7"]
    assert current_store.get_state(conn, "foreign") is True


def test_three_active_batches_before_cold_and_persisted_turn(setup):
    settings, conn = setup
    seed(conn, [event(key) for key in range(1, 401)], age=21 * 3600)
    seed(conn, [event(999, True)], age=61 * 86400)
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert len(calls) == 5
    assert calls[3].url.params.get_list("id") == ["999"]
    assert current_store.get_state(conn, "reconcile")["turn"] == 1


def test_unknown_lifecycle_is_due_and_native_404_preserves_market(setup):
    settings, conn = setup
    unknown = event(1)
    unknown.update(active=None, closed=None)
    seed(conn, [unknown])
    market = event(7)
    current_store.apply(
        conn,
        project_response(
            [market],
            "markets",
            received_at=(NOW - timedelta(days=60)).isoformat(),
            response_id="market",
        ),
    )
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path == "/markets/7":
            return httpx.Response(404)
        return httpx.Response(200, json=[market] if request.url.path == "/markets" else [])

    run(settings, conn, handler)
    assert calls[0].url.path == "/events"
    assert sum(r.url.path == "/markets/7" for r in calls) == 1
    assert conn.execute("SELECT market_id FROM markets").fetchall() == [("7",)]
    assert conn.execute("SELECT count(*) FROM _identity").fetchone()[0] == 1


def test_old_omissions_do_not_starve_later_ids(setup):
    settings, conn = setup
    seed(conn, [event(key) for key in range(1, 202)])
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert [len(r.url.params.get_list("id")) for r in calls] == [100, 100, 1]
    assert calls[-1].url.params.get_list("id") == ["201"]


def test_missing_parent_omission_is_bounded_and_empty_windows_leave_no_id_state(setup):
    settings, conn = setup
    conn.execute("INSERT INTO _parent_repairs VALUES ('99')")
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": 3}}})
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert len(calls) == 2
    assert conn.execute("SELECT kind,id FROM _reconcile_attempts").fetchall() == [("events", "99")]
    assert conn.execute("SELECT event_id FROM _parent_repairs").fetchall() == [("99",)]


@pytest.mark.parametrize("closed", [False, True, None])
@pytest.mark.parametrize(
    "age_days,due",
    [
        (0, False),
        (5 / 24, False),
        (58 - 1 / 86400, False),
        (58, True),
        (60 - 1 / 86400, True),
        (60, True),
    ],
)
def test_all_known_markets_use_sixty_day_cold_lane(setup, closed, age_days, due):
    settings, conn = setup
    market = {"id": "7", "closed": closed}
    current_store.apply(
        conn,
        project_response(
            [market],
            "markets",
            received_at=(NOW - timedelta(days=age_days)).isoformat(),
            response_id="market",
        ),
    )
    calls = []
    run(
        settings,
        conn,
        lambda r: (
            calls.append(r)
            or (httpx.Response(404) if r.url.path == "/markets/7" else httpx.Response(200, json=[]))
        ),
    )
    assert [r.url.path for r in calls] == (["/markets", "/markets/7"] if due else [])
    status = reconcile.freshness(conn, now=NOW)
    assert status["active_count"] == status["active_overdue_count"] == 0
    assert status["cold_count"] == 1
    assert status["cold_overdue_count"] == int(age_days >= 60)


def test_active_event_rechecks_before_daily_target(setup):
    settings, conn = setup
    seed(conn, [event(1)], age=20 * 3600)
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert [r.url.params.get_list("id") for r in calls] == [["1"]]


def test_urgent_event_overrides_persisted_cold_turn(setup):
    settings, conn = setup
    seed(conn, [event(1)], age=22 * 3600)
    seed(conn, [event(999, True)], age=61 * 86400)
    current_store.set_state(conn, "reconcile", {"turn": 3, "cold_slot": 0, "windows": {}})
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert [r.url.params.get_list("id") for r in calls] == [["1"], ["999"]]
    assert current_store.get_state(conn, "reconcile")["turn"] == 0


def test_historical_cycle_freezes_frontier_and_restarts_from_start_time(setup):
    settings, conn = setup
    state = {"cold_slot": 2, "windows": {}}
    start = datetime(2026, 1, 1, tzinfo=UTC)
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": 200}}})
    reconcile._initialize(conn)
    lane, ids, slot, window = reconcile._cold(conn, settings, start, state)
    assert (lane, ids, slot) == ("events", list(range(1, 101)), 2)
    assert window == {"next": 101, "cycle": None, "started_at": start.timestamp(), "high": 200}
    state["windows"][lane] = window
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": 400}}})
    lane, ids, _, window = reconcile._cold(conn, settings, start + timedelta(days=2), state)
    assert ids == list(range(101, 201))
    assert window == {"next": 1, "cycle": start.timestamp()}
    state["windows"][lane] = window
    assert reconcile._cold(conn, settings, start + timedelta(days=57), state) is None
    _, ids, _, window = reconcile._cold(conn, settings, start + timedelta(days=58), state)
    assert ids == list(range(1, 101))
    assert window["high"] == 400
    assert window["started_at"] == (start + timedelta(days=58)).timestamp()


@pytest.mark.parametrize("frontier", [0, 100])
def test_historical_empty_frontier_and_exact_batch_boundary(setup, frontier):
    settings, conn = setup
    reconcile._initialize(conn)
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": frontier}}})
    result = reconcile._cold(conn, settings, NOW, {"cold_slot": 2, "windows": {}})
    if frontier == 0:
        assert result is None
    else:
        assert result == ("events", list(range(1, 101)), 2, {"next": 1, "cycle": NOW.timestamp()})


def test_legacy_partial_historical_cycle_pins_frontier_on_resume(setup):
    settings, conn = setup
    reconcile._initialize(conn)
    state = {"cold_slot": 2, "windows": {"events": {"next": 101, "cycle": None}}}
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": 300}}})
    lane, ids, _, window = reconcile._cold(conn, settings, NOW, state)
    assert ids == list(range(101, 201))
    assert window["high"] == 300
    state["windows"][lane] = window
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": 500}}})
    _, ids, _, window = reconcile._cold(conn, settings, NOW + timedelta(days=1), state)
    assert ids == list(range(201, 301))
    assert window == {"next": 1, "cycle": NOW.timestamp()}


def test_historical_long_cycle_is_due_immediately_after_completion(setup):
    settings, conn = setup
    reconcile._initialize(conn)
    started = NOW - timedelta(days=61)
    state = {
        "cold_slot": 2,
        "windows": {
            "events": {"next": 101, "cycle": None, "started_at": started.timestamp(), "high": 200}
        },
    }
    current_store.set_state(conn, "discovery", {"lanes": {"events": {"frontier": 200}}})
    _, ids, _, window = reconcile._cold(conn, settings, NOW, state)
    assert ids == list(range(101, 201))
    state["windows"]["events"] = window
    _, ids, _, window = reconcile._cold(conn, settings, NOW, state)
    assert ids == list(range(1, 101))
    assert window["started_at"] == NOW.timestamp()


def test_historical_market_progress_commits_only_after_all_passes(setup):
    settings, conn = setup
    reconcile._initialize(conn)
    policy = current_store.get_state(conn, "bulk_policy")
    policy["lanes"]["markets"]["passes"] = [False, True]
    current_store.set_state(conn, "bulk_policy", policy)
    state = {"turn": 0, "cold_slot": 3, "windows": {}}
    current_store.set_state(conn, "reconcile", state)
    current_store.set_state(conn, "discovery", {"lanes": {"markets": {"frontier": 200}}})
    lane, ids, slot, window = reconcile._cold(conn, settings, NOW, state)
    progress = {**state, "cold_slot": (slot + 1) % 4, "windows": {lane: window}}
    calls = []

    def interrupted(request):
        calls.append(request)
        if len(calls) == 2:
            raise reconcile.CaptureStopped("stop between passes")
        return httpx.Response(200, json=[])

    with (
        GammaClient(settings.gamma, transport=httpx.MockTransport(interrupted)) as client,
        pytest.raises(reconcile.CaptureStopped),
    ):
        current_sync.fetch_apply(
            settings, conn, client, lane, ids, progress={"reconcile": progress}
        )
    assert current_store.get_state(conn, "reconcile") == state
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["identity"]["progress"]["reconcile"] == progress
    assert len(pending["remaining"]) == 1
    with GammaClient(
        settings.gamma, transport=httpx.MockTransport(lambda _: httpx.Response(200, json=[]))
    ) as client:
        current_sync.resume_pending(settings, conn, client)
    assert current_store.get_state(conn, "reconcile") == progress
    assert current_store.get_state(conn, "fetch_pending") is None


@pytest.mark.parametrize("age,due", [(20 * 3600 - 1, False), (20 * 3600, True)])
def test_active_due_boundary(setup, age, due):
    settings, conn = setup
    seed(conn, [event(1)], age=age)
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert len(calls) == int(due)


@pytest.mark.parametrize("age,active_first", [(22 * 3600 - 1, False), (22 * 3600, True)])
def test_active_urgency_boundary_preserves_cold_turn(setup, age, active_first):
    settings, conn = setup
    seed(conn, [event(1)], age=age)
    seed(conn, [event(999, True)], age=61 * 86400)
    current_store.set_state(conn, "reconcile", {"turn": 3, "cold_slot": 0, "windows": {}})
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert calls[0].url.params.get_list("id") == (["1"] if active_first else ["999"])


@pytest.mark.parametrize("age,overdue", [(86400 - 1, False), (86400, True)])
def test_active_status_uses_latest_complete_membership_receipt(setup, age, overdue):
    _, conn = setup
    seed(conn, [event(1)], age=age)
    assert reconcile.freshness(conn, now=NOW)["active_overdue_count"] == int(overdue)
    payload = event(1)
    payload.pop("markets")
    seed(conn, [payload], age=0)
    status = reconcile.freshness(conn, now=NOW)
    assert datetime.fromisoformat(status["active_oldest_complete_receipt"]) == NOW - timedelta(
        seconds=age
    )
    assert status["active_overdue_count"] == 1


@pytest.mark.parametrize("elapsed,due", [(3600 - 1, False), (3600, True)])
def test_retry_cooldown_boundary_does_not_refresh_omission(setup, elapsed, due):
    settings, conn = setup
    seed(conn, [event(1)], age=86400)
    reconcile._initialize(conn)
    reconcile._attempt(conn, "events", ["1"], NOW - timedelta(seconds=elapsed))
    calls = []
    run(settings, conn, lambda r: calls.append(r) or httpx.Response(200, json=[]))
    assert len(calls) == int(due)
    assert reconcile.freshness(conn, now=NOW)["active_overdue_count"] == 1
