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


def seed(conn, records, age=20000):
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
    seed(conn, [event(1, True)], age=31 * 86400)
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
    seed(conn, [event(key) for key in range(1, 401)], age=17000)
    seed(conn, [event(999, True)], age=31 * 86400)
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
            received_at=(NOW - timedelta(hours=7)).isoformat(),
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
