"""Forced full pass: gates bypassed until every known entity has a newer receipt."""

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
