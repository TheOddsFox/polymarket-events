"""Forced-pass and prefetch edges: receipt boundary, cursor reset, deadline, failure, bounds."""

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
from oddsfox_catalogue.gamma.http import GammaClient, GammaError

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


def seed(conn, records, received_at):
    current_store.apply(
        conn,
        project_response(records, "events", received_at=received_at, response_id="seed"),
    )


def client_for(settings, handler, **limits):
    return GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None, **limits
    )


def run(settings, conn, handler, *, deadline=None):
    client = client_for(settings, handler)
    return reconcile.reconcile(settings, conn, client, deadline=deadline or time.monotonic() + 30)


def test_receipt_at_pass_start_is_still_owed_and_only_a_newer_receipt_clears(setup):
    settings, conn = setup
    seed(conn, [event(1)], received_at=NOW.isoformat())
    reconcile.start_full_pass(conn, now=NOW)
    # A receipt equal to the pass start predates the pass: the entity is still owed.
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 1
    seed(conn, [event(1)], received_at=(NOW + timedelta(microseconds=1)).isoformat())
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 0


def test_a_new_pass_restarts_from_the_lowest_id_after_a_completed_pass(setup):
    settings, conn = setup
    seed(conn, [event(i) for i in (1, 2, 3)], received_at=(NOW - timedelta(hours=1)).isoformat())
    reconcile.start_full_pass(conn, now=NOW)
    run(settings, conn, lambda r: httpx.Response(200, json=[event(i) for i in (1, 2, 3)]))
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None
    # The previous pass left its cursor at id 3; a fresh pass must not inherit it.
    # Real receipts are stamped by the client clock, so the new pass starts between runs.
    time.sleep(0.01)
    started = datetime.now(UTC)
    reconcile.start_full_pass(conn, now=started)
    assert reconcile.full_pass_status(conn, now=started)["remaining"] == 3
    requested = []

    def handler(request):
        requested.append(request.url.params.get_list("id"))
        return httpx.Response(200, json=[event(i) for i in (1, 2, 3)])

    time.sleep(0.01)
    run(settings, conn, handler)
    assert requested == [["1", "2", "3"]]
    assert reconcile.full_pass_status(conn, now=started)["remaining"] == 0


def test_deadline_between_bulk_apply_and_omission_confirmation_keeps_the_omitted_event(setup):
    settings, conn = setup
    seed(
        conn, [event(1), event(2, closed=True)], received_at=(NOW - timedelta(hours=1)).isoformat()
    )
    reconcile.start_full_pass(conn, now=NOW)

    def last_batch_straddles_deadline(request):
        if request.url.path == "/events":
            # The bulk listing omits event 2 and uses up the remaining time budget.
            time.sleep(0.6)
            return httpx.Response(200, json=[event(1)])
        pytest.fail(f"unexpected request {request.url}")

    run(settings, conn, last_batch_straddles_deadline, deadline=time.monotonic() + 1.5)

    seen = []

    def resumed(request):
        seen.append(request.url.path)
        if request.url.path == "/events":
            ids = request.url.params.get_list("id")
            return httpx.Response(200, json=[event(int(value)) for value in ids if value != "2"])
        return httpx.Response(404, json={"error": "not found"})

    run(settings, conn, resumed)
    assert "/events/2" in seen
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None


def test_prefetch_failure_surfaces_to_the_fetch_that_needs_it(setup):
    settings, conn = setup
    seed(conn, [event(1)], received_at=(NOW - timedelta(hours=1)).isoformat())
    settings = replace(settings, gamma=replace(settings.gamma, max_retries=0))
    client = client_for(settings, lambda request: httpx.Response(500))
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    try:
        prefetcher.submit(current_sync.first_bulk_request(settings, conn, "events", ["1"]))
        with pytest.raises(GammaError):
            current_sync.fetch_unit(settings, conn, client, "events", ["1"], prefetcher=prefetcher)
    finally:
        prefetcher.close()
    # The failed unit stays durable, so the next invocation resumes it instead of dropping it.
    assert current_store.get_state(conn, "fetch_pending") is not None


def test_oversized_prefetched_listing_splits_like_a_direct_fetch(setup):
    settings, conn = setup
    seed(conn, [event(1), event(2)], received_at=(NOW - timedelta(hours=1)).isoformat())
    seen = []

    def handler(request):
        ids = request.url.params.get_list("id")
        seen.append(ids)
        return httpx.Response(200, json=[event(int(value)) for value in ids])

    # Two events exceed this body limit; one event fits, so the unit must split.
    pair_bytes = len(httpx.Response(200, json=[event(1), event(2)]).content)
    single_bytes = len(httpx.Response(200, json=[event(2)]).content)
    assert single_bytes < pair_bytes - 1
    client = client_for(settings, handler, max_body_bytes=pair_bytes - 1)
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    try:
        prefetcher.submit(current_sync.first_bulk_request(settings, conn, "events", ["1", "2"]))
        result, omitted = current_sync.fetch_unit(
            settings, conn, client, "events", ["1", "2"], prefetcher=prefetcher
        )
    finally:
        prefetcher.close()
    assert omitted == []
    assert result["responses"] == 2
    assert sorted(seen, key=str) == sorted([["1", "2"], ["1"], ["2"]], key=str)
    assert current_store.get_state(conn, "fetch_pending") is None
    renewed = conn.execute(
        "SELECT count(*) FROM _membership WHERE receipt > ?",
        [NOW - timedelta(minutes=30)],
    ).fetchone()[0]
    assert renewed == 2


def test_prefetch_holds_at_most_two_requests_behind_the_one_in_flight(setup):
    settings, conn = setup
    entered, release, calls = threading.Event(), threading.Event(), []

    def handler(request):
        calls.append(request.url.params.get_list("id"))
        if len(calls) == 1:
            entered.set()
            release.wait(5)
        return httpx.Response(200, json=[])

    client = client_for(settings, handler)
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    requests = [
        {"endpoint": "/events", "params": {"limit": 1, "id": [str(i)]}} for i in range(1, 6)
    ]
    try:
        prefetcher.submit(requests[0])
        assert entered.wait(2)
        for request in requests[1:]:
            prefetcher.submit(request)
        release.set()
        deadline = time.monotonic() + 3
        while len(calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.05)
    finally:
        prefetcher.close()
    # One in flight plus two queued; the fourth and fifth were refused at submission.
    assert calls == [["1"], ["2"], ["3"]]
