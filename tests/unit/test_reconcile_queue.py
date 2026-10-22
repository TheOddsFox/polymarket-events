"""Full-pass queue behavior for batching, urgency and prefetch."""

from collections import deque
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import duckdb
import httpx
import pytest

from oddsfox_catalogue import current_reconcile as reconcile
from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import GammaClient

NOW = datetime(2026, 10, 14, tzinfo=UTC)


def event(key, *, closed=True):
    return {
        "id": str(key),
        "active": not closed,
        "closed": closed,
        "archived": False,
        "markets": [],
    }


class CapturePrefetch:
    def __init__(self, client, deadline):
        self.submitted = []

    def submit(self, request):
        self.submitted.append(request)

    def take(self, request):
        return None

    def close(self):
        pass


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
    monkeypatch.setattr(reconcile, "ResponsePrefetcher", CapturePrefetch)
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


def run(settings, conn, handler, monkeypatch):
    from fakes.harness import FakeClock

    clock = FakeClock(t=1000.0)
    monkeypatch.setattr(reconcile, "time", SimpleNamespace(monotonic=clock))
    client = GammaClient(
        settings.gamma,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: NOW + timedelta(seconds=1 + clock()),
    )
    return reconcile.reconcile(settings, conn, client, deadline=clock() + 30)


def test_take_and_preview_skip_settled_ids_and_preview_does_not_mutate():
    queue = deque(str(i) for i in range(1, 8))
    skipped = {"2", "5"}

    assert reconcile._peek_pending(queue, skipped, 3) == ["1", "3", "4"]
    assert list(queue) == [str(i) for i in range(1, 8)]
    assert reconcile._take_pending(queue, skipped, 3) == ["1", "3", "4"]
    assert reconcile._take_pending(queue, skipped, 3) == ["6", "7"]
    assert reconcile._take_pending(queue, skipped, 3) == []


def test_scattered_urgent_ids_are_skipped_and_prefetch_tracks_next_eligible_batch(
    setup, monkeypatch
):
    settings, conn = setup
    urgent_ids = {"2", "55", "201"}
    seed(
        conn,
        [event(i, closed=str(i) not in urgent_ids) for i in range(1, 251)],
        age=23 * 3600,
    )
    reconcile.start_full_pass(conn, now=NOW)
    calls, prefetches = [], []

    class RecordingPrefetch(CapturePrefetch):
        def __init__(self, client, deadline):
            super().__init__(client, deadline)
            self.submitted = prefetches

    monkeypatch.setattr(reconcile, "ResponsePrefetcher", RecordingPrefetch)

    def handler(request):
        ids = request.url.params.get_list("id")
        calls.append(ids)
        return httpx.Response(200, json=[event(key) for key in ids])

    run(settings, conn, handler, monkeypatch)

    expected = [str(i) for i in range(1, 251) if str(i) not in urgent_ids]
    assert calls == [["2", "55", "201"], expected[:100], expected[100:200], expected[200:]]
    assert len({key for batch in calls for key in batch}) == 250
    assert [item["params"]["id"] for item in prefetches] == [
        [int(key) for key in expected[100:200]],
        [int(key) for key in expected[200:]],
    ]
    assert all(item["params"]["id"] for item in prefetches)
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 0


def test_all_remaining_urgent_batches_stay_nonempty_and_bounded(setup, monkeypatch):
    settings, conn = setup
    ids = [str(i) for i in range(1, 251)]
    seed(conn, [event(i, closed=False) for i in ids], age=23 * 3600)
    reconcile.start_full_pass(conn, now=NOW)
    position = 0

    def urgent_batches(_conn, _settings, _now):
        nonlocal position
        start = position
        position += 100
        return ids[start : start + 100] or None

    monkeypatch.setattr(reconcile, "_urgent_active_ids", urgent_batches)
    calls = []

    def handler(request):
        batch = request.url.params.get_list("id")
        calls.append(batch)
        return httpx.Response(200, json=[event(key) for key in batch])

    run(settings, conn, handler, monkeypatch)

    assert calls and all(1 <= len(batch) <= 100 for batch in calls)
    flattened = [key for batch in calls for key in batch]
    assert len(flattened) == len(set(flattened)) == 250


def test_id_becoming_urgent_during_the_pass_is_skipped_from_the_cold_queue(setup, monkeypatch):
    settings, conn = setup
    ids = [str(i) for i in range(1, 151)]
    seed(conn, [event(i) for i in ids], age=23 * 3600)
    reconcile.start_full_pass(conn, now=NOW)
    checks = 0

    def newly_urgent(_conn, _settings, _now):
        nonlocal checks
        checks += 1
        return ["120"] if checks == 2 else None

    monkeypatch.setattr(reconcile, "_urgent_active_ids", newly_urgent)
    calls = []

    def handler(request):
        batch = request.url.params.get_list("id")
        calls.append(batch)
        return httpx.Response(200, json=[event(key) for key in batch])

    run(settings, conn, handler, monkeypatch)

    assert calls[0] == ids[:100]
    assert calls[1] == ["120"]
    assert [key for batch in calls for key in batch] == [
        *ids[:100],
        "120",
        *ids[100:119],
        *ids[120:],
    ]
    assert len({key for batch in calls for key in batch}) == 150
