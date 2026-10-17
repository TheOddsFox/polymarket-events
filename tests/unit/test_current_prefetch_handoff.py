import queue
import threading
import time

import duckdb
import httpx

from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.gamma.http import GammaClient


class PausedPutQueue:
    """Pause the worker at the handoff while the consumer tries to take."""

    def __init__(self):
        self.inner = queue.Queue()
        self.at_put = threading.Event()
        self.release_put = threading.Event()

    def put(self, item):
        self.at_put.set()
        assert self.release_put.wait(5)
        self.inner.put(item)

    def get_nowait(self):
        return self.inner.get_nowait()


class PausedEmptyQueue:
    def __init__(self):
        self.inner = queue.Queue()
        self.at_empty = threading.Event()
        self.release_empty = threading.Event()
        self.at_put = threading.Event()

    def put(self, item):
        self.at_put.set()
        self.inner.put(item)

    def get_nowait(self):
        try:
            return self.inner.get_nowait()
        except queue.Empty:
            self.at_empty.set()
            assert self.release_empty.wait(5)
            raise


def test_take_waits_for_response_during_worker_handoff(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        gamma=GammaSettings(
            base_url="https://gamma.fake.test", requests_per_second=10.0, max_retries=0
        ),
        limits=CurrentLimits(max_requests=1),
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
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(
            200,
            json=[{"id": "1", "active": True, "closed": False, "archived": False, "markets": []}],
        )

    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None, max_requests=1
    )
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    controlled_queue = PausedPutQueue()
    prefetcher._done = controlled_queue
    request = current_sync.first_bulk_request(settings, conn, "events", [1])
    try:
        prefetcher.submit(request)
        assert controlled_queue.at_put.wait(3)
        assert prefetcher._lock.locked()
        controlled_queue.release_put.set()
        result, _omitted = current_sync.fetch_unit(
            settings, conn, client, "events", [1], prefetcher=prefetcher
        )
        assert result["responses"] == 1
        assert current_store.get_state(conn, "fetch_pending") is None
    finally:
        controlled_queue.release_put.set()
        prefetcher.close()
        client.close()
        conn.close()

    assert calls == ["/events"]


def test_empty_observation_and_inflight_check_are_atomic(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        gamma=GammaSettings(base_url="https://gamma.fake.test", max_retries=0),
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
    response_started = threading.Event()
    release_response = threading.Event()

    def handler(_request):
        response_started.set()
        assert release_response.wait(5)
        return httpx.Response(
            200,
            json=[{"id": "1", "active": True, "closed": False, "archived": False, "markets": []}],
        )

    client = GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    prefetcher = current_sync.ResponsePrefetcher(client, deadline=time.monotonic() + 30)
    controlled_queue = PausedEmptyQueue()
    prefetcher._done = controlled_queue
    request = current_sync.first_bulk_request(settings, conn, "events", [1])
    result = []
    take_done = threading.Event()

    def take():
        result.append(prefetcher.take(request))
        take_done.set()

    try:
        prefetcher.submit(request)
        assert response_started.wait(3)
        taker = threading.Thread(target=take)
        taker.start()
        assert controlled_queue.at_empty.wait(3)
        release_response.set()
        if not prefetcher._lock.locked():
            assert controlled_queue.at_put.wait(3)
        controlled_queue.release_empty.set()
        assert take_done.wait(3)
        taker.join(timeout=3)
        assert not taker.is_alive()
        assert result[0] is not None
    finally:
        release_response.set()
        controlled_queue.release_empty.set()
        prefetcher.close()
        client.close()
        conn.close()
