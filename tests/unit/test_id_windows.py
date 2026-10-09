"""Id-window fetches: tail termination, single-id failures, resume state, and Retry-After caps."""

from __future__ import annotations

import random
from datetime import UTC, datetime

import httpx
import pytest

from fakes.harness import FakeClock
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.gamma.paginators import PageState, id_range_pages

NOW = datetime(2026, 10, 8, 6, 0, tzinfo=UTC)


def _client(handler) -> tuple[GammaClient, FakeClock]:
    settings = GammaSettings(base_url="https://gamma.fake.test", requests_per_second=1000.0)
    clock = FakeClock()
    client = GammaClient(
        settings,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: NOW,
        rng=random.Random(7),
    )
    return client, clock


def _tail(lo: int = 1, step: int = 100) -> dict:
    return {"lo": lo, "step": step, "tail": True, "empty_stop": 3}


def test_resumed_tail_keeps_its_empty_run() -> None:
    """Two empty windows seen before a crash mean one more empty window ends the tail."""
    calls = {"n": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"events": []})

    client, _ = _client(handler)
    try:
        resumed = list(
            id_range_pages(
                client, "/events/keyset", _tail(), "events", PageState(seq=2, empty_run=2)
            )
        )
        assert len(resumed) == 1
        assert resumed[0].terminal

        fresh = list(id_range_pages(client, "/events/keyset", _tail(), "events", PageState(seq=2)))
        assert len(fresh) == 3
        assert fresh[-1].terminal
    finally:
        client.close()
    assert calls["n"] == 4


def test_tail_stops_when_every_window_fails() -> None:
    """A persistent outage above the mark must not run the tail forever."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "down"})

    client, _ = _client(handler)
    try:
        pages = list(id_range_pages(client, "/events/keyset", _tail(step=2), "events"))
    finally:
        client.close()

    assert [page.terminal for page in pages] == [False, False, True]
    assert all(page.record_count == 0 for page in pages)
    failed = [item["id"] for page in pages for item in page.response.json["fetch_failed"]]
    assert failed == ["1", "2", "3", "4", "5", "6"]


@pytest.mark.parametrize(("status", "quarantined"), [(400, True), (403, True), (404, False)])
def test_single_id_hard_error_is_quarantined_and_404_is_not(status: int, quarantined: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/events/keyset":
            return httpx.Response(500, json={"error": "down"})
        return httpx.Response(status, json={"error": "nope"})

    client, _ = _client(handler)
    try:
        (page,) = id_range_pages(client, "/events/keyset", {"lo": 7, "step": 1, "hi": 7}, "events")
    finally:
        client.close()

    assert page.terminal
    if quarantined:
        assert page.response.status == 200
        assert page.response.json["fetch_failed"] == [{"id": "7", "reason": "fetch_failed"}]
    else:
        assert page.response.status == 404
        assert page.records == []


def test_id_window_sends_the_event_include_flags() -> None:
    seen: list[httpx.QueryParams] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.params)
        return httpx.Response(200, json={"events": []})

    client, _ = _client(handler)
    params = {"lo": 1, "step": 1, "hi": 1, "include_chat": True}
    try:
        list(id_range_pages(client, "/events/keyset", params, "events"))
    finally:
        client.close()

    assert seen[0]["include_chat"] == "true"
    assert "include_template" not in seen[0]


def test_id_window_retry_after_is_capped_at_the_window_cap() -> None:
    """One long Retry-After must not hold the shared bucket for an hour."""
    calls = {"n": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "3600"})
        return httpx.Response(200, json={"events": []})

    client, clock = _client(handler)
    try:
        response = client.get("/events/keyset", {"limit": 1}, max_retries=4, backoff_cap_s=30.0)
    finally:
        client.close()

    assert response.status == 200
    assert clock.sleeps == [30.0]
