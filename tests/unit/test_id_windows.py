"""Id-window fetches: tail termination, single-id failures, resume state, and Retry-After ceilings."""

from __future__ import annotations

import random
from datetime import UTC, datetime

import httpx
import pytest

from fakes.harness import FakeClock
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.gamma.http import GammaClient, MalformedResponse, RetriesExhausted
from oddsfox_catalogue.gamma.paginators import PageState, id_list_pages, id_range_pages

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


def test_an_unresolved_window_fails_without_yielding_empty_evidence() -> None:
    client, _ = _client(lambda _: httpx.Response(500, json={"error": "down"}))
    try:
        with pytest.raises(RetriesExhausted):
            next(id_range_pages(client, "/events/keyset", _tail(step=2), "events"))
        assert 0 < client.stats.requests < 100
    finally:
        client.close()


@pytest.mark.parametrize("status", [400, 403, 404])
def test_single_id_hard_error_blocks_and_404_is_confirmed_absence(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500 if request.url.path == "/events/keyset" else status, json={"error": "nope"}
        )

    client, _ = _client(handler)
    try:
        if status != 404:
            with pytest.raises(MalformedResponse):
                next(
                    id_range_pages(
                        client, "/events/keyset", {"lo": 7, "step": 1, "hi": 7}, "events"
                    )
                )
        else:
            (page,) = id_range_pages(
                client, "/events/keyset", {"lo": 7, "step": 1, "hi": 7}, "events"
            )
            assert page.terminal and page.response.status == 404 and page.records == []
    finally:
        client.close()


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


@pytest.mark.parametrize(("retry_after", "waited"), [("3600", 900.0), ("60", 60.0)])
def test_retry_after_is_honoured_up_to_the_ceiling_not_the_window_cap(
    retry_after: str, waited: float
) -> None:
    """A long throttle is waited out up to the ceiling. The window's 30 s cap does not shorten it."""
    calls = {"n": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": retry_after})
        return httpx.Response(200, json={"events": []})

    client, clock = _client(handler)
    try:
        response = client.get("/events/keyset", {"limit": 1}, max_retries=4, backoff_cap_s=30.0)
    finally:
        client.close()

    assert response.status == 200
    assert clock.sleeps == [waited]


def test_a_market_answered_by_id_still_asks_for_its_tags() -> None:
    """A window that falls back to /markets/{id} must send include_tag, or the market has no tags."""
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/markets/7":
            seen.append(request.url.params.get("include_tag"))
            return httpx.Response(200, json={"id": "7", "tags": [{"id": "2"}]})
        return httpx.Response(500, json={"error": "down"})

    client, _ = _client(handler)
    try:
        (page,) = id_range_pages(
            client, "/markets/keyset", {"lo": 7, "step": 1, "hi": 7, "closed": True}, "markets"
        )
    finally:
        client.close()

    assert seen == ["true"]
    assert page.records == [{"id": "7", "tags": [{"id": "2"}]}]


def test_single_id_that_returns_another_record_blocks_completion() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/events/keyset":
            return httpx.Response(500, json={"error": "down"})
        return httpx.Response(200, json={"id": "8", "title": "other"})

    client, _ = _client(handler)
    try:
        with pytest.raises(MalformedResponse):
            next(id_range_pages(client, "/events/keyset", {"lo": 7, "step": 1, "hi": 7}, "events"))
    finally:
        client.close()


def test_a_transient_window_error_is_retried_before_any_bisection() -> None:
    """Four 500s fit the retry budget: the same window is retried, and it is never split."""
    seen: list[tuple[str, ...]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        ids = tuple(request.url.params.get_list("id"))
        seen.append(ids)
        if len(seen) <= 4:
            return httpx.Response(500, json={"error": "down"})
        return httpx.Response(200, json={"events": [{"id": i} for i in ids]})

    client, _ = _client(handler)
    try:
        pages = list(
            id_range_pages(client, "/events/keyset", {"lo": 1, "step": 2, "hi": 2}, "events")
        )
    finally:
        client.close()

    assert len(seen) == 5
    assert len(seen[0]) >= 2
    assert len(set(seen)) == 1
    assert not any("fetch_failed" in page.response.json for page in pages)
    assert sum(len(page.records) for page in pages) == len(seen[0])


def _rejects_id_two(request: httpx.Request) -> httpx.Response:
    """Gamma rejects any window that holds id 2 (422), and fails id 2 alone (500)."""
    ids = request.url.params.get_list("id")
    if request.url.path == "/events/2" or ids == ["2"]:
        return httpx.Response(500, json={"error": "down"})
    if "2" in ids:
        return httpx.Response(422, json={"error": "bad window"})
    return httpx.Response(200, json={"events": [{"id": i} for i in ids]})


def test_a_rejected_multi_id_window_splits_but_unresolved_singleton_blocks() -> None:
    client, _ = _client(_rejects_id_two)
    try:
        with pytest.raises(RetriesExhausted):
            next(id_range_pages(client, "/events/keyset", {"lo": 1, "step": 2, "hi": 2}, "events"))
    finally:
        client.close()


def _undecodable_for_id_two(request: httpx.Request) -> httpx.Response:
    """Gamma claims a gzip body for any window or lookup that holds id 2. The body is not gzip."""
    ids = request.url.params.get_list("id")
    if request.url.path == "/events/2" or "2" in ids:
        return httpx.Response(200, content=b"not gzip", headers={"Content-Encoding": "gzip"})
    return httpx.Response(200, json={"events": [{"id": i} for i in ids]})


def test_an_undecodable_singleton_blocks_a_split_window() -> None:
    client, _ = _client(_undecodable_for_id_two)
    try:
        with pytest.raises(RetriesExhausted):
            next(id_range_pages(client, "/events/keyset", {"lo": 1, "step": 2, "hi": 2}, "events"))
    finally:
        client.close()


def test_a_keyset_ids_chunk_resumes_after_provider_recovery() -> None:
    recovered = {"ready": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if not recovered["ready"]:
            return _rejects_id_two(request)
        return httpx.Response(
            200, json={"events": [{"id": value} for value in request.url.params.get_list("id")]}
        )

    chunk = {"limit": 100, "id": [1, 2, 3]}
    client, _ = _client(handler)
    try:
        with pytest.raises(RetriesExhausted):
            next(id_list_pages(client, "/events/keyset", chunk, "events"))
        recovered["ready"] = True
        (page,) = id_list_pages(client, "/events/keyset", chunk, "events")
        assert page.seq == 1 and page.terminal
        assert [record["id"] for record in page.records] == ["1", "2", "3"]
        assert (
            list(id_list_pages(client, "/events/keyset", chunk, "events", PageState(seq=1))) == []
        )
    finally:
        client.close()


def test_a_429_without_retry_after_waits_at_least_the_base_backoff() -> None:
    """Full jitter can return almost nothing. A rate limit with no Retry-After still slows the pool."""
    calls = {"n": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={"error": "slow down"})
        return httpx.Response(200, json={"events": []})

    settings = GammaSettings(
        base_url="https://gamma.fake.test", requests_per_second=1000.0, backoff_base_s=5.0
    )
    clock = FakeClock()
    client = GammaClient(
        settings,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: NOW,
        rng=random.Random(7),
    )
    try:
        response = client.get("/events/keyset", {"limit": 1}, max_retries=4, backoff_cap_s=30.0)
    finally:
        client.close()

    assert response.status == 200
    assert clock.sleeps == [5.0]


def test_a_single_id_hard_window_error_uses_lookup_and_blocks_unresolved_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400 if request.url.path == "/events/keyset" else 500, json={"error": "down"}
        )

    client, _ = _client(handler)
    try:
        with pytest.raises(RetriesExhausted):
            next(id_range_pages(client, "/events/keyset", {"lo": 7, "step": 1, "hi": 7}, "events"))
    finally:
        client.close()
