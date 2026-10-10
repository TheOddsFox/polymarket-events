import json
import random
import sys
import threading
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest

from fakes.harness import FakeClock
from oddsfox_catalogue.config import GammaSettings, load_settings
from oddsfox_catalogue.gamma.http import (
    CursorExpired,
    GammaClient,
    MalformedResponse,
    RetriesExhausted,
    TokenBucket,
    backoff_delay,
    encode_params,
    parse_retry_after,
)

NOW = datetime(2026, 10, 8, 6, 0, tzinfo=UTC)
REPO_ROOT = Path(__file__).resolve().parents[2]


def _client(
    handler, *, clock: FakeClock | None = None, **overrides
) -> tuple[GammaClient, FakeClock]:
    settings = GammaSettings(
        base_url="https://gamma.fake.test",
        requests_per_second=1000.0,
        max_retries=overrides.pop("max_retries", 5),
        backoff_base_s=1.0,
        backoff_cap_s=60.0,
        **overrides,
    )
    clock = clock or FakeClock()
    client = GammaClient(
        settings,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: NOW,
        rng=random.Random(7),
    )
    return client, clock


def test_retry_after_seconds_and_http_date() -> None:
    assert parse_retry_after("12", NOW) == 12.0
    later = format_datetime(NOW + timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(later, NOW) == pytest.approx(30.0)
    past = format_datetime(NOW - timedelta(seconds=30), usegmt=True)
    assert parse_retry_after(past, NOW) == 0.0
    assert parse_retry_after("soon", NOW) is None
    assert parse_retry_after(None, NOW) is None


def test_backoff_is_bounded_by_cap_and_grows() -> None:
    rng = random.Random(1)
    samples = [backoff_delay(attempt, 1.0, 8.0, rng) for attempt in range(6) for _ in range(50)]
    assert all(0.0 <= s <= 8.0 for s in samples)
    assert max(backoff_delay(0, 1.0, 8.0, random.Random(i)) for i in range(50)) <= 1.0
    assert max(backoff_delay(5, 1.0, 8.0, random.Random(i)) for i in range(200)) > 4.0


def test_token_bucket_spaces_requests() -> None:
    clock = FakeClock()
    bucket = TokenBucket(2.0, clock=clock, sleep=clock.sleep)
    for _ in range(4):
        bucket.acquire()
    assert clock.t == pytest.approx(1.5)  # four requests at 2/s need three 0.5s gaps


def test_grants_from_several_threads_stay_one_interval_apart() -> None:
    """Overlapping waits must not let two grants land inside one interval.

    Each thread's last clock read is its grant time: the bucket reads the clock under its
    lock and grants in the same step. A tiny switch interval lets threads interleave between
    that read and the bucket's write, so a bucket without its lock grants twice at one instant.
    """
    clock_lock = threading.Lock()
    now = {"t": 0.0}
    local = threading.local()

    def clock() -> float:
        with clock_lock:
            local.grant = now["t"]
            return now["t"]

    def sleep(seconds: float) -> None:
        with clock_lock:
            now["t"] += seconds

    bucket = TokenBucket(5.0, clock=clock, sleep=sleep)
    grants: list[float] = []
    grants_lock = threading.Lock()

    def worker() -> None:
        for _ in range(100):
            bucket.acquire()
            with grants_lock:
                grants.append(local.grant)

    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        threads = [threading.Thread(target=worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        sys.setswitchinterval(previous_interval)

    grants.sort()
    assert len(grants) == 1600
    gaps = [later - earlier for earlier, later in zip(grants, grants[1:], strict=False)]
    assert min(gaps) >= 0.2 - 1e-9


def test_encode_params_uses_gamma_query_conventions() -> None:
    encoded = encode_params({"closed": False, "limit": 100, "id": [1, 2], "offset": None})
    assert encoded == {"closed": "false", "limit": 100, "id": ["1", "2"]}


def test_penalize_pauses_a_spawned_client_once() -> None:
    calls = {"n": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "4"})
        return httpx.Response(200, json={"events": []})

    client, clock = _client(handler)
    child = client.spawn()
    try:
        assert client.get("/events/keyset", {"limit": 1}).status == 200
        assert clock.sleeps == [4.0]
        assert child.get("/events/keyset", {"limit": 1}).status == 200
        assert clock.sleeps[0] == 4.0
        assert all(pause < 1 for pause in clock.sleeps[1:])
        assert client.stats.requests == 3
        assert child.stats is client.stats
    finally:
        child.close()
        client.close()


def test_overlapping_penalties_share_one_deadline() -> None:
    clock = FakeClock()
    bucket = TokenBucket(1000.0, clock=clock, sleep=clock.sleep)
    bucket.penalize(10)
    bucket.penalize(10)
    bucket.acquire()
    assert clock.t == pytest.approx(10)


def test_429_honours_retry_after_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "4"})
        return httpx.Response(200, json={"events": []})

    client, clock = _client(handler)
    response = client.get("/events/keyset", {"limit": 1})
    assert response.status == 200
    assert response.retries == 1
    assert clock.sleeps == [4.0]
    assert client.stats.rate_limited == 1


def test_5xx_is_retried_with_backoff_and_counted(caplog: pytest.LogCaptureFixture) -> None:
    statuses = iter([503, 502, 200])

    def handler(_: httpx.Request) -> httpx.Response:
        status = next(statuses)
        return httpx.Response(status, json={"events": [], "after_cursor": "secret-cursor"})

    client, clock = _client(handler)
    budget = client._settings.max_retries  # the budget this client actually enforces
    with caplog.at_level("WARNING", logger="oddsfox_catalogue.gamma.http"):
        response = client.get("/events/keyset", {"after_cursor": "secret-cursor"})
    assert response.retries == 2
    assert len(clock.sleeps) == 2
    assert client.stats.server_errors == 2
    warnings = [record.getMessage() for record in caplog.records]
    assert warnings == [
        f"gamma /events/keyset HTTP 503, retry 1 of {budget}",
        f"gamma /events/keyset HTTP 502, retry 2 of {budget}",
    ]
    assert all("secret-cursor" not in message for message in warnings)


def test_retry_budget_is_finite() -> None:
    client, _ = _client(lambda r: httpx.Response(500), max_retries=3)
    with pytest.raises(RetriesExhausted):
        client.get("/events/keyset", {})
    assert client.stats.requests == 4  # one attempt plus three retries


def test_transport_errors_are_retried() -> None:
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise httpx.ConnectError("reset", request=request)
        return httpx.Response(200, json={"events": []})

    client, _ = _client(handler)
    assert client.get("/events/keyset", {}).status == 200
    assert client.stats.transport_errors == 2


def test_422_with_cursor_means_cursor_expired() -> None:
    client, _ = _client(lambda r: httpx.Response(422, json={"error": "bad cursor"}))
    with pytest.raises(CursorExpired):
        client.get("/events/keyset", {"after_cursor": "abc"})


def test_422_without_cursor_is_malformed_not_expired() -> None:
    client, _ = _client(lambda r: httpx.Response(422, json={"error": "offset not allowed"}))
    with pytest.raises(MalformedResponse):
        client.get("/events/keyset", {"offset": 5})


def test_non_json_success_body_is_malformed() -> None:
    client, _ = _client(lambda r: httpx.Response(200, content=b"<html>"))
    with pytest.raises(MalformedResponse):
        client.get("/events/keyset", {})


def test_404_is_returned_not_raised() -> None:
    client, _ = _client(lambda r: httpx.Response(404, json={"error": "gone"}))
    response = client.get("/events/999", {})
    assert response.status == 404
    assert response.json is None
    assert client.stats.not_found == 1


def test_other_client_errors_fail_fast() -> None:
    client, _ = _client(lambda r: httpx.Response(400, text="bad"))
    with pytest.raises(MalformedResponse):
        client.get("/events/keyset", {})
    assert client.stats.requests == 1


def test_requests_carry_gamma_params_and_user_agent() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=json.dumps({"events": []}).encode())

    client, _ = _client(handler)
    client.get("/events/keyset", {"closed": False, "id": [10, 11], "limit": 100})
    url = str(seen[0].url)
    assert "closed=false" in url and "id=10" in url and "id=11" in url
    assert seen[0].headers["user-agent"].startswith("oddsfox-catalogue")


def _committed_client(handler) -> tuple[GammaClient, FakeClock]:
    """A client built exactly as production builds it: committed TOML defaults, no env overrides."""
    settings = load_settings(root=REPO_ROOT, env={}).gamma
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


def test_committed_budget_survives_six_server_errors() -> None:
    responses = iter([500, 500, 500, 500, 500, 500, 200])

    def handler(_: httpx.Request) -> httpx.Response:
        status = next(responses)
        return httpx.Response(status, json={"events": []})

    client, clock = _committed_client(handler)
    response = client.get("/events/keyset", {})
    assert response.status == 200
    assert response.retries == 6
    assert client.stats.server_errors == 6
    assert client.stats.requests == 7
    assert len(clock.sleeps) == 6


def test_committed_budget_is_exhausted_on_the_thirteenth_response() -> None:
    client, clock = _committed_client(lambda r: httpx.Response(500, json={"error": "down"}))
    with pytest.raises(RetriesExhausted):
        client.get("/events/keyset", {})
    assert client.stats.requests == 13
    assert client.stats.server_errors == 13
    assert len(clock.sleeps) == 12


def test_committed_settings_fail_400_on_first_response() -> None:
    client, clock = _committed_client(lambda r: httpx.Response(400, text="bad"))
    with pytest.raises(MalformedResponse):
        client.get("/events/keyset", {})
    assert client.stats.requests == 1
    assert clock.sleeps == []
