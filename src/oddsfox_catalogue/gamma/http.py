"""Pooled, rate-limited, retrying HTTP access to the Gamma API."""

from __future__ import annotations

import json
import random
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from oddsfox_catalogue.config import GammaSettings

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
DEFAULT_USER_AGENT = "oddsfox-catalogue/0.1 (+local batch pipeline)"


class GammaError(Exception):
    """Base class for capture failures."""


class MalformedResponse(GammaError):
    """Non-JSON, wrong shape, or a non-retryable HTTP error. Fails the scan."""


class RetriesExhausted(GammaError):
    """Transient failures persisted beyond the retry budget."""


class CursorExpired(GammaError):
    """The server rejected an ``after_cursor`` (HTTP 422). The scan must restart."""


class ScanFailed(GammaError):
    """A pagination invariant was violated (repeated cursor, loop, and so on)."""


@dataclass(frozen=True)
class Response:
    endpoint: str
    params: Mapping[str, Any]
    status: int
    body: bytes
    json: Any
    retries: int
    latency_s: float
    received_at: datetime
    headers: Mapping[str, str] = field(default_factory=dict)


class TokenBucket:
    """Blocking limiter that spaces requests at ``rate`` per second."""

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self._interval = 1.0 / rate
        self._clock = clock
        self._sleep = sleep
        self._next_at = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            wait = self._next_at - now
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
            self._next_at = max(now, self._next_at) + self._interval


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Seconds to wait from a ``Retry-After`` header (delta-seconds or HTTP-date)."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        return float(text)
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now).total_seconds())


def backoff_delay(
    attempt: int,
    base: float,
    cap: float,
    rng: random.Random,
) -> float:
    """Full-jitter exponential backoff: uniform in [0, min(cap, base * 2**attempt)]."""
    ceiling = min(cap, base * (2**attempt))
    return rng.uniform(0.0, ceiling)


def encode_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """Convert Python values to Gamma query encoding. None is dropped, bools are lowercase."""
    out: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            out[key] = "true" if value else "false"
        elif isinstance(value, list | tuple):
            out[key] = [str(v) for v in value]
        else:
            out[key] = value
    return out


@dataclass
class RequestStats:
    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    server_errors: int = 0
    transport_errors: int = 0
    not_found: int = 0
    total_latency_s: float = 0.0

    def as_dict(self) -> dict[str, float | int]:
        return {
            "requests": self.requests,
            "retries": self.retries,
            "rate_limited": self.rate_limited,
            "server_errors": self.server_errors,
            "transport_errors": self.transport_errors,
            "not_found": self.not_found,
            "total_latency_s": round(self.total_latency_s, 6),
        }


class GammaClient:
    """Single pooled client used for every capture request."""

    def __init__(
        self,
        settings: GammaSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings
        self._now = now
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._limiter = TokenBucket(settings.requests_per_second, clock=clock, sleep=sleep)
        self._clock = clock
        self.stats = RequestStats()
        self._client = httpx.Client(
            base_url=settings.base_url,
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_s,
                read=settings.read_timeout_s,
                write=10.0,
                pool=10.0,
            ),
            headers={"Accept": "application/json", "User-Agent": DEFAULT_USER_AGENT},
            transport=transport,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=2),
            follow_redirects=False,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> GammaClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get(self, endpoint: str, params: Mapping[str, Any] | None = None) -> Response:
        """GET an endpoint with retries. 404 returns a Response; callers decide what it means."""
        clean = encode_params(params or {})
        retries = 0
        while True:
            self._limiter.acquire()
            started = self._clock()
            self.stats.requests += 1
            try:
                raw = self._client.get(endpoint, params=clean)
            except httpx.TransportError as exc:
                self.stats.transport_errors += 1
                if retries >= self._settings.max_retries:
                    raise RetriesExhausted(f"{endpoint}: transport error: {exc}") from exc
                retries += 1
                self.stats.retries += 1
                self._sleep(self._delay(retries, None))
                continue

            latency = self._clock() - started
            self.stats.total_latency_s += latency
            status = raw.status_code

            if status in RETRYABLE_STATUS:
                if status == 429:
                    self.stats.rate_limited += 1
                else:
                    self.stats.server_errors += 1
                if retries >= self._settings.max_retries:
                    raise RetriesExhausted(f"{endpoint}: HTTP {status} after {retries} retries")
                retries += 1
                self.stats.retries += 1
                retry_after = parse_retry_after(raw.headers.get("Retry-After"), self._now())
                self._sleep(self._delay(retries, retry_after))
                continue

            if status == 422 and "after_cursor" in clean:
                raise CursorExpired(f"{endpoint}: cursor rejected: {raw.text[:200]}")
            if status == 404:
                self.stats.not_found += 1
                return Response(
                    endpoint=endpoint,
                    params=clean,
                    status=status,
                    body=raw.content,
                    json=None,
                    retries=retries,
                    latency_s=latency,
                    received_at=self._now(),
                    headers=dict(raw.headers),
                )
            if status >= 400:
                raise MalformedResponse(f"{endpoint}: HTTP {status}: {raw.text[:200]}")

            try:
                parsed = json.loads(raw.content)
            except (ValueError, UnicodeDecodeError) as exc:
                raise MalformedResponse(f"{endpoint}: body is not JSON") from exc

            return Response(
                endpoint=endpoint,
                params=clean,
                status=status,
                body=raw.content,
                json=parsed,
                retries=retries,
                latency_s=latency,
                received_at=self._now(),
                headers=dict(raw.headers),
            )

    def _delay(self, retry_number: int, retry_after: float | None) -> float:
        """The server's Retry-After wins when present; otherwise jittered backoff from base."""
        if retry_after is not None:
            return retry_after
        return backoff_delay(
            retry_number - 1,
            self._settings.backoff_base_s,
            self._settings.backoff_cap_s,
            self._rng,
        )
