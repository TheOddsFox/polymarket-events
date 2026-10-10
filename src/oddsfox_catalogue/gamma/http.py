"""Pooled, rate-limited, retrying HTTP access to the Gamma API."""

from __future__ import annotations

import json
import logging
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

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
DEFAULT_USER_AGENT = "oddsfox-catalogue/0.1 (+local batch pipeline)"
# The longest pause a Retry-After header can impose. A longer header is cut to this, so one
# response cannot stall the shared bucket for hours. The retry budget still applies.
RETRY_AFTER_CEILING_S = 900.0


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


class CaptureStopped(GammaError):
    """The worker pool was asked to drain. The scan stays running for resume."""


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
        self._stop: threading.Event | None = None

    def bind_stop(self, stop: threading.Event | None) -> None:
        """Once ``stop`` is set, a pause raises ``CaptureStopped`` instead of sleeping on.

        A real sleep checks ``stop`` every 0.25 s, so a rate-limit pause cannot hold the drain.
        """
        self._stop = stop

    def _pause(self, seconds: float) -> None:
        if self._stop is not None and self._stop.is_set():
            raise CaptureStopped()
        # Tests use an instant clock. Only a real sleep needs to be sliced.
        if self._stop is None or self._sleep is not time.sleep or seconds <= 0.25:
            self._sleep(seconds)
            return
        remaining = seconds
        while remaining > 0:
            if self._stop is not None and self._stop.is_set():
                raise CaptureStopped()
            step = min(0.25, remaining)
            self._sleep(step)
            remaining -= step

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = self._clock()
                wait = self._next_at - now
                if wait <= 0:
                    self._next_at = max(now, self._next_at) + self._interval
                    return
            # Sleep outside the lock so another worker can record a penalty
            # against the same deadline instead of queueing behind this wait.
            self._pause(wait)

    def penalize(self, seconds: float) -> None:
        """Hold every client on this bucket for ``seconds`` after a retryable failure.

        The next ``acquire`` waits until the pause has elapsed. Overlapping
        penalties share that deadline instead of stacking.
        """
        if seconds <= 0:
            return
        with self._lock:
            now = self._clock()
            # Push the schedule out. Do not sleep here: the next acquire waits
            # until _next_at, and a sleep under this lock would stack penalties.
            self._next_at = max(self._next_at, now + seconds)


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
    """Pooled client for Gamma. ``spawn`` shares the rate limiter and stats."""

    def __init__(
        self,
        settings: GammaSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        rng: random.Random | None = None,
        limiter: TokenBucket | None = None,
        stats: RequestStats | None = None,
        stats_lock: threading.Lock | None = None,
    ) -> None:
        self._settings = settings
        self._now = now
        self._sleep = sleep
        self._clock = clock
        self._rng = rng or random.Random()
        self._transport = transport
        self._limiter = limiter or TokenBucket(
            settings.requests_per_second, clock=clock, sleep=sleep
        )
        self.stats = stats or RequestStats()
        self._stats_lock = stats_lock or threading.Lock()
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

    def now(self) -> datetime:
        return self._now()

    def spawn(self) -> GammaClient:
        """A worker client with its own connection and its own backoff RNG.

        The token bucket, request stats, clock, and transport are shared with
        the parent, so the pool stays inside one rate limit.
        """
        return GammaClient(
            self._settings,
            transport=self._transport,
            clock=self._clock,
            sleep=self._sleep,
            now=self._now,
            limiter=self._limiter,
            stats=self.stats,
            stats_lock=self._stats_lock,
        )

    def _add(self, **amounts: float) -> None:
        with self._stats_lock:
            for name, amount in amounts.items():
                setattr(self.stats, name, getattr(self.stats, name) + amount)

    def get(
        self,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        *,
        max_retries: int | None = None,
        backoff_cap_s: float | None = None,
    ) -> Response:
        """GET an endpoint with retries. 404 returns a Response; callers decide what it means.

        ``max_retries`` and ``backoff_cap_s`` override the client settings for this call.
        Id-window fetches use a shorter budget than keyset crawls.
        """
        clean = encode_params(params or {})
        retry_budget = self._settings.max_retries if max_retries is None else max_retries
        backoff_cap = self._settings.backoff_cap_s if backoff_cap_s is None else backoff_cap_s
        retries = 0
        while True:
            self._limiter.acquire()
            started = self._clock()
            self._add(requests=1)
            try:
                raw = self._client.get(endpoint, params=clean)
            except (httpx.TransportError, httpx.DecodingError) as exc:
                # A body that will not decode is a failed request, as a dropped connection is.
                self._add(transport_errors=1)
                if retries >= retry_budget:
                    raise RetriesExhausted(f"{endpoint}: transport error: {exc}") from exc
                retries += 1
                self._add(retries=1)
                logger.warning(
                    "gamma %s transport error, retry %s of %s: %s",
                    endpoint,
                    retries,
                    retry_budget,
                    exc,
                )
                self._limiter.penalize(self._delay(retries, None, backoff_cap))
                continue

            latency = self._clock() - started
            self._add(total_latency_s=latency)
            status = raw.status_code

            if status in RETRYABLE_STATUS:
                if status == 429:
                    self._add(rate_limited=1)
                else:
                    self._add(server_errors=1)
                if retries >= retry_budget:
                    raise RetriesExhausted(f"{endpoint}: HTTP {status} after {retries} retries")
                retries += 1
                self._add(retries=1)
                logger.warning(
                    "gamma %s HTTP %s, retry %s of %s",
                    endpoint,
                    status,
                    retries,
                    retry_budget,
                )
                retry_after = parse_retry_after(raw.headers.get("Retry-After"), self._now())
                self._limiter.penalize(self._delay(retries, retry_after, backoff_cap, status))
                continue

            if status == 422 and "after_cursor" in clean:
                raise CursorExpired(f"{endpoint}: cursor rejected: {raw.text[:200]}")
            if status == 404:
                self._add(not_found=1)
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

    def _delay(
        self,
        retry_number: int,
        retry_after: float | None,
        backoff_cap: float,
        status: int | None = None,
    ) -> float:
        """Retry-After when the server sends one, otherwise jittered backoff from base.

        A Retry-After is honoured up to ``RETRY_AFTER_CEILING_S``, whatever the window cap, so
        a long throttle is waited out instead of being cut short at the cap. Jittered backoff
        never exceeds ``backoff_cap``. A 429 without Retry-After waits at least the base backoff.
        Full jitter can return almost nothing, and a near-zero penalty would not slow the pool.
        """
        if retry_after is not None:
            return min(retry_after, RETRY_AFTER_CEILING_S)
        delay = backoff_delay(
            retry_number - 1,
            self._settings.backoff_base_s,
            backoff_cap,
            self._rng,
        )
        if status == 429:
            return max(delay, min(self._settings.backoff_base_s, backoff_cap))
        return delay
