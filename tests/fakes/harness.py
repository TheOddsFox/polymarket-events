"""Deterministic clock for bounded HTTP and retry tests."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime

FIXED_NOW = datetime(2026, 10, 8, 6, 0, 0, tzinfo=UTC)


@dataclass
class FakeClock:
    """Monotonic clock whose ``sleep`` advances time instantly and records the request."""

    t: float = 0.0
    sleeps: list[float] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def __call__(self) -> float:
        with self._lock:
            return self.t

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            self.t += seconds
