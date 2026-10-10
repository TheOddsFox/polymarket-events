"""Shared wiring for capture tests: deterministic clock, runtime, and temp project roots."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from fakes.fake_gamma import FakeGamma
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import CaptureRuntime
from oddsfox_catalogue.config import Settings, load_settings
from oddsfox_catalogue.dbt_runner import DBT_DIR
from oddsfox_catalogue.gamma.http import GammaClient

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


def make_settings(root: Path, overrides: dict[str, str] | None = None) -> Settings:
    env = {
        "CATALOGUE_GAMMA_BASE_URL": "https://gamma.fake.test",
        "CATALOGUE_GAMMA_BACKOFF_BASE_S": "1.0",
        # Temporary roots have no dbt project, so point at the repository's.
        "CATALOGUE_PATHS_DBT_PROJECT_DIR": str(DBT_DIR),
        "CATALOGUE_PATHS_DBT_PROFILES_DIR": str(DBT_DIR),
    }
    env.update(overrides or {})
    return load_settings(root=root, env=env)


def build_runtime(
    root: Path,
    fake: FakeGamma,
    *,
    now: datetime = FIXED_NOW,
    open_event_ids=None,
    open_market_ids=None,
    clock: FakeClock | None = None,
    max_scan_attempts: int = 3,
    env: dict[str, str] | None = None,
) -> tuple[CaptureRuntime, FakeClock]:
    settings = make_settings(root, env)
    clock = clock or FakeClock()
    client = GammaClient(
        settings.gamma,
        transport=fake.transport(),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: now,
        max_requests=settings.capture.max_requests,
        max_download_bytes=settings.capture.max_download_bytes,
        max_body_bytes=settings.capture.max_response_bytes,
        max_duration_s=settings.capture.max_duration_s,
    )
    ledger = Ledger(settings.ledger_path)
    runtime = CaptureRuntime(
        settings=settings,
        client=client,
        ledger=ledger,
        now=lambda: now,
        open_event_ids=open_event_ids,
        open_market_ids=(
            open_market_ids
            if open_market_ids is not None
            else (lambda: set())
            if open_event_ids is not None
            else None
        ),
        git_sha="testsha",
        max_scan_attempts=max_scan_attempts,
    )
    return runtime, clock
