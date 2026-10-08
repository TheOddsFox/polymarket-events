"""Stage functions shared by the CLI and Dagster. Each runs one stage under the run lock.

Capture, load, dbt, and publish are kept separate so an operator can rerun any suffix
(for example, replay: load, dbt, publish) without refetching from Gamma.

Every stage writes one row to ``stage_runs`` in the ledger, with its status, counts, and
error, so operators can see what ran and how long it took without reading logs.
"""

from __future__ import annotations

import subprocess
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import CaptureRuntime, CaptureSummary, run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.dbt_runner import run_dbt
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.ids import MODES, iso_utc, utc_now
from oddsfox_catalogue.load.runner import LoadRuntime, LoadSummary, load_pending
from oddsfox_catalogue.publish import ReleaseInfo, publish_release
from oddsfox_catalogue.runlock import current_git_sha, run_lock
from oddsfox_catalogue.warehouse import read_open_event_ids

__all__ = [
    "MODES",
    "capture_stage",
    "dbt_stage",
    "load_stage",
    "publish_stage",
    "refresh",
]


@dataclass
class _StageRun:
    """What a stage reports about itself. The context manager persists it."""

    counts: dict[str, Any] = field(default_factory=dict)
    status: str = "succeeded"
    error: str | None = None


@contextmanager
def _recorded(settings: Settings, stage: str, batch_id: str | None = None) -> Iterator[_StageRun]:
    """Record one ``stage_runs`` row whether the stage succeeds, reports failure, or raises."""
    run = _StageRun()
    run_id = uuid.uuid4().hex
    started = iso_utc(utc_now())
    git_sha = current_git_sha(settings.root)
    try:
        yield run
    except BaseException as exc:
        run.status, run.error = "failed", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        ledger = Ledger(settings.ledger_path)
        try:
            ledger.record_stage_run(
                run_id=run_id,
                batch_id=batch_id,
                stage=stage,
                started_at=started,
                finished_at=iso_utc(utc_now()),
                status=run.status,
                counts=run.counts,
                git_sha=git_sha,
                error=run.error,
            )
        finally:
            ledger.close()


def capture_stage(
    settings: Settings,
    mode: str,
    *,
    transport: httpx.BaseTransport | None = None,
    now: Callable[[], datetime] = utc_now,
) -> CaptureSummary:
    if mode not in MODES:
        raise ValueError(f"unknown capture mode {mode!r}")
    with run_lock(settings.run_lock_path):
        client = GammaClient(settings.gamma, transport=transport, now=now)
        ledger = Ledger(settings.ledger_path)
        try:
            runtime = CaptureRuntime(
                settings=settings,
                client=client,
                ledger=ledger,
                now=now,
                open_event_ids=lambda: read_open_event_ids(settings.warehouse_path),
                git_sha=current_git_sha(settings.root),
            )
            with _recorded(settings, f"capture:{mode}") as run:
                summary = run_capture(runtime, mode)
                run.counts = {
                    "pages_written": summary.pages_written,
                    "pages_adopted": summary.pages_adopted,
                    "records": summary.records,
                    "status": summary.status,
                }
                run.status = "succeeded" if summary.status == "captured" else "failed"
                if summary.status != "captured":
                    run.error = f"capture ended with status {summary.status}"
                return summary
        finally:
            ledger.close()
            client.close()


def load_stage(
    settings: Settings,
    *,
    batch_id: str | None = None,
    now: Callable[[], datetime] = utc_now,
) -> LoadSummary:
    with run_lock(settings.run_lock_path), _recorded(settings, "load", batch_id) as run:
        ledger = Ledger(settings.ledger_path)
        try:
            summary = load_pending(LoadRuntime(settings=settings, ledger=ledger, now=now), batch_id)
        finally:
            ledger.close()
        run.counts = {
            "pages_loaded": summary.pages_loaded,
            "event_rows": summary.event_rows,
            "market_rows": summary.market_rows,
            "quarantined": summary.quarantined,
        }
        return summary


def dbt_stage(settings: Settings, args: list[str]) -> subprocess.CompletedProcess[str]:
    with run_lock(settings.run_lock_path), _recorded(settings, f"dbt:{' '.join(args)}") as run:
        result = run_dbt(settings, args)
        run.counts = {"returncode": result.returncode}
        if result.returncode != 0:
            run.status = "failed"
            run.error = result.stdout[-2000:]
        return result


def publish_stage(
    settings: Settings,
    *,
    now: Callable[[], datetime] = utc_now,
) -> ReleaseInfo:
    with run_lock(settings.run_lock_path), _recorded(settings, "publish") as run:
        release = publish_release(settings, now=now(), git_sha=current_git_sha(settings.root))
        run.counts = {"release_id": release.release_id}
        return release


def refresh(settings: Settings, mode: str) -> dict[str, Any]:
    """Capture, load, build, and publish, in that order. Stops at the first failure."""
    capture = capture_stage(settings, mode)
    if capture.status != "captured":
        return {"stage": "capture", "status": capture.status, "batch_id": capture.batch_id}
    load = load_stage(settings)
    built = dbt_stage(settings, ["build"])
    if built.returncode != 0:
        return {"stage": "dbt", "status": "failed", "stderr": built.stdout[-4000:]}
    release = publish_stage(settings)
    return {
        "stage": "publish",
        "status": "published",
        "batch_id": capture.batch_id,
        "loaded_pages": load.pages_loaded,
        "release_id": release.release_id,
    }
