"""Stage functions shared by the CLI and Dagster. Each runs one stage under the run lock.

Capture, load, dbt, and publish are kept separate so an operator can rerun any suffix
(for example, replay: load, dbt, publish) without refetching from Gamma.

Every stage writes one row to ``stage_runs`` in the ledger, with its status, counts, and
error, so operators can see what ran and how long it took without reading logs.
"""

from __future__ import annotations

import logging
import subprocess
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import CaptureRuntime, CaptureSummary, run_capture
from oddsfox_catalogue.certification import BuildInvalid, certify_build, mark_dirty
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.dbt_runner import is_selected_dbt_command, run_dbt, with_quality_vars
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.ids import MODES, iso_utc, utc_now
from oddsfox_catalogue.limits import enforce_storage_limits
from oddsfox_catalogue.load.runner import LoadRuntime, LoadSummary, load_pending
from oddsfox_catalogue.publish import PublishBlocked, ReleaseInfo, current_release, publish_release
from oddsfox_catalogue.runlock import current_git_sha, run_lock
from oddsfox_catalogue.semantics import bounded_connection
from oddsfox_catalogue.warehouse import read_refresh_event_ids, read_refresh_market_ids
from oddsfox_catalogue.warehouse_version import ensure_warehouse_contract

logger = logging.getLogger(__name__)

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


@contextmanager
def capture_runtime(
    settings: Settings,
    *,
    transport: httpx.BaseTransport | None = None,
    now: Callable[[], datetime] = utc_now,
) -> Iterator[CaptureRuntime]:
    enforce_storage_limits(settings)
    ensure_warehouse_contract(settings)
    client = GammaClient(
        settings.gamma,
        transport=transport,
        now=now,
        max_body_bytes=settings.capture.max_response_bytes,
        max_requests=settings.capture.max_requests,
        max_download_bytes=settings.capture.max_download_bytes,
        max_duration_s=settings.capture.max_duration_s,
    )
    ledger = Ledger(settings.ledger_path)
    try:
        runtime = CaptureRuntime(
            settings=settings,
            client=client,
            ledger=ledger,
            now=now,
            open_event_ids=lambda: read_refresh_event_ids(
                settings.warehouse_path, settings=settings
            ),
            open_market_ids=lambda: read_refresh_market_ids(
                settings.warehouse_path, settings=settings
            ),
            git_sha=current_git_sha(settings.root),
        )
        yield runtime
    except Exception as exc:
        exc.http_attempts = client.stats.requests
        exc.downloaded_bytes = client.stats.downloaded_bytes
        raise
    finally:
        ledger.close()
        client.close()


def capture_stage(
    settings: Settings,
    mode: str,
    *,
    market_ids: Sequence[str] = (),
    event_ids: Sequence[str] = (),
    resume: str | None = None,
    transport: httpx.BaseTransport | None = None,
    now: Callable[[], datetime] = utc_now,
) -> CaptureSummary:
    if mode not in MODES:
        raise ValueError(f"unknown capture mode {mode!r}")
    with (
        run_lock(settings.run_lock_path),
        capture_runtime(settings, transport=transport, now=now) as runtime,
    ):
        return run_capture(runtime, mode, market_ids=market_ids, event_ids=event_ids, resume=resume)


def load_stage(
    settings: Settings,
    *,
    batch_id: str | None = None,
    now: Callable[[], datetime] = utc_now,
) -> LoadSummary:
    with run_lock(settings.run_lock_path), _recorded(settings, "load", batch_id) as run:
        enforce_storage_limits(settings)
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
        enforce_storage_limits(settings)
        return summary


def open_event_drop_warning(
    settings: Settings, warn: Callable[..., object] | None = None
) -> dict[str, Any] | None:
    """Compare with the last published baseline; failed builds cannot move it."""
    pointer = current_release(settings)
    if pointer is None:
        return None
    with bounded_connection(settings) as connection:
        previous = int(
            connection.execute(
                "SELECT count(*) FROM read_parquet(?) WHERE is_open = true",
                [str(settings.published_dir / pointer["path"] / "events.parquet")],
            ).fetchone()[0]
        )
        latest = int(
            connection.execute(
                "SELECT count(*) FROM marts.mart_event_catalogue WHERE is_open = true"
            ).fetchone()[0]
        )
    if previous <= 0 or latest >= previous:
        return None
    drop_pct = (previous - latest) / previous * 100.0
    quality = settings.quality
    if drop_pct > quality.open_events_drop_error_pct:
        raise PublishBlocked(
            f"open events dropped {drop_pct:.2f}% against the published baseline "
            f"({previous} to {latest}); limit {quality.open_events_drop_error_pct}%"
        )
    if not quality.open_events_drop_warn_pct <= drop_pct <= quality.open_events_drop_error_pct:
        return None
    return {
        "open_events_drop_warn": True,
        "open_events_previous": previous,
        "open_events_latest": latest,
        "open_events_drop_pct": round(drop_pct, 2),
    }


@contextmanager
def dbt_execution(settings: Settings, args: Sequence[str]):
    """CLI and Dagster hold the same lock through execution and certification."""
    with run_lock(settings.run_lock_path), _recorded(settings, f"dbt:{' '.join(args)}") as run:
        enforce_storage_limits(settings)
        ensure_warehouse_contract(settings)
        if args and args[0] != "parse":
            mark_dirty(settings, "dbt execution")
        yield run
        enforce_storage_limits(settings)


def dbt_stage(settings: Settings, args: list[str]) -> subprocess.CompletedProcess[str]:
    with dbt_execution(settings, args) as run:
        result = run_dbt(settings, with_quality_vars(settings, args))
        enforce_storage_limits(settings)
        run.counts = {"returncode": result.returncode}
        if result.returncode != 0:
            run.status = "failed"
            run.error = result.stdout[-2000:]
        elif args and args[0] == "build" and not is_selected_dbt_command(args):
            try:
                receipt = certify_build(settings, result.target_path)
                warning = receipt["warning"]
                run.counts["certified"] = True
            except (BuildInvalid, PublishBlocked) as exc:
                run.status, run.error = "failed", str(exc)
                run.counts["returncode"] = 1
                return subprocess.CompletedProcess(
                    result.args, 1, result.stdout + str(exc), result.stderr
                )
            if warning is not None:
                run.counts.update(warning)
                logger.warning(
                    "open events dropped %.2f%% (%s to %s), inside the warn band",
                    warning["open_events_drop_pct"],
                    warning["open_events_previous"],
                    warning["open_events_latest"],
                )
        elif args and args[0] != "parse":
            run.counts["certified"] = False
        return result


def publish_stage(
    settings: Settings,
    *,
    now: Callable[[], datetime] = utc_now,
) -> ReleaseInfo:
    with run_lock(settings.run_lock_path), _recorded(settings, "publish") as run:
        enforce_storage_limits(settings)
        release = publish_release(settings, now=now(), git_sha=current_git_sha(settings.root))
        run.counts = {"release_id": release.release_id}
        return release


def refresh(
    settings: Settings,
    mode: str,
    *,
    market_ids: Sequence[str] = (),
    event_ids: Sequence[str] = (),
    resume: str | None = None,
) -> dict[str, Any]:
    """Capture, load, build, and publish, in that order. Stops at the first failure."""
    logger.info("refresh %s: capture", mode)
    capture = capture_stage(
        settings, mode, market_ids=market_ids, event_ids=event_ids, resume=resume
    )
    if capture.status != "captured":
        return {"stage": "capture", "status": capture.status, "batch_id": capture.batch_id}
    logger.info("refresh %s: load", mode)
    load = load_stage(settings)
    logger.info("refresh %s: dbt build", mode)
    built = dbt_stage(settings, ["build"])
    if built.returncode != 0:
        return {"stage": "dbt", "status": "failed", "stderr": built.stdout[-4000:]}
    logger.info("refresh %s: publish", mode)
    release = publish_stage(settings)
    return {
        "stage": "publish",
        "status": "published",
        "batch_id": capture.batch_id,
        "loaded_pages": load.pages_loaded,
        "release_id": release.release_id,
        "http_attempts": capture.http_attempts,
        "downloaded_bytes": capture.downloaded_bytes,
        "duration_s": capture.duration_s,
    }
