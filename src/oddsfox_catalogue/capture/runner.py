"""Capture runner: plans a batch, executes scans, persists pages, and recovers.

Lifecycle of a batch:

1. ``capturing``. List scans are planned at creation (stage 0).
2. Once list scans complete, reference IDs are planned as ``keyset_ids``
   chunks (stage 1). Daily mode plans re-fetches of previously open events.
3. Once those complete, IDs they did not return are planned as ``single_ids``
   lookups (stage 2).
4. ``captured`` when every scan's latest attempt is complete.

Durability order per page: raw gz, manifest, ledger commit. A crash between
steps leaves at most an orphan file, which the next run adopts (if its
manifest verifies) or overwrites. The page sequence and IDs are reused, so
replays do not create duplicate observations.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import iter_scan_pages, scan_dir_for
from oddsfox_catalogue.capture.writer import (
    batch_marker_path,
    manifest_path,
    read_body,
    read_manifest,
    scan_marker_path,
    verify_page,
    write_marker,
    write_page,
)
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import fault_point
from oddsfox_catalogue.gamma.http import CaptureStopped, CursorExpired, GammaClient, ScanFailed
from oddsfox_catalogue.gamma.paginators import (
    PageResult,
    PageState,
    id_range_pages,
    keyset_pages,
    offset_pages,
    single_event_page,
)
from oddsfox_catalogue.gamma.scans import (
    ScanSpec,
    chunk,
    id_chunk_scan,
    list_scans_for,
    scan_spec_from_row,
    single_id_scan,
)
from oddsfox_catalogue.ids import (
    canonical_json,
    iso_utc,
    make_batch_id,
    make_page_id,
    make_scan_id,
    observation_date,
    utc_now,
)

logger = logging.getLogger(__name__)

MAX_SCAN_ATTEMPTS = 3
FOLLOW_UP_KINDS = frozenset({"keyset_ids", "single_ids"})
PROGRESS_EVERY_PAGES = 100


def page_progress_due(seq: int) -> bool:
    """Page 1 and every 100th page are the progress lines an operator sees."""
    return seq == 1 or seq % PROGRESS_EVERY_PAGES == 0


@dataclass
class CaptureRuntime:
    settings: Settings
    client: GammaClient
    ledger: Ledger
    now: Callable[[], datetime] = utc_now
    # Returns event IDs open at the last loaded state. Used only by daily mode.
    open_event_ids: Callable[[], set[str]] | None = None
    git_sha: str | None = None
    max_scan_attempts: int = MAX_SCAN_ATTEMPTS


@dataclass
class CaptureSummary:
    batch_id: str
    status: str
    resumed: bool
    pages_written: int = 0
    pages_adopted: int = 0
    records: int = 0
    scans: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def note_scan(self, scan_id: str) -> None:
        with self._lock:
            self.scans.append(scan_id)

    def note_pages(self, *, written: int = 0, adopted: int = 0, records: int = 0) -> None:
        with self._lock:
            self.pages_written += written
            self.pages_adopted += adopted
            self.records += records


# ---------------------------------------------------------------------------
# Batch lifecycle
# ---------------------------------------------------------------------------


def _scan_row(
    batch_id: str,
    spec: ScanSpec,
    *,
    attempt: int,
    plan_order: int,
    started_at: str,
) -> dict[str, Any]:
    return {
        "scan_id": make_scan_id(batch_id, spec.name, attempt),
        "batch_id": batch_id,
        "scan_name": spec.name,
        "attempt": attempt,
        "plan_order": plan_order,
        "kind": spec.kind,
        "endpoint": spec.endpoint,
        "record_key": spec.record_key,
        "params_json": canonical_json(spec.param_dict),
        "input_ids_json": canonical_json(list(spec.input_ids)) if spec.input_ids else None,
        "started_at": started_at,
    }


def _start_batch(rt: CaptureRuntime, mode: str) -> dict[str, Any]:
    now = rt.now()
    started = iso_utc(now)
    batch_id = make_batch_id(mode, now)
    obs_date = observation_date(now)
    specs = list_scans_for(mode, rt.settings.gamma, capture=rt.settings.capture, client=rt.client)
    rows = [
        _scan_row(batch_id, spec, attempt=1, plan_order=index, started_at=started)
        for index, spec in enumerate(specs, start=1)
    ]
    rt.ledger.create_batch(batch_id, mode, obs_date, started, rt.git_sha, rows)
    write_marker(
        batch_marker_path(_batch_dir(rt.settings, obs_date, batch_id)),
        {
            "batch_id": batch_id,
            "mode": mode,
            "observation_date": obs_date,
            "status": "capturing",
            "started_at": started,
            "git_sha": rt.git_sha,
        },
    )
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    return batch


def _batch_dir(settings: Settings, obs_date: str, batch_id: str) -> Path:
    return settings.raw_dir / obs_date / batch_id


def _latest_by_name(scans: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for scan in scans:
        current = latest.get(scan["scan_name"])
        if current is None or scan["attempt"] > current["attempt"]:
            latest[scan["scan_name"]] = scan
    return latest


def _all_complete(scans: list[dict[str, Any]]) -> bool:
    return all(s["status"] == "complete" for s in scans)


def _durable_records(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    predicate: Callable[[dict[str, Any]], bool],
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    """Yield ``(scan, (manifest, records))`` for every attempt matching ``predicate``."""
    for scan in rt.ledger.list_scans(batch["batch_id"]):
        if not predicate(scan):
            continue
        for manifest, records in iter_scan_pages(rt.settings, batch, scan):
            yield scan, {"manifest": manifest, "records": records}


def _event_ids_from(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    predicate: Callable[[dict[str, Any]], bool],
) -> set[str]:
    """IDs of event records returned by the matching scans (all durable attempts)."""
    ids: set[str] = set()
    for _, page in _durable_records(rt, batch, predicate):
        ids.update(
            str(record["id"])
            for record in page["records"]
            if isinstance(record, dict) and "id" in record
        )
    return ids


def _fetch_failed_ids(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    predicate: Callable[[dict[str, Any]], bool],
) -> set[str]:
    """Ids a list scan could not read. They must not be fetched again by keyset."""
    failed: set[str] = set()
    for scan in rt.ledger.list_scans(batch["batch_id"]):
        if not predicate(scan):
            continue
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
        )
        for manifest, _records in iter_scan_pages(rt.settings, batch, scan):
            try:
                body = json.loads(read_body(directory, manifest))
            except json.JSONDecodeError:
                continue
            if not isinstance(body, dict):
                continue
            for item in body.get("fetch_failed") or []:
                if isinstance(item, dict) and "id" in item:
                    failed.add(str(item["id"]))
    return failed


def _market_stub_ids(rt: CaptureRuntime, batch: dict[str, Any]) -> set[str]:
    """Event IDs referenced by ``market.events`` stubs, in both market shapes."""
    stubs: set[str] = set()
    for _, page in _durable_records(rt, batch, lambda s: s["kind"] not in FOLLOW_UP_KINDS):
        record_key = page["manifest"]["record_key"]
        for record in page["records"]:
            if not isinstance(record, dict):
                continue
            markets = [record] if record_key == "markets" else record.get("markets", [])
            for market in markets if isinstance(markets, list) else []:
                if not isinstance(market, dict):
                    continue
                for stub in market.get("events", []) or []:
                    if isinstance(stub, dict) and "id" in stub:
                        stubs.add(str(stub["id"]))
    return stubs


def _plan_stage0_ids(rt: CaptureRuntime, batch: dict[str, Any]) -> list[str]:
    """Event IDs that must be fetched by ID after the list scans."""
    if batch["mode"] == "daily":
        if rt.open_event_ids is None:
            raise RuntimeError("daily mode requires an open-event baseline")
        baseline = rt.open_event_ids()
        returned = _event_ids_from(rt, batch, lambda s: s["scan_name"] == "events_keyset_open")
        return sorted(baseline - returned, key=int)

    def event_scans(scan: dict[str, Any]) -> bool:
        return scan["kind"] == "id_range" and scan["record_key"] == "events"

    known = _event_ids_from(rt, batch, event_scans)
    known.update(_fetch_failed_ids(rt, batch, event_scans))
    stubs = _market_stub_ids(rt, batch)
    candidates = {i for i in stubs if i.isdigit()}
    return sorted(candidates - known, key=int)


def _advance_plan(rt: CaptureRuntime, batch_id: str) -> bool:
    """Plan the next stage if the current stage has completed. Returns True if it advanced."""
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    stage = batch["plan_stage"]
    latest = _latest_by_name(rt.ledger.list_scans(batch_id))
    now = iso_utc(rt.now())

    if stage == 0:
        list_scans = [s for s in latest.values() if s["kind"] not in FOLLOW_UP_KINDS]
        if not _all_complete(list_scans):
            return False
        ids = _plan_stage0_ids(rt, batch)
        start = rt.ledger.max_plan_order(batch_id)
        rows = [
            _scan_row(
                batch_id,
                id_chunk_scan(index, ids_chunk),
                attempt=1,
                plan_order=start + index,
                started_at=now,
            )
            for index, ids_chunk in enumerate(chunk(ids), start=1)
        ]
        rt.ledger.add_plan(batch_id, 1, rows, now)
        return True

    if stage == 1:
        id_scans = [s for s in latest.values() if s["kind"] == "keyset_ids"]
        if not _all_complete(id_scans):
            return False
        requested: list[str] = []
        for scan in id_scans:
            requested.extend(json.loads(scan["input_ids_json"] or "[]"))
        returned = _event_ids_from(rt, batch, lambda s: s["kind"] == "keyset_ids")
        missing = sorted(set(requested) - returned, key=int)
        start = rt.ledger.max_plan_order(batch_id)
        rows = [
            _scan_row(
                batch_id,
                single_id_scan(index, ids_chunk),
                attempt=1,
                plan_order=start + index,
                started_at=now,
            )
            for index, ids_chunk in enumerate(chunk(missing), start=1)
        ]
        rt.ledger.add_plan(batch_id, 2, rows, now)
        return True
    return False


def _finalise_if_complete(rt: CaptureRuntime, batch_id: str) -> str:
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    if batch["status"] != "capturing":
        return batch["status"]
    latest = _latest_by_name(rt.ledger.list_scans(batch_id))
    if batch["plan_stage"] == 2 and _all_complete(list(latest.values())):
        finished = iso_utc(rt.now())
        rt.ledger.set_batch_status(batch_id, "captured", finished)
        write_marker(
            batch_marker_path(_batch_dir(rt.settings, batch["observation_date"], batch_id)),
            {
                "batch_id": batch_id,
                "mode": batch["mode"],
                "observation_date": batch["observation_date"],
                "status": "captured",
                "started_at": batch["started_at"],
                "finished_at": finished,
                "git_sha": batch["git_sha"],
                "scans": {name: s["scan_id"] for name, s in latest.items()},
            },
        )
        return "captured"
    return "capturing"


# ---------------------------------------------------------------------------
# Scan execution
# ---------------------------------------------------------------------------


def _iterate(rt: CaptureRuntime, spec: ScanSpec, start: PageState) -> Iterator[PageResult]:
    client = rt.client
    if spec.kind == "keyset" or spec.kind == "keyset_ids":
        return keyset_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "id_range":
        return id_range_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "offset":
        return offset_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "single_ids":
        return _single_iter(client, list(spec.input_ids), start.seq)
    raise ValueError(f"unknown scan kind {spec.kind!r}")


def _single_iter(client: GammaClient, ids: list[str], done: int) -> Iterator[PageResult]:
    for index in range(done, len(ids)):
        yield single_event_page(client, ids[index], seq=index + 1)


def _state_from_row(rt: CaptureRuntime, scan: dict[str, Any]) -> PageState:
    pages = rt.ledger.pages_for_scan(scan["scan_id"])
    seen = frozenset(p["output_cursor"] for p in pages if p["output_cursor"])
    return PageState(
        seq=int(scan["fetched_seq"]),
        cursor=scan["fetched_cursor"],
        offset=int(scan["fetched_offset"] or 0),
        seen_cursors=seen,
        last_ids_hash=scan["last_ids_hash"],
    )


def _row_from_manifest(manifest: dict[str, Any], batch_id: str, scan_id: str) -> dict[str, Any]:
    return {
        "page_id": manifest["page_id"],
        "batch_id": batch_id,
        "seq": manifest["seq"],
        "endpoint": manifest["endpoint"],
        "params_json": canonical_json(manifest["params"]),
        "input_cursor": manifest["input_cursor"],
        "output_cursor": manifest["output_cursor"],
        "offset_start": manifest["offset_start"],
        "offset_end": manifest["offset_end"],
        "record_count": manifest["record_count"],
        "http_status": manifest["http_status"],
        "retries": manifest["retries"],
        "latency_s": manifest["latency_s"],
        "terminal": 1 if manifest["terminal"] else 0,
        "body_sha256": manifest["body_sha256"],
        "gz_sha256": manifest["gz_sha256"],
        "observed_at": manifest["observed_at"],
        "ids_hash": manifest["ids_hash"],
        "scan_id": scan_id,
    }


def _adopt_durable_pages(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan: dict[str, Any],
    directory: Path,
) -> int:
    """Record pages whose files are durable but missing from the ledger. Returns count."""
    adopted = 0
    expected = int(scan["fetched_seq"]) + 1
    while True:
        path = manifest_path(directory, expected)
        if not path.exists():
            break
        manifest = read_manifest(path)
        if (
            manifest is None
            or manifest.get("seq") != expected
            or not verify_page(directory, manifest)
        ):
            break
        rt.ledger.record_page(
            _row_from_manifest(manifest, batch["batch_id"], scan["scan_id"]),
            scan["scan_id"],
            bool(manifest["terminal"]),
        )
        adopted += 1
        expected += 1
        scan = rt.ledger.get_scan(scan["scan_id"]) or scan
    return adopted


def _persist_page(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan: dict[str, Any],
    directory: Path,
    page: PageResult,
) -> None:
    manifest_fields: dict[str, Any] = {
        "seq": page.seq,
        "page_id": make_page_id(scan["scan_id"], page.seq),
        "batch_id": batch["batch_id"],
        "scan_id": scan["scan_id"],
        "scan_name": scan["scan_name"],
        "attempt": scan["attempt"],
        "plan_order": scan["plan_order"],
        "observation_date": batch["observation_date"],
        "endpoint": page.endpoint,
        "params": dict(page.params),
        "record_key": page.record_key,
        "input_cursor": page.input_cursor,
        "output_cursor": page.output_cursor,
        "offset_start": page.offset,
        "offset_end": page.output_offset,
        "record_count": page.record_count,
        "http_status": page.http_status,
        "retries": page.response.retries,
        "latency_s": round(page.response.latency_s, 6),
        "terminal": page.terminal,
        "ids_hash": page.ids_hash,
        "observed_at": iso_utc(page.response.received_at),
    }
    written = write_page(directory, page.seq, page.response.body, manifest_fields)
    fault_point("after_page_rename")
    row = {
        **manifest_fields,
        "body_sha256": written.body_sha256,
        "gz_sha256": written.gz_sha256,
    }
    rt.ledger.record_page(
        _row_from_manifest(row, batch["batch_id"], scan["scan_id"]),
        scan["scan_id"],
        page.terminal,
    )
    fault_point("after_ledger_commit")


def _write_scan_marker(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan: dict[str, Any],
    directory: Path,
    status: str,
    error: str | None = None,
) -> None:
    write_marker(
        scan_marker_path(directory),
        {
            "scan_id": scan["scan_id"],
            "batch_id": batch["batch_id"],
            "scan_name": scan["scan_name"],
            "attempt": scan["attempt"],
            "plan_order": scan["plan_order"],
            "kind": scan["kind"],
            "endpoint": scan["endpoint"],
            "record_key": scan["record_key"],
            "params": json.loads(scan["params_json"]),
            "input_ids": json.loads(scan["input_ids_json"] or "[]"),
            "status": status,
            "error": error,
            "started_at": scan["started_at"],
            "finished_at": iso_utc(rt.now()) if status != "running" else None,
        },
    )


def _run_scan(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    row: dict[str, Any],
    summary: CaptureSummary,
    stop: threading.Event | None = None,
) -> None:
    scan_id = row["scan_id"]
    if row["status"] == "failed":
        rt.ledger.set_scan_running(scan_id, iso_utc(rt.now()))
    if stop is not None and stop.is_set():
        return
    scan = rt.ledger.get_scan(scan_id)
    assert scan is not None
    directory = scan_dir_for(rt.settings, batch["observation_date"], batch["batch_id"], scan_id)
    _write_scan_marker(rt, batch, scan, directory, "running")
    spec = scan_spec_from_row(scan)
    summary.note_scan(scan_id)
    if scan["fetched_seq"]:
        logger.info(
            "capture scan %s resuming after page %s",
            scan["scan_name"],
            scan["fetched_seq"],
        )
    else:
        logger.info("capture scan %s starting", scan["scan_name"])

    try:
        adopted = _adopt_durable_pages(rt, batch, scan, directory)
        summary.note_pages(adopted=adopted)
        scan = rt.ledger.get_scan(scan_id)
        assert scan is not None
        if scan["terminal"]:
            _finish(rt, batch, scan, directory, "complete")
            return

        state = _state_from_row(rt, scan)
        pages = _iterate(rt, spec, state)
        while True:
            if stop is not None and stop.is_set():
                return
            try:
                page = next(pages)
            except StopIteration:
                break
            _persist_page(rt, batch, scan, directory, page)
            summary.note_pages(written=1, records=page.record_count)
            if page_progress_due(page.seq):
                logger.info(
                    "capture %s page %s (%s records)",
                    scan["scan_name"],
                    page.seq,
                    page.record_count,
                )
        scan = rt.ledger.get_scan(scan_id)
        assert scan is not None
        _finish(rt, batch, scan, directory, "complete")
    except CaptureStopped:
        return
    except CursorExpired as exc:
        _abandon(rt, batch, scan_id, directory, str(exc))
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"[:500]
        rt.ledger.set_scan_status(scan_id, "failed", None, message)
        scan = rt.ledger.get_scan(scan_id)
        assert scan is not None
        _write_scan_marker(rt, batch, scan, directory, "failed", message)
        raise


def _finish(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan: dict[str, Any],
    directory: Path,
    status: str,
) -> None:
    rt.ledger.set_scan_status(scan["scan_id"], status, iso_utc(rt.now()), None)
    refreshed = rt.ledger.get_scan(scan["scan_id"])
    assert refreshed is not None
    logger.info(
        "capture scan %s %s, %s pages, %s records",
        refreshed["scan_name"],
        status,
        refreshed["fetched_seq"],
        refreshed["record_count"],
    )
    _write_scan_marker(rt, batch, refreshed, directory, status)


def _abandon(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan_id: str,
    directory: Path,
    reason: str,
) -> None:
    """Cursor expired: mark the attempt abandoned and start a fresh attempt under a new scan ID.

    Pages from the abandoned attempt remain valid observations but never count
    toward coverage, because only a complete attempt does.
    """
    old = rt.ledger.get_scan(scan_id)
    assert old is not None
    if old["attempt"] >= rt.max_scan_attempts:
        message = f"{reason}; exceeded {rt.max_scan_attempts} attempts"[:500]
        rt.ledger.set_scan_status(scan_id, "failed", iso_utc(rt.now()), message)
        failed = rt.ledger.get_scan(scan_id)
        assert failed is not None
        _write_scan_marker(rt, batch, failed, directory, "failed", message)
        raise ScanFailed(f"{old['scan_name']}: exceeded {rt.max_scan_attempts} attempts")

    rt.ledger.set_scan_status(scan_id, "abandoned", iso_utc(rt.now()), reason[:500])
    abandoned = rt.ledger.get_scan(scan_id)
    assert abandoned is not None
    _write_scan_marker(rt, batch, abandoned, directory, "abandoned", reason[:500])
    rt.ledger.add_scan_attempt(
        _scan_row(
            batch["batch_id"],
            scan_spec_from_row(old),
            attempt=old["attempt"] + 1,
            plan_order=old["plan_order"],
            started_at=iso_utc(rt.now()),
        )
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def _record_capture_stage(
    rt: CaptureRuntime,
    *,
    mode: str,
    batch_id: str | None,
    started: str,
    summary: CaptureSummary | None,
    error: str | None,
) -> None:
    """Write the single ``stage_runs`` row for one capture attempt, success or failure."""
    captured = summary is not None and summary.status == "captured" and error is None
    counts: dict[str, Any] = {"mode": mode, "http": rt.client.stats.as_dict()}
    if summary is not None:
        counts.update(
            {
                "pages_written": summary.pages_written,
                "pages_adopted": summary.pages_adopted,
                "records": summary.records,
                "scans": len(summary.scans),
                "batch_status": summary.status,
            }
        )
    rt.ledger.record_stage_run(
        run_id=uuid.uuid4().hex,
        batch_id=batch_id,
        stage=f"capture:{mode}",
        started_at=started,
        finished_at=iso_utc(rt.now()),
        status="succeeded" if captured else "failed",
        counts=counts,
        git_sha=rt.git_sha,
        error=error,
    )


def _runtime_for(rt: CaptureRuntime, client: GammaClient) -> CaptureRuntime:
    return CaptureRuntime(
        settings=rt.settings,
        client=client,
        ledger=rt.ledger,
        now=rt.now,
        open_event_ids=rt.open_event_ids,
        git_sha=rt.git_sha,
        max_scan_attempts=rt.max_scan_attempts,
    )


def _run_ready_scans(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    rows: list[dict[str, Any]],
    summary: CaptureSummary,
) -> None:
    """Run every runnable scan in this stage, up to ``capture.workers`` at once.

    A failure or ``Terminated`` sets the stop event. Scans that have not
    started are left ``running`` and are not fetched. A scan already on a
    page finishes that page, then returns still ``running`` so resume continues
    it. The scan that raised is marked failed by ``_run_scan``.
    """
    stop = threading.Event()
    worker_count = min(rt.settings.capture.workers, len(rows))
    local = threading.local()
    spawned: list[GammaClient] = []
    spawned_lock = threading.Lock()

    def init_worker() -> None:
        client = rt.client.spawn()
        local.client = client
        with spawned_lock:
            spawned.append(client)

    def run_row(row: dict[str, Any]) -> None:
        _run_scan(_runtime_for(rt, local.client), batch, row, summary, stop)

    rt.client._limiter.bind_stop(stop)
    executor = ThreadPoolExecutor(
        max_workers=worker_count, thread_name_prefix="capture", initializer=init_worker
    )
    futures = []
    try:
        futures = [executor.submit(run_row, row) for row in rows]
        pending = set(futures)
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                future.result()
    except BaseException:
        stop.set()
        for future in futures:
            future.cancel()
        raise
    finally:
        # A second SIGTERM raises inside shutdown. Keep joining until the
        # workers have finished, then surface that signal to the stage row.
        interrupted: BaseException | None = None
        while True:
            try:
                executor.shutdown(wait=True, cancel_futures=True)
                break
            except BaseException as exc:
                if exc.__class__.__name__ != "Terminated":
                    raise
                stop.set()
                interrupted = exc
        rt.client._limiter.bind_stop(None)
        for client in spawned:
            client.close()
        if interrupted is not None:
            raise interrupted


def run_capture(rt: CaptureRuntime, mode: str) -> CaptureSummary:
    """Start, or resume, a capture batch for ``mode`` until it is captured or a scan fails.

    Writes exactly one ``stage_runs`` row (stage ``capture:<mode>``) whether the batch
    captures, ends short of captured, or raises.
    """
    started = iso_utc(rt.now())
    batch_id: str | None = None
    summary: CaptureSummary | None = None
    try:
        resumable = rt.ledger.find_resumable_batch(mode)
        resumed = resumable is not None
        batch = resumable if resumable is not None else _start_batch(rt, mode)
        batch_id = batch["batch_id"]
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=resumed)

        try:
            while True:
                progressed = _advance_plan(rt, batch_id)
                batch = rt.ledger.get_batch(batch_id)
                assert batch is not None
                runnable = [
                    s
                    for s in rt.ledger.list_scans(batch_id)
                    if s["status"] in {"running", "failed"}
                ]
                if runnable:
                    runnable.sort(key=lambda s: (s["plan_order"], s["attempt"]))
                    if rt.settings.capture.workers <= 1:
                        _run_scan(rt, batch, runnable[0], summary)
                    else:
                        _run_ready_scans(rt, batch, runnable, summary)
                    continue
                if not progressed:
                    break
        except Exception as exc:
            rt.ledger.set_batch_status(
                batch_id, "capturing", iso_utc(rt.now()), f"{type(exc).__name__}: {exc}"[:500]
            )
            raise

        summary.status = _finalise_if_complete(rt, batch_id)
    except BaseException as exc:
        _record_capture_stage(
            rt,
            mode=mode,
            batch_id=batch_id,
            started=started,
            summary=summary,
            error=f"{type(exc).__name__}: {exc}"[:2000],
        )
        raise

    error = None
    if summary.status != "captured":
        error = f"capture ended with status {summary.status}"
    _record_capture_stage(
        rt, mode=mode, batch_id=batch_id, started=started, summary=summary, error=error
    )
    return summary


def abandon_batch(ledger: Ledger, batch_id: str, now: datetime, reason: str) -> None:
    """Operator action: stop resuming a capturing batch. Its raw pages are kept."""
    batch = ledger.get_batch(batch_id)
    if batch is None:
        raise KeyError(batch_id)
    if batch["status"] != "capturing":
        raise ValueError(f"batch {batch_id} is {batch['status']}, not capturing")
    ledger.set_batch_status(batch_id, "abandoned", iso_utc(now), reason[:500])


def rebuild_from_raw(settings: Settings, ledger: Ledger) -> dict[str, int]:
    """Reconstruct batches, scans, and pages from raw markers and manifests.

    Loaded checkpoints are not recoverable from raw data, so every page comes
    back unloaded. Re-loading is safe because observation IDs are unique.
    """
    counts = {"batches": 0, "scans": 0, "pages": 0}
    if not settings.raw_dir.exists():
        return counts
    for batch_marker in sorted(settings.raw_dir.glob("*/*/_batch.json")):
        batch_dir = batch_marker.parent
        meta = json.loads(batch_marker.read_text(encoding="utf-8"))
        batch_id = meta["batch_id"]
        if ledger.get_batch(batch_id) is None:
            ledger.create_batch(
                batch_id,
                meta["mode"],
                meta["observation_date"],
                meta["started_at"],
                meta.get("git_sha"),
                [],
            )
            counts["batches"] += 1

        for scan_dir in sorted(p for p in batch_dir.iterdir() if p.is_dir()):
            marker = scan_dir / "_scan.json"
            if not marker.exists():
                continue
            scan_meta = json.loads(marker.read_text(encoding="utf-8"))
            scan_id = scan_meta["scan_id"]
            if ledger.get_scan(scan_id) is None:
                ledger.add_scan_attempt(
                    {
                        "scan_id": scan_id,
                        "batch_id": batch_id,
                        "scan_name": scan_meta["scan_name"],
                        "attempt": scan_meta["attempt"],
                        "plan_order": scan_meta["plan_order"],
                        "kind": scan_meta["kind"],
                        "endpoint": scan_meta["endpoint"],
                        "record_key": scan_meta["record_key"],
                        "params_json": canonical_json(scan_meta["params"]),
                        "input_ids_json": canonical_json(scan_meta["input_ids"])
                        if scan_meta["input_ids"]
                        else None,
                        "started_at": scan_meta["started_at"],
                    }
                )
                counts["scans"] += 1
            # The marker records the last decision for the scan (complete, abandoned, failed).
            ledger.set_scan_status(
                scan_id,
                scan_meta["status"],
                scan_meta.get("finished_at"),
                scan_meta.get("error"),
            )

            known_seqs = {p["seq"] for p in ledger.pages_for_scan(scan_id)}
            seq = 0
            while True:
                seq += 1
                path = manifest_path(scan_dir, seq)
                if not path.exists():
                    break
                manifest = read_manifest(path)
                if manifest is None or not verify_page(scan_dir, manifest):
                    break
                if seq in known_seqs:
                    continue
                ledger.record_page(
                    _row_from_manifest(manifest, batch_id, scan_id),
                    scan_id,
                    bool(manifest["terminal"]),
                )
                counts["pages"] += 1

        _restore_batch_state(ledger, batch_id, meta)
    return counts


def _restore_batch_state(ledger: Ledger, batch_id: str, meta: dict[str, Any]) -> None:
    """Infer the plan stage from the scans on disk and apply the batch marker's status."""
    scans = ledger.list_scans(batch_id)
    kinds = {s["kind"] for s in scans}
    if "single_ids" in kinds:
        stage = 2
    elif "keyset_ids" in kinds:
        stage = 1
    else:
        stage = 0
    if meta.get("status") == "captured":
        stage = 2
    status = "captured" if meta.get("status") == "captured" else "capturing"
    ledger.restore_batch_state(
        batch_id, status, stage, meta.get("finished_at") or meta["started_at"]
    )


__all__ = [
    "CaptureRuntime",
    "CaptureSummary",
    "abandon_batch",
    "rebuild_from_raw",
    "run_capture",
]
