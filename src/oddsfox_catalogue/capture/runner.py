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
import re
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
    id_list_pages,
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
from oddsfox_catalogue.signals import SIGNALS, Terminated

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
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    _write_batch_marker(rt, batch)
    _mark_planned(rt, batch, rows)
    return batch


def _plan_entry(scan: dict[str, Any]) -> dict[str, Any]:
    """One planned scan as the batch marker records it.

    Accepts a planned row from the ledger or a scan row built for planning. The batch
    marker keeps the whole plan, so a rebuild can recreate a planned scan whose markers
    were never written.
    """
    input_ids = scan.get("input_ids_json")
    return {
        "scan_name": scan["scan_name"],
        "plan_order": scan["plan_order"],
        "kind": scan["kind"],
        "endpoint": scan["endpoint"],
        "record_key": scan["record_key"],
        "params": json.loads(scan["params_json"]),
        "input_ids": json.loads(input_ids) if input_ids else [],
        "started_at": scan["started_at"],
    }


def _write_batch_marker(rt: CaptureRuntime, batch: dict[str, Any], **fields: Any) -> None:
    """Write the batch marker with every attempt-1 scan planned so far, in one atomic write.

    The write runs before the scan markers for the same plan. A crash therefore leaves
    either the old plan or the new one, never a scan marker that the plan does not list.
    ``fields`` override the defaults, as the captured marker does for its status.
    """
    planned = sorted(
        (s for s in rt.ledger.list_scans(batch["batch_id"]) if s["attempt"] == 1),
        key=lambda s: s["plan_order"],
    )
    payload: dict[str, Any] = {
        "batch_id": batch["batch_id"],
        "mode": batch["mode"],
        "observation_date": batch["observation_date"],
        "status": "capturing",
        "started_at": batch["started_at"],
        "git_sha": batch["git_sha"],
        "plan": [_plan_entry(scan) for scan in planned],
        **fields,
    }
    write_marker(
        batch_marker_path(_batch_dir(rt.settings, batch["observation_date"], batch["batch_id"])),
        payload,
    )


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


OPEN_MARKETS_SCAN = "markets_keyset_open"
CLOSED_MARKET_WINDOW_PREFIX = "markets_closed_ids_"
ID_RANGE_MODES = frozenset({"bootstrap", "reconcile"})
# Scan names the id-range plan produces. A deep-keyset or offset plan uses other names, and its
# coverage cannot be shown, so a batch with any other name is not resumed.
ID_RANGE_SCAN_NAME = re.compile(
    r"markets_keyset_open|events_ids_(\d{4,}|tail)|markets_closed_ids_(\d{4,}|tail)"
    r"|events_by_id_(single_)?\d{4,}"
)
# Seconds the pool waits before it looks again. The main thread runs signal handlers only
# between waits, so a bounded wait keeps a signal from sitting behind a busy worker.
POOL_POLL_S = 1.0


def _open_crawl_complete(latest: dict[str, dict[str, Any]]) -> bool:
    open_scan = latest.get(OPEN_MARKETS_SCAN)
    return open_scan is not None and open_scan["status"] == "complete"


def _held_closed_windows(scans: list[dict[str, Any]]) -> set[str]:
    """Scan IDs of closed-market windows that must wait for the open-market crawl.

    A market that closes during the batch is missed if a closed window passes it before it
    closes and the open crawl passes it after it closes. While the open crawl is not complete,
    the closed windows are held, so each market is seen while open or after it has closed.
    A batch with no open crawl holds its closed windows too: nothing else covers them.
    """
    if _open_crawl_complete(_latest_by_name(scans)):
        return set()
    return {s["scan_id"] for s in scans if s["scan_name"].startswith(CLOSED_MARKET_WINDOW_PREFIX)}


def _unsafe_to_resume(scans: list[dict[str, Any]]) -> bool:
    """True when an id-range batch cannot be resumed on a plan that shows every market is seen.

    Four shapes qualify. A batch with no list scans was never planned, and the planner refuses
    to plan from it. A batch holding a scan name the id-range plan does not produce came from a
    deep-keyset or offset plan. A batch planned before the market-race fix runs its closed windows
    ahead of the open crawl, and a closed window that finished first can pass a market the crawl
    never returns. A batch with scans but no open crawl has nothing to cover its closed windows.
    """
    if not scans:
        return True
    if any(ID_RANGE_SCAN_NAME.fullmatch(s["scan_name"]) is None for s in scans):
        return True
    latest = _latest_by_name(scans)
    open_scan = latest.get(OPEN_MARKETS_SCAN)
    if open_scan is None:
        return True
    return any(
        name.startswith(CLOSED_MARKET_WINDOW_PREFIX) and s["plan_order"] < open_scan["plan_order"]
        for name, s in latest.items()
    )


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
        # A batch with no list scans is corrupt. Planning from it would "capture" nothing.
        if not list_scans or not _all_complete(list_scans):
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
        _write_batch_marker(rt, batch)
        _mark_planned(rt, batch, rows)
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
        _write_batch_marker(rt, batch)
        _mark_planned(rt, batch, rows)
        return True
    return False


def _resume_orphaned_attempts(rt: CaptureRuntime, batch: dict[str, Any]) -> None:
    """Start the successor of an attempt that was abandoned but never replaced.

    _abandon writes the abandoned status and the successor in two commits. A signal between
    them leaves an abandoned latest attempt. It is never runnable, so it would hold its stage.
    """
    for row in _latest_by_name(rt.ledger.list_scans(batch["batch_id"])).values():
        if row["status"] != "abandoned":
            continue
        successor = _scan_row(
            batch["batch_id"],
            scan_spec_from_row(row),
            attempt=row["attempt"] + 1,
            plan_order=row["plan_order"],
            started_at=iso_utc(rt.now()),
        )
        rt.ledger.add_scan_attempt(successor)
        _mark_planned(rt, batch, [successor])
        logger.info("capture scan %s resumed as attempt %s", row["scan_name"], successor["attempt"])


def _finalise_if_complete(rt: CaptureRuntime, batch_id: str) -> str:
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    if batch["status"] != "capturing":
        return batch["status"]
    latest = _latest_by_name(rt.ledger.list_scans(batch_id))
    # A market-list batch is captured only once its open-market crawl has completed.
    crawl_ok = batch["mode"] not in ID_RANGE_MODES or _open_crawl_complete(latest)
    if batch["plan_stage"] == 2 and _all_complete(list(latest.values())) and crawl_ok:
        finished = iso_utc(rt.now())
        rt.ledger.set_batch_status(batch_id, "captured", finished)
        _write_batch_marker(
            rt,
            batch,
            status="captured",
            finished_at=finished,
            scans={name: s["scan_id"] for name, s in latest.items()},
        )
        return "captured"
    return "capturing"


# ---------------------------------------------------------------------------
# Scan execution
# ---------------------------------------------------------------------------


def _iterate(rt: CaptureRuntime, spec: ScanSpec, start: PageState) -> Iterator[PageResult]:
    client = rt.client
    if spec.kind == "keyset":
        return keyset_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "keyset_ids":
        return id_list_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "id_range":
        return id_range_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "offset":
        return offset_pages(client, spec.endpoint, spec.param_dict, spec.record_key, start)
    if spec.kind == "single_ids":
        return _single_iter(client, list(spec.input_ids), start.seq)
    raise ValueError(f"unknown scan kind {spec.kind!r}")


def _single_iter(client: GammaClient, ids: list[str], done: int) -> Iterator[PageResult]:
    last = len(ids) - 1
    for index in range(done, len(ids)):
        yield single_event_page(client, ids[index], seq=index + 1, terminal=index == last)


def _state_from_row(rt: CaptureRuntime, scan: dict[str, Any]) -> PageState:
    pages = rt.ledger.pages_for_scan(scan["scan_id"])
    seen = frozenset(p["output_cursor"] for p in pages if p["output_cursor"])
    return PageState(
        seq=int(scan["fetched_seq"]),
        cursor=scan["fetched_cursor"],
        offset=int(scan["fetched_offset"] or 0),
        seen_cursors=seen,
        last_ids_hash=scan["last_ids_hash"],
        empty_run=_trailing_empty(pages),
    )


def _trailing_empty(pages: list[dict[str, Any]]) -> int:
    """Consecutive record-less pages at the end of a scan, so a resumed tail keeps its stop count."""
    run = 0
    for page in pages:
        run = run + 1 if int(page["record_count"]) == 0 else 0
    return run


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


def _mark_planned(rt: CaptureRuntime, batch: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """Write a marker for each planned scan, so a raw-store rebuild restores scans that never ran."""
    for row in rows:
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], row["scan_id"]
        )
        _write_scan_marker(rt, batch, row, directory, "running")


def _reopen_unstarted(rt: CaptureRuntime, rows: list[dict[str, Any]], started: set[str]) -> None:
    """A failed scan the pool never started keeps its resume state: reopen it as running."""
    for row in rows:
        if row["status"] == "failed" and row["scan_id"] not in started:
            rt.ledger.set_scan_running(row["scan_id"], iso_utc(rt.now()))


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

    A failure or a stop signal sets the stop event. Scans that have not started are left
    ``running`` and are not fetched, and a previously failed scan the pool never started is
    reopened as ``running``. A scan on a page finishes that page, unless it is waiting out a
    rate-limit pause, which it abandons. Either way it returns still ``running`` so resume
    continues it. The scan that raised is marked failed by ``_run_scan``. The drain waits for
    every worker, even if more signals or interrupts arrive, and then raises the first failure.
    A signal recorded in the hold is raised only when nothing else failed.
    """
    stop = threading.Event()
    worker_count = min(rt.settings.capture.workers, len(rows))
    local = threading.local()
    spawned: list[GammaClient] = []
    spawned_lock = threading.Lock()
    started: set[str] = set()
    started_lock = threading.Lock()

    def init_worker() -> None:
        client = rt.client.spawn()
        local.client = client
        with spawned_lock:
            spawned.append(client)

    def run_row(row: dict[str, Any]) -> None:
        with started_lock:
            started.add(row["scan_id"])
        _run_scan(_runtime_for(rt, local.client), batch, row, summary, stop)

    # The hold covers the whole pool, from before the first worker starts. A signal is then
    # recorded and never raised inside the pool's own bookkeeping. The wait loop raises the
    # recorded signal at its next tick, so the drain and the cleanup always run to the end.
    failure: BaseException | None = None
    with SIGNALS.hold() as held:
        executor = ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="capture", initializer=init_worker
        )
        futures = []
        try:
            rt.client._limiter.bind_stop(stop)
            futures = [executor.submit(run_row, row) for row in rows]
            pending = set(futures)
            while pending:
                if SIGNALS.pending is not None:
                    raise Terminated(SIGNALS.pending)
                done, pending = wait(pending, timeout=POOL_POLL_S, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
        except BaseException as exc:
            failure = exc
            stop.set()
            for future in futures:
                future.cancel()
        finally:
            stop.set()
            # A ledger or run lock released while a worker still runs would let a second run
            # write beside it, so the join goes on whatever arrives in the meantime.
            while True:
                try:
                    executor.shutdown(wait=True, cancel_futures=True)
                    break
                except BaseException as exc:
                    # Only an interrupt that bypasses the hold (Ctrl-C) gets here. The join goes
                    # on. The first failure stays the one raised, so a later interrupt is logged.
                    if failure is None:
                        failure = exc
                    else:
                        logger.warning("interrupted while capture workers stopped", exc_info=exc)
            rt.client._limiter.bind_stop(None)
            with started_lock:
                ran = set(started)
            try:
                _reopen_unstarted(rt, rows, ran)
            except Exception:
                logger.warning("reopening unstarted scans failed; resume runs them", exc_info=True)
            for client in spawned:
                try:
                    client.close()
                except Exception:
                    # A failing close must not skip the other clients.
                    logger.warning("closing a capture worker client failed", exc_info=True)
    if failure is None and held.signum is not None:
        failure = Terminated(held.signum)
    if failure is not None:
        raise failure


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
        if (
            resumable is not None
            and mode in ID_RANGE_MODES
            and _unsafe_to_resume(rt.ledger.list_scans(resumable["batch_id"]))
        ):
            reason = "not resumed: its plan is not the current id-range plan"
            logger.warning("abandoning batch %s: %s", resumable["batch_id"], reason)
            abandon_batch(rt.ledger, resumable["batch_id"], rt.now(), reason)
            resumable = None
        resumed = resumable is not None
        batch = resumable if resumable is not None else _start_batch(rt, mode)
        batch_id = batch["batch_id"]
        # A crash between a stage's plan commit and its batch marker leaves the ledger ahead of
        # the marker. Rewriting the marker before any scan runs keeps every planned scan in it.
        _write_batch_marker(rt, batch)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=resumed)

        try:
            while True:
                progressed = _advance_plan(rt, batch_id)
                batch = rt.ledger.get_batch(batch_id)
                assert batch is not None
                _resume_orphaned_attempts(rt, batch)
                listed = rt.ledger.list_scans(batch_id)
                held = _held_closed_windows(listed)
                runnable = [
                    s
                    for s in listed
                    if s["status"] in {"running", "failed"} and s["scan_id"] not in held
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


def _planned_row(batch_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    """The attempt-1 ledger row for a scan that a batch marker plans."""
    input_ids = entry["input_ids"]
    return {
        "scan_id": make_scan_id(batch_id, entry["scan_name"], 1),
        "batch_id": batch_id,
        "scan_name": entry["scan_name"],
        "attempt": 1,
        "plan_order": entry["plan_order"],
        "kind": entry["kind"],
        "endpoint": entry["endpoint"],
        "record_key": entry["record_key"],
        "params_json": canonical_json(entry["params"]),
        "input_ids_json": canonical_json(input_ids) if input_ids else None,
        "started_at": entry["started_at"],
    }


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

        # The batch marker lists the whole plan. A planned scan whose markers were never written
        # (a crash during planning) is recreated as a running attempt, so resume runs it. A batch
        # is never captured without a scan its plan lists.
        for entry in meta.get("plan", []):
            if ledger.get_scan(make_scan_id(batch_id, entry["scan_name"], 1)) is None:
                ledger.add_scan_attempt(_planned_row(batch_id, entry))
                counts["scans"] += 1

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
            known_seqs = {p["seq"] for p in ledger.pages_for_scan(scan_id)}
            seq = 0
            terminal_adopted = False
            while True:
                seq += 1
                path = manifest_path(scan_dir, seq)
                if not path.exists():
                    break
                manifest = read_manifest(path)
                if manifest is None or not verify_page(scan_dir, manifest):
                    break
                terminal_adopted = bool(manifest["terminal"])
                if seq in known_seqs:
                    continue
                ledger.record_page(
                    _row_from_manifest(manifest, batch_id, scan_id),
                    scan_id,
                    bool(manifest["terminal"]),
                )
                counts["pages"] += 1
            # The marker's last decision (complete, abandoned, failed) stands only if the pages
            # back it. A complete marker without its terminal page resumes from the last durable
            # page, instead of being skipped as finished.
            status = scan_meta["status"]
            finished_at, error = scan_meta.get("finished_at"), scan_meta.get("error")
            if status == "complete" and not terminal_adopted:
                status, finished_at, error = "running", None, None
            ledger.set_scan_status(scan_id, status, finished_at, error)

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
    # A captured marker is believed only while every latest scan is complete. A scan whose
    # terminal page is missing runs again, so the batch goes back to capturing with it.
    captured = meta.get("status") == "captured" and _all_complete(
        list(_latest_by_name(scans).values())
    )
    if captured:
        stage = 2
    status = "captured" if captured else "capturing"
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
