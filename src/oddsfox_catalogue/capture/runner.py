"""Capture runner: plans a batch, executes scans, persists pages, and recovers.

Lifecycle of a batch:

1. Persist finite selected scope or discover and commit fixed source high-water marks.
2. Capture the committed list/direct-ID scans (phase 0).
3. Daily mode reobserves missing market baseline IDs (phase 1).
4. Capture referenced parent and missing baseline events (phases 2 and 3).
5. Mark captured only when the sealed plan and every latest scan are complete.

SQLite transitions and their exact marker bytes share a pending-control journal.
Explicit named resume repairs a proven pending write, then verifies committed evidence
before any source request. Deliberate new acquisitions never resume older work.

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
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import (
    DurabilityError,
    iter_scan_pages,
    read_page_records,
    scan_dir_for,
    validate_page_identity,
)
from oddsfox_catalogue.capture.writer import (
    batch_marker_path,
    manifest_path,
    read_body,
    read_manifest,
    read_regular_bytes,
    scan_marker_path,
    verify_page,
    write_marker,
    write_page,
)
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import fault_point
from oddsfox_catalogue.gamma.http import (
    CaptureStopped,
    CursorExpired,
    GammaClient,
    RequestBudgetExceeded,
    ScanFailed,
)
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
    high_water,
    id_chunk_scan,
    native_single_scan,
    scan_spec_from_row,
    sealed_plan,
    single_id_scan,
)
from oddsfox_catalogue.ids import (
    canonical_json,
    iso_utc,
    make_batch_id,
    make_page_id,
    make_scan_id,
    observation_date,
    parse_batch_id,
    sha256_bytes,
    utc_now,
)
from oddsfox_catalogue.limits import enforce_storage_limits, retained_bytes, temporary_bytes
from oddsfox_catalogue.signals import SIGNALS, Terminated

logger = logging.getLogger(__name__)

MAX_SCAN_ATTEMPTS = 3
FOLLOW_UP_KINDS = frozenset({"keyset_ids", "single_ids"})
PROGRESS_EVERY_PAGES = 100


def page_progress_due(seq: int) -> bool:
    """Page 1 and every 100th page are the progress lines an operator sees."""
    return seq == 1 or seq % PROGRESS_EVERY_PAGES == 0


MAX_PLAN_SCANS = 20_000
MAX_MARKER_BYTES = 16 * 1024**2


@dataclass
class StorageBudget:
    settings: Settings
    allocated: int
    existing_temporary: int
    held_temporary: dict[str, int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def reserve(self, size: int, token: str) -> None:
        # Conservative reservations count write copies; no refunds hide partial writes.
        with self.lock:
            if self.allocated + size > self.settings.capture.max_retained_bytes:
                raise RequestBudgetExceeded("retained storage allowance exhausted")
            if token in self.held_temporary:
                raise RuntimeError("capture write already holds a temporary reservation")
            if (
                self.existing_temporary + sum(self.held_temporary.values()) + size
                > self.settings.capture.max_temp_bytes
            ):
                raise RequestBudgetExceeded("temporary storage allowance exhausted")
            self.allocated += size
            self.held_temporary[token] = size

    def release_temporary(self, token: str) -> None:
        with self.lock:
            self.held_temporary.pop(token, None)


def _storage_budget(settings: Settings) -> StorageBudget:
    enforce_storage_limits(settings)
    return StorageBudget(settings, retained_bytes(settings), temporary_bytes(settings))


def _reserve(rt: CaptureRuntime, size: int, token: str) -> None:
    if rt.storage is not None:
        rt.storage.reserve(size, token)


def _release_temporary(rt: CaptureRuntime, token: str) -> None:
    if rt.storage is not None:
        rt.storage.release_temporary(token)


def _explicit_ids(values: Any) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValueError("selected IDs must be an explicit sequence")
    values = list(islice(values, 101))
    if len(values) > 100:
        raise ValueError("selected mode permits at most 100 IDs per entity")
    if any(
        not isinstance(v, str)
        or not v.isascii()
        or not v.isdecimal()
        or len(v) > 20
        or int(v) <= 0
        or str(int(v)) != v
        for v in values
    ):
        raise ValueError("selected IDs must be canonical positive decimal IDs")
    return sorted(set(values), key=int)


def _scope(batch: dict[str, Any]) -> dict[str, Any]:
    scope = json.loads(batch.get("scope_json") or "{}")
    if scope.get("revision") != 2 or scope.get("kind") not in {"selected", "catalogue"}:
        raise ValueError("unsupported capture scope; start a fresh acquisition")
    return scope


def _read_marker(path: Path, settings: Settings) -> dict[str, Any]:
    try:
        result = json.loads(
            read_regular_bytes(path, max_bytes=MAX_MARKER_BYTES, trusted_root=settings.raw_dir)
        )
    except (OSError, ValueError) as exc:
        raise DurabilityError("capture marker is missing, unsafe or malformed") from exc
    if not isinstance(result, dict):
        raise DurabilityError("capture marker is not an object")
    return result


@dataclass
class CaptureRuntime:
    settings: Settings
    client: GammaClient
    ledger: Ledger
    now: Callable[[], datetime] = utc_now
    # Returns event IDs open at the last loaded state. Used only by daily mode.
    open_event_ids: Callable[[], set[str]] | None = None
    open_market_ids: Callable[[], set[str]] | None = None
    storage: StorageBudget | None = None
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
    http_attempts: int = 0
    downloaded_bytes: int = 0
    duration_s: float = 0.0
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
    phase: int = 0,
) -> dict[str, Any]:
    return {
        "scan_id": make_scan_id(batch_id, spec.name, attempt),
        "batch_id": batch_id,
        "scan_name": spec.name,
        "attempt": attempt,
        "phase": phase,
        "plan_order": plan_order,
        "kind": spec.kind,
        "endpoint": spec.endpoint,
        "record_key": spec.record_key,
        "params_json": canonical_json(spec.param_dict),
        "input_ids_json": canonical_json(list(spec.input_ids)) if spec.input_ids else None,
        "started_at": started_at,
    }


def _start_batch(
    rt: CaptureRuntime, mode: str, market_ids: list[str], event_ids: list[str]
) -> dict[str, Any]:
    now = rt.now()
    started = iso_utc(now)
    base = make_batch_id(mode, now)
    batch_id = base
    suffix = 2
    while (
        rt.ledger.get_batch(batch_id) is not None
        or _batch_dir(rt.settings, observation_date(now), batch_id).exists()
    ):
        batch_id = f"{base}-{suffix}"
        suffix += 1
    baseline = {"events": [], "markets": []}
    if mode == "daily":
        if rt.open_event_ids is None or rt.open_market_ids is None:
            raise RuntimeError("daily mode requires open-or-unknown event and market baselines")
        baseline = {
            "events": sorted(rt.open_event_ids(), key=int),
            "markets": sorted(rt.open_market_ids(), key=int),
        }
    scope = {
        "revision": 2,
        "kind": "selected" if mode == "selected" else "catalogue",
        "market_ids": market_ids,
        "event_ids": event_ids,
        "parent_event_ids": [],
        "high_water": {},
        "baseline": baseline,
        "sealed": mode == "selected",
    }
    if len(canonical_json(scope).encode()) > MAX_MARKER_BYTES // 2:
        raise ValueError("capture scope exceeds the manifest allowance")
    # Persist the incomplete acquisition before any source discovery request.
    initial = {
        "batch_id": batch_id,
        "mode": mode,
        "observation_date": observation_date(now),
        "status": "capturing",
        "plan_stage": -1,
        "started_at": started,
        "git_sha": rt.git_sha,
        "scope_json": canonical_json(scope),
    }
    payload = _batch_payload(rt, initial)
    control = _prepare_control(
        rt, batch_marker_path(_batch_dir(rt.settings, observation_date(now), batch_id)), payload
    )
    rt.ledger.create_batch(
        batch_id,
        mode,
        observation_date(now),
        started,
        rt.git_sha,
        [],
        scope=scope,
        plan_stage=-1,
        control=control,
    )
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    _write_batch_marker(rt, batch)
    return batch


def _seal_plan(rt: CaptureRuntime, batch: dict[str, Any]) -> None:
    scope = _scope(batch)
    if batch["mode"] == "selected":
        specs = []
        if scope["market_ids"]:
            specs.append(native_single_scan("markets_selected", scope["market_ids"], "markets"))
        if scope["event_ids"]:
            specs.append(native_single_scan("events_selected", scope["event_ids"], "events"))
    else:
        for entity in ("events", "markets"):
            if entity not in scope["high_water"]:
                scope["high_water"][entity] = high_water(rt.client, entity)
                if rt.settings.capture.max_id_override:
                    scope["high_water"][entity] = min(
                        scope["high_water"][entity], rt.settings.capture.max_id_override
                    )
                proposed = {**batch, "scope_json": canonical_json(scope)}
                control = _prepare_control(
                    rt,
                    batch_marker_path(
                        _batch_dir(rt.settings, batch["observation_date"], batch["batch_id"])
                    ),
                    _batch_payload(rt, proposed),
                )
                rt.ledger.update_scope(batch["batch_id"], scope, iso_utc(rt.now()), control=control)
                batch = rt.ledger.get_batch(batch["batch_id"])
                assert batch is not None
                _write_batch_marker(rt, batch)
        specs = sealed_plan(
            batch["mode"], rt.settings.gamma, rt.settings.capture, scope["high_water"]
        )
    scope["sealed"] = True
    rows = [
        _scan_row(batch["batch_id"], spec, attempt=1, plan_order=i, started_at=iso_utc(rt.now()))
        for i, spec in enumerate(specs, 1)
    ]
    _commit_plan(rt, batch, 0, rows, scope)


def _commit_plan(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    phase: int,
    rows: list[dict[str, Any]],
    scope: dict[str, Any] | None = None,
) -> None:
    planned = [s for s in rt.ledger.list_scans(batch["batch_id"]) if s["attempt"] == 1]
    if (
        len(planned) + len(rows) > MAX_PLAN_SCANS
        or len(canonical_json([_plan_entry(s) for s in planned + rows]).encode())
        > MAX_MARKER_BYTES // 2
    ):
        raise ValueError("capture plan exceeds the finite manifest allowance")
    proposed = {**batch, "plan_stage": phase, "scope_json": canonical_json(scope or _scope(batch))}
    control = _prepare_control(
        rt,
        batch_marker_path(_batch_dir(rt.settings, batch["observation_date"], batch["batch_id"])),
        _batch_payload(rt, proposed, additional_plan=rows),
    )
    rt.ledger.add_plan(
        batch["batch_id"], phase, rows, iso_utc(rt.now()), scope=scope, control=control
    )
    committed = rt.ledger.get_batch(batch["batch_id"])
    assert committed is not None
    _write_batch_marker(rt, committed)
    _mark_planned(rt, committed, rows)


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
        "phase": scan.get("phase", 0),
        "kind": scan["kind"],
        "endpoint": scan["endpoint"],
        "record_key": scan["record_key"],
        "params": json.loads(scan["params_json"]),
        "input_ids": json.loads(input_ids) if input_ids else [],
        "started_at": scan["started_at"],
    }


def _batch_payload(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    *,
    additional_plan: list[dict[str, Any]] = (),
    **fields: Any,
) -> dict[str, Any]:
    planned = sorted(
        (s for s in rt.ledger.list_scans(batch["batch_id"]) if s["attempt"] == 1),
        key=lambda s: s["plan_order"],
    )
    return {
        "batch_id": batch["batch_id"],
        "mode": batch["mode"],
        "observation_date": batch["observation_date"],
        "status": batch["status"],
        "scope": _scope(batch),
        "plan_stage": batch["plan_stage"],
        "started_at": batch["started_at"],
        "git_sha": batch["git_sha"],
        "plan": [_plan_entry(scan) for scan in planned + list(additional_plan)],
        **fields,
    }


def _prepare_control(rt: CaptureRuntime, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    encoded = (canonical_json(payload) + "\n").encode()
    if len(encoded) > MAX_MARKER_BYTES:
        raise ValueError("capture manifest exceeds the finite allowance")
    control_id = path.relative_to(rt.settings.raw_dir).as_posix()
    _reserve(rt, 3 * len(encoded) + 4096, control_id)
    previous = None
    if path.exists() or path.is_symlink():
        previous = sha256_bytes(
            read_regular_bytes(path, max_bytes=MAX_MARKER_BYTES, trusted_root=rt.settings.raw_dir)
        )
    return {
        "control_id": control_id,
        "batch_id": payload["batch_id"],
        "payload_json": encoded.decode(),
        "payload_sha256": sha256_bytes(encoded),
        "previous_sha256": previous,
    }


def _promote_control(rt: CaptureRuntime, control: dict[str, Any]) -> None:
    payload_bytes = control["payload_json"].encode()
    if (
        len(payload_bytes) > MAX_MARKER_BYTES
        or sha256_bytes(payload_bytes) != control["payload_sha256"]
    ):
        raise DurabilityError("pending capture control is corrupt")
    target = Path(control["control_id"])
    if (
        target.is_absolute()
        or target.as_posix() != control["control_id"]
        or any(part in {"", ".", ".."} for part in target.parts)
    ):
        raise DurabilityError("pending capture control has an unsafe path")
    path = rt.settings.raw_dir / target
    existing = None
    if path.exists() or path.is_symlink():
        existing = sha256_bytes(
            read_regular_bytes(path, max_bytes=MAX_MARKER_BYTES, trusted_root=rt.settings.raw_dir)
        )
    if existing not in {control["previous_sha256"], control["payload_sha256"]}:
        raise DurabilityError("pending capture control does not match committed marker bytes")
    payload = json.loads(payload_bytes)
    batch = rt.ledger.get_batch(control["batch_id"])
    if batch is None:
        raise DurabilityError("pending capture control has no ledger batch")
    if path.name == "_batch.json":
        if path != batch_marker_path(
            _batch_dir(rt.settings, batch["observation_date"], batch["batch_id"])
        ):
            raise DurabilityError("pending batch control is outside its canonical path")
        expected = _batch_payload(rt, batch)
        if any(payload.get(key) != value for key, value in expected.items()):
            raise DurabilityError("pending batch control does not match ledger state")
    elif path.name == "_scan.json":
        scan = rt.ledger.get_scan(payload.get("scan_id", ""))
        if scan is not None and path != scan_marker_path(
            scan_dir_for(rt.settings, batch["observation_date"], batch["batch_id"], scan["scan_id"])
        ):
            raise DurabilityError("pending scan control is outside its canonical path")
        if (
            scan is None
            or payload
            != _scan_payload(
                rt,
                batch,
                scan,
                payload["status"],
                payload.get("error"),
                finished_at=payload.get("finished_at"),
            )
            or scan["status"] != payload["status"]
        ):
            raise DurabilityError("pending scan control does not match ledger state")
    else:
        raise DurabilityError("unsupported pending capture control")
    write_marker(path, payload)
    _release_temporary(rt, control["control_id"])
    fault_point("after_control_marker_commit")
    rt.ledger.clear_control(control["control_id"])


def _recover_controls(rt: CaptureRuntime, batch_id: str) -> None:
    for control in rt.ledger.pending_controls(batch_id):
        _reserve(rt, 2 * len(control["payload_json"].encode()) + 4096, control["control_id"])
        _promote_control(rt, control)


def _write_batch_marker(rt: CaptureRuntime, batch: dict[str, Any], **fields: Any) -> None:
    path = batch_marker_path(_batch_dir(rt.settings, batch["observation_date"], batch["batch_id"]))
    pending = next(
        (
            c
            for c in rt.ledger.pending_controls(batch["batch_id"])
            if c["control_id"] == path.relative_to(rt.settings.raw_dir).as_posix()
        ),
        None,
    )
    if pending is None:
        pending = _prepare_control(rt, path, _batch_payload(rt, batch, **fields))
        rt.ledger.put_control(pending)
    fault_point("after_control_ledger_commit")
    _promote_control(rt, pending)


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


def _durable_records(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    predicate: Callable[[dict[str, Any]], bool],
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    """Yield ``(scan, (manifest, records))`` for every attempt matching ``predicate``."""
    for scan in _latest_by_name(rt.ledger.list_scans(batch["batch_id"])).values():
        if scan["status"] != "complete" or not predicate(scan):
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
    for scan in _latest_by_name(rt.ledger.list_scans(batch["batch_id"])).values():
        if scan["status"] != "complete" or not predicate(scan):
            continue
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
        )
        for manifest, _records in iter_scan_pages(rt.settings, batch, scan):
            try:
                body = json.loads(
                    read_body(
                        directory,
                        manifest,
                        trusted_root=rt.settings.raw_dir,
                        max_body_bytes=rt.settings.capture.max_response_bytes,
                    )
                )
            except json.JSONDecodeError:
                continue
            if not isinstance(body, dict):
                continue
            for item in body.get("fetch_failed") or []:
                if isinstance(item, dict) and "id" in item:
                    failed.add(str(item["id"]))
    return failed


def _market_stub_ids(rt: CaptureRuntime, batch: dict[str, Any]) -> set[str]:
    scope = _scope(batch)
    stubs: set[str] = set()
    for scan, page in _durable_records(rt, batch, lambda s: s.get("phase", 0) <= 1):
        for record in page["records"]:
            if not isinstance(record, dict):
                continue
            markets = [record] if scan["record_key"] == "markets" else record.get("markets", [])
            for market in markets if isinstance(markets, list) else []:
                if not isinstance(market, dict):
                    continue
                value = str(market.get("id", ""))
                if scope["kind"] == "selected" and value not in scope["market_ids"]:
                    continue
                if (
                    scope["kind"] == "catalogue"
                    and value.isascii()
                    and value.isdecimal()
                    and int(value) > scope["high_water"]["markets"]
                    and value not in scope["baseline"]["markets"]
                ):
                    continue
                for stub in market.get("events", []) or []:
                    if isinstance(stub, dict):
                        event_id = str(stub.get("id", ""))
                        if (
                            event_id.isascii()
                            and event_id.isdecimal()
                            and 0 < len(event_id) <= 20
                            and int(event_id) > 0
                            and str(int(event_id)) == event_id
                        ):
                            stubs.add(event_id)
    return stubs


def _advance_plan(rt: CaptureRuntime, batch_id: str) -> bool:
    batch = rt.ledger.get_batch(batch_id)
    assert batch is not None
    phase = batch["plan_stage"]
    if phase == -1:
        _seal_plan(rt, batch)
        return True
    if phase >= 4:
        return False
    latest = _latest_by_name(rt.ledger.list_scans(batch_id))
    active = [s for s in latest.values() if s.get("phase", 0) == phase]
    if not _all_complete(active):
        return False
    scope = _scope(batch)
    specs: list[ScanSpec] = []
    next_phase = phase + 1
    if phase == 0 and batch["mode"] == "daily":
        returned = _event_ids_from(
            rt, batch, lambda s: s["record_key"] == "markets" and s.get("phase", 0) == 0
        )
        missing = sorted(set(scope["baseline"]["markets"]) - returned, key=int)
        specs = [
            native_single_scan(f"markets_refresh_{i:04d}", ids, "markets")
            for i, ids in enumerate(chunk(missing), 1)
        ]
    elif phase == 1:
        known = _event_ids_from(
            rt, batch, lambda s: s["record_key"] == "events" and s.get("phase", 0) == 0
        )
        known.update(
            _fetch_failed_ids(
                rt, batch, lambda s: s["record_key"] == "events" and s.get("phase", 0) == 0
            )
        )
        parents = _market_stub_ids(rt, batch)
        scope["parent_event_ids"] = sorted(parents - set(scope["event_ids"]), key=int)
        candidates = parents | set(scope["baseline"]["events"])
        ids = sorted(candidates - known, key=int)
        if scope["kind"] == "selected":
            specs = [
                native_single_scan(f"events_parents_{i:04d}", values, "events")
                for i, values in enumerate(chunk(ids), 1)
            ]
        else:
            specs = [id_chunk_scan(i, values) for i, values in enumerate(chunk(ids), 1)]
    elif phase == 2:
        requested = {v for s in active for v in json.loads(s["input_ids_json"] or "[]")}
        returned = _event_ids_from(rt, batch, lambda s: s.get("phase", 0) == 2)
        failed = _fetch_failed_ids(rt, batch, lambda s: s.get("phase", 0) == 2)
        # Individual 404s are confirmed absence, not a reason to request twice.
        singles = {
            v
            for s in active
            if s["kind"] == "single_ids"
            for v in json.loads(s["input_ids_json"] or "[]")
        }
        missing = sorted(requested - returned - failed - singles, key=int)
        specs = [single_id_scan(i, values) for i, values in enumerate(chunk(missing), 1)]
    start = rt.ledger.max_plan_order(batch_id)
    rows = [
        _scan_row(
            batch_id,
            spec,
            attempt=1,
            plan_order=start + i,
            started_at=iso_utc(rt.now()),
            phase=next_phase,
        )
        for i, spec in enumerate(specs, 1)
    ]
    _commit_plan(rt, batch, next_phase, rows, scope)
    return True


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
            phase=row.get("phase", 0),
            started_at=iso_utc(rt.now()),
        )
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], successor["scan_id"]
        )
        control = _prepare_control(
            rt, scan_marker_path(directory), _scan_payload(rt, batch, successor, "running")
        )
        rt.ledger.add_scan_attempt(successor, control=control)
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
    if batch["plan_stage"] == 4 and _all_complete(list(latest.values())) and crawl_ok:
        finished = iso_utc(rt.now())
        payload = _batch_payload(
            rt,
            {**batch, "status": "captured"},
            finished_at=finished,
            scans={name: s["scan_id"] for name, s in latest.items()},
        )
        control = _prepare_control(
            rt,
            batch_marker_path(_batch_dir(rt.settings, batch["observation_date"], batch_id)),
            payload,
        )
        rt.ledger.set_batch_status(batch_id, "captured", finished, control=control)
        _write_batch_marker(rt, {**batch, "status": "captured"})
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
        return _single_iter(client, list(spec.input_ids), start.seq, spec.record_key)
    raise ValueError(f"unknown scan kind {spec.kind!r}")


def _single_iter(
    client: GammaClient, ids: list[str], done: int, record_key: str
) -> Iterator[PageResult]:
    last = len(ids) - 1
    for index in range(done, len(ids)):
        yield single_event_page(
            client, ids[index], seq=index + 1, terminal=index == last, record_key=record_key
        )


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
        manifest = read_manifest(path, trusted_root=rt.settings.raw_dir)
        if (
            manifest is None
            or manifest.get("seq") != expected
            or not verify_page(
                directory,
                manifest,
                trusted_root=rt.settings.raw_dir,
                max_body_bytes=rt.settings.capture.max_response_bytes,
            )
        ):
            raise DurabilityError("orphan page is corrupt")
        previous = rt.ledger.page_for_scan(scan["scan_id"], expected - 1)
        validate_page_identity(batch, scan, manifest, seq=expected, previous=previous)
        read_page_records(
            rt.settings, directory, manifest, scan["record_key"], scan=scan, previous=previous
        )
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
    write_token = "page:" + manifest_fields["page_id"]
    _reserve(rt, 2 * len(page.response.body) + 16_384, write_token)
    written = write_page(directory, page.seq, page.response.body, manifest_fields)
    _release_temporary(rt, write_token)
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


def _scan_payload(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan: dict[str, Any],
    status: str,
    error: str | None = None,
    *,
    finished_at: str | None = None,
) -> dict[str, Any]:
    return {
        "scan_id": scan["scan_id"],
        "batch_id": batch["batch_id"],
        "scan_name": scan["scan_name"],
        "attempt": scan["attempt"],
        "plan_order": scan["plan_order"],
        "phase": scan.get("phase", 0),
        "kind": scan["kind"],
        "endpoint": scan["endpoint"],
        "record_key": scan["record_key"],
        "params": json.loads(scan["params_json"]),
        "input_ids": json.loads(scan["input_ids_json"] or "[]"),
        "status": status,
        "error": error,
        "started_at": scan["started_at"],
        "finished_at": finished_at
        if finished_at is not None
        else (iso_utc(rt.now()) if status != "running" else None),
    }


def _write_scan_marker(
    rt: CaptureRuntime,
    batch: dict[str, Any],
    scan: dict[str, Any],
    directory: Path,
    status: str,
    error: str | None = None,
) -> None:
    path = scan_marker_path(directory)
    pending = next(
        (
            c
            for c in rt.ledger.pending_controls(batch["batch_id"])
            if c["control_id"] == path.relative_to(rt.settings.raw_dir).as_posix()
        ),
        None,
    )
    if pending is None:
        pending = _prepare_control(rt, path, _scan_payload(rt, batch, scan, status, error))
        rt.ledger.put_control(pending)
    fault_point("after_control_ledger_commit")
    _promote_control(rt, pending)


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
        target = scan_marker_path(directory).relative_to(rt.settings.raw_dir).as_posix()
        if any(c["control_id"] == target for c in rt.ledger.pending_controls(batch["batch_id"])):
            # A committed pending transition must keep the exact state its journal proves.
            raise
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
    finished = iso_utc(rt.now())
    control = _prepare_control(
        rt,
        scan_marker_path(directory),
        _scan_payload(rt, batch, scan, status, finished_at=finished),
    )
    rt.ledger.set_scan_status(scan["scan_id"], status, finished, None, control=control)
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
    successor = _scan_row(
        batch["batch_id"],
        scan_spec_from_row(old),
        attempt=old["attempt"] + 1,
        plan_order=old["plan_order"],
        phase=old.get("phase", 0),
        started_at=iso_utc(rt.now()),
    )
    new_directory = scan_dir_for(
        rt.settings, batch["observation_date"], batch["batch_id"], successor["scan_id"]
    )
    control = _prepare_control(
        rt, scan_marker_path(new_directory), _scan_payload(rt, batch, successor, "running")
    )
    rt.ledger.add_scan_attempt(successor, control=control)
    _mark_planned(rt, batch, [successor])


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
    requests_before: int = 0,
    bytes_before: int = 0,
) -> None:
    """Write the single ``stage_runs`` row for one capture attempt, success or failure."""
    captured = summary is not None and summary.status == "captured" and error is None
    http = rt.client.stats.as_dict()
    http["requests"] -= requests_before
    http["downloaded_bytes"] -= bytes_before
    counts: dict[str, Any] = {"mode": mode, "http": http}
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
        open_market_ids=rt.open_market_ids,
        storage=rt.storage,
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
            remaining = iter(rows)
            futures = [executor.submit(run_row, row) for row in islice(remaining, worker_count)]
            pending = set(futures)
            while pending:
                if SIGNALS.pending is not None:
                    raise Terminated(SIGNALS.pending)
                done, pending = wait(pending, timeout=POOL_POLL_S, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
                    row = next(remaining, None)
                    if row is not None:
                        successor = executor.submit(run_row, row)
                        futures.append(successor)
                        pending.add(successor)
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


def _verify_resume(rt: CaptureRuntime, batch: dict[str, Any]) -> None:
    scope = _scope(batch)
    marker = _read_marker(
        batch_marker_path(_batch_dir(rt.settings, batch["observation_date"], batch["batch_id"])),
        rt.settings,
    )
    if (
        marker.get("batch_id") != batch["batch_id"]
        or marker.get("mode") != batch["mode"]
        or marker.get("scope") != scope
        or marker.get("plan_stage") != batch["plan_stage"]
    ):
        raise DurabilityError("committed capture scope does not match the ledger")
    planned = sorted(
        (s for s in rt.ledger.list_scans(batch["batch_id"]) if s["attempt"] == 1),
        key=lambda s: s["plan_order"],
    )
    if marker.get("plan") != [_plan_entry(s) for s in planned]:
        raise DurabilityError("committed capture plan does not match the ledger")
    if batch["status"] in {"captured", "loaded"}:
        expected_latest = {
            name: scan["scan_id"]
            for name, scan in _latest_by_name(rt.ledger.list_scans(batch["batch_id"])).items()
        }
        if marker.get("status") != "captured" or marker.get("scans") != expected_latest:
            raise DurabilityError("completed batch marker does not identify its complete attempts")
    for scan in rt.ledger.list_scans(batch["batch_id"]):
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
        )
        pages = rt.ledger.pages_for_scan(scan["scan_id"])
        scan_path = scan_marker_path(directory)
        if scan_path.exists() or pages:
            scan_marker = _read_marker(scan_path, rt.settings)
            expected = {
                "scan_id": scan["scan_id"],
                "batch_id": batch["batch_id"],
                "scan_name": scan["scan_name"],
                "attempt": scan["attempt"],
                "plan_order": scan["plan_order"],
                "phase": scan.get("phase", 0),
                "kind": scan["kind"],
                "endpoint": scan["endpoint"],
                "record_key": scan["record_key"],
                "params": json.loads(scan["params_json"]),
                "input_ids": json.loads(scan["input_ids_json"] or "[]"),
            }
            if any(scan_marker.get(k) != v for k, v in expected.items()):
                raise DurabilityError("committed scan marker differs from its ledger plan")
            if scan["status"] == "complete" and scan_marker.get("status") != "complete":
                raise DurabilityError("completed scan marker has inconsistent status")
        previous_manifest = None
        for page in pages:
            manifest = read_manifest(
                manifest_path(directory, page["seq"]), trusted_root=rt.settings.raw_dir
            )
            if manifest is None or not verify_page(
                directory,
                manifest,
                trusted_root=rt.settings.raw_dir,
                max_body_bytes=rt.settings.capture.max_response_bytes,
            ):
                raise DurabilityError("committed capture page is missing or corrupt")
            validate_page_identity(
                batch, scan, manifest, seq=page["seq"], previous=previous_manifest
            )
            read_page_records(
                rt.settings,
                directory,
                manifest,
                scan["record_key"],
                scan=scan,
                previous=previous_manifest,
            )
            previous_manifest = manifest
            actual = _row_from_manifest(manifest, batch["batch_id"], scan["scan_id"])
            if any(actual[key] != page[key] for key in actual if key != "ids_hash"):
                raise DurabilityError("committed capture page differs from its ledger evidence")
        expected_seq = len(pages) + 1
        while manifest_path(directory, expected_seq).exists():
            orphan = read_manifest(
                manifest_path(directory, expected_seq), trusted_root=rt.settings.raw_dir
            )
            if orphan is None or not verify_page(
                directory,
                orphan,
                trusted_root=rt.settings.raw_dir,
                max_body_bytes=rt.settings.capture.max_response_bytes,
            ):
                raise DurabilityError("orphan capture page is corrupt")
            validate_page_identity(
                batch, scan, orphan, seq=expected_seq, previous=previous_manifest
            )
            read_page_records(
                rt.settings,
                directory,
                orphan,
                scan["record_key"],
                scan=scan,
                previous=previous_manifest,
            )
            previous_manifest = orphan
            expected_seq += 1
        if scan["status"] == "complete" and (not pages or not pages[-1]["terminal"]):
            raise DurabilityError("completed scan has no committed terminal page")


def run_capture(
    rt: CaptureRuntime, mode: str, *, market_ids=(), event_ids=(), resume: str | None = None
) -> CaptureSummary:
    """Start a deliberate new acquisition, or explicitly resume a verified named batch."""
    started = iso_utc(rt.now())
    began = time.monotonic()
    requests_before = rt.client.stats.requests
    bytes_before = rt.client.stats.downloaded_bytes
    batch_id: str | None = None
    summary: CaptureSummary | None = None
    try:
        if mode not in {"bootstrap", "daily", "reconcile", "selected"}:
            raise ValueError("unsupported capture mode")
        selected_markets, selected_events = _explicit_ids(market_ids), _explicit_ids(event_ids)
        if mode != "selected" and (selected_markets or selected_events):
            raise ValueError("explicit IDs require selected mode")
        if mode == "selected" and resume is None and not (selected_markets or selected_events):
            raise ValueError("selected mode requires at least one explicit ID")
        if resume is not None:
            batch = rt.ledger.get_batch(resume)
            if (
                batch is None
                or batch["mode"] != mode
                or batch["status"] not in {"capturing", "captured", "loaded"}
            ):
                raise ValueError("resume requires an existing batch of the same mode")
            batch_id = batch["batch_id"]
            scope = _scope(batch)
            if (selected_markets and selected_markets != scope["market_ids"]) or (
                selected_events and selected_events != scope["event_ids"]
            ):
                raise ValueError("resume cannot change committed selection")
            rt.storage = _storage_budget(rt.settings)
            _recover_controls(rt, batch_id)
            batch = rt.ledger.get_batch(batch_id)
            assert batch is not None
            _verify_resume(rt, batch)
            summary = CaptureSummary(
                batch_id,
                "captured" if batch["status"] in {"captured", "loaded"} else "capturing",
                True,
            )
        else:
            rt.storage = _storage_budget(rt.settings)
            batch = _start_batch(rt, mode, selected_markets, selected_events)
            batch_id = batch["batch_id"]
            summary = CaptureSummary(batch_id, "capturing", False)
        if summary.status != "captured":
            while True:
                progressed = _advance_plan(rt, batch_id)
                batch = rt.ledger.get_batch(batch_id)
                assert batch is not None
                _resume_orphaned_attempts(rt, batch)
                listed = rt.ledger.list_scans(batch_id)
                held = _held_closed_windows(listed)
                runnable = [
                    s
                    for s in _latest_by_name(listed).values()
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
            summary.status = _finalise_if_complete(rt, batch_id)
    except BaseException as exc:
        if batch_id is None:
            candidate = rt.ledger.find_resumable_batch(mode)
            if candidate is not None and candidate["started_at"] == started:
                batch_id = candidate["batch_id"]
        if batch_id is not None:
            exc.batch_id = batch_id
            rt.ledger.record_batch_error(
                batch_id, iso_utc(rt.now()), f"{type(exc).__name__}: {exc}"[:500]
            )
        _record_capture_stage(
            rt,
            mode=mode,
            batch_id=batch_id,
            started=started,
            summary=summary,
            error=f"{type(exc).__name__}: {exc}"[:2000],
            requests_before=requests_before,
            bytes_before=bytes_before,
        )
        raise
    assert summary is not None
    summary.http_attempts = rt.client.stats.requests - requests_before
    summary.downloaded_bytes = rt.client.stats.downloaded_bytes - bytes_before
    summary.duration_s = time.monotonic() - began
    error = None if summary.status == "captured" else f"capture ended with status {summary.status}"
    _record_capture_stage(
        rt,
        mode=mode,
        batch_id=batch_id,
        started=started,
        summary=summary,
        error=error,
        requests_before=requests_before,
        bytes_before=bytes_before,
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
        "phase": entry.get("phase", 0),
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
        meta = _read_marker(batch_marker, settings)
        if meta.get("scope", {}).get("revision") != 2:
            raise DurabilityError("raw recovery requires a supported committed capture scope")
        batch_id = meta["batch_id"]
        started, mode = parse_batch_id(batch_id)
        if (
            batch_id != batch_dir.name
            or meta["mode"] != mode
            or meta["observation_date"] != observation_date(started)
            or batch_dir.parent.name != observation_date(started)
        ):
            raise DurabilityError("raw batch marker is outside its declared identity")
        if ledger.get_batch(batch_id) is None:
            ledger.create_batch(
                batch_id,
                meta["mode"],
                meta["observation_date"],
                meta["started_at"],
                meta.get("git_sha"),
                [],
                scope=meta["scope"],
                plan_stage=meta["plan_stage"],
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
            scan_meta = _read_marker(marker, settings)
            scan_id = scan_meta["scan_id"]
            if (
                not re.fullmatch(r"[a-z][a-z0-9_]*", scan_meta["scan_name"])
                or scan_meta["batch_id"] != batch_id
                or scan_id != make_scan_id(batch_id, scan_meta["scan_name"], scan_meta["attempt"])
                or scan_id != scan_dir.name
            ):
                raise DurabilityError("raw scan marker is outside its declared identity")
            if ledger.get_scan(scan_id) is None:
                ledger.add_scan_attempt(
                    {
                        "scan_id": scan_id,
                        "batch_id": batch_id,
                        "scan_name": scan_meta["scan_name"],
                        "attempt": scan_meta["attempt"],
                        "plan_order": scan_meta["plan_order"],
                        "phase": scan_meta.get("phase", 0),
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
            previous_manifest = None
            while True:
                seq += 1
                path = manifest_path(scan_dir, seq)
                if not path.exists():
                    break
                manifest = read_manifest(path, trusted_root=settings.raw_dir)
                if manifest is None or not verify_page(
                    scan_dir,
                    manifest,
                    trusted_root=settings.raw_dir,
                    max_body_bytes=settings.capture.max_response_bytes,
                ):
                    raise DurabilityError("raw recovery encountered a corrupt page")
                if (
                    manifest["seq"] != seq
                    or manifest["batch_id"] != batch_id
                    or manifest["scan_id"] != scan_id
                    or manifest["page_id"] != make_page_id(scan_id, seq)
                ):
                    raise DurabilityError("raw page manifest is outside its declared identity")
                recovered_scan = ledger.get_scan(scan_id)
                assert recovered_scan is not None
                validate_page_identity(
                    meta, recovered_scan, manifest, seq=seq, previous=previous_manifest
                )
                read_page_records(
                    settings,
                    scan_dir,
                    manifest,
                    recovered_scan["record_key"],
                    scan=recovered_scan,
                    previous=previous_manifest,
                )
                previous_manifest = manifest
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
                raise DurabilityError("completed raw scan has no terminal page")
            ledger.set_scan_status(scan_id, status, finished_at, error)

        _restore_batch_state(ledger, batch_id, meta)
    return counts


def _restore_batch_state(ledger: Ledger, batch_id: str, meta: dict[str, Any]) -> None:
    """Infer the plan stage from the scans on disk and apply the batch marker's status."""
    scans = ledger.list_scans(batch_id)
    captured = meta.get("status") == "captured" and _all_complete(
        list(_latest_by_name(scans).values())
    )
    if meta.get("status") == "captured" and not captured:
        raise DurabilityError("captured raw batch has incomplete scans")
    status = "captured" if captured else "capturing"
    ledger.restore_batch_state(
        batch_id, status, meta["plan_stage"], meta.get("finished_at") or meta["started_at"]
    )


__all__ = [
    "CaptureRuntime",
    "CaptureSummary",
    "abandon_batch",
    "rebuild_from_raw",
    "run_capture",
]
