"""Verified complete-root capture evidence and its public coverage accounting."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import (
    DurabilityError,
    read_page_records,
    validate_page_identity,
)
from oddsfox_catalogue.capture.runner import _plan_entry, _row_from_manifest
from oddsfox_catalogue.capture.writer import manifest_path, read_manifest, read_regular_bytes
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.ids import parse_batch_id

JSON_LIMIT = 128 * 1024**2


def _append_bounded(items, value, used):
    # Bound accumulation as well as the later JSON writer. A complete inventory
    # cannot first allocate arbitrarily many Python entries and then discover
    # that its public evidence exceeds the declared 128 MiB file limit.
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), allow_nan=False)
    size = 1
    for token in encoder.iterencode(value):
        size += len(token.encode("utf-8"))
        if used + size > JSON_LIMIT:
            raise ValueError("capture inventory exceeds its finite JSON allowance")
    items.append(value)
    return used + size


def _marker(path: Path, root: Path) -> dict[str, Any]:
    value = json.loads(read_regular_bytes(path, max_bytes=16 * 1024**2, trusted_root=root))
    if not isinstance(value, dict):
        raise ValueError("invalid capture marker")
    return value


def _file_descriptor(path: Path, root: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("unsafe capture inventory file")
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
            size += len(chunk)
    if path.stat().st_size != size:
        raise ValueError("capture inventory changed during verification")
    return {"path": path.relative_to(root).as_posix(), "sha256": digest.hexdigest(), "bytes": size}


def capture_inventory(settings: Settings, batch_ids: list[str], page_counts=None):
    try:
        return _capture_inventory(settings, batch_ids, page_counts)
    except DurabilityError as exc:
        raise ValueError("capture unit evidence is inconsistent") from exc


def _capture_inventory(settings: Settings, batch_ids: list[str], page_counts=None):
    """Validate every registered capture and reject omitted or unfinished root work.

    The immutable marker/page contracts, rather than a warehouse row count alone,
    prove completion. Historical abandoned cursor attempts remain evidence; only
    the latest attempt of every sealed unit must be complete.
    """
    if not settings.ledger_path.is_file() or settings.ledger_path.is_symlink():
        raise ValueError("capture ledger is missing")
    if not batch_ids or len(set(batch_ids)) != len(batch_ids):
        raise ValueError("capture inventory requires distinct registered batches")
    files, batches, units = [], [], []
    file_bytes, coverage_bytes = 3, 512
    with Ledger(settings.ledger_path) as ledger:
        indexed_batches = {b["batch_id"]: b for b in ledger.list_batches()}
        if set(indexed_batches) != set(batch_ids) or any(
            b["status"] != "loaded" for b in indexed_batches.values()
        ):
            raise ValueError("registered capture inventory is not the complete loaded root")
        # Every page contributes two descriptors containing two 64-byte hashes.
        # This loose lower bound rejects impossible inventories before fetching
        # all page rows from SQLite; exact streamed accounting follows below.
        page_total = ledger._one("SELECT count(*) AS count FROM pages")["count"]
        if page_total > JSON_LIMIT // 256:
            raise ValueError("capture inventory exceeds its finite entry allowance")
        raw_batches = {p.parent.name for p in settings.raw_dir.glob("*/*/_batch.json")}
        if raw_batches != set(batch_ids):
            raise ValueError("raw and registered batch inventories differ")
        for batch_id in sorted(batch_ids):
            batch = indexed_batches[batch_id]
            stamp, mode = parse_batch_id(batch_id)
            date = stamp.date().isoformat()
            directory = settings.raw_dir / date / batch_id
            if any(p.is_symlink() for p in (directory, *directory.parents)):
                raise ValueError("symlink capture directory")
            marker = _marker(directory / "_batch.json", settings.raw_dir)
            scope = json.loads(batch["scope_json"])
            if (
                marker.get("batch_id") != batch_id
                or marker.get("mode") != mode
                or mode != batch["mode"]
                or marker.get("observation_date") != date
                or date != batch["observation_date"]
                or marker.get("status") != "captured"
                or marker.get("scope") != scope
                or type(scope.get("revision")) is not int
                or scope.get("revision") != 2
                or scope.get("sealed") is not True
                or marker.get("plan_stage") != batch["plan_stage"]
            ):
                raise ValueError("capture scope is incomplete or inconsistent")
            if ledger.pending_controls(batch_id):
                raise ValueError(
                    "capture has an uncommitted control marker; resume the named batch"
                )
            scans = ledger.list_scans(batch_id)
            pages = ledger.pages_for_batch(batch_id)
            if page_counts is not None and len(pages) != page_counts[batch_id]:
                raise ValueError("registered capture count mismatch")
            if any(page["loaded_at"] is None for page in pages):
                raise ValueError("capture has unregistered loaded pages")
            planned = sorted((s for s in scans if s["attempt"] == 1), key=lambda s: s["plan_order"])
            if marker.get("plan") != [_plan_entry(s) for s in planned]:
                raise ValueError("capture plan differs from its registered scans")
            latest = {}
            for scan in scans:
                name = scan["scan_name"]
                if name not in latest or scan["attempt"] > latest[name]["attempt"]:
                    latest[name] = scan
            if (
                not latest
                or marker.get("scans") != {name: s["scan_id"] for name, s in latest.items()}
                or any(s["status"] != "complete" for s in latest.values())
            ):
                raise ValueError("capture has unfinished scans")
            scan_paths = sorted(directory.glob("*/_scan.json"))
            if {p.parent.name for p in scan_paths} != {s["scan_id"] for s in scans}:
                raise ValueError("capture scan inventory mismatch")
            members = [directory / "_batch.json"]
            for scan in scans:
                scan_dir = directory / scan["scan_id"]
                scan_path = scan_dir / "_scan.json"
                recorded = _marker(scan_path, settings.raw_dir)
                expected = {
                    "scan_id": scan["scan_id"],
                    "batch_id": batch_id,
                    "scan_name": scan["scan_name"],
                    "attempt": scan["attempt"],
                    "plan_order": scan["plan_order"],
                    "phase": scan["phase"],
                    "kind": scan["kind"],
                    "endpoint": scan["endpoint"],
                    "record_key": scan["record_key"],
                    "params": json.loads(scan["params_json"]),
                    "input_ids": json.loads(scan["input_ids_json"] or "[]"),
                    "status": scan["status"],
                }
                if any(recorded.get(k) != v for k, v in expected.items()):
                    raise ValueError("capture scan marker differs from its registered state")
                members.append(scan_path)
                previous = None
                expected_manifests, expected_bodies = set(), set()
                scan_pages = ledger.pages_for_scan(scan["scan_id"])
                for seq, page in enumerate(scan_pages, 1):
                    path = manifest_path(scan_dir, seq)
                    manifest = read_manifest(path, trusted_root=settings.raw_dir)
                    if manifest is None or page["seq"] != seq:
                        raise ValueError("capture page sequence is incomplete")
                    validate_page_identity(batch, scan, manifest, seq=seq, previous=previous)
                    read_page_records(
                        settings,
                        scan_dir,
                        manifest,
                        scan["record_key"],
                        scan=scan,
                        previous=previous,
                    )
                    actual = _row_from_manifest(manifest, batch_id, scan["scan_id"])
                    if any(actual[k] != page[k] for k in actual if k != "ids_hash"):
                        raise ValueError("capture page differs from its registered evidence")
                    previous = manifest
                    body_path = scan_dir / manifest["file"]
                    members.extend((path, body_path))
                    expected_manifests.add(path.name)
                    expected_bodies.add(body_path.name)
                    coverage_bytes = _append_bounded(
                        units,
                        {
                            "batch_id": batch_id,
                            "page_id": manifest["page_id"],
                            "endpoint": manifest["endpoint"],
                            "params": manifest["params"],
                            "received_at": manifest["observed_at"],
                            "records": manifest["record_count"],
                            "status": "absent"
                            if manifest["http_status"] == 404
                            else "success_empty"
                            if manifest["record_count"] == 0
                            else "success",
                        },
                        coverage_bytes,
                    )
                if (
                    {p.name for p in scan_dir.glob("p*.manifest.json")} != expected_manifests
                    or {p.name for p in scan_dir.glob("*.json.gz")} != expected_bodies
                    or scan["fetched_seq"] != len(scan_pages)
                    or (
                        scan["status"] == "complete"
                        and (previous is None or not previous["terminal"])
                    )
                ):
                    raise ValueError("capture scan evidence inventory is incomplete")
            coverage_bytes = _append_bounded(
                batches, {"batch_id": batch_id, "mode": mode, "scope": scope}, coverage_bytes
            )
            for member in members:
                file_bytes = _append_bounded(
                    files, _file_descriptor(member, settings.raw_dir), file_bytes
                )
    coverage = {
        "batches": batches,
        "units": units,
        "declared_scans_complete": True,
        "source_catalogue_complete": False,
        "discovery_limits": [
            "Inactive events absent from lists and without captured market references are not exhaustively discovered."
        ],
    }
    return {"files": sorted(files, key=lambda f: f["path"]), "coverage": coverage}
