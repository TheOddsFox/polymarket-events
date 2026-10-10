"""Read verified records back from the raw store, using the ledger as the index."""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.writer import manifest_path, read_body, read_manifest
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import MalformedResponse, encode_params
from oddsfox_catalogue.gamma.paginators import (
    _describes,
    _ids,
    _reject_window_mismatch,
    _window_params,
    unpack,
)
from oddsfox_catalogue.gamma.scans import ScanSpec, scan_spec_from_row
from oddsfox_catalogue.ids import canonical_json, ids_hash, make_page_id


class DurabilityError(RuntimeError):
    """A page the ledger considers fetched is missing or fails its checksum."""


def scan_dir_for(settings: Settings, observation_date: str, batch_id: str, scan_id: str) -> Path:
    return settings.raw_dir / observation_date / batch_id / scan_id


def read_indexed_manifest(settings: Settings, directory: Path, page: dict[str, Any] | None):
    """Read the actual page revision while binding its position to the ledger index."""
    if page is None:
        return None
    manifest = read_manifest(manifest_path(directory, page["seq"]), trusted_root=settings.raw_dir)
    fields = (
        "page_id",
        "batch_id",
        "scan_id",
        "seq",
        "endpoint",
        "input_cursor",
        "output_cursor",
        "offset_start",
        "offset_end",
        "record_count",
        "http_status",
        "retries",
        "latency_s",
        "body_sha256",
        "gz_sha256",
        "observed_at",
    )
    if (
        manifest is None
        or any(manifest.get(key) != page[key] for key in fields)
        or bool(manifest["terminal"]) != bool(page["terminal"])
        or canonical_json(manifest["params"]) != page["params_json"]
    ):
        raise DurabilityError("raw page manifest differs from the committed ledger evidence")
    return manifest


def _page_revision(manifest: dict[str, Any], spec: ScanSpec) -> int:
    revision = manifest.get("page_unit_revision", 1)
    if type(revision) is not int or revision not in {1, 2}:
        raise DurabilityError("unsupported capture page unit revision")
    if revision == 2 and (
        spec.kind not in {"id_range", "keyset_ids"} or spec.param_dict.get("tail")
    ):
        raise DurabilityError("native leaf pages require a finite ID scan")
    return revision


def _leaf_ids(spec: ScanSpec, manifest: dict[str, Any]) -> list[int]:
    start, end = manifest.get("offset_start"), manifest.get("offset_end")
    if type(start) is not int or type(end) is not int:
        raise DurabilityError("native leaf positions must be integers")
    if spec.kind == "id_range":
        params = spec.param_dict
        lo, hi, step = params["lo"], params.get("hi"), params["step"]
        if hi is None or not lo <= start <= end <= hi:
            raise DurabilityError("native leaf is outside the sealed ID interval")
        boundary = min(lo + ((start - lo) // step + 1) * step - 1, hi)
        if end > boundary or not 1 <= end - start + 1 <= step <= 100:
            raise DurabilityError("native leaf crosses its committed request window")
        return list(range(start, end + 1))
    if not 0 <= start < end <= len(spec.input_ids) or end - start > 100:
        raise DurabilityError("native leaf is outside its frozen input list")
    return [int(value) for value in spec.input_ids[start:end]]


def validate_page_identity(
    batch: dict[str, Any],
    scan: dict[str, Any],
    manifest: dict[str, Any],
    *,
    seq: int,
    previous: dict[str, Any] | None = None,
) -> None:
    """A checksummed page must answer the exact committed scan unit."""
    if previous is not None and previous.get("terminal"):
        raise DurabilityError("capture page follows a committed terminal unit")
    expected = {
        "seq": seq,
        "page_id": make_page_id(scan["scan_id"], seq),
        "batch_id": batch["batch_id"],
        "scan_id": scan["scan_id"],
        "scan_name": scan["scan_name"],
        "attempt": scan["attempt"],
        "plan_order": scan["plan_order"],
        "observation_date": batch["observation_date"],
        "record_key": scan["record_key"],
    }
    spec = scan_spec_from_row(scan)
    revision = _page_revision(manifest, spec)
    if seq > 1 and (
        previous is None
        or previous.get("seq") != seq - 1
        or previous.get("scan_id") != scan["scan_id"]
        or previous.get("page_id") != make_page_id(scan["scan_id"], seq - 1)
    ):
        raise DurabilityError("capture page has no verified contiguous predecessor")
    if previous is not None and _page_revision(previous, spec) > revision:
        raise DurabilityError("capture page unit revision cannot be downgraded")
    params = spec.param_dict
    cursor = None if previous is None else previous["output_cursor"]
    endpoint = spec.endpoint
    input_cursor = None
    offset = None
    if revision == 2:
        wanted = _leaf_ids(spec, manifest)
        if spec.kind == "id_range":
            offset = params["lo"] if previous is None else previous["offset_end"] + 1
            terminal = manifest["offset_end"] == params["hi"]
            closed = params.get("closed")
            extra = {
                key: True
                for key in ("include_chat", "include_template", "include_best_lines")
                if params.get(key)
            }
        else:
            offset = 0 if previous is None else previous["offset_end"]
            terminal = manifest["offset_end"] == len(spec.input_ids)
            closed, extra = None, {}
        params = _window_params(wanted, spec.record_key, closed, extra)
        native_single = "/" + spec.record_key + "/" + str(wanted[0])
        if len(wanted) == 1 and manifest.get("endpoint") == native_single:
            endpoint = native_single
            params = {"include_tag": True} if spec.record_key == "markets" else {}
            params.update(extra)
        expected.update(
            {"terminal": terminal, "output_cursor": None, "offset_end": manifest["offset_end"]}
        )
        params = encode_params(params)
        if type(manifest.get("seq")) is not int or type(manifest.get("terminal")) is not bool:
            raise DurabilityError("native leaf has invalid sequence or terminal accounting")
    elif spec.kind == "single_ids":
        if seq < 1 or seq > len(spec.input_ids):
            raise DurabilityError("single-ID page sequence is outside its committed inputs")
        endpoint = "/" + spec.record_key + "/" + spec.input_ids[seq - 1]
        params = {"include_tag": True} if spec.record_key == "markets" else {}
        expected.update(
            {"terminal": seq == len(spec.input_ids), "output_cursor": None, "offset_end": None}
        )
    elif spec.kind == "keyset":
        params = {key: value for key, value in params.items() if not key.startswith("_")}
        input_cursor = cursor
        if cursor is not None:
            params["after_cursor"] = cursor
        expected["offset_end"] = None
    elif spec.kind == "id_range":
        offset = int(params["lo"]) + (seq - 1) * int(params["step"])
        end = offset + int(params["step"]) - 1
        if params.get("hi") is not None:
            end = min(end, int(params["hi"]))
        if end < offset:
            raise DurabilityError("range page sequence is outside its committed interval")
        extra = {
            key: True
            for key in ("include_chat", "include_template", "include_best_lines")
            if params.get(key)
        }
        closed = params.get("closed")
        params = _window_params(list(range(offset, end + 1)), spec.record_key, closed, extra)
        expected.update({"output_cursor": None, "offset_end": end})
        if not spec.param_dict.get("tail"):
            expected["terminal"] = end == spec.param_dict["hi"]
    elif spec.kind == "keyset_ids":
        if seq != 1:
            raise DurabilityError("ID-list page sequence is outside its committed unit")
        params = _window_params([int(value) for value in spec.input_ids], spec.record_key, None)
        expected.update({"terminal": True, "output_cursor": None, "offset_end": None})
    elif spec.kind == "offset":
        offset = 0 if previous is None else previous["offset_end"]
        params["offset"] = offset
        expected["output_cursor"] = None
    else:
        raise DurabilityError("unsupported committed scan kind")
    expected.update(
        {
            "endpoint": endpoint,
            "params": params,
            "input_cursor": input_cursor,
            "offset_start": offset,
        }
    )
    if any(manifest.get(key) != value for key, value in expected.items()) or canonical_json(
        manifest.get("params")
    ) != canonical_json(params):
        raise DurabilityError("page manifest does not match its committed scan identity")


def read_page_records(
    settings: Settings,
    directory: Path,
    manifest: dict[str, Any],
    record_key: str,
    *,
    scan: dict[str, Any],
    previous: dict[str, Any] | None = None,
) -> list[Any]:
    """Checksums and decoded evidence must support the exact committed source unit."""
    spec = scan_spec_from_row(scan)
    if spec.record_key != record_key or manifest.get("record_key") != record_key:
        raise DurabilityError("capture page declares an inconsistent entity type")
    status = manifest.get("http_status")
    count = manifest.get("record_count")
    if isinstance(status, bool) or not isinstance(status, int) or status not in {200, 404}:
        raise DurabilityError("capture page has an unsupported HTTP status")
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        or not isinstance(manifest.get("terminal"), bool)
    ):
        raise DurabilityError("capture page has invalid accounting fields")
    try:
        raw = read_body(
            directory,
            manifest,
            trusted_root=settings.raw_dir,
            max_body_bytes=settings.capture.max_response_bytes,
        )
    except (OSError, ValueError) as exc:
        raise DurabilityError("capture page body is missing or corrupt") from exc
    if status == 404:
        if count != 0 or spec.kind not in {"single_ids", "id_range", "keyset_ids"}:
            raise DurabilityError("confirmed absence cannot contain declared records")
        if manifest.get("ids_hash") != ids_hash([]):
            raise DurabilityError("confirmed absence has inconsistent identity accounting")
        if _page_revision(manifest, spec) == 2:
            wanted = _leaf_ids(spec, manifest)
            if len(wanted) != 1 or manifest["endpoint"] != f"/{record_key}/{wanted[0]}":
                raise DurabilityError("native absence requires a singleton by-ID response")
        return []

    def reject_constant(value: str):
        raise ValueError("non-finite JSON number")

    try:
        body = json.loads(raw, parse_float=Decimal, parse_constant=reject_constant)
        records, next_cursor = unpack(body, record_key)
    except (ValueError, MalformedResponse) as exc:
        raise DurabilityError("capture page envelope is untrustworthy") from exc
    if len(records) != count:
        raise DurabilityError("decoded capture record count does not match its manifest")
    if isinstance(body, dict) and body.get("fetch_failed"):
        raise DurabilityError("unresolved fetch failures cannot be a completed capture unit")
    source_ids = _ids(records)
    if manifest.get("ids_hash") != ids_hash(source_ids):
        raise DurabilityError("decoded identities do not match capture accounting")
    try:
        if spec.kind == "single_ids":
            target = spec.input_ids[manifest["seq"] - 1]
            if next_cursor is not None or not _describes(records, target):
                raise MalformedResponse("single-ID evidence does not describe its target")
        elif spec.kind in {"id_range", "keyset_ids"}:
            wanted = (
                _leaf_ids(spec, manifest)
                if _page_revision(manifest, spec) == 2
                else manifest["params"]["id"]
            )
            if manifest["endpoint"] != spec.endpoint:
                if next_cursor is not None or not _describes(records, str(wanted[0])):
                    raise MalformedResponse("native by-ID evidence does not describe its target")
            else:
                _reject_window_mismatch(spec.endpoint, wanted, records, next_cursor)
        elif spec.kind == "keyset":
            if next_cursor is not None and (
                not records or next_cursor == manifest.get("input_cursor")
            ):
                raise MalformedResponse("keyset evidence has an invalid continuation")
            high_water = spec.param_dict.get("_high_water")
            numeric_ids = [
                int(value) for value in source_ids if value.isascii() and value.isdigit()
            ]
            reached = False
            if high_water is not None:
                if numeric_ids != sorted(numeric_ids):
                    raise MalformedResponse("sealed keyset evidence is not ordered")
                reached = high_water == 0 or any(value >= high_water for value in numeric_ids)
                if previous and records and previous.get("ids_hash") == ids_hash(source_ids):
                    raise MalformedResponse("sealed keyset evidence repeats its prior records")
            if manifest.get("output_cursor") != next_cursor or manifest["terminal"] != (
                next_cursor is None or reached
            ):
                raise MalformedResponse("keyset continuation differs from decoded evidence")
        elif spec.kind == "offset":
            if manifest["terminal"] != (not records) or manifest.get("offset_end") != (
                manifest["offset_start"] + len(records)
            ):
                raise MalformedResponse("offset continuation differs from decoded evidence")
    except (KeyError, IndexError, TypeError, MalformedResponse) as exc:
        raise DurabilityError("capture page does not answer its committed source unit") from exc
    return records


def iter_scan_pages(
    settings: Settings,
    batch: dict[str, Any],
    scan: dict[str, Any],
) -> Iterator[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Yield ``(manifest, records)`` for each durable page of one scan attempt, in order."""
    directory = scan_dir_for(
        settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
    )
    seq = 0
    previous = None
    while True:
        seq += 1
        path = manifest_path(directory, seq)
        if not path.exists():
            break
        manifest = read_manifest(path, trusted_root=settings.raw_dir)
        if manifest is None:
            raise DurabilityError(f"page {seq} of scan {scan['scan_id']} is not durable")
        validate_page_identity(batch, scan, manifest, seq=seq, previous=previous)
        records = read_page_records(
            settings, directory, manifest, scan["record_key"], scan=scan, previous=previous
        )
        previous = manifest
        yield manifest, records
