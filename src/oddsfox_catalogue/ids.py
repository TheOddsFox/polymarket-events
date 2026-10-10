"""Deterministic identifiers, canonical JSON, and checksums.

Identifiers are derived from logical position, never from wall-clock reads
during replay, so re-running a recovery produces the same keys.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

BATCH_STAMP_FORMAT = "%Y%m%dT%H%M%SZ"
_BATCH_ID_RE = re.compile(
    r"^\d{8}T\d{6}Z-(bootstrap|daily|reconcile|selected)(?:-(?:[2-9]|[1-9][0-9]+))?$"
)
MODES = ("bootstrap", "daily", "reconcile", "selected")


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("naive datetimes are not allowed; pass timezone-aware UTC")
    return value.astimezone(UTC)


def iso_utc(value: datetime) -> str:
    return ensure_utc(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def observation_date(batch_started: datetime) -> str:
    return ensure_utc(batch_started).date().isoformat()


def make_batch_id(mode: str, started: datetime) -> str:
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}")
    return f"{ensure_utc(started).strftime(BATCH_STAMP_FORMAT)}-{mode}"


def parse_batch_id(batch_id: str) -> tuple[datetime, str]:
    if not _BATCH_ID_RE.match(batch_id):
        raise ValueError(f"malformed batch id {batch_id!r}")
    stamp, mode, *_ = batch_id.split("-")
    return datetime.strptime(stamp, BATCH_STAMP_FORMAT).replace(tzinfo=UTC), mode


def make_scan_id(batch_id: str, scan_name: str, attempt: int) -> str:
    if attempt < 1:
        raise ValueError("attempt starts at 1")
    return f"{batch_id}.{scan_name}.a{attempt}"


def make_page_id(scan_id: str, seq: int) -> str:
    if seq < 1:
        raise ValueError("page sequence starts at 1")
    return f"{scan_id}.p{seq:06d}"


MAX_CANONICAL_BYTES = 16 * 1024**2


def validate_exact_size(value: Any, *, max_bytes: int = MAX_CANONICAL_BYTES) -> None:
    """Count encoded tokens before Decimal expansion or whole-output allocation."""
    encoder = json.JSONEncoder(
        sort_keys=True, separators=(",", ":"), allow_nan=False, default=_exact_number
    )
    size = 0
    try:
        for token in encoder.iterencode(value):
            size += len(token.encode("utf-8"))
            if size > max_bytes:
                raise ValueError("canonical JSON expansion exceeds its byte allowance")
    except RecursionError as exc:
        raise ValueError("canonical JSON nesting exceeds its limit") from exc


def canonical_json(value: Any, *, max_bytes: int = MAX_CANONICAL_BYTES) -> str:
    """Stable JSON text: sorted keys, compact separators, no NaN."""
    validate_exact_size(value, max_bytes=max_bytes)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=_exact_number
    )


def _exact_number(value: Any) -> str:
    if isinstance(value, Decimal) and value.is_finite():
        from oddsfox_catalogue.normalization import decimal_string

        return decimal_string(value)
    raise TypeError("unsupported canonical JSON value")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def payload_hash(value: Any) -> str:
    return sha256_text(canonical_json(value))


def observation_id(page_id: str, json_pointer: str) -> str:
    """Identity of one observed record: a page plus the record's logical pointer."""
    if not json_pointer.startswith("/"):
        raise ValueError("json pointers start with '/'")
    return sha256_text(f"{page_id}\x00{json_pointer}")


def ids_hash(ids: list[str]) -> str:
    """Order-independent fingerprint of a set of identifiers."""
    return sha256_text("\n".join(sorted(ids)))
