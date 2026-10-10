"""Turn one verified raw page into bronze envelope rows.

This module is pure Python with no dlt or DuckDB, so every rule here is
unit-testable. Rules:

* ``observed_at`` comes from the manifest, never from the clock, so replays
  produce identical rows.
* ``observation_id = SHA256(page_id, JSON pointer)``. Pointers are logical:
  ``/events/<i>``, ``/events/<i>/markets/<j>``, and ``/markets/<i>``.
* Event stubs nested under market ``events`` are references, not observations.
* Records that are not objects, or lack an ``id``, are quarantined. The rest
  of the page still loads.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from oddsfox_catalogue.ids import iso_utc, observation_id, payload_hash, sha256_text
from oddsfox_catalogue.normalization import (
    ID_RE,
    NormalizationError,
    exact_json,
    normalize_event,
    normalize_market,
    parse_timestamp,
)

VENUE = "polymarket"
SOURCE_EVENT_EMBEDDED = "event_embedded"
SOURCE_MARKET_DIRECT = "market_direct"


class DuplicateObservation(ValueError):
    """Two rows in one chunk share an observation ID. Never expected, always a bug."""


@dataclass(frozen=True)
class PageContext:
    page_id: str
    batch_id: str
    endpoint: str
    observed_at: datetime
    record_key: str
    body_sha256: str | None = None


@dataclass
class RowSet:
    events: list[dict[str, Any]] = field(default_factory=list)
    markets: list[dict[str, Any]] = field(default_factory=list)
    quarantine: list[dict[str, Any]] = field(default_factory=list)

    def extend(self, other: RowSet) -> None:
        self.events.extend(other.events)
        self.markets.extend(other.markets)
        self.quarantine.extend(other.quarantine)


def _quarantine_id(page_id: str, pointer: str) -> str:
    return sha256_text(f"quarantine\x00{page_id}\x00{pointer}")


def _quarantine(
    ctx: PageContext,
    entity: str,
    pointer: str,
    reason: str,
    raw: Any,
) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {"value": raw}
    return {
        "quarantine_id": _quarantine_id(ctx.page_id, pointer),
        "batch_id": ctx.batch_id,
        "page_id": ctx.page_id,
        "entity": entity,
        "json_pointer": pointer,
        "reason": reason,
        "observed_at": ctx.observed_at,
        "payload": exact_json(payload),
    }


def _envelope(
    ctx: PageContext,
    record: dict[str, Any],
    pointer: str,
    entity_id: str,
) -> dict[str, Any]:
    return {
        "observation_id": observation_id(ctx.page_id, pointer),
        "venue": VENUE,
        "entity_id": entity_id,
        "batch_id": ctx.batch_id,
        "page_id": ctx.page_id,
        "observed_at": ctx.observed_at,
        "source_updated_at": parse_timestamp(record.get("updatedAt")),
        "endpoint": ctx.endpoint,
        "payload_hash": payload_hash(record),
        "payload": exact_json(record),
    }


def _check_record(record: Any) -> str | None:
    if not isinstance(record, dict):
        return "record is not an object"
    if record.get("id") in (None, ""):
        return "record has no id"
    if not ID_RE.fullmatch(str(record["id"])):
        return "record has an invalid id"
    return None


def _market_rows(
    ctx: PageContext,
    market: Any,
    pointer: str,
    source_kind: str,
    out: RowSet,
    enclosing_event_id: str | None = None,
) -> None:
    problem = _check_record(market)
    if problem:
        out.quarantine.append(_quarantine(ctx, "market", pointer, problem, market))
        return
    row = _envelope(ctx, market, pointer, str(market["id"]))
    row["source_kind"] = source_kind
    row["json_pointer"] = pointer
    provenance = {
        "observation_id": row["observation_id"],
        "page_id": ctx.page_id,
        "capture_id": ctx.batch_id,
        "received_at": iso_utc(ctx.observed_at),
        "source_kind": source_kind,
        "json_pointer": pointer,
        "payload_sha256": ctx.body_sha256 or row["payload_hash"],
    }
    try:
        row["normalized"] = normalize_market(market, provenance, enclosing_event_id)
    except NormalizationError as exc:
        out.quarantine.append(_quarantine(ctx, "market", pointer, str(exc), market))
        return
    out.markets.append(row)


def rows_for_page(
    ctx: PageContext,
    records: list[Any],
    fetch_failed: list[Any] | None = None,
    *,
    scope: dict[str, Any] | None = None,
) -> RowSet:
    """Envelope rows for every record on one page. Records are already envelope-checked.

    ``fetch_failed`` entries are ids the capture could not read. They become
    quarantine rows with reason ``fetch_failed`` and count toward the load gate.
    """
    out = RowSet()

    def selected(record: Any, entity: str) -> bool:
        if scope is None:
            return True
        if not isinstance(record, dict):
            return scope["kind"] != "selected"
        value = str(record.get("id", ""))
        if scope["kind"] == "selected":
            allowed = set(
                scope["market_ids"]
                if entity == "markets"
                else scope["event_ids"] + scope["parent_event_ids"]
            )
            return value in allowed
        mark = scope["high_water"].get(entity)
        if mark is None or not value.isascii() or not value.isdecimal():
            return True
        if value in scope["baseline"][entity] or (
            entity == "events" and value in scope["parent_event_ids"]
        ):
            return True
        return int(value) <= mark

    for index, record in enumerate(records):
        if not selected(record, ctx.record_key):
            continue
        if ctx.record_key == "markets":
            _market_rows(ctx, record, f"/markets/{index}", SOURCE_MARKET_DIRECT, out)
            continue

        pointer = f"/events/{index}"
        problem = _check_record(record)
        if problem:
            out.quarantine.append(_quarantine(ctx, "event", pointer, problem, record))
            continue
        event_row = _envelope(ctx, record, pointer, str(record["id"]))
        try:
            event_row["normalized"] = normalize_event(record)
        except NormalizationError as exc:
            out.quarantine.append(_quarantine(ctx, "event", pointer, str(exc), record))
            continue
        out.events.append(event_row)

        nested = record.get("markets", [])
        if nested is None:
            continue
        if not isinstance(nested, list):
            if scope is not None and scope["kind"] == "selected":
                continue
            out.quarantine.append(
                _quarantine(ctx, "market", f"{pointer}/markets", "markets is not a list", nested)
            )
            continue
        for market_index, market in enumerate(nested):
            if not selected(market, "markets"):
                continue
            _market_rows(
                ctx,
                market,
                f"{pointer}/markets/{market_index}",
                SOURCE_EVENT_EMBEDDED,
                out,
                str(record["id"]),
            )
    entity = "market" if ctx.record_key == "markets" else "event"
    for index, failed in enumerate(fetch_failed or []):
        out.quarantine.append(
            _quarantine(ctx, entity, f"/fetch_failed/{index}", "fetch_failed", failed)
        )
    return out


def assert_unique(rows: list[dict[str, Any]], key: str = "observation_id") -> None:
    seen: set[str] = set()
    for row in rows:
        value = row[key]
        if value in seen:
            raise DuplicateObservation(f"duplicate {key} {value}")
        seen.add(value)
