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
from datetime import UTC, datetime
from typing import Any

from oddsfox_catalogue.ids import observation_id, payload_hash, sha256_text

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


@dataclass
class RowSet:
    events: list[dict[str, Any]] = field(default_factory=list)
    markets: list[dict[str, Any]] = field(default_factory=list)
    quarantine: list[dict[str, Any]] = field(default_factory=list)

    def extend(self, other: RowSet) -> None:
        self.events.extend(other.events)
        self.markets.extend(other.markets)
        self.quarantine.extend(other.quarantine)


def parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp to aware UTC. Unparseable or missing values give None."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
        "payload": payload,
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
        "payload": record,
    }


def _check_record(record: Any) -> str | None:
    if not isinstance(record, dict):
        return "record is not an object"
    if record.get("id") in (None, ""):
        return "record has no id"
    return None


def _market_rows(
    ctx: PageContext,
    market: Any,
    pointer: str,
    source_kind: str,
    out: RowSet,
) -> None:
    problem = _check_record(market)
    if problem:
        out.quarantine.append(_quarantine(ctx, "market", pointer, problem, market))
        return
    row = _envelope(ctx, market, pointer, str(market["id"]))
    row["source_kind"] = source_kind
    row["json_pointer"] = pointer
    out.markets.append(row)


def rows_for_page(
    ctx: PageContext,
    records: list[Any],
    fetch_failed: list[Any] | None = None,
) -> RowSet:
    """Envelope rows for every record on one page. Records are already envelope-checked.

    ``fetch_failed`` entries are ids the capture could not read. They become
    quarantine rows with reason ``fetch_failed`` and count toward the load gate.
    """
    out = RowSet()
    for index, record in enumerate(records):
        if ctx.record_key == "markets":
            _market_rows(ctx, record, f"/markets/{index}", SOURCE_MARKET_DIRECT, out)
            continue

        pointer = f"/events/{index}"
        problem = _check_record(record)
        if problem:
            out.quarantine.append(_quarantine(ctx, "event", pointer, problem, record))
            continue
        out.events.append(_envelope(ctx, record, pointer, str(record["id"])))

        nested = record.get("markets", [])
        if nested is None:
            continue
        if not isinstance(nested, list):
            out.quarantine.append(
                _quarantine(ctx, "market", f"{pointer}/markets", "markets is not a list", nested)
            )
            continue
        for market_index, market in enumerate(nested):
            _market_rows(
                ctx,
                market,
                f"{pointer}/markets/{market_index}",
                SOURCE_EVENT_EMBEDDED,
                out,
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
