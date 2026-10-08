"""Pagination over Gamma list endpoints, with explicit termination and loop checks.

Every iterator yields one ``PageResult`` per HTTP request. A page is the unit
of durability: the capture runner persists each page before asking for the
next one, so an interrupted scan resumes from its last durable page.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from oddsfox_catalogue.gamma.http import GammaClient, MalformedResponse, Response, ScanFailed
from oddsfox_catalogue.ids import ids_hash

RECORD_KEYS = ("events", "markets")


@dataclass(frozen=True)
class PageState:
    """Position of a scan. ``seq`` is the last durable page sequence number."""

    seq: int = 0
    cursor: str | None = None
    offset: int = 0
    seen_cursors: frozenset[str] = frozenset()
    last_ids_hash: str | None = None


@dataclass(frozen=True)
class PageResult:
    seq: int
    endpoint: str
    params: Mapping[str, Any]
    record_key: str
    input_cursor: str | None
    output_cursor: str | None
    offset: int | None
    output_offset: int | None
    records: list[Any] = field(repr=False)
    response: Response = field(repr=False)
    terminal: bool
    ids_hash: str

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def http_status(self) -> int:
        return self.response.status


def unpack(body: Any, record_key: str) -> tuple[list[Any], str | None]:
    """Return ``(records, next_cursor)`` for any documented Gamma response shape.

    Shapes:
      * keyset list:   ``{"<record_key>": [...], "next_cursor": "..."}``
      * offset list:   ``[...]``
      * single object: ``{"id": ..., ...}`` (for ``/events/{id}``)
    """
    if isinstance(body, list):
        records, next_cursor = body, None
    elif isinstance(body, dict) and record_key in body:
        records = body[record_key]
        next_cursor = body.get("next_cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise MalformedResponse("next_cursor must be a string")
        if not isinstance(records, list):
            raise MalformedResponse(f"{record_key} must be a list")
    elif isinstance(body, dict) and "id" in body:
        records, next_cursor = [body], None
    else:
        raise MalformedResponse(f"unrecognised response shape (expected {record_key!r})")

    # Record-level validity (objects with an id) is checked at load time, where
    # invalid records are quarantined instead of failing the whole page.
    return records, next_cursor


def _ids(records: list[Any]) -> list[str]:
    return [str(r["id"]) for r in records if isinstance(r, dict) and "id" in r]


def keyset_pages(
    client: GammaClient,
    endpoint: str,
    base_params: Mapping[str, Any],
    record_key: str,
    start: PageState | None = None,
) -> Iterator[PageResult]:
    """Follow ``next_cursor`` as ``after_cursor`` until the cursor is absent.

    Raises ``ScanFailed`` for a repeated cursor, a cursor that does not
    advance, or an empty page that still carries a cursor.
    """
    if record_key not in RECORD_KEYS:
        raise ValueError(f"unknown record key {record_key!r}")
    start = start or PageState()
    cursor = start.cursor
    seen = set(start.seen_cursors)
    seq = start.seq

    while True:
        params = dict(base_params)
        if cursor is not None:
            params["after_cursor"] = cursor
        response = client.get(endpoint, params)
        if response.status != 200:
            raise MalformedResponse(f"{endpoint}: unexpected HTTP {response.status}")
        records, next_cursor = unpack(response.json, record_key)

        if next_cursor is not None:
            if next_cursor == cursor or next_cursor in seen:
                raise ScanFailed(f"{endpoint}: repeated cursor {next_cursor[:24]!r}")
            if not records:
                raise ScanFailed(f"{endpoint}: empty page carried a cursor")
            seen.add(next_cursor)

        seq += 1
        yield PageResult(
            seq=seq,
            endpoint=endpoint,
            params=params,
            record_key=record_key,
            input_cursor=cursor,
            output_cursor=next_cursor,
            offset=None,
            output_offset=None,
            records=records,
            response=response,
            terminal=next_cursor is None,
            ids_hash=ids_hash(_ids(records)),
        )
        if next_cursor is None:
            return
        cursor = next_cursor


def offset_pages(
    client: GammaClient,
    endpoint: str,
    base_params: Mapping[str, Any],
    record_key: str,
    start: PageState | None = None,
) -> Iterator[PageResult]:
    """Offset pagination. Offsets advance by the number of records returned.

    Stops at the first empty page, which is itself recorded as terminal
    evidence. Fails if a page repeats the previous page's ID set.
    """
    start = start or PageState()
    offset = start.offset
    seq = start.seq
    previous_hash = start.last_ids_hash

    while True:
        params = dict(base_params)
        params["offset"] = offset
        response = client.get(endpoint, params)
        if response.status != 200:
            raise MalformedResponse(f"{endpoint}: unexpected HTTP {response.status}")
        records, _ = unpack(response.json, record_key)
        current_hash = ids_hash(_ids(records))
        if records and current_hash == previous_hash:
            raise ScanFailed(f"{endpoint}: offset {offset} repeated the previous page")

        next_offset = offset + len(records)
        seq += 1
        terminal = not records
        yield PageResult(
            seq=seq,
            endpoint=endpoint,
            params=params,
            record_key=record_key,
            input_cursor=None,
            output_cursor=None,
            offset=offset,
            output_offset=next_offset,
            records=records,
            response=response,
            terminal=terminal,
            ids_hash=current_hash,
        )
        if terminal:
            return
        previous_hash = current_hash
        offset = next_offset


def single_event_page(client: GammaClient, event_id: str, seq: int) -> PageResult:
    """Fetch ``/events/{id}``. A 404 becomes a terminal, empty, recorded page."""
    if not event_id.isdigit():
        raise ValueError(f"event ids are numeric: {event_id!r}")
    endpoint = f"/events/{event_id}"
    response = client.get(endpoint, {})
    if response.status == 404:
        records: list[Any] = []
    elif response.status == 200:
        records, _ = unpack(response.json, "events")
        if len(records) != 1 or str(records[0].get("id")) != event_id:
            raise MalformedResponse(f"{endpoint}: body does not describe event {event_id}")
    else:
        raise MalformedResponse(f"{endpoint}: unexpected HTTP {response.status}")
    return PageResult(
        seq=seq,
        endpoint=endpoint,
        params={},
        record_key="events",
        input_cursor=None,
        output_cursor=None,
        offset=None,
        output_offset=None,
        records=records,
        response=response,
        terminal=True,
        ids_hash=ids_hash(_ids(records)),
    )
