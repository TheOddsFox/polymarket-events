"""Pagination over Gamma list endpoints, with explicit termination and loop checks.

Every iterator yields one ``PageResult`` per HTTP request. A page is the unit
of durability: the capture runner persists each page before asking for the
next one, so an interrupted scan resumes from its last durable page.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from oddsfox_catalogue.gamma.http import (
    GammaClient,
    MalformedResponse,
    Response,
    RetriesExhausted,
    ScanFailed,
)
from oddsfox_catalogue.ids import canonical_json, ids_hash

ID_WINDOW_MAX_RETRIES = 4
ID_WINDOW_BACKOFF_CAP_S = 30.0

RECORD_KEYS = ("events", "markets")
# Event payload flags that keyset daily scans send. Id windows must send the same ones.
_INCLUDE_FLAGS = ("include_chat", "include_template", "include_best_lines")


@dataclass(frozen=True)
class PageState:
    """Position of a scan. ``seq`` is the last durable page sequence number."""

    seq: int = 0
    cursor: str | None = None
    offset: int = 0
    seen_cursors: frozenset[str] = frozenset()
    last_ids_hash: str | None = None
    # Consecutive record-less windows at the end of a tail scan. Resume keeps the count.
    empty_run: int = 0


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


def _describes(records: list[Any], entity_id: str) -> bool:
    """True when a single-entity body holds exactly one record, and it is ``entity_id``."""
    return (
        len(records) == 1
        and isinstance(records[0], dict)
        and str(records[0].get("id")) == entity_id
    )


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


def _reject_window_mismatch(
    endpoint: str, wanted: list[int], records: list[Any], cursor: str | None
) -> None:
    """A window that ignores the id list would never end, or would skip ids."""
    wanted_ids = set(wanted)
    outside = [
        record.get("id")
        for record in records
        if isinstance(record, dict)
        and str(record.get("id", "")).isdigit()
        and int(record["id"]) not in wanted_ids
    ]
    if outside:
        raise MalformedResponse(f"{endpoint}: id window returned ids outside the request")
    if cursor and len(records) < len(wanted):
        raise MalformedResponse(f"{endpoint}: id window returned a short page with a cursor")


def _window_params(
    ids: list[int],
    record_key: str,
    closed: bool | None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": len(ids), "id": ids}
    if closed is not None:
        params["closed"] = closed
    if record_key == "markets":
        params["include_tag"] = True
    if extra:
        params.update(extra)
    return params


def _single_path(endpoint: str, entity_id: int) -> str:
    if endpoint.startswith("/events"):
        return f"/events/{entity_id}"
    if endpoint.startswith("/markets"):
        return f"/markets/{entity_id}"
    raise ValueError(f"no single-id route for {endpoint}")


def _synthetic_response(
    client: GammaClient,
    endpoint: str,
    params: Mapping[str, Any],
    record_key: str,
    records: list[Any],
    failed: list[int],
    parts: list[Response],
) -> Response:
    """One page body covering a window that took more than one HTTP call."""
    if len(parts) == 1 and not failed:
        return parts[0]
    body: dict[str, Any] = {record_key: records}
    if failed:
        body["fetch_failed"] = [
            {"id": str(entity_id), "reason": "fetch_failed"} for entity_id in failed
        ]
    raw = canonical_json(body).encode("utf-8")
    return Response(
        endpoint=endpoint,
        params=params,
        status=200,
        body=raw,
        json=body,
        retries=sum(part.retries for part in parts),
        latency_s=sum(part.latency_s for part in parts),
        received_at=parts[-1].received_at if parts else client.now(),
    )


def _fetch_id_window(
    client: GammaClient,
    endpoint: str,
    record_key: str,
    ids: list[int],
    closed: bool | None,
    extra: Mapping[str, Any] | None = None,
) -> tuple[list[Any], list[int], Response]:
    """Fetch one id window. A window that keeps failing is split down to one id.

    Any failure on a window of several ids, a non-retryable error included, splits
    it in half, so one bad id cannot fail the scan. A single id that still fails
    after ``/events/{id}`` or ``/markets/{id}`` is returned in the failed list,
    whatever the hard error. A 404 means the id does not exist.
    """
    params = _window_params(ids, record_key, closed, extra)

    def fetch(wanted: list[int]) -> tuple[list[Any], list[int], list[Response]]:
        if not wanted:
            return [], [], []
        request = _window_params(wanted, record_key, closed, extra)
        try:
            response = client.get(
                endpoint,
                request,
                max_retries=ID_WINDOW_MAX_RETRIES,
                backoff_cap_s=ID_WINDOW_BACKOFF_CAP_S,
            )
            if response.status != 200:
                raise MalformedResponse(f"{endpoint}: unexpected HTTP {response.status}")
            records, cursor = unpack(response.json, record_key)
            _reject_window_mismatch(endpoint, wanted, records, cursor)
        except (RetriesExhausted, MalformedResponse):
            if len(wanted) == 1:
                # A one-id window that cannot answer is answered by ID, as after bisection.
                return _fetch_single(wanted[0])
            mid = len(wanted) // 2
            left_records, left_failed, left_parts = fetch(wanted[:mid])
            right_records, right_failed, right_parts = fetch(wanted[mid:])
            return (
                left_records + right_records,
                left_failed + right_failed,
                left_parts + right_parts,
            )
        return records, [], [response]

    def _fetch_single(entity_id: int) -> tuple[list[Any], list[int], list[Response]]:
        path = _single_path(endpoint, entity_id)
        # The by-id route must ask for the same payload the window asked for. A market
        # without include_tag comes back with no tags.
        query: dict[str, Any] = {}
        if record_key == "markets":
            query["include_tag"] = True
        if extra:
            query.update(extra)
        try:
            response = client.get(
                path,
                query,
                max_retries=ID_WINDOW_MAX_RETRIES,
                backoff_cap_s=ID_WINDOW_BACKOFF_CAP_S,
            )
            if response.status == 404:
                return [], [], [response]
            if response.status != 200:
                raise MalformedResponse(f"{path}: unexpected HTTP {response.status}")
            records, _ = unpack(response.json, record_key)
            if not _describes(records, str(entity_id)):
                raise MalformedResponse(f"{path}: body does not describe {entity_id}")
        except (RetriesExhausted, MalformedResponse):
            # Still failing for this id alone: quarantine it so the window can finish.
            return [], [entity_id], []
        return records, [], [response]

    records, failed, parts = fetch(ids)
    return (
        records,
        failed,
        _synthetic_response(client, endpoint, params, record_key, records, failed, parts),
    )


def id_range_pages(
    client: GammaClient,
    endpoint: str,
    base_params: Mapping[str, Any],
    record_key: str,
    start: PageState | None = None,
) -> Iterator[PageResult]:
    """Request explicit id windows. Page ``seq`` covers ``[lo+(seq-1)*step, ...)``.

    An empty window is stored. A tail scan (no ``hi``) stops after
    ``empty_stop`` consecutive windows with no records. A window whose ids all
    failed counts as empty, so a persistent outage above the mark still ends.
    Resume uses ``start.seq`` and ``start.empty_run`` and does not repeat durable pages.
    """
    if record_key not in RECORD_KEYS:
        raise ValueError(f"unknown record key {record_key!r}")
    start = start or PageState()
    lo = int(base_params["lo"])
    step = int(base_params["step"])
    if step < 1 or step > 100:
        raise ValueError(f"id window step must be 1..100, got {step}")
    hi = None if base_params.get("hi") is None else int(base_params["hi"])
    tail = bool(base_params.get("tail"))
    empty_stop = int(base_params.get("empty_stop", 3))
    closed = base_params.get("closed")
    if isinstance(closed, str):
        closed = closed == "true"
    extra = {key: True for key in _INCLUDE_FLAGS if base_params.get(key)}
    seq = start.seq
    index = start.seq
    empty_run = start.empty_run

    while True:
        window_lo = lo + index * step
        if hi is not None and window_lo > hi:
            return
        window_hi = window_lo + step - 1
        if hi is not None:
            window_hi = min(window_hi, hi)
        ids = list(range(window_lo, window_hi + 1))
        records, _failed, response = _fetch_id_window(
            client, endpoint, record_key, ids, closed, extra
        )
        seq += 1
        index += 1
        if tail:
            if records:
                empty_run = 0
                terminal = False
            else:
                empty_run += 1
                terminal = empty_run >= empty_stop
        else:
            terminal = hi is not None and window_hi >= hi
        yield PageResult(
            seq=seq,
            endpoint=endpoint,
            params=_window_params(ids, record_key, closed, extra),
            record_key=record_key,
            input_cursor=None,
            output_cursor=None,
            offset=window_lo,
            output_offset=window_hi,
            records=records,
            response=response,
            terminal=terminal,
            ids_hash=ids_hash(_ids(records)),
        )
        if terminal:
            return


def id_list_pages(
    client: GammaClient,
    endpoint: str,
    base_params: Mapping[str, Any],
    record_key: str,
    start: PageState | None = None,
) -> Iterator[PageResult]:
    """Fetch one ``keyset_ids`` chunk as a single id window, with the window policy.

    The chunk is split in half on any failure, and a single id that still fails is
    quarantined by ID, as an id-range window is. A chunk is one terminal page, so a
    durable page means the chunk is done and nothing is fetched again.
    """
    if record_key not in RECORD_KEYS:
        raise ValueError(f"unknown record key {record_key!r}")
    if start is not None and start.seq >= 1:
        return
    ids = [int(entity_id) for entity_id in base_params["id"]]
    records, _failed, response = _fetch_id_window(client, endpoint, record_key, ids, None)
    yield PageResult(
        seq=1,
        endpoint=endpoint,
        params=_window_params(ids, record_key, None),
        record_key=record_key,
        input_cursor=None,
        output_cursor=None,
        offset=None,
        output_offset=None,
        records=records,
        response=response,
        terminal=True,
        ids_hash=ids_hash(_ids(records)),
    )


def single_event_page(
    client: GammaClient, event_id: str, seq: int, *, terminal: bool = True
) -> PageResult:
    """Fetch ``/events/{id}``. A 404 is an empty page. A failing id is quarantined.

    Retries use the id-window policy, so a bad id cannot stall the batch on the global
    retry budget. An id that still fails becomes a ``fetch_failed`` body, as a window does.
    ``terminal`` is true only for the last id of a chunk, so a resume after an earlier page
    still fetches the rest.
    """
    if not event_id.isdigit():
        raise ValueError(f"event ids are numeric: {event_id!r}")
    endpoint = f"/events/{event_id}"
    records: list[Any] = []
    try:
        response = client.get(
            endpoint,
            {},
            max_retries=ID_WINDOW_MAX_RETRIES,
            backoff_cap_s=ID_WINDOW_BACKOFF_CAP_S,
        )
        if response.status == 200:
            records, _ = unpack(response.json, "events")
            if not _describes(records, event_id):
                raise MalformedResponse(f"{endpoint}: body does not describe event {event_id}")
        elif response.status != 404:
            raise MalformedResponse(f"{endpoint}: unexpected HTTP {response.status}")
    except (RetriesExhausted, MalformedResponse):
        records = []
        response = _synthetic_response(client, endpoint, {}, "events", [], [int(event_id)], [])
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
        terminal=terminal,
        ids_hash=ids_hash(_ids(records)),
    )
