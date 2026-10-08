"""Scan catalogue: what to request for each capture mode, and in what order.

Plan shape (see the design notes in the plan):

* bootstrap / reconcile: six list scans, then reference resolution by ID.
* daily: the open-event scan, then re-fetch previously open IDs missing from it.

Follow-up scans (ID chunks, then single lookups) are created after their
inputs are known, so they are persisted with their input ID lists. This keeps
resume deterministic even if the warehouse changes between attempts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from oddsfox_catalogue.config import GammaSettings

ScanKind = Literal["keyset", "offset", "keyset_ids", "single_ids"]
MODE_ORDER = ("bootstrap", "reconcile", "daily")
ID_CHUNK = 100


@dataclass(frozen=True)
class ScanSpec:
    name: str
    kind: ScanKind
    endpoint: str
    record_key: str
    params: tuple[tuple[str, Any], ...] = ()
    input_ids: tuple[str, ...] = ()

    @property
    def param_dict(self) -> dict[str, Any]:
        return dict(self.params)

    @property
    def is_follow_up(self) -> bool:
        return self.kind in {"keyset_ids", "single_ids"}


def _event_params(settings: GammaSettings, closed: bool | None) -> dict[str, Any]:
    params: dict[str, Any] = {
        "limit": settings.page_limit,
        "order": "id",
        "ascending": True,
        "include_children": True,
    }
    if closed is not None:
        params["closed"] = closed
    if settings.include_chat:
        params["include_chat"] = True
    if settings.include_template:
        params["include_template"] = True
    if settings.include_best_lines:
        params["include_best_lines"] = True
    return params


def _market_params(settings: GammaSettings, closed: bool) -> dict[str, Any]:
    return {
        "limit": settings.page_limit,
        "order": "id",
        "ascending": True,
        "closed": closed,
        "include_tag": True,
    }


def _freeze(params: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(params.items()))


def event_keyset_all(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "events_keyset_all",
        "keyset",
        "/events/keyset",
        "events",
        _freeze(_event_params(settings, None)),
    )


def event_keyset_open(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "events_keyset_open",
        "keyset",
        "/events/keyset",
        "events",
        _freeze(_event_params(settings, False)),
    )


def event_keyset_closed(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "events_keyset_closed",
        "keyset",
        "/events/keyset",
        "events",
        _freeze(_event_params(settings, True)),
    )


def events_archived_offset(settings: GammaSettings) -> ScanSpec:
    params = {
        "limit": settings.page_limit,
        "order": "id",
        "ascending": True,
        "archived": True,
    }
    return ScanSpec("events_archived_offset", "offset", "/events", "events", _freeze(params))


def markets_keyset_open(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "markets_keyset_open",
        "keyset",
        "/markets/keyset",
        "markets",
        _freeze(_market_params(settings, False)),
    )


def markets_keyset_closed(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "markets_keyset_closed",
        "keyset",
        "/markets/keyset",
        "markets",
        _freeze(_market_params(settings, True)),
    )


def list_scans_for(mode: str, settings: GammaSettings) -> list[ScanSpec]:
    if mode in {"bootstrap", "reconcile"}:
        return [
            event_keyset_all(settings),
            event_keyset_open(settings),
            event_keyset_closed(settings),
            events_archived_offset(settings),
            markets_keyset_open(settings),
            markets_keyset_closed(settings),
        ]
    if mode == "daily":
        return [event_keyset_open(settings)]
    raise ValueError(f"unknown mode {mode!r}")


def chunk(ids: list[str], size: int = ID_CHUNK) -> list[list[str]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def id_chunk_scan(index: int, ids: list[str]) -> ScanSpec:
    params = {
        "limit": ID_CHUNK,
        "order": "id",
        "ascending": True,
        "include_children": True,
        "id": [int(i) for i in ids],
    }
    return ScanSpec(
        name=f"events_by_id_{index:04d}",
        kind="keyset_ids",
        endpoint="/events/keyset",
        record_key="events",
        params=_freeze(params),
        input_ids=tuple(ids),
    )


def single_id_scan(index: int, ids: list[str]) -> ScanSpec:
    return ScanSpec(
        name=f"events_by_id_single_{index:04d}",
        kind="single_ids",
        endpoint="/events/{id}",
        record_key="events",
        input_ids=tuple(ids),
    )


def scan_spec_from_row(row: dict[str, Any]) -> ScanSpec:
    return ScanSpec(
        name=row["scan_name"],
        kind=row["kind"],
        endpoint=row["endpoint"],
        record_key=row["record_key"],
        params=_freeze(json.loads(row["params_json"])),
        input_ids=tuple(json.loads(row["input_ids_json"] or "[]")),
    )
