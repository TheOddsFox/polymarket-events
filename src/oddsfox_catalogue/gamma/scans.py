"""Scan catalogue: what to request for each capture mode, and in what order.

Plan shape:

* bootstrap / reconcile: the open-market keyset first, then id-range scans for
  every event and every closed market, sealed at their
  high-water marks. Closed-market windows wait for the open crawl at run time.
  Finite ID windows avoid deep closed-catalogue cursors and offset ceilings.
* daily: direct open-event and open-market keysets, then re-fetch previously
  open or unknown records missing from those lists.

Follow-up scans (ID chunks, then single lookups) are created after their
inputs are known, so they are persisted with their input ID lists. This keeps
resume deterministic even if the warehouse changes between attempts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from oddsfox_catalogue.config import CaptureSettings, GammaSettings
from oddsfox_catalogue.gamma.http import GammaClient, MalformedResponse
from oddsfox_catalogue.gamma.paginators import unpack

ScanKind = Literal["keyset", "offset", "id_range", "keyset_ids", "single_ids"]
MAX_PLAN_SCANS = 20_000
ID_CHUNK = 100
ID_STEP = 100
TAIL_EMPTY_WINDOWS = 3


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


def _event_flags(settings: GammaSettings) -> dict[str, Any]:
    """Payload flags that daily keyset and bootstrap id-range event requests share."""
    flags: dict[str, Any] = {}
    if settings.include_chat:
        flags["include_chat"] = True
    if settings.include_template:
        flags["include_template"] = True
    if settings.include_best_lines:
        flags["include_best_lines"] = True
    return flags


def _event_params(settings: GammaSettings, closed: bool | None) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": settings.page_limit}
    if closed is not None:
        params["closed"] = closed
    params.update(_event_flags(settings))
    return params


def _market_params(settings: GammaSettings, closed: bool) -> dict[str, Any]:
    return {
        "limit": settings.page_limit,
        "closed": closed,
        "include_tag": True,
    }


def _freeze(params: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(params.items()))


def event_keyset_open(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "events_keyset_open",
        "keyset",
        "/events/keyset",
        "events",
        _freeze(_event_params(settings, False)),
    )


def markets_keyset_open(settings: GammaSettings) -> ScanSpec:
    return ScanSpec(
        "markets_keyset_open",
        "keyset",
        "/markets/keyset",
        "markets",
        _freeze(_market_params(settings, False)),
    )


def id_range_scan(
    name: str,
    endpoint: str,
    record_key: str,
    *,
    lo: int,
    hi: int | None,
    closed: bool | None,
    tail: bool,
    extra: dict[str, Any] | None = None,
) -> ScanSpec:
    params: dict[str, Any] = {"lo": lo, "step": ID_STEP, "tail": tail}
    if hi is not None:
        params["hi"] = hi
    if closed is not None:
        params["closed"] = closed
    if tail:
        params["empty_stop"] = TAIL_EMPTY_WINDOWS
    if extra:
        params.update(extra)
    return ScanSpec(name, "id_range", endpoint, record_key, _freeze(params))


def _high_water(client: GammaClient, endpoint: str, record_key: str) -> int:
    """Highest source id, from ``limit=1&order=id&ascending=false``."""
    response = client.get(endpoint, {"limit": 1, "order": "id", "ascending": False})
    if response.status != 200:
        raise MalformedResponse(f"{endpoint}: high-water HTTP {response.status}")
    records, _ = unpack(response.json, record_key)
    if not records:
        return 0
    if len(records) != 1 or not isinstance(records[0], dict):
        raise MalformedResponse("high-water response has an invalid ID")
    value = str(records[0].get("id", ""))
    if (
        not value.isascii()
        or not value.isdecimal()
        or not 0 < len(value) <= 20
        or int(value) <= 0
        or str(int(value)) != value
    ):
        raise MalformedResponse("high-water response has an invalid ID")
    return int(records[0]["id"])


def _partitions(
    prefix: str,
    endpoint: str,
    record_key: str,
    hi: int,
    partition: int,
    closed: bool | None,
    extra: dict[str, Any] | None = None,
) -> list[ScanSpec]:
    if partition < 1 or (hi + partition - 1) // partition > MAX_PLAN_SCANS:
        raise ValueError("source high-water implies an excessive finite scan plan")
    scans: list[ScanSpec] = []
    start = 1
    index = 1
    while start <= hi:
        end = min(hi, start + partition - 1)
        scans.append(
            id_range_scan(
                f"{prefix}_{index:04d}",
                endpoint,
                record_key,
                lo=start,
                hi=end,
                closed=closed,
                tail=False,
                extra=extra,
            )
        )
        start = end + 1
        index += 1
    return scans


def high_water(client: GammaClient, record_key: str) -> int:
    return _high_water(client, "/" + record_key, record_key)


def sealed_plan(
    mode: str, gamma: GammaSettings, capture: CaptureSettings, marks: dict[str, int]
) -> list[ScanSpec]:
    """A source high-water is a finite observation boundary, never an expanding tail."""
    planned_count = 1 + sum(
        (marks[key] + capture.id_partition_size - 1) // capture.id_partition_size
        for key in ("events", "markets")
    )
    if mode != "daily" and planned_count > MAX_PLAN_SCANS:
        raise ValueError("source high-water implies an excessive finite scan plan")
    market_open = markets_keyset_open(gamma)
    market_open = ScanSpec(
        market_open.name,
        market_open.kind,
        market_open.endpoint,
        market_open.record_key,
        _freeze(
            {
                **market_open.param_dict,
                "_high_water": marks["markets"],
                "order": "id",
                "ascending": True,
            }
        ),
    )
    if mode == "daily":
        event_open = event_keyset_open(gamma)
        event_open = ScanSpec(
            event_open.name,
            event_open.kind,
            event_open.endpoint,
            event_open.record_key,
            _freeze(
                {
                    **event_open.param_dict,
                    "_high_water": marks["events"],
                    "order": "id",
                    "ascending": True,
                }
            ),
        )
        return [event_open, market_open]
    return [
        market_open,
        *_partitions(
            "events_ids",
            "/events/keyset",
            "events",
            marks["events"],
            capture.id_partition_size,
            None,
            _event_flags(gamma),
        ),
        *_partitions(
            "markets_closed_ids",
            "/markets/keyset",
            "markets",
            marks["markets"],
            capture.id_partition_size,
            True,
        ),
    ]


def id_range_plan(
    gamma: GammaSettings, capture: CaptureSettings, client: GammaClient
) -> list[ScanSpec]:
    marks = {key: high_water(client, key) for key in ("events", "markets")}
    if capture.max_id_override:
        marks = {key: min(value, capture.max_id_override) for key, value in marks.items()}
    return sealed_plan("bootstrap", gamma, capture, marks)


def list_scans_for(
    mode: str,
    gamma: GammaSettings,
    *,
    capture: CaptureSettings | None = None,
    client: GammaClient | None = None,
) -> list[ScanSpec]:
    if mode not in {"bootstrap", "daily", "reconcile"} or capture is None or client is None:
        raise ValueError("catalogue planning requires a supported mode, settings and client")
    marks = {key: high_water(client, key) for key in ("events", "markets")}
    if capture.max_id_override:
        marks = {key: min(value, capture.max_id_override) for key, value in marks.items()}
    return sealed_plan(mode, gamma, capture, marks)


def chunk(ids: list[str], size: int = ID_CHUNK) -> list[list[str]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def id_chunk_scan(index: int, ids: list[str]) -> ScanSpec:
    params = {
        "limit": ID_CHUNK,
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


def native_single_scan(name: str, ids: list[str], record_key: str) -> ScanSpec:
    return ScanSpec(
        name, "single_ids", "/" + record_key + "/{id}", record_key, input_ids=tuple(ids)
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
