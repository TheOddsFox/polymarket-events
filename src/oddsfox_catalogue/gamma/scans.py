"""Scan catalogue: what to request for each capture mode, and in what order.

Plan shape:

* bootstrap / reconcile: id-range scans for every event and every closed market,
  a short tail above each high-water mark, then the open-market keyset.
  Deep keyset cursors and offset lists are not used. Gamma returns HTTP 500
  on deep cursors and HTTP 422 once an offset passes a few thousand.
* daily: the open-event keyset, then re-fetch previously open IDs missing from it.

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
    if not records or not isinstance(records[0], dict) or "id" not in records[0]:
        raise MalformedResponse(f"{endpoint}: high-water response has no id")
    return int(records[0]["id"])


def _cap(mark: int, override: int) -> tuple[int, bool]:
    """Return ``(mark, include_tail)``. An override below the server mark drops the tail."""
    if override > 0 and override < mark:
        return override, False
    return mark, True


def _partitions(
    prefix: str,
    endpoint: str,
    record_key: str,
    hi: int,
    partition: int,
    closed: bool | None,
    extra: dict[str, Any] | None = None,
) -> list[ScanSpec]:
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


def id_range_plan(
    gamma: GammaSettings, capture: CaptureSettings, client: GammaClient
) -> list[ScanSpec]:
    """Event and closed-market id ranges, optional tails, then the open-market keyset."""
    event_mark, event_tail = _cap(_high_water(client, "/events", "events"), capture.max_id_override)
    market_mark, market_tail = _cap(
        _high_water(client, "/markets", "markets"), capture.max_id_override
    )
    scans: list[ScanSpec] = []
    event_flags = _event_flags(gamma)
    scans.extend(
        _partitions(
            "events_ids",
            "/events/keyset",
            "events",
            event_mark,
            capture.id_partition_size,
            None,
            event_flags,
        )
    )
    if event_tail:
        scans.append(
            id_range_scan(
                "events_ids_tail",
                "/events/keyset",
                "events",
                lo=event_mark + 1,
                hi=None,
                closed=None,
                tail=True,
                extra=event_flags,
            )
        )
    scans.extend(
        _partitions(
            "markets_closed_ids",
            "/markets/keyset",
            "markets",
            market_mark,
            capture.id_partition_size,
            True,
        )
    )
    if market_tail:
        scans.append(
            id_range_scan(
                "markets_closed_ids_tail",
                "/markets/keyset",
                "markets",
                lo=market_mark + 1,
                hi=None,
                closed=True,
                tail=True,
            )
        )
    scans.append(markets_keyset_open(gamma))
    return scans


def list_scans_for(
    mode: str,
    gamma: GammaSettings,
    *,
    capture: CaptureSettings | None = None,
    client: GammaClient | None = None,
) -> list[ScanSpec]:
    if mode == "daily":
        return [event_keyset_open(gamma)]
    if mode in {"bootstrap", "reconcile"}:
        if capture is None or client is None:
            raise ValueError(f"{mode} planning needs capture settings and a Gamma client")
        return id_range_plan(gamma, capture, client)
    raise ValueError(f"unknown mode {mode!r}")


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


def scan_spec_from_row(row: dict[str, Any]) -> ScanSpec:
    return ScanSpec(
        name=row["scan_name"],
        kind=row["kind"],
        endpoint=row["endpoint"],
        record_key=row["record_key"],
        params=_freeze(json.loads(row["params_json"])),
        input_ids=tuple(json.loads(row["input_ids_json"] or "[]")),
    )
