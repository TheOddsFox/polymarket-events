"""Decode legacy scan identities for immutable raw-evidence validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

ScanKind = Literal["keyset", "offset", "id_range", "keyset_ids", "single_ids"]


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


def _freeze(params: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(params.items()))


def scan_spec_from_row(row: dict[str, Any]) -> ScanSpec:
    return ScanSpec(
        name=row["scan_name"],
        kind=row["kind"],
        endpoint=row["endpoint"],
        record_key=row["record_key"],
        params=_freeze(json.loads(row["params_json"])),
        input_ids=tuple(json.loads(row["input_ids_json"] or "[]")),
    )
