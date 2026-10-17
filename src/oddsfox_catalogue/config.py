"""Typed Gamma settings and the strict section loader shared by catalogue roots."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

ENV_PREFIX = "CATALOGUE"


@dataclass(frozen=True)
class GammaSettings:
    base_url: str = "https://gamma-api.polymarket.com"
    requests_per_second: float = 2.0
    allow_loopback: bool = False
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    max_retries: int = 12
    backoff_base_s: float = 5.0
    backoff_cap_s: float = 300.0
    page_limit: int = 100
    include_chat: bool = False
    include_template: bool = False
    include_best_lines: bool = False


def _coerce(value: str, target: Any) -> Any:
    if isinstance(target, bool):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"not a boolean: {value!r}")
    if isinstance(target, int):
        return int(value)
    if isinstance(target, float):
        return float(value)
    return value


def _build_section(cls: type, raw: Mapping[str, Any], section: str, env: Mapping[str, str]) -> Any:
    known = {f.name: f for f in fields(cls)}
    unknown = set(raw) - set(known)
    if unknown:
        raise ValueError(f"unknown keys in [{section}]: {sorted(unknown)}")
    defaults = cls()
    values: dict[str, Any] = {}
    for name in known:
        target = getattr(defaults, name)
        base = raw.get(name, target)
        env_key = f"{ENV_PREFIX}_{section}_{name}".upper()
        if env_key in env:
            base = _coerce(env[env_key], target)
        if isinstance(target, bool):
            valid = isinstance(base, bool)
        elif isinstance(target, int):
            valid = isinstance(base, int) and not isinstance(base, bool)
        elif isinstance(target, float):
            valid = (
                isinstance(base, int | float) and not isinstance(base, bool) and math.isfinite(base)
            )
        else:
            valid = isinstance(base, str)
        if not valid:
            raise ValueError(f"[{section}] {name} has an invalid type or non-finite value")
        values[name] = base
    return cls(**values)
