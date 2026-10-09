"""Typed settings loaded from ``config/catalogue.toml`` with ``CATALOGUE_*`` overrides."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT_ENV = "CATALOGUE_ROOT"
CONFIG_FILE_ENV = "CATALOGUE_CONFIG"
ENV_PREFIX = "CATALOGUE"


@dataclass(frozen=True)
class GammaSettings:
    base_url: str = "https://gamma-api.polymarket.com"
    requests_per_second: float = 2.0
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    max_retries: int = 12
    backoff_base_s: float = 5.0
    backoff_cap_s: float = 300.0
    page_limit: int = 100
    include_chat: bool = False
    include_template: bool = False
    include_best_lines: bool = False


@dataclass(frozen=True)
class PathSettings:
    data_dir: str = "data"
    state_dir: str = ".state"
    warehouse_file: str = "data/warehouse/catalogue.duckdb"
    dbt_project_dir: str = "dbt"
    dbt_profiles_dir: str = "dbt"


@dataclass(frozen=True)
class LoadSettings:
    dataset_name: str = "bronze"
    max_pages_per_run: int = 200
    duckdb_memory_limit: str = "2GB"
    duckdb_threads: int = 2


@dataclass(frozen=True)
class QualitySettings:
    quarantine_max_ratio: float = 0.01
    unresolved_reference_max_ratio: float = 0.01
    open_events_drop_warn_pct: float = 5.0
    open_events_drop_error_pct: float = 10.0


@dataclass(frozen=True)
class PublishSettings:
    retain_releases: int = 5


@dataclass(frozen=True)
class ScheduleSettings:
    daily_cron: str = "0 6 * * 1-6"
    weekly_cron: str = "0 3 * * 0"


@dataclass(frozen=True)
class Settings:
    root: Path
    gamma: GammaSettings = field(default_factory=GammaSettings)
    paths: PathSettings = field(default_factory=PathSettings)
    load: LoadSettings = field(default_factory=LoadSettings)
    quality: QualitySettings = field(default_factory=QualitySettings)
    publish: PublishSettings = field(default_factory=PublishSettings)
    schedule: ScheduleSettings = field(default_factory=ScheduleSettings)

    # Resolved absolute locations -------------------------------------------------
    def _abs(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else self.root / path

    @property
    def data_dir(self) -> Path:
        return self._abs(self.paths.data_dir)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def state_dir(self) -> Path:
        return self._abs(self.paths.state_dir)

    @property
    def ledger_path(self) -> Path:
        return self.state_dir / "ledger.sqlite"

    @property
    def dlt_pipelines_dir(self) -> Path:
        return self.state_dir / "dlt"

    @property
    def dagster_home(self) -> Path:
        return self.state_dir / "dagster_home"

    @property
    def run_lock_path(self) -> Path:
        return self.state_dir / "catalogue.lock"

    @property
    def warehouse_path(self) -> Path:
        return self._abs(self.paths.warehouse_file)

    @property
    def dbt_project_dir(self) -> Path:
        return self._abs(self.paths.dbt_project_dir)

    @property
    def dbt_profiles_dir(self) -> Path:
        return self._abs(self.paths.dbt_profiles_dir)

    @property
    def published_dir(self) -> Path:
        return self.data_dir / "published"


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
        base = raw.get(name, getattr(defaults, name))
        env_key = f"{ENV_PREFIX}_{section}_{name}".upper()
        if env_key in env:
            base = _coerce(env[env_key], getattr(defaults, name))
        values[name] = base
    return cls(**values)


def load_settings(
    root: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Settings:
    """Load settings from the TOML file, then apply environment overrides."""
    environ = dict(os.environ if env is None else env)
    project_root = Path(root or environ.get(PROJECT_ROOT_ENV) or Path.cwd()).resolve()
    config_path = Path(environ.get(CONFIG_FILE_ENV, project_root / "config" / "catalogue.toml"))

    raw: dict[str, Any] = {}
    if config_path.exists():
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)

    known_sections = {
        "gamma": GammaSettings,
        "paths": PathSettings,
        "load": LoadSettings,
        "quality": QualitySettings,
        "publish": PublishSettings,
        "schedule": ScheduleSettings,
    }
    unknown_sections = set(raw) - set(known_sections)
    if unknown_sections:
        raise ValueError(f"unknown config sections: {sorted(unknown_sections)}")

    built = {
        name: _build_section(cls, raw.get(name, {}), name, environ)
        for name, cls in known_sections.items()
    }
    settings = Settings(root=project_root, **built)
    _validate(settings)
    return settings


def _validate(settings: Settings) -> None:
    """Reject quality limits that would silently disable a gate or make no sense."""
    quality = settings.quality
    for name in ("quarantine_max_ratio", "unresolved_reference_max_ratio"):
        value = getattr(quality, name)
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"[quality] {name} must be between 0 and 1, got {value}")
    for name in ("open_events_drop_warn_pct", "open_events_drop_error_pct"):
        value = getattr(quality, name)
        if not 0.0 <= value <= 100.0:
            raise ValueError(f"[quality] {name} must be between 0 and 100, got {value}")
    if quality.open_events_drop_warn_pct > quality.open_events_drop_error_pct:
        raise ValueError(
            "[quality] open_events_drop_warn_pct must not exceed open_events_drop_error_pct"
        )


def settings_as_dict(settings: Settings) -> dict[str, Any]:
    """Serialise settings for `catalogue config show` and run metadata."""
    out: dict[str, Any] = {"root": str(settings.root)}
    for f in fields(settings):
        value = getattr(settings, f.name)
        if is_dataclass(value):
            out[f.name] = {g.name: getattr(value, g.name) for g in fields(value)}
    return out
