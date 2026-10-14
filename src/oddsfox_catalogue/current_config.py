"""Confined, explicitly versioned roots for the current catalogue."""

from __future__ import annotations

import json
import math
import os
import shutil
import stat
import tomllib
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

from oddsfox_catalogue.capture.writer import atomic_write_bytes, read_regular_bytes
from oddsfox_catalogue.config import GammaSettings, _build_section
from oddsfox_catalogue.current_contract import STORE_CONTRACT, CurrentError
from oddsfox_catalogue.limits import directory_bytes


@dataclass(frozen=True)
class CurrentLimits:
    max_duration_s: float = 300.0
    max_requests: int = 25_000
    max_download_bytes: int = 4 * 1024**3
    max_response_bytes: int = 16 * 1024**2
    max_retained_bytes: int = 64 * 1024**3
    max_temp_bytes: int = 8 * 1024**3
    free_reserve_bytes: int = 32 * 1024**3
    max_raw_bytes: int = 8 * 1024**3
    raw_max_age_s: int = 7 * 86400
    active_recheck_s: int = 16_200
    cold_recheck_s: int = 29 * 86400
    publication_reserve_s: float = 60.0


@dataclass(frozen=True)
class CurrentSettings:
    root: Path
    gamma: GammaSettings = field(
        default_factory=lambda: GammaSettings(
            requests_per_second=10.0, max_retries=4, backoff_cap_s=30.0
        )
    )
    limits: CurrentLimits = field(default_factory=CurrentLimits)

    @property
    def store_path(self) -> Path:
        return self.root / "catalogue.duckdb"

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def published_dir(self) -> Path:
        return self.root / "published"

    @property
    def temporary_dir(self) -> Path:
        return self.root / "tmp"

    @property
    def backups_dir(self) -> Path:
        return self.root / "backups"

    @property
    def run_lock_path(self) -> Path:
        return self.root / ".catalogue.lock"


def safe_path(path: Path) -> Path:
    """Check the supplied spelling before resolving away dangerous ancestors."""
    if ".." in path.parts:
        raise CurrentError("parent traversal is not allowed")
    path = Path(os.path.abspath(path))
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise CurrentError("catalogue path contains a symlink")
        if (
            ancestor.exists()
            and not stat.S_ISDIR(ancestor.stat().st_mode)
            and (ancestor != path or not stat.S_ISREG(ancestor.stat().st_mode))
        ):
            raise CurrentError("catalogue path contains a special file")
    return path


def validate_root(root: Path, *, initialize: bool = False) -> Path:
    """Reject an old/unknown root before locks, logs or database files are created."""
    root = safe_path(root)
    for ancestor in root.parents:
        if (ancestor / "catalogue.json").exists():
            raise CurrentError("a catalogue root cannot be nested inside another root")
        if any((ancestor / name).exists() for name in ("ledger.sqlite", "warehouse")):
            raise CurrentError("a catalogue root cannot be nested inside a legacy root")
        if (ancestor / ".state" / "ledger.sqlite").exists():
            raise CurrentError("a catalogue root cannot be nested inside a legacy root")
    stamp = root / "catalogue.json"
    if stamp.exists():
        try:
            value = json.loads(read_regular_bytes(stamp, max_bytes=4096, trusted_root=root))
        except (OSError, ValueError) as exc:
            raise CurrentError("invalid catalogue root stamp") from exc
        if value != {"contract": STORE_CONTRACT}:
            raise CurrentError("unsupported root contract; use a fresh catalogue.v3 root")
        for marker in ("ledger.sqlite", "warehouse", ".state", "data", "dlt"):
            if (root / marker).exists():
                raise CurrentError("legacy markers in catalogue root")
        return root
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise CurrentError("nonempty unstamped root; preserve it and select a fresh v3 root")
    if not initialize:
        raise CurrentError("catalogue root is not initialized")
    root.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(stamp, json.dumps({"contract": STORE_CONTRACT}).encode())
    descriptor = os.open(root.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return root


def load_current_settings(root: Path | None = None, *, env=None) -> CurrentSettings:
    environ = dict(os.environ if env is None else env)
    root = safe_path(
        Path(root or environ.get("CATALOGUE_ROOT") or Path.cwd() / "data/catalogue-v3")
    )
    config_path = safe_path(Path(environ.get("CATALOGUE_CONFIG", root / "config.toml")))
    raw = {}
    if config_path.exists():
        raw = tomllib.loads(read_regular_bytes(config_path, max_bytes=65536).decode())
    if set(raw) - {"gamma", "catalogue"}:
        raise CurrentError("legacy or unknown configuration; use catalogue.v3 config.toml")
    defaults = asdict(CurrentSettings(root).gamma)
    gamma = _build_section(GammaSettings, {**defaults, **raw.get("gamma", {})}, "gamma", environ)
    limits = _build_section(CurrentLimits, raw.get("catalogue", {}), "catalogue", environ)
    for name, value in asdict(limits).items():
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise CurrentError(f"catalogue {name} must be positive and finite")
    ceilings = CurrentLimits()
    for name in (
        "max_requests",
        "max_download_bytes",
        "max_response_bytes",
        "max_retained_bytes",
        "max_temp_bytes",
        "max_raw_bytes",
        "raw_max_age_s",
    ):
        if getattr(limits, name) > getattr(ceilings, name):
            raise CurrentError(f"catalogue {name} exceeds the accepted ceiling")
    if limits.max_duration_s > 300 or limits.publication_reserve_s >= limits.max_duration_s:
        raise CurrentError("refresh deadline must be <=300s with time reserved for publication")
    if limits.free_reserve_bytes < ceilings.free_reserve_bytes:
        raise CurrentError("free-space reserve must be at least 32 GiB")
    if limits.publication_reserve_s < ceilings.publication_reserve_s:
        raise CurrentError("publication reserve must be at least 60 seconds")
    if not 0 < gamma.requests_per_second <= 10:
        raise CurrentError("Gamma rate must be positive and at most 10 requests/second")
    if gamma.max_retries < 0:
        raise CurrentError("Gamma retries cannot be negative")
    for name in ("connect_timeout_s", "read_timeout_s", "backoff_base_s", "backoff_cap_s"):
        value = getattr(gamma, name)
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise CurrentError(f"Gamma {name} must be positive and finite")
    return CurrentSettings(root, gamma, limits)


def admit(settings: CurrentSettings, *, additional_bytes: int = 0) -> dict[str, int]:
    validate_root(settings.root)
    if additional_bytes < 0:
        raise CurrentError("negative storage reservation")
    retained = directory_bytes(settings.root)
    temporary = directory_bytes(settings.temporary_dir)
    free = shutil.disk_usage(settings.root).free
    limits = settings.limits
    if retained + additional_bytes > limits.max_retained_bytes:
        raise CurrentError("retained storage allowance exhausted")
    if temporary > limits.max_temp_bytes:
        raise CurrentError("temporary storage allowance exhausted")
    if free - additional_bytes < limits.free_reserve_bytes:
        raise CurrentError("free-space reserve exhausted")
    return {"retained_bytes": retained, "temporary_bytes": temporary, "free_bytes": free}


@contextmanager
def connect_store(settings: CurrentSettings, *, read_only: bool = False):
    validate_root(settings.root)
    usage = admit(settings)
    settings.temporary_dir.mkdir(exist_ok=True)
    connection = duckdb.connect(str(settings.store_path), read_only=read_only)
    try:
        connection.execute("SET memory_limit='2GB'")
        connection.execute("SET enable_progress_bar=false")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("SET threads=2")
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET temp_directory=?", [str(settings.temporary_dir)])
        remaining = settings.limits.max_temp_bytes - usage["temporary_bytes"]
        if remaining <= 0:
            raise CurrentError("temporary storage allowance exhausted")
        connection.execute("SET max_temp_directory_size=?", [f"{remaining}B"])
        yield connection
    finally:
        connection.close()


@contextmanager
def memory_connection(settings: CurrentSettings):
    """Independent verification uses the same memory and aggregate spill allowance."""
    usage = admit(settings)
    settings.temporary_dir.mkdir(exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("SET memory_limit='2GB'")
        connection.execute("SET enable_progress_bar=false")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("SET threads=2")
        connection.execute("SET temp_directory=?", [str(settings.temporary_dir)])
        remaining = settings.limits.max_temp_bytes - usage["temporary_bytes"]
        if remaining <= 0:
            raise CurrentError("temporary storage allowance exhausted")
        connection.execute("SET max_temp_directory_size=?", [f"{remaining}B"])
        yield connection
    finally:
        connection.close()
