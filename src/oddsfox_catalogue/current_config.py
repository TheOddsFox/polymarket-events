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
from oddsfox_catalogue.limits import StorageMeter, directory_bytes

ACTIVE_FRESHNESS_S = 86400
COLD_FRESHNESS_S = 60 * 86400
GIB = 1024**3


@dataclass(frozen=True)
class CurrentLimits:
    """Operator-tunable limits. Defaults are starting points; a root's config.toml sets them.

    Ceilings are set from measured peaks, so every value is bounded by ``HARD_MAXIMUMS``
    rather than by these defaults.
    """

    max_duration_s: float = 1200.0
    max_requests: int = 25_000
    max_download_bytes: int = 4 * GIB
    max_response_bytes: int = 16 * 1024**2
    max_retained_bytes: int = 160 * GIB
    max_temp_bytes: int = 16 * GIB
    free_reserve_bytes: int = 8 * GIB
    active_recheck_s: int = 20 * 3600
    cold_recheck_s: int = 58 * 86400
    publication_reserve_s: float = 60.0
    full_pass_publish_interval_s: float = 21600.0
    duckdb_memory_bytes: int = 2 * GIB
    duckdb_threads: int = 2


@dataclass(frozen=True)
class HardMaximums:
    """Upper bounds no root configuration may exceed."""

    max_duration_s: float = 1200.0
    max_requests: int = 25_000
    max_download_bytes: int = 4 * GIB
    max_response_bytes: int = 16 * 1024**2
    max_retained_bytes: int = 512 * GIB
    max_temp_bytes: int = 64 * GIB
    free_reserve_bytes: int = 256 * GIB


HARD_MAXIMUMS = HardMaximums()
MINIMUM_PUBLICATION_RESERVE_S = 60.0
# A 0.1.0 root's config.toml may still carry these; v1 ignores them rather than refusing it.
# Any other unknown key still fails, so typos and future keys stay visible.
RETIRED_SECTIONS = frozenset({"load"})
RETIRED_KEYS = frozenset({"max_raw_bytes", "raw_max_age_s"})


@dataclass(frozen=True)
class CurrentSettings:
    root: Path
    gamma: GammaSettings = field(
        default_factory=lambda: GammaSettings(
            requests_per_second=10.0, max_retries=4, backoff_cap_s=30.0
        )
    )
    limits: CurrentLimits = field(default_factory=CurrentLimits)
    meter: StorageMeter = field(default_factory=StorageMeter, compare=False, repr=False)

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
    if set(raw) - {"gamma", "catalogue"} - RETIRED_SECTIONS:
        raise CurrentError("legacy or unknown configuration; use catalogue.v3 config.toml")
    catalogue = raw.get("catalogue", {})
    if not isinstance(catalogue, dict):
        raise CurrentError("[catalogue] must be a table")
    gamma_config = raw.get("gamma", {})
    if not isinstance(gamma_config, dict):
        raise CurrentError("[gamma] must be a table")
    catalogue = {name: value for name, value in catalogue.items() if name not in RETIRED_KEYS}
    defaults = asdict(CurrentSettings(root).gamma)
    gamma = _build_section(GammaSettings, {**defaults, **gamma_config}, "gamma", environ)
    limits = _build_section(CurrentLimits, catalogue, "catalogue", environ)
    for name, value in asdict(limits).items():
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise CurrentError(f"catalogue {name} must be positive and finite")
    for name in (
        "max_requests",
        "max_download_bytes",
        "max_response_bytes",
        "max_retained_bytes",
        "max_temp_bytes",
        "free_reserve_bytes",
    ):
        if getattr(limits, name) > getattr(HARD_MAXIMUMS, name):
            raise CurrentError(f"catalogue {name} exceeds its hard maximum")
    if (
        limits.max_duration_s > HARD_MAXIMUMS.max_duration_s
        or limits.publication_reserve_s >= limits.max_duration_s
    ):
        raise CurrentError("refresh deadline must be <=1200s with time reserved for publication")
    if limits.publication_reserve_s < MINIMUM_PUBLICATION_RESERVE_S:
        raise CurrentError("publication reserve must be at least 60 seconds")
    if (
        limits.duckdb_memory_bytes > 16 * GIB
        or int(limits.duckdb_memory_bytes) != limits.duckdb_memory_bytes
    ):
        raise CurrentError("catalogue duckdb_memory_bytes exceeds its hard maximum")
    if int(limits.duckdb_threads) != limits.duckdb_threads or not 1 <= limits.duckdb_threads <= 8:
        raise CurrentError("catalogue duckdb_threads exceeds its hard maximum")
    if not 0 < gamma.requests_per_second <= 10:
        raise CurrentError("Gamma rate must be positive and at most 10 requests/second")
    if gamma.max_retries < 0:
        raise CurrentError("Gamma retries cannot be negative")
    for name in ("connect_timeout_s", "read_timeout_s", "backoff_base_s", "backoff_cap_s"):
        value = getattr(gamma, name)
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise CurrentError(f"Gamma {name} must be positive and finite")
    return CurrentSettings(root, gamma, limits)


def _configure_connection(connection, settings: CurrentSettings) -> None:
    memory = int(settings.limits.duckdb_memory_bytes)
    threads = int(settings.limits.duckdb_threads)
    connection.execute(f"SET memory_limit='{memory}B'")
    connection.execute("SET enable_progress_bar=false")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute(f"SET threads={threads}")


def publication_reserve(settings: CurrentSettings, conn) -> float:
    floor = settings.limits.publication_reserve_s
    from oddsfox_catalogue.current_store import get_state

    return max(floor, 1.5 * get_state(conn, "previous_publish_seconds", 0))


def admit(settings: CurrentSettings, *, additional_bytes: int = 0) -> dict[str, int]:
    validate_root(settings.root)
    if additional_bytes < 0:
        raise CurrentError("negative storage reservation")
    if settings.meter.retained is None:
        settings.meter.snapshot(settings.root)
    retained = settings.meter.retained
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
        _configure_connection(connection, settings)
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
        _configure_connection(connection, settings)
        connection.execute("SET temp_directory=?", [str(settings.temporary_dir)])
        remaining = settings.limits.max_temp_bytes - usage["temporary_bytes"]
        if remaining <= 0:
            raise CurrentError("temporary storage allowance exhausted")
        connection.execute("SET max_temp_directory_size=?", [f"{remaining}B"])
        yield connection
    finally:
        connection.close()
