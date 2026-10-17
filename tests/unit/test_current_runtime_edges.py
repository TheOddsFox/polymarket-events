"""Runtime edges: cadence boundary, exit-code contract, DuckDB limit wiring, storage meter."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from oddsfox_catalogue import current_reconcile, current_runtime, limits
from oddsfox_catalogue.cli import main
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    admit,
    connect_store,
    load_current_settings,
    memory_connection,
    validate_root,
)
from oddsfox_catalogue.current_runtime import execute
from oddsfox_catalogue.current_store import initialize, set_state
from oddsfox_catalogue.limits import StorageMeter

GIB = 1024**3
FIXED = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
ENV_MEMORY = "CATALOGUE_CATALOGUE_DUCKDB_MEMORY_BYTES"
ENV_THREADS = "CATALOGUE_CATALOGUE_DUCKDB_THREADS"
ENV_INTERVAL = "CATALOGUE_CATALOGUE_FULL_PASS_PUBLISH_INTERVAL_S"


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FIXED


class _Client:
    def __init__(self, *_args, **kwargs):
        self.deadline = kwargs["deadline"]
        self.stats = self

    requests = 0
    downloaded_bytes = 0

    def as_dict(self):
        return {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


@pytest.fixture
def settings(tmp_path):
    settings = CurrentSettings(
        tmp_path / "root",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=60),
    )
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        initialize(conn)
    return settings


@pytest.fixture
def published(monkeypatch):
    """Stub acquisition and publication; record each publication and freeze the clock."""
    calls = []

    def fake_reconcile(settings, conn, client, *, deadline, publishing=True):
        return {"responses": 0, "source_complete": False}

    def fake_publish(settings, conn, deadline):
        calls.append(True)
        return {"noop": True}, 0.0

    monkeypatch.setattr(current_runtime, "GammaClient", _Client)
    monkeypatch.setattr(current_runtime, "_publish", fake_publish)
    monkeypatch.setattr(current_reconcile, "reconcile", fake_reconcile)
    monkeypatch.setattr(current_runtime, "datetime", _FrozenDatetime)
    return calls


def test_first_forced_pass_publishes_when_nothing_was_ever_published(settings, published):
    result = execute(settings, "reconcile", {"full_pass": True}, 10**9)
    assert published == [True]
    assert result["published"] is True
    assert result["status"] == "published"


@pytest.mark.parametrize(
    "age_s,publishes",
    [(21600, True), (21599, False)],
    ids=["exactly-at-interval-publishes", "one-second-short-defers"],
)
def test_forced_pass_cadence_is_inclusive_at_the_interval(settings, published, age_s, publishes):
    with connect_store(settings) as conn:
        set_state(conn, "last_publish_at", (FIXED - timedelta(seconds=age_s)).isoformat())
    result = execute(settings, "reconcile", {"full_pass": True}, 10**9)
    assert bool(published) is publishes
    assert result["published"] is publishes
    assert result["status"] == ("published" if publishes else "deferred")


def test_routine_reconcile_publishes_regardless_of_the_cadence(settings, published):
    with connect_store(settings) as conn:
        set_state(conn, "last_publish_at", FIXED.isoformat())
    result = execute(settings, "reconcile", {"full_pass": False}, 10**9)
    assert published == [True]
    assert result["published"] is True


@pytest.mark.parametrize(
    "supervised,exit_code",
    [
        ((True, {"status": "published", "published": True}), 0),
        ((True, {"status": "deferred", "published": False}), 3),
        ((True, {"status": "ready"}), 0),
        ((False, {"status": "blocked", "message": "boom"}), 3),
    ],
    ids=["published", "deferred-is-not-success", "no-publication-field", "run-failed"],
)
def test_reconcile_exit_code_reports_whether_the_run_published(
    tmp_path, monkeypatch, capsys, supervised, exit_code
):
    root = tmp_path / "root"
    validate_root(root, initialize=True)
    monkeypatch.setattr(current_runtime, "supervise", lambda *_a, **_k: supervised)
    assert main(["--root", str(root), "reconcile", "--full-pass"]) == exit_code
    printed = json.loads(capsys.readouterr().out)
    assert printed["status"] == supervised[1]["status"]


def test_configured_duckdb_limits_reach_both_connection_paths(tmp_path):
    limit = 3 * GIB
    settings = load_current_settings(
        tmp_path / "root",
        env={ENV_MEMORY: str(limit), ENV_THREADS: "3"},
    )
    validate_root(settings.root, initialize=True)
    # Compare against DuckDB's own rendering of the same byte count.
    reference = duckdb.connect(":memory:")
    reference.execute(f"SET memory_limit='{limit}B'")
    expected = reference.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    reference.close()

    with connect_store(settings) as conn:
        assert conn.execute("SELECT current_setting('memory_limit')").fetchone()[0] == expected
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    with memory_connection(settings) as conn:
        assert conn.execute("SELECT current_setting('memory_limit')").fetchone()[0] == expected
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0] == 3


@pytest.mark.parametrize(
    "name,value,field,expected",
    [
        (ENV_MEMORY, str(16 * GIB), "duckdb_memory_bytes", 16 * GIB),
        (ENV_THREADS, "1", "duckdb_threads", 1),
        (ENV_THREADS, "8", "duckdb_threads", 8),
    ],
    ids=["memory-at-16GiB-ceiling", "threads-at-floor", "threads-at-ceiling"],
)
def test_accepted_limit_boundaries(tmp_path, name, value, field, expected):
    settings = load_current_settings(tmp_path / "root", env={name: value})
    assert getattr(settings.limits, field) == expected


@pytest.mark.parametrize(
    "name,value",
    [
        (ENV_MEMORY, str(16 * GIB + 1)),
        (ENV_THREADS, "0"),
        (ENV_THREADS, "9"),
        (ENV_INTERVAL, "0"),
        (ENV_INTERVAL, "-1"),
        (ENV_INTERVAL, "nan"),
        (ENV_INTERVAL, "inf"),
    ],
    ids=[
        "memory-over-ceiling",
        "threads-zero",
        "threads-over-ceiling",
        "interval-zero",
        "interval-negative",
        "interval-nan",
        "interval-inf",
    ],
)
def test_rejected_limit_boundaries_fail_closed(tmp_path, name, value):
    # Non-finite values are refused by the shared parser (plain ValueError);
    # CurrentError is a ValueError subclass, so this accepts either.
    with pytest.raises(ValueError):
        load_current_settings(tmp_path / "root", env={name: value})


def test_admission_walks_the_root_once_and_then_trusts_the_meter(tmp_path, monkeypatch):
    settings = CurrentSettings(
        validate_root(tmp_path / "root", initialize=True),
        limits=replace(CurrentLimits(), free_reserve_bytes=1),
    )
    walks = []
    original = limits.directory_bytes

    def counting(directory):
        walks.append(directory)
        return original(directory)

    monkeypatch.setattr(limits, "directory_bytes", counting)
    for _ in range(3):
        admit(settings)
    assert walks.count(settings.root) == 1

    base = settings.meter.retained
    settings.meter.add(4096)
    usage = admit(settings)
    assert walks.count(settings.root) == 1
    assert settings.meter.retained == base + 4096
    assert usage["retained_bytes"] == base + 4096


def test_meter_ignores_bytes_before_a_snapshot_and_never_counts_down(tmp_path):
    meter = StorageMeter()
    meter.add(100)
    assert meter.retained is None

    assert meter.snapshot(tmp_path) == 0
    meter.add(-50)
    assert meter.retained == 0
    meter.add(10)
    assert meter.retained == 10
