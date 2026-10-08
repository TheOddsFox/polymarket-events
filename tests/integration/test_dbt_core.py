"""dbt against a real warehouse: capture, load, then build the core models."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import capture_and_load
from fakes.dbt_run import run_dbt
from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import demo_world, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.warehouse import read_open_event_ids


def _scalar(warehouse: Path, sql: str):
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Path, Settings]:
    root = tmp_path_factory.mktemp("dbt_root")
    settings = capture_and_load(root)
    result = run_dbt(["build"], settings.warehouse_path, root)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]
    return root, settings


def test_dbt_parses_the_project(tmp_path: Path) -> None:
    result = run_dbt(["parse"], tmp_path / "unused.duckdb", tmp_path)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]


def test_build_produces_one_row_per_event(built) -> None:
    _, settings = built
    distinct_events = _scalar(
        settings.warehouse_path,
        "SELECT COUNT(DISTINCT entity_id) FROM bronze.event_observations",
    )
    current = _scalar(settings.warehouse_path, "SELECT COUNT(*) FROM core.events_current")
    assert current == distinct_events > 0


def test_outcomes_align_and_nothing_is_quarantined(built) -> None:
    _, settings = built
    assert (
        _scalar(settings.warehouse_path, "SELECT COUNT(*) FROM core.quarantine_market_outcomes")
        == 0
    )
    outcomes = _scalar(settings.warehouse_path, "SELECT COUNT(*) FROM core.outcomes_current")
    assert outcomes > 0


def _load(settings: Settings) -> None:
    ledger = Ledger(settings.ledger_path)
    try:
        load_pending(LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW))
    finally:
        ledger.close()


def _snapshot_query(warehouse: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _snapshot(warehouse: Path, table: str, key: str) -> list[tuple]:
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(f"SELECT * FROM {table} ORDER BY {key}").fetchall()
    finally:
        con.close()


STATE_TABLES = {
    "core.events_current": "venue, event_id",
    "core.markets_current": "venue, market_id",
    "history.event_history": "venue, event_id, version_no",
    "history.market_history": "venue, market_id, version_no",
    "history.event_metrics": "observation_id",
    "history.market_metrics": "observation_id",
}
INCREMENTAL_MODELS = (
    "events_current markets_current event_history market_history event_metrics market_metrics"
)
DROP_THRESHOLD = '{"max_open_events_drop_pct": 0.9}'


def _daily_batch(settings: Settings, root: Path, world, days: int) -> None:
    """Capture one daily batch of ``world`` taken ``days`` after bootstrap, then load it."""
    runtime, _ = build_runtime(
        root,
        FakeGamma(world),
        now=FIXED_NOW + timedelta(days=days),
        open_event_ids=lambda: read_open_event_ids(settings.warehouse_path),
    )
    try:
        assert run_capture(runtime, "daily").status == "captured"
    finally:
        runtime.ledger.close()
    _load(settings)


def _state(settings: Settings) -> dict[str, list[tuple]]:
    return {t: _snapshot(settings.warehouse_path, t, k) for t, k in STATE_TABLES.items()}


def test_incremental_build_matches_full_rebuild_after_a_closure(tmp_path: Path) -> None:
    settings = capture_and_load(tmp_path)
    assert run_dbt(["build"], settings.warehouse_path, tmp_path).returncode == 0

    # Event 202 closes between batches. Daily capture must fetch it by ID and reflect it.
    closed_world = demo_world()
    closed_world.events["202"]["closed"] = True
    closed_world.events["202"]["updatedAt"] = "2026-10-08T07:00:00Z"
    _daily_batch(settings, tmp_path, closed_world, days=1)

    # The demo world has two open events, so closing one is a 50% drop by design. The
    # regression threshold is loosened for this test only; the blocking test below keeps it.
    incremental = run_dbt(["build", "--vars", DROP_THRESHOLD], settings.warehouse_path, tmp_path)
    assert incremental.returncode == 0, incremental.stdout[-3000:]

    closed = _scalar(
        settings.warehouse_path,
        "SELECT closed FROM core.events_current WHERE event_id = '202'",
    )
    assert closed is True
    observations = _scalar(
        settings.warehouse_path,
        "SELECT observation_count FROM core.events_current WHERE event_id = '202'",
    )
    # Two bootstrap observations (list scan and by-ID scan) plus one from the daily batch.
    assert observations == 3

    versions = _snapshot_query(
        settings.warehouse_path,
        "SELECT version_no, closed, is_current FROM history.event_history"
        " WHERE event_id = '202' ORDER BY version_no",
    )
    assert versions == [(1, False, False), (2, True, True)], "closing starts a new SCD2 version"
    unchanged = _snapshot_query(
        settings.warehouse_path,
        "SELECT COUNT(*) FROM history.event_history WHERE event_id = '101'",
    )
    assert unchanged == [(1,)], "an unchanged event keeps a single version"

    # A second batch on top of the incremental state. The metrics tables must gain its
    # observations, each exactly once.
    metric_rows = _scalar(settings.warehouse_path, "SELECT COUNT(*) FROM history.event_metrics")
    _daily_batch(settings, tmp_path, closed_world, days=2)
    second = run_dbt(["build", "--vars", DROP_THRESHOLD], settings.warehouse_path, tmp_path)
    assert second.returncode == 0, second.stdout[-3000:]
    assert _scalar(settings.warehouse_path, "SELECT COUNT(*) FROM history.event_metrics") > (
        metric_rows
    ), "the second batch must add metric rows"
    for table in ("history.event_metrics", "history.market_metrics"):
        duplicates = _snapshot_query(
            settings.warehouse_path,
            f"SELECT observation_id FROM {table} GROUP BY observation_id HAVING COUNT(*) > 1",
        )
        assert duplicates == [], f"{table} repeats an observation: {duplicates[:3]}"
    after_incremental = _state(settings)

    full = run_dbt(
        ["run", "--select", INCREMENTAL_MODELS, "--full-refresh"], settings.warehouse_path, tmp_path
    )
    assert full.returncode == 0, full.stdout[-3000:]
    assert after_incremental == _state(settings), "incremental state must equal a full rebuild"


def test_count_regression_blocks_the_build(tmp_path: Path) -> None:
    """A sudden drop in open events must fail the build, so publication never runs on it."""
    settings = capture_and_load(tmp_path)
    assert run_dbt(["build"], settings.warehouse_path, tmp_path).returncode == 0

    closed_world = demo_world()
    closed_world.events["202"]["closed"] = True
    closed_world.events["202"]["updatedAt"] = "2026-10-08T07:00:00Z"
    runtime, _ = build_runtime(
        tmp_path,
        FakeGamma(closed_world),
        now=FIXED_NOW + timedelta(days=1),
        open_event_ids=lambda: read_open_event_ids(settings.warehouse_path),
    )
    try:
        run_capture(runtime, "daily")
    finally:
        runtime.ledger.close()
    _load(settings)

    blocked = run_dbt(["build"], settings.warehouse_path, tmp_path)
    assert blocked.returncode != 0
    assert "assert_open_events_not_dropping" in blocked.stdout


def test_unresolved_event_reference_blocks_the_build(tmp_path: Path) -> None:
    """A market whose nested event stub names an event that was never captured.

    The demo world resolves every reference, so it builds. This world adds one market whose
    ``events`` stub points at event 999999, which is not in the captured events. That single
    unresolved reference is far above the 1% cap, so the build must fail.
    """
    world = demo_world()
    ghost = {"id": "999999", "ticker": None, "slug": None, "title": "gone"}
    world.add_direct_market(make_market("9002", "Ghost market?", event_stub=ghost))
    settings = capture_and_load(tmp_path, world=world)

    result = run_dbt(["build"], settings.warehouse_path, tmp_path)
    assert result.returncode != 0
    assert "assert_unresolved_event_refs_within_limit" in result.stdout


def test_demo_world_resolves_every_event_reference(tmp_path: Path) -> None:
    """Control for the test above: the unmodified demo world builds cleanly."""
    settings = capture_and_load(tmp_path)
    result = run_dbt(["build"], settings.warehouse_path, tmp_path)
    assert result.returncode == 0, result.stdout[-3000:]
