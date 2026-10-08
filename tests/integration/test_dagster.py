"""Dagster jobs end to end: bootstrap publishes, a job on an empty root fails, and the
instance config selects the queued run coordinator."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from dagster import DagsterInstance
from dagster._core.run_coordinator import QueuedRunCoordinator

from fakes.built_warehouse import capture_and_load
from fakes.fake_gamma import FakeGamma
from fakes.harness import make_settings
from fakes.world import demo_world
from oddsfox_catalogue.orchestration.definitions import build_definitions
from oddsfox_catalogue.pipeline import dbt_stage
from oddsfox_catalogue.publish import current_release

REPO = Path(__file__).resolve().parents[2]


def _definitions(root: Path, world=None):
    settings = make_settings(root)
    fake = FakeGamma(world or demo_world())
    return settings, build_definitions(settings, transport=fake.transport())


def test_bootstrap_job_captures_loads_builds_and_publishes(tmp_path: Path) -> None:
    settings, defs = _definitions(tmp_path)
    result = defs.resolve_job_def("bootstrap").execute_in_process(raise_on_error=False)
    assert result.success, [
        e.event_specific_data.error.message
        for e in result.all_events
        if e.is_step_failure and e.event_specific_data
    ]
    pointer = current_release(settings)
    assert pointer is not None, "a successful bootstrap must publish a release"
    assert (settings.published_dir / pointer["path"] / "events.parquet").exists()


def test_publish_job_fails_and_publishes_nothing_without_a_build(tmp_path: Path) -> None:
    settings, defs = _definitions(tmp_path)
    result = defs.resolve_job_def("publish").execute_in_process(raise_on_error=False)
    assert not result.success
    assert current_release(settings) is None


def _snapshot_count(warehouse: Path) -> int:
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return int(con.execute("SELECT COUNT(*) FROM marts.catalogue_snapshots").fetchone()[0])
    finally:
        con.close()


def test_validate_job_runs_dbt_test_and_appends_no_snapshot(tmp_path: Path) -> None:
    settings = capture_and_load(tmp_path)
    assert dbt_stage(settings, ["build"]).returncode == 0
    before = _snapshot_count(settings.warehouse_path)
    assert before >= 1

    defs = build_definitions(settings, transport=FakeGamma(demo_world()).transport())
    result = defs.resolve_job_def("validate").execute_in_process(raise_on_error=False)

    assert result.success
    assert _snapshot_count(settings.warehouse_path) == before, "validate must not run build"


def test_validate_job_fails_when_dbt_tests_fail(tmp_path: Path) -> None:
    # No build has run, so the tests have no marts to read and the job must fail.
    settings = make_settings(tmp_path)
    defs = build_definitions(settings, transport=FakeGamma(demo_world()).transport())
    result = defs.resolve_job_def("validate").execute_in_process(raise_on_error=False)
    assert not result.success


def test_ops_dagster_yaml_selects_a_single_run_queue(tmp_path: Path) -> None:
    home = tmp_path / "dagster_home"
    home.mkdir()
    shutil.copy(REPO / "ops" / "dagster.yaml", home / "dagster.yaml")
    instance = DagsterInstance.from_config(str(home))
    coordinator = instance.run_coordinator
    assert isinstance(coordinator, QueuedRunCoordinator)
    assert coordinator._max_concurrent_runs == 1
