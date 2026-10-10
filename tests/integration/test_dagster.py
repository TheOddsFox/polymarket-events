"""Dagster jobs end to end: bootstrap publishes, a job on an empty root fails, and the
instance config selects the queued run coordinator."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
from dagster import DagsterInstance, job, op
from dagster._core.run_coordinator import QueuedRunCoordinator

from fakes.built_warehouse import build_warehouse, copy_built
from fakes.fake_gamma import FakeGamma
from fakes.harness import make_settings
from fakes.world import demo_world
from oddsfox_catalogue.orchestration.definitions import build_definitions
from oddsfox_catalogue.pipeline import open_event_drop_warning
from oddsfox_catalogue.publish import current_release, publish_release

REPO = Path(__file__).resolve().parents[2]


def _definitions(root: Path, world=None):
    settings = make_settings(root)
    fake = FakeGamma(world or demo_world())
    return settings, build_definitions(settings, transport=fake.transport())


def test_bootstrap_job_captures_loads_builds_and_publishes(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    project = tmp_path / "installed_dbt"
    shutil.copytree(
        settings.dbt_project_dir,
        project,
        ignore=shutil.ignore_patterns("target", "logs", ".user.yml", "__pycache__"),
    )
    before = {p.relative_to(project): p.read_bytes() for p in project.rglob("*") if p.is_file()}
    for path in (project, *project.rglob("*")):
        path.chmod(0o555 if path.is_dir() else 0o444)
    settings = make_settings(
        tmp_path,
        {
            "CATALOGUE_PATHS_DBT_PROJECT_DIR": str(project),
            "CATALOGUE_PATHS_DBT_PROFILES_DIR": str(project),
        },
    )
    try:
        defs = build_definitions(settings, transport=FakeGamma(demo_world()).transport())
        result = defs.resolve_job_def("bootstrap").execute_in_process(raise_on_error=False)
        after = {p.relative_to(project): p.read_bytes() for p in project.rglob("*") if p.is_file()}
    finally:
        for path in (project, *project.rglob("*")):
            path.chmod(0o755 if path.is_dir() else 0o644)
    assert result.success, [
        e.event_specific_data.error.message
        for e in result.all_events
        if e.is_step_failure and e.event_specific_data
    ]
    pointer = current_release(settings)
    assert pointer is not None, "a successful bootstrap must publish a release"
    assert (settings.published_dir / pointer["path"] / "events.parquet").exists()
    assert before == after
    assert list((settings.state_dir / "dbt" / "dagster").glob("*/run_results.json"))


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


@pytest.fixture(scope="module")
def built_root(tmp_path_factory) -> Path:
    """One built warehouse for the module. Tests that need one copy it first."""
    root = tmp_path_factory.mktemp("dagster_built")
    build_warehouse(root)
    return root


def test_validate_job_runs_dbt_test_and_appends_no_snapshot(built_root, tmp_path: Path) -> None:
    settings = copy_built(built_root, tmp_path)
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


def test_unreadable_published_baseline_check_fails_the_dagster_run(
    built_root, tmp_path: Path
) -> None:
    settings = copy_built(built_root, tmp_path)
    publish_release(settings)
    settings.warehouse_path.write_bytes(b"not a duckdb file" * 4096)

    @op
    def check_snapshots(context) -> None:
        open_event_drop_warning(settings, warn=context.log.warning)

    @job
    def snapshot_job() -> None:
        check_snapshots()

    with DagsterInstance.ephemeral() as instance:
        result = snapshot_job.execute_in_process(instance=instance, raise_on_error=False)
        errors = [
            str(event.event_specific_data.error)
            for event in result.all_events
            if event.is_step_failure
        ]

    assert not result.success
    assert any("not a valid DuckDB database file" in error for error in errors)


def test_ops_dagster_yaml_selects_a_single_run_queue(tmp_path: Path) -> None:
    home = tmp_path / "dagster_home"
    home.mkdir()
    shutil.copy(REPO / "ops" / "dagster.yaml", home / "dagster.yaml")
    instance = DagsterInstance.from_config(str(home))
    assert instance.telemetry_enabled is False
    coordinator = instance.run_coordinator
    assert isinstance(coordinator, QueuedRunCoordinator)
    assert coordinator._max_concurrent_runs == 1
