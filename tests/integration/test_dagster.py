"""Dagster jobs end to end: bootstrap publishes, a job on an empty root fails, and the
instance config selects the queued run coordinator."""

from __future__ import annotations

import shutil
from pathlib import Path

from dagster import DagsterInstance
from dagster._core.run_coordinator import QueuedRunCoordinator

from fakes.fake_gamma import FakeGamma
from fakes.harness import make_settings
from fakes.world import demo_world
from oddsfox_catalogue.orchestration.definitions import build_definitions
from oddsfox_catalogue.publish import current_release

REPO = Path(__file__).resolve().parents[2]


def _definitions(root: Path, world=None):
    settings = make_settings(root)
    fake = FakeGamma(world or demo_world())
    return settings, build_definitions(settings, transport=fake.transport())


def test_bootstrap_job_captures_loads_builds_and_publishes(tmp_path: Path) -> None:
    settings, defs = _definitions(tmp_path)
    result = defs.get_job_def("bootstrap").execute_in_process(raise_on_error=False)
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
    result = defs.get_job_def("publish").execute_in_process(raise_on_error=False)
    assert not result.success
    assert current_release(settings) is None


def test_ops_dagster_yaml_selects_a_single_run_queue(tmp_path: Path) -> None:
    home = tmp_path / "dagster_home"
    home.mkdir()
    shutil.copy(REPO / "ops" / "dagster.yaml", home / "dagster.yaml")
    instance = DagsterInstance.from_config(str(home))
    coordinator = instance.run_coordinator
    assert isinstance(coordinator, QueuedRunCoordinator)
    assert coordinator._max_concurrent_runs == 1
