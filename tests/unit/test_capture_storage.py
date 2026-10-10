"""Capture atomic-write admission shares the root's existing temporary allowance."""

from dataclasses import replace

import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import build_runtime
from fakes.world import demo_world
from oddsfox_catalogue.capture.runner import StorageBudget, run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


def test_simultaneous_writes_subtract_existing_work_and_release_only_temp(tmp_path):
    settings = Settings(tmp_path)
    settings = replace(settings, capture=replace(settings.capture, max_temp_bytes=100))
    budget = StorageBudget(settings, allocated=0, existing_temporary=90)
    budget.reserve(6, "first")
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        budget.reserve(5, "second")
    budget.release_temporary("first")
    budget.reserve(10, "second")
    assert budget.allocated == 16
    assert budget.held_temporary == {"second": 10}


def test_existing_dlt_work_blocks_capture_marker_before_any_http(tmp_path):
    fake = FakeGamma(demo_world())
    runtime, _ = build_runtime(tmp_path, fake)
    runtime.settings = replace(
        runtime.settings, capture=replace(runtime.settings.capture, max_temp_bytes=100_000)
    )
    runtime.settings.dlt_pipelines_dir.mkdir(parents=True)
    (runtime.settings.dlt_pipelines_dir / "retained-work").write_bytes(b"x" * 99_999)
    try:
        with pytest.raises(RequestBudgetExceeded, match="temporary"):
            run_capture(runtime, "selected", market_ids=["5001"])
        assert fake.requests == []
        assert not list(runtime.settings.raw_dir.glob("**/_batch.json"))
    finally:
        runtime.client.close()
        runtime.ledger.close()
