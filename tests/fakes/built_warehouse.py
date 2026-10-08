"""Build a real warehouse (capture, load, dbt build) for tests that need marts or releases."""

from __future__ import annotations

from pathlib import Path

from fakes.dbt_run import run_dbt
from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import demo_world
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending


def capture_and_load(root: Path) -> Settings:
    runtime, _ = build_runtime(root, FakeGamma(demo_world()))
    try:
        run_capture(runtime, "bootstrap")
    finally:
        runtime.ledger.close()
    settings = make_settings(root)
    ledger = Ledger(settings.ledger_path)
    try:
        load_pending(LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW))
    finally:
        ledger.close()
    return settings


def build_warehouse(root: Path) -> Settings:
    settings = capture_and_load(root)
    result = run_dbt(["build"], settings.warehouse_path, root)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]
    return settings
