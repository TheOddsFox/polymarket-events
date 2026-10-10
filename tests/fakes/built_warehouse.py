"""Build a real warehouse (capture, load, dbt build) for tests that need marts or releases."""

from __future__ import annotations

import shutil
from pathlib import Path

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import demo_world
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.pipeline import dbt_stage


def capture_and_load(root: Path, world=None) -> Settings:
    runtime, _ = build_runtime(root, FakeGamma(world or demo_world()))
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
    result = dbt_stage(settings, ["build"])
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]
    return settings


def copy_built(built_root: Path, dest: Path) -> Settings:
    """Copy a built project root (raw pages, ledger, dlt state, warehouse, dbt work dir) to dest.

    Modules build once in a module-scoped fixture. A test that mutates state works on its own
    copy, so a tamper or closure in one test cannot change the input of the next.
    """
    shutil.copytree(built_root, dest, dirs_exist_ok=True)
    return make_settings(dest)
