"""Capture then load into a real DuckDB warehouse through dlt (no network)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.load.runner import LoadBlocked, LoadRuntime, load_pending
from oddsfox_catalogue.load.source import event_resource, make_pipeline

TESTS_DIR = Path(__file__).resolve().parents[1]
LOAD_CHILD = TESTS_DIR / "fakes" / "load_child.py"


def _capture(root: Path) -> str:
    runtime, _ = build_runtime(root, FakeGamma(demo_world()))
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        return summary.batch_id
    finally:
        runtime.ledger.close()


def _load(settings: Settings) -> Any:
    ledger = Ledger(settings.ledger_path)
    try:
        return load_pending(LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW))
    finally:
        ledger.close()


def _query(settings: Settings, sql: str) -> Any:
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


def _table_count(settings: Settings, table: str) -> int:
    return int(_query(settings, f"SELECT COUNT(*) FROM bronze.{table}"))


def _distinct_observations(settings: Settings, table: str) -> int:
    return int(_query(settings, f"SELECT COUNT(DISTINCT observation_id) FROM bronze.{table}"))


def _registry_rows(settings: Settings) -> list[tuple[str, str]]:
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        return con.execute("SELECT batch_id, status FROM bronze.batch_registry").fetchall()
    except duckdb.CatalogException:
        return []  # table is created by the first registry insert
    finally:
        con.close()


def _batch_status(settings: Settings, batch_id: str) -> str:
    ledger = Ledger(settings.ledger_path)
    try:
        batch = ledger.get_batch(batch_id)
        assert batch is not None
        return str(batch["status"])
    finally:
        ledger.close()


def test_bootstrap_loads_rows_and_registers_batch(tmp_path: Path) -> None:
    batch_id = _capture(tmp_path)
    settings = make_settings(tmp_path)

    summary = _load(settings)

    assert summary.batches_registered == [batch_id]
    assert summary.event_rows > 0
    assert _table_count(settings, "event_observations") == summary.event_rows
    assert _table_count(settings, "market_observations") == summary.market_rows
    assert _distinct_observations(settings, "event_observations") == summary.event_rows
    assert _registry_rows(settings) == [(batch_id, "loaded")]
    assert _batch_status(settings, batch_id) == "loaded"


def test_rerun_is_a_noop_and_rows_never_duplicate(tmp_path: Path) -> None:
    batch_id = _capture(tmp_path)
    settings = make_settings(tmp_path)
    first = _load(settings)
    events_before = _table_count(settings, "event_observations")
    markets_before = _table_count(settings, "market_observations")

    assert _load(settings).pages_loaded == 0, "a second run has nothing pending"

    # Force a re-load of already-committed pages: insert-only merge must ignore them.
    ledger = Ledger(settings.ledger_path)
    try:
        with ledger.transaction() as conn:
            conn.execute("UPDATE pages SET loaded_at = NULL, dlt_load_id = NULL")
    finally:
        ledger.close()
    replay = _load(settings)

    assert replay.pages_loaded == first.pages_loaded
    assert _table_count(settings, "event_observations") == events_before
    assert _table_count(settings, "market_observations") == markets_before
    assert _distinct_observations(settings, "event_observations") == events_before
    assert _registry_rows(settings) == [(batch_id, "loaded")], "registry is insert-only"


def test_crash_mid_dlt_load_recovers_without_duplicates(tmp_path: Path) -> None:
    _capture(tmp_path)
    settings = make_settings(tmp_path)
    env = {**os.environ, "CATALOGUE_FAULT": "mid_dlt_load", "PYTHONPATH": str(TESTS_DIR)}

    crashed = subprocess.run(
        [sys.executable, str(LOAD_CHILD), str(tmp_path)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr

    recovered = _load(settings)

    assert recovered.pages_loaded > 0
    assert _table_count(settings, "event_observations") == _distinct_observations(
        settings, "event_observations"
    )
    assert _table_count(settings, "event_observations") > 0


def test_crash_before_registry_recovers_and_registers_once(tmp_path: Path) -> None:
    batch_id = _capture(tmp_path)
    settings = make_settings(tmp_path)
    env = {**os.environ, "CATALOGUE_FAULT": "before_registry", "PYTHONPATH": str(TESTS_DIR)}

    crashed = subprocess.run(
        [sys.executable, str(LOAD_CHILD), str(tmp_path)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr
    assert _batch_status(settings, batch_id) == "captured"
    assert _registry_rows(settings) == []

    summary = _load(settings)

    assert summary.batches_registered == [batch_id]
    assert _registry_rows(settings) == [(batch_id, "loaded")]
    assert _batch_status(settings, batch_id) == "loaded"


def test_schema_drift_in_envelope_is_rejected(tmp_path: Path) -> None:
    _capture(tmp_path)
    settings = make_settings(tmp_path)
    _load(settings)
    pipeline = make_pipeline(settings.warehouse_path, settings.dlt_pipelines_dir, settings.load)
    rows = [
        {
            "observation_id": "drift-1",
            "venue": "polymarket",
            "entity_id": "1",
            "batch_id": "drift",
            "page_id": "drift",
            "observed_at": FIXED_NOW,
            "source_updated_at": None,
            "endpoint": "/events/keyset",
            "payload_hash": "h",
            "payload": {},
            "unexpected_new_column": "boom",
        }
    ]

    with pytest.raises(PipelineStepFailed):
        pipeline.run([event_resource(rows)])

    assert _table_count(settings, "event_observations") == _distinct_observations(
        settings, "event_observations"
    )


def test_quarantined_records_load_without_blocking_the_page(tmp_path: Path) -> None:
    world = demo_world()
    world.add_event(
        make_event(
            "555",
            "Event with a malformed nested market",
            markets=[make_market("8555", "Malformed?")],
        )
    )
    fake = FakeGamma(world)
    fake.nested_overrides["8555"] = "not-an-object"  # a non-object element of markets[]
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        batch_id = run_capture(runtime, "bootstrap").batch_id
    finally:
        runtime.ledger.close()
    # One malformed record in a small world is above the 1% default; this test is about
    # the record not blocking the page, so it raises the cap to let the batch through.
    settings = make_settings(tmp_path, {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "0.5"})

    summary = _load(settings)

    assert summary.quarantined >= 1
    assert _batch_status(settings, batch_id) == "loaded"
    assert (
        _query(settings, "SELECT COUNT(*) FROM bronze.quarantined_records") == summary.quarantined
    )
    # Observed by the list scan and the by-ID scan, so two observations; both must load.
    assert (
        _query(
            settings,
            "SELECT COUNT(*) FROM bronze.event_observations WHERE entity_id = '555'",
        )
        >= 1
    ), "the event still loads; only its malformed nested market is quarantined"


def _fake_that_drops_event_150() -> FakeGamma:
    from fakes.world import World

    world = World()
    kept = make_event("50", "Kept")
    kept["markets"] = [make_market("60", "Kept market", event_stub=event_stub(kept))]
    world.add_event(kept)
    world.add_event(make_event("150", "Dropped"))
    fake = FakeGamma(world)

    def hits_150(request: httpx.Request) -> bool:
        if request.url.path == "/events/150":
            return True
        return request.url.path == "/events/keyset" and "150" in request.url.params.get_list("id")

    fake.rules.append(
        Rule(hits_150, lambda request: httpx.Response(500, json={"error": "down"}), remaining=500)
    )
    return fake


def test_fetch_failed_is_loaded_as_quarantine_and_counts_against_the_gate(
    tmp_path: Path,
) -> None:
    runtime, _ = build_runtime(tmp_path, _fake_that_drops_event_150())
    try:
        run_capture(runtime, "bootstrap")
    finally:
        runtime.ledger.close()

    permissive = make_settings(tmp_path, {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "0.5"})
    summary = _load(permissive)
    assert summary.quarantined >= 1
    assert (
        _query(
            permissive,
            "SELECT reason FROM bronze.quarantined_records "
            "WHERE json_extract_string(payload, '$.id') = '150'",
        )
        == "fetch_failed"
    )
    assert (
        _query(permissive, "SELECT COUNT(*) FROM bronze.event_observations WHERE entity_id = '150'")
        == 0
    )

    blocked_root = tmp_path / "blocked"
    runtime, _ = build_runtime(blocked_root, _fake_that_drops_event_150())
    try:
        blocked_id = run_capture(runtime, "bootstrap").batch_id
    finally:
        runtime.ledger.close()
    blocked_settings = make_settings(blocked_root, {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "0"})
    with pytest.raises(LoadBlocked, match="quarantine_max_ratio"):
        _load(blocked_settings)
    assert _batch_status(blocked_settings, blocked_id) == "captured"
    assert _registry_rows(blocked_settings) == []


def test_batch_over_quarantine_cap_is_blocked_and_not_registered(tmp_path: Path) -> None:
    world = demo_world()
    world.add_event(
        make_event(
            "555",
            "Event with a malformed nested market",
            markets=[make_market("8555", "Malformed?")],
        )
    )
    fake = FakeGamma(world)
    fake.nested_overrides["8555"] = "not-an-object"
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        batch_id = run_capture(runtime, "bootstrap").batch_id
    finally:
        runtime.ledger.close()
    # A cap of zero blocks any quarantined record.
    settings = make_settings(tmp_path, {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "0"})

    with pytest.raises(LoadBlocked, match="quarantine_max_ratio"):
        _load(settings)

    assert _batch_status(settings, batch_id) == "captured", "a blocked batch stays unloaded"
    assert _registry_rows(settings) == [], "a blocked batch is never registered"


def test_insert_only_merge_keys_on_observation_id_not_payload(tmp_path: Path) -> None:
    """Regression: two observations with identical payloads must both be kept.

    Shared column-hint dicts once made dlt merge on payload_hash, which dropped the second
    observation of any unchanged record.
    """
    from datetime import datetime

    settings = make_settings(tmp_path)
    pipeline = make_pipeline(settings.warehouse_path, settings.dlt_pipelines_dir, settings.load)
    observed = datetime(2026, 10, 8, 6, 0, tzinfo=datetime.now().astimezone().tzinfo)

    def row(observation_id: str) -> dict:
        return {
            "observation_id": observation_id,
            "venue": "polymarket",
            "entity_id": "1",
            "batch_id": "b",
            "page_id": observation_id,
            "observed_at": observed,
            "source_updated_at": None,
            "endpoint": "/events/keyset",
            "payload_hash": "same-hash",
            "payload": {"id": "1"},
        }

    for observation_id in ["obs-a", "obs-b"]:
        pipeline.run(event_resource([row(observation_id)]))

    assert _query(settings, "SELECT COUNT(*) FROM bronze.event_observations") == 2
