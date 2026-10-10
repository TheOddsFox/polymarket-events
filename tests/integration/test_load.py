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
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded, RetriesExhausted
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


@pytest.mark.parametrize("selection", ["event", "market"])
@pytest.mark.parametrize("incidental", [None, {"question": "Missing incidental identity"}])
def test_selected_load_excludes_malformed_incidental_parent_markets(
    tmp_path, selection, incidental
):
    world = demo_world()
    fake = FakeGamma(world)
    fake.nested_overrides["5002"] = incidental
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        summary = run_capture(
            runtime,
            "selected",
            market_ids=["5001"] if selection == "market" else [],
            event_ids=["101"] if selection == "event" else [],
        )
        assert summary.status == "captured"
        settings = runtime.settings
    finally:
        runtime.client.close()
        runtime.ledger.close()
    result = _load(settings)
    assert len(result.batches_registered) == 1
    assert _table_count(settings, "quarantined_records") == 0
    assert _table_count(settings, "event_observations") == 1
    assert _table_count(settings, "market_observations") == (2 if selection == "market" else 0)


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
            "normalized": {},
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


def test_unresolved_fetch_cannot_register_partial_load_and_resumes_without_relaxing_gate(
    tmp_path: Path,
) -> None:
    fake = _fake_that_drops_event_150()
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        with pytest.raises(RetriesExhausted):
            run_capture(runtime, "bootstrap")
        batch_id = runtime.ledger.list_batches()[0]["batch_id"]
        settings = runtime.settings
        partial = load_pending(
            LoadRuntime(settings=settings, ledger=runtime.ledger, now=lambda: FIXED_NOW)
        )
        assert partial.batches_registered == []
        assert runtime.ledger.get_batch(batch_id)["status"] == "capturing"
        assert (
            _query(settings, "SELECT COUNT(*) FROM bronze.event_observations WHERE entity_id='150'")
            == 0
        )
        fake.rules.clear()
        captured = run_capture(runtime, "bootstrap", resume=batch_id)
        assert captured.status == "captured"
        complete = load_pending(
            LoadRuntime(settings=settings, ledger=runtime.ledger, now=lambda: FIXED_NOW)
        )
        assert complete.batches_registered == [batch_id]
        assert (
            _query(settings, "SELECT COUNT(*) FROM bronze.event_observations WHERE entity_id='150'")
            == 1
        )
        assert _query(settings, "SELECT COUNT(*) FROM bronze.quarantined_records") == 0
    finally:
        runtime.ledger.close()
        runtime.client.close()


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
            "normalized": {},
        }

    for observation_id in ["obs-a", "obs-b"]:
        pipeline.run(event_resource([row(observation_id)]))

    assert _query(settings, "SELECT COUNT(*) FROM bronze.event_observations") == 2


def test_replay_blocks_conflicting_persisted_normalization(tmp_path: Path) -> None:
    """An existing key cannot silently swallow a changed semantic projection."""
    _capture(tmp_path)
    settings = make_settings(tmp_path)
    _load(settings)
    connection = duckdb.connect(str(settings.warehouse_path))
    try:
        connection.execute(
            "UPDATE bronze.event_observations SET normalized = '{}' WHERE entity_id = '101'"
        )
    finally:
        connection.close()
    ledger = Ledger(settings.ledger_path)
    try:
        with ledger.transaction() as connection:
            connection.execute("UPDATE pages SET loaded_at = NULL, dlt_load_id = NULL")
    finally:
        ledger.close()
    with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
        _load(settings)


def test_replay_blocks_conflicting_persisted_receipt_timestamp(tmp_path: Path) -> None:
    """A mutated locator/envelope must fail even when normalized semantics are identical."""
    _capture(tmp_path)
    settings = make_settings(tmp_path)
    _load(settings)
    connection = duckdb.connect(str(settings.warehouse_path))
    try:
        connection.execute(
            "UPDATE bronze.event_observations SET observed_at = observed_at + INTERVAL '1 second' WHERE entity_id = '101'"
        )
    finally:
        connection.close()
    ledger = Ledger(settings.ledger_path)
    try:
        with ledger.transaction() as connection:
            connection.execute("UPDATE pages SET loaded_at = NULL, dlt_load_id = NULL")
    finally:
        ledger.close()
    with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
        _load(settings)


def test_empty_materialization_keeps_insert_only_merge_on_next_real_record(tmp_path: Path) -> None:
    """An empty first capture has canonical schemas without inventing an evidence row."""
    from oddsfox_catalogue.load.rows import PageContext, rows_for_page
    from oddsfox_catalogue.load.source import market_resource, quarantine_resource

    settings = make_settings(tmp_path)
    pipeline = make_pipeline(settings.warehouse_path, settings.dlt_pipelines_dir, settings.load)
    for factory in (event_resource, market_resource, quarantine_resource):
        pipeline.run(factory([], materialize_only=True))
    for table in ("event_observations", "market_observations", "quarantined_records"):
        assert _table_count(settings, table) == 0
    context = PageContext("page", "batch", "/events", FIXED_NOW, "events")
    rows = rows_for_page(context, [make_event("1", "First")])
    for _ in range(2):
        pipeline.run(event_resource(rows.events))
    assert _table_count(settings, "event_observations") == 1


def test_load_preflight_blocks_before_dlt_when_working_allowance_is_too_small(
    tmp_path: Path,
) -> None:
    batch_id = _capture(tmp_path)
    settings = make_settings(tmp_path, {"CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(8 * 1024**2 - 1)})
    with pytest.raises(RequestBudgetExceeded, match="temporary storage allowance"):
        _load(settings)
    assert _batch_status(settings, batch_id) == "captured"
    assert not settings.warehouse_path.exists()
    with Ledger(settings.ledger_path) as ledger:
        assert len(ledger.pending_load_pages(batch_id)) == len(ledger.pages_for_batch(batch_id))


@pytest.mark.parametrize("allowance", ["retained", "temporary"])
def test_load_resource_admission_precedes_fresh_warehouse_stamp(tmp_path, allowance):
    from dataclasses import replace

    from oddsfox_catalogue.limits import retained_bytes

    batch_id = _capture(tmp_path)
    settings = make_settings(tmp_path)
    overrides = (
        {"max_retained_bytes": retained_bytes(settings) + 4096}
        if allowance == "retained"
        else {"max_temp_bytes": 4096}
    )
    settings = replace(settings, capture=replace(settings.capture, **overrides))
    with pytest.raises(RequestBudgetExceeded, match="allowance"):
        _load(settings)
    assert not settings.warehouse_path.exists()
    assert _batch_status(settings, batch_id) == "captured"
    with Ledger(settings.ledger_path) as ledger:
        assert len(ledger.pending_load_pages(batch_id)) == len(ledger.pages_for_batch(batch_id))


def test_byte_heavy_selected_load_stays_inside_documented_reservation(tmp_path: Path) -> None:
    import hashlib

    from fakes.world import World
    from oddsfox_catalogue.limits import retained_bytes
    from oddsfox_catalogue.load.runner import (
        LOAD_EXPANSION_FACTOR,
        LOAD_FIXED_RESERVE,
        _encoded_row_bytes,
        _rows_for_chunk,
    )

    noise = "".join(hashlib.sha256(str(value).encode()).hexdigest() for value in range(8192))
    world = World()
    event = make_event("11", "Byte-heavy event")
    market = make_market("201", "Byte-heavy market", event_stub=event_stub(event))
    market["synthetic_payload"] = noise
    event["synthetic_payload"] = noise
    event["markets"] = [market]
    world.add_event(event)
    runtime, _ = build_runtime(
        tmp_path,
        FakeGamma(world),
        env={
            "CATALOGUE_CAPTURE_MAX_RETAINED_BYTES": str(128 * 1024**2),
            "CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(128 * 1024**2),
        },
    )
    try:
        captured = run_capture(runtime, "selected", market_ids=["201"])
        load_runtime = LoadRuntime(
            settings=runtime.settings, ledger=runtime.ledger, now=lambda: FIXED_NOW
        )
        rows, _ = _rows_for_chunk(
            load_runtime, runtime.ledger.pending_load_pages(captured.batch_id)
        )
        encoded = sum(
            _encoded_row_bytes(row)
            for relation in (rows.events, rows.markets, rows.quarantine)
            for row in relation
        )
        before = retained_bytes(runtime.settings)
        loaded = load_pending(load_runtime)
        after = retained_bytes(runtime.settings)
        assert encoded > 1024**2
        assert loaded.batches_registered == [captured.batch_id]
        assert 0 < after - before <= LOAD_FIXED_RESERVE + LOAD_EXPANSION_FACTOR * encoded
        assert after < runtime.settings.capture.max_retained_bytes
    finally:
        runtime.ledger.close()
        runtime.client.close()


def test_existing_dlt_working_bytes_are_subtracted_before_load(tmp_path: Path) -> None:
    batch_id = _capture(tmp_path)
    settings = make_settings(tmp_path, {"CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(16 * 1024**2)})
    settings.dlt_pipelines_dir.mkdir(parents=True, exist_ok=True)
    retained = settings.dlt_pipelines_dir / "synthetic-existing-package.bin"
    retained.write_bytes(b"x" * (9 * 1024**2))
    with pytest.raises(RequestBudgetExceeded, match="temporary storage allowance"):
        _load(settings)
    assert retained.stat().st_size == 9 * 1024**2
    assert _batch_status(settings, batch_id) == "captured"
    with Ledger(settings.ledger_path) as ledger:
        assert len(ledger.pending_load_pages(batch_id)) == len(ledger.pages_for_batch(batch_id))


def test_cached_pipeline_refreshes_spill_allowance_before_every_run(tmp_path: Path) -> None:
    from oddsfox_catalogue.limits import remaining_temp_bytes
    from oddsfox_catalogue.load.runner import _run_resource
    from oddsfox_catalogue.load.source import market_resource

    settings = make_settings(tmp_path, {"CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(64 * 1024**2)})
    with Ledger(settings.ledger_path) as ledger:
        runtime = LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW)
        _run_resource(runtime, event_resource([], materialize_only=True))
        pipeline = runtime.pipeline
        assert pipeline is not None
        first_destination = pipeline.destination
        first = first_destination.config_params["credentials"]["global_config"][
            "max_temp_directory_size"
        ]
        (settings.dlt_pipelines_dir / "additional-working-bytes").write_bytes(b"x" * 1024**2)
        expected = f"{remaining_temp_bytes(settings)}B"
        _run_resource(runtime, market_resource([], materialize_only=True))
        assert runtime.pipeline is pipeline
        assert pipeline.destination is not first_destination
        second = pipeline.destination.config_params["credentials"]["global_config"][
            "max_temp_directory_size"
        ]
        assert second == expected
        assert int(second[:-1]) < int(first[:-1])
