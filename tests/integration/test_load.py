"""Capture then load into a real DuckDB warehouse through dlt (no network)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from decimal import Decimal
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
from oddsfox_catalogue.load.source import (
    QUARANTINE_COLUMNS,
    event_columns,
    event_resource,
    make_pipeline,
    market_columns,
    market_resource,
)
from oddsfox_catalogue.load.storage import (
    EVENT_FIELDS,
    MARKET_FIELDS,
    ensure_observation_views,
    stored_rows,
)
from oddsfox_catalogue.normalization import normalize_event, normalize_market

TESTS_DIR = Path(__file__).resolve().parents[1]
LOAD_CHILD = TESTS_DIR / "fakes" / "load_child.py"


def _materialize_views(settings):
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        ensure_observation_views(connection, settings.load.dataset_name)


def _normalized(table):
    if table == "event_observations":
        return normalize_event(make_event("1", "Event"))
    return normalize_market(
        make_market("1", "Market"),
        {
            "observation_id": "obs",
            "page_id": "page",
            "capture_id": "batch",
            "received_at": FIXED_NOW.isoformat(),
            "source_kind": "market_direct",
            "json_pointer": "/markets/0",
            "payload_sha256": "hash",
        },
    )


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


def _bronze_semantics(settings):
    from oddsfox_catalogue.fingerprints import semantic_fingerprint
    from oddsfox_catalogue.semantics import semantic_query

    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        return {
            table: semantic_fingerprint(connection, semantic_query(connection, f"bronze.{table}"))
            for table in (
                "event_observations",
                "market_observations",
                "quarantined_records",
                "batch_registry",
            )
        }


def _parquet_codecs(info, table):
    import pyarrow.parquet as pq

    jobs = {}
    for package in info.load_packages:
        for job in package.jobs["completed_jobs"]:
            if job.job_file_info.table_name == table and job.job_file_info.file_format == "parquet":
                path = Path(job.file_path)
                metadata = pq.read_metadata(path)
                jobs[path] = {
                    metadata.row_group(group).column(column).compression
                    for group in range(metadata.num_row_groups)
                    for column in range(metadata.num_columns)
                }
    assert jobs, "the exercised table must retain a Parquet job"
    return jobs


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


def test_adaptive_parquet_load_recovers_committed_parent_resource(tmp_path, monkeypatch):
    from dataclasses import replace
    from shutil import copytree

    from fakes.world import World
    from oddsfox_catalogue.load import runner

    world = World()
    for index in (1, 2):
        event = make_event(str(index), f"Event {index}")
        event["markets"] = [
            make_market(str(index + 3), f"Market {index}", event_stub=event_stub(event))
        ]
        world.add_event(event)
    fake = FakeGamma(world)
    fake.nested_overrides["5"] = "not-an-object"
    capture, _ = build_runtime(tmp_path / "capture", fake)
    try:
        batch_id = run_capture(capture, "bootstrap").batch_id
        pages = capture.ledger.pending_load_pages(batch_id)
        page_ids = tuple(page["page_id"] for page in pages)
        assert 3 <= len(pages) <= 200
    finally:
        capture.client.close()
        capture.ledger.close()
    for name in ("reference", "adaptive"):
        copytree(tmp_path / "capture", tmp_path / name)
    options = {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "1.0"}
    reference = make_settings(tmp_path / "reference", options)
    reference = replace(reference, load=replace(reference.load, max_pages_per_run=1))
    adaptive = make_settings(tmp_path / "adaptive", options)
    expected = _load(reference)
    assert expected.quarantined > 0

    original_chunk = runner._load_chunk
    original_write = runner._run_resource
    original_preflight = runner._preflight_load_bytes
    attempts, owners, current_part = [], [], ()
    reject_market, rejected = False, False

    def load_chunk(rt, chunk, summary):
        nonlocal current_part
        current_part = tuple(page["page_id"] for page in chunk)
        attempts.append(current_part)
        return original_chunk(rt, chunk, summary)

    def write(rt, resource):
        nonlocal reject_market
        assert rt.connection is not None
        owners.append(rt.connection)
        info = original_write(rt, resource)
        if (
            resource.name == "event_observations_values"
            and current_part == page_ids
            and not rejected
        ):
            formats = {
                job.job_file_info.file_format
                for package in info.load_packages
                for job in package.jobs["completed_jobs"]
                if job.job_file_info.table_name == "event_observations_values"
            }
            assert "parquet" in formats and "insert_values" not in formats
            with rt.connection.cursor() as connection:
                assert (
                    connection.execute("SELECT count(*) FROM bronze.event_observations").fetchone()[
                        0
                    ]
                    > 0
                )
                registry_exists = connection.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema='bronze' AND table_name='batch_registry'"
                ).fetchone()[0]
                if registry_exists:
                    assert (
                        connection.execute("SELECT count(*) FROM bronze.batch_registry").fetchone()[
                            0
                        ]
                        == 0
                    )
            assert len(rt.ledger.pending_load_pages(batch_id)) == len(pages)
            reject_market = True
        return info

    def preflight(rt, size):
        nonlocal reject_market, rejected
        if reject_market:
            reject_market, rejected = False, True
            # Simulate growth between committed resources; use the actual estimate guard.
            assert rt.storage_snapshot is not None
            rt.storage_snapshot = (rt.storage_snapshot[0], rt.settings.capture.max_temp_bytes)
            with pytest.raises(runner.LoadReservationExceeded) as failure:
                original_preflight(rt, size)
            raise failure.value
        return original_preflight(rt, size)

    monkeypatch.setattr(runner, "_load_chunk", load_chunk)
    monkeypatch.setattr(runner, "_run_resource", write)
    monkeypatch.setattr(runner, "_preflight_load_bytes", preflight)
    recovered = _load(adaptive)
    assert rejected
    assert len({id(owner) for owner in owners}) == 1
    with pytest.raises(duckdb.ConnectionException, match="already closed"):
        owners[0].execute("SELECT 1")
    assert attempts == [page_ids, page_ids[: len(pages) // 2], page_ids[len(pages) // 2 :]]
    for field in ("pages_loaded", "event_rows", "market_rows", "quarantined", "batches_registered"):
        assert getattr(recovered, field) == getattr(expected, field)

    def page_counts(settings):
        with Ledger(settings.ledger_path) as ledger:
            assert not ledger.pending_load_pages(batch_id)
            assert ledger.get_batch(batch_id)["status"] == "loaded"
            return sorted(
                (page["page_id"], page["event_rows"], page["market_rows"], page["quarantine_rows"])
                for page in ledger.pages_for_batch(batch_id)
            )

    baseline = _bronze_semantics(reference)
    assert _bronze_semantics(adaptive) == baseline
    assert page_counts(adaptive) == page_counts(reference)
    assert _registry_rows(adaptive) == [(batch_id, "loaded")]
    assert _load(adaptive).pages_loaded == 0
    with Ledger(adaptive.ledger_path) as ledger, ledger.transaction() as connection:
        connection.execute("UPDATE pages SET loaded_at = NULL, dlt_load_id = NULL")
    replay = _load(adaptive)
    assert replay.pages_loaded == expected.pages_loaded
    assert _bronze_semantics(adaptive) == baseline
    assert page_counts(adaptive) == page_counts(reference)


def test_market_merge_sql_failure_rolls_back_and_retries_with_same_owner(tmp_path, monkeypatch):
    from shutil import copytree

    from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

    from oddsfox_catalogue.load import runner

    batch_id = _capture(tmp_path / "capture")
    for name in ("reference", "retry"):
        copytree(tmp_path / "capture", tmp_path / name)
    reference = make_settings(tmp_path / "reference")
    retry = make_settings(tmp_path / "retry")
    expected = _load(reference)
    expected_semantics = _bronze_semantics(reference)
    original_many = DuckDbSqlClient.execute_many
    original_sql = DuckDbSqlClient.execute_sql
    failed, rollback_observed = False, False
    owners = []
    original_resource = runner._run_resource

    def resource(rt, item):
        owners.append(rt.connection)
        return original_resource(rt, item)

    def execute_many(self, statements, *args, **kwargs):
        nonlocal failed, rollback_observed
        rendered = " ".join(statements)
        target_merge = (
            '"bronze"."market_observations_values"' in rendered and "MERGE INTO" in rendered
        )
        if target_merge and not failed:
            # dlt wraps this physical merge in a transaction; fail after writing its rows.
            result = original_many(self, statements, *args, **kwargs)
            assert (
                self.native_connection.execute(
                    "SELECT count(*) FROM bronze.market_observations_values"
                ).fetchone()[0]
                > 0
            )
            assert (
                self.native_connection.execute(
                    "SELECT count(*) FROM bronze.event_observations"
                ).fetchone()[0]
                > 0
            )
            failed = True
            original_sql(self, "SELEC synthetic_transaction_failure")
            return result
        if target_merge and failed and not rollback_observed:
            assert self.native_connection.execute(
                "SELECT count(*) FROM bronze.market_observations_values"
            ).fetchone() == (0,)
            rollback_observed = True
        return original_many(self, statements, *args, **kwargs)

    monkeypatch.setattr(runner, "_run_resource", resource)
    monkeypatch.setattr(DuckDbSqlClient, "execute_many", execute_many)
    recovered = _load(retry)
    assert failed and rollback_observed
    assert len({id(owner) for owner in owners}) == 1
    assert recovered.batches_registered == [batch_id]
    assert (
        recovered.pages_loaded,
        recovered.event_rows,
        recovered.market_rows,
        recovered.quarantined,
    ) == (expected.pages_loaded, expected.event_rows, expected.market_rows, expected.quarantined)
    assert _bronze_semantics(retry) == expected_semantics
    assert _load(retry).pages_loaded == 0


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


@pytest.mark.parametrize("changed_record", [False, True])
def test_committed_owner_wal_survives_process_crash_and_resume_checks_conflicts(
    tmp_path, changed_record
):
    from hashlib import sha256
    from shutil import copytree

    batch_id = _capture(tmp_path / "capture")
    for name in ("reference", "crashed"):
        copytree(tmp_path / "capture", tmp_path / name)
    reference = make_settings(tmp_path / "reference")
    expected = _load(reference)
    expected_semantics = _bronze_semantics(reference)
    settings = make_settings(tmp_path / "crashed")
    raw_hashes = {
        path: sha256(path.read_bytes()).hexdigest()
        for path in settings.raw_dir.rglob("*")
        if path.is_file()
    }
    code = r"""
import os, socket, sys
from pathlib import Path
def offline(*args, **kwargs):
    raise AssertionError('crash fixture is offline')
socket.socket.connect = socket.create_connection = offline
from fakes.harness import FIXED_NOW, make_settings
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.load import runner
settings = make_settings(Path(sys.argv[1]))
original = runner._run_resource
def committed_parent(rt, resource):
    result = original(rt, resource)
    if resource.name == 'event_observations_values':
        assert rt.connection.execute('SELECT count(*) FROM bronze.event_observations').fetchone()[0] > 0
        wal = Path(str(settings.warehouse_path)+'.wal')
        assert wal.is_file() and wal.stat().st_size > 0, 'owner must retain committed WAL before crash'
        assert rt.ledger.pending_load_pages(), 'page checkpoint must still be pending'
        os._exit(CRASH_EXIT_CODE)
    return result
runner._run_resource = committed_parent
with Ledger(settings.ledger_path) as ledger:
    runner.load_pending(runner.LoadRuntime(settings, ledger, now=lambda: FIXED_NOW))
raise AssertionError('crash hook not reached')
"""
    crashed = subprocess.run(
        [sys.executable, "-c", code, str(settings.root)],
        env={**os.environ, "PYTHONPATH": str(TESTS_DIR), "RUNTIME__DLTHUB_TELEMETRY": "false"},
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr
    wal = Path(str(settings.warehouse_path) + ".wal")
    assert wal.stat().st_size > 0
    assert len(sha256(wal.read_bytes()).hexdigest()) == 64
    with Ledger(settings.ledger_path) as ledger:
        assert len(ledger.pending_load_pages(batch_id)) == len(ledger.pages_for_batch(batch_id))
    if changed_record:
        # A writable reopen recovers the retained WAL before simulating changed persisted evidence.
        with duckdb.connect(str(settings.warehouse_path)) as connection:
            connection.execute(
                "UPDATE bronze.event_observations_values SET n_title='null' WHERE entity_id='101'"
            )
        with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
            _load(settings)
        assert _registry_rows(settings) == []
        assert _batch_status(settings, batch_id) == "captured"
        with Ledger(settings.ledger_path) as ledger:
            assert len(ledger.pending_load_pages(batch_id)) == len(ledger.pages_for_batch(batch_id))
    else:
        recovered = _load(settings)
        assert recovered.batches_registered == [batch_id]
        assert recovered.pages_loaded == expected.pages_loaded
        assert _bronze_semantics(settings) == expected_semantics
        assert not wal.exists(), "normal final close checkpoints the retained WAL"
        assert _load(settings).pages_loaded == 0
    assert {path: sha256(path.read_bytes()).hexdigest() for path in raw_hashes} == raw_hashes


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


@pytest.mark.parametrize("old_format", ["insert_values", "parquet_snappy"])
def test_parquet_recovers_unacknowledged_old_format_commit(
    tmp_path: Path, monkeypatch, old_format
) -> None:
    from hashlib import sha256
    from shutil import copytree

    from dlt.common.configuration.container import Container
    from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
    from dlt.pipeline.pipeline import Pipeline

    from oddsfox_catalogue.load.runtime import CatalogueRunContext

    batch_id = _capture(tmp_path / "capture")
    settings = make_settings(tmp_path / "capture")
    copytree(settings.root, tmp_path / "reference")
    reference = make_settings(tmp_path / "reference")
    _load(reference)
    expected = _bronze_semantics(reference)
    original_run = Pipeline.run
    original_providers = CatalogueRunContext.initial_providers
    old_jobs = {}

    def snappy_providers(self):
        providers = original_providers(self)
        providers[0].set_value("compression", "snappy", None, "data_writer")
        return providers

    def old_format_then_stop(self, *args, **kwargs):
        kwargs["loader_file_format"] = "parquet" if old_format == "parquet_snappy" else old_format
        info = original_run(self, *args, **kwargs)
        if old_format == "parquet_snappy":
            jobs = _parquet_codecs(info, "event_observations_values")
            assert all(codecs == {"SNAPPY"} for codecs in jobs.values())
            old_jobs.update({path: sha256(path.read_bytes()).hexdigest() for path in jobs})
        raise RuntimeError("stop after old-format resource commit")

    with monkeypatch.context() as changed:
        changed.setattr(Pipeline, "run", old_format_then_stop)
        if old_format == "parquet_snappy":
            changed.setattr(CatalogueRunContext, "initial_providers", snappy_providers)
        with pytest.raises(RuntimeError, match="old-format resource commit"):
            _load(settings)
    # Replace process configuration as a restarted operator would, retaining all jobs.
    Container()[PluggableRunContext] = PluggableRunContext(
        CatalogueRunContext(settings.dlt_pipelines_dir)
    )
    _materialize_views(settings)
    committed = _table_count(settings, "event_observations")
    assert committed > 0
    assert _registry_rows(settings) == []
    with Ledger(settings.ledger_path) as ledger:
        assert len(ledger.pending_load_pages(batch_id)) == len(ledger.pages_for_batch(batch_id))
    new_codecs = []

    def current_format_run(self, resource, *args, **kwargs):
        info = original_run(self, resource, *args, **kwargs)
        if resource.name in ("event_observations_values", "market_observations_values"):
            new_codecs.extend(_parquet_codecs(info, resource.name).values())
        return info

    monkeypatch.setattr(Pipeline, "run", current_format_run)
    recovered = _load(settings)
    assert recovered.batches_registered == [batch_id]
    assert _table_count(settings, "event_observations") == committed
    assert _distinct_observations(settings, "event_observations") == committed
    assert _registry_rows(settings) == [(batch_id, "loaded")]
    assert _bronze_semantics(settings) == expected
    with Ledger(settings.ledger_path) as ledger, ledger.transaction() as connection:
        connection.execute("UPDATE pages SET loaded_at = NULL, dlt_load_id = NULL")
    replay = _load(settings)
    assert replay.pages_loaded == recovered.pages_loaded
    assert _bronze_semantics(settings) == expected
    assert new_codecs and all(codecs == {"ZSTD"} for codecs in new_codecs)
    assert {path: sha256(path.read_bytes()).hexdigest() for path in old_jobs} == old_jobs


@pytest.mark.parametrize("file_format", ["insert_values", "parquet"])
def test_schema_drift_in_envelope_is_rejected(tmp_path: Path, file_format) -> None:
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
            "normalized": _normalized("event_observations"),
            "unexpected_new_column": "boom",
        }
    ]

    with pytest.raises(PipelineStepFailed):
        pipeline.run([event_resource(rows)], loader_file_format=file_format)

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


@pytest.mark.parametrize(
    "table,resource",
    [("event_observations", event_resource), ("market_observations", market_resource)],
)
@pytest.mark.parametrize("file_format", ["insert_values", "parquet"])
def test_insert_only_merge_keys_on_observation_id_not_payload(
    tmp_path: Path, table, resource, file_format
) -> None:
    """Regression: two observations with identical payloads must both be kept.

    Shared column-hint dicts once made dlt merge on payload_hash, which dropped the second
    observation of any unchanged record.
    """
    from datetime import datetime

    settings = make_settings(tmp_path)
    pipeline = make_pipeline(settings.warehouse_path, settings.dlt_pipelines_dir, settings.load)
    observed = datetime(2026, 10, 8, 6, 0, tzinfo=datetime.now().astimezone().tzinfo)

    def row(observation_id: str) -> dict:
        result = {
            "observation_id": observation_id,
            "venue": "polymarket",
            "entity_id": "1",
            "batch_id": "b",
            "page_id": observation_id,
            "observed_at": observed,
            "source_updated_at": None,
            "endpoint": "/events/keyset",
            "payload_hash": "same-hash",
            "normalized": _normalized(table),
        }
        if table == "market_observations":
            result.update(source_kind="market_direct", json_pointer="/markets/0")
        return result

    for observation_id in ["obs-a", "obs-b"]:
        pipeline.run(resource([row(observation_id)]), loader_file_format=file_format)
    _materialize_views(settings)

    assert _query(settings, f"SELECT COUNT(*) FROM bronze.{table}") == 2
    assert _query(settings, f"SELECT COUNT(DISTINCT payload_hash) FROM bronze.{table}") == 1


def test_replay_blocks_conflicting_persisted_normalization(tmp_path: Path) -> None:
    """An existing key cannot silently swallow a changed semantic projection."""
    _capture(tmp_path)
    settings = make_settings(tmp_path)
    _load(settings)
    connection = duckdb.connect(str(settings.warehouse_path))
    try:
        connection.execute(
            "UPDATE bronze.event_observations_values SET n_title = 'null' WHERE entity_id = '101'"
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
            "UPDATE bronze.event_observations_values SET observed_at = observed_at + INTERVAL '1 second' WHERE entity_id = '101'"
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


@pytest.mark.parametrize("file_format", ["insert_values", "parquet"])
def test_empty_materialization_keeps_insert_only_merge_on_next_real_record(
    tmp_path: Path, file_format
) -> None:
    """An empty first capture has canonical schemas without inventing an evidence row."""
    from oddsfox_catalogue.load.rows import PageContext, rows_for_page
    from oddsfox_catalogue.load.source import market_resource, quarantine_resource

    settings = make_settings(tmp_path)
    pipeline = make_pipeline(settings.warehouse_path, settings.dlt_pipelines_dir, settings.load)
    for factory in (event_resource, market_resource, quarantine_resource):
        pipeline.run(factory([], materialize_only=True), loader_file_format=file_format)
    _materialize_views(settings)
    for table in ("event_observations", "market_observations", "quarantined_records"):
        assert _table_count(settings, table) == 0
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        for table, expected in (
            ("event_observations", event_columns()),
            ("market_observations", market_columns()),
            ("quarantined_records", QUARANTINE_COLUMNS),
        ):
            actual = {
                row[0]
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='bronze' AND table_name=?",
                    [table],
                ).fetchall()
            }
            assert actual - {name for name in actual if name.startswith("_dlt_")} == set(expected)
            assert ("payload" in actual) == (table == "quarantined_records")
    context = PageContext("page", "batch", "/events", FIXED_NOW, "events")
    rows = rows_for_page(
        context,
        [
            make_event("1", "First"),
            {
                "title": "Missing ID",
                "unmodeled_source_field": {
                    "exact": Decimal("0.12345678901234567890123456789"),
                    "nullable": None,
                },
            },
        ],
    )
    for _ in range(2):
        pipeline.run(event_resource(rows.events), loader_file_format=file_format)
        pipeline.run(quarantine_resource(rows.quarantine), loader_file_format=file_format)
    assert _table_count(settings, "event_observations") == 1
    assert _table_count(settings, "quarantined_records") == 1
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT json_extract_string(payload, '$.unmodeled_source_field.exact'), "
            "json_type(payload, '$.unmodeled_source_field.nullable') FROM bronze.quarantined_records"
        ).fetchone() == ("0.12345678901234567890123456789", "NULL")


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
        rows, _, sizes = _rows_for_chunk(
            load_runtime, runtime.ledger.pending_load_pages(captured.batch_id)
        )
        encoded = sum(
            _encoded_row_bytes(row)
            for relation in (
                stored_rows(rows.events, EVENT_FIELDS),
                stored_rows(rows.markets, MARKET_FIELDS),
                rows.quarantine,
            )
            for row in relation
        )
        assert sum(sizes) == encoded
        before = retained_bytes(runtime.settings)
        loaded = load_pending(load_runtime)
        after = retained_bytes(runtime.settings)
        raw_bytes = sum(
            json.loads(path.read_bytes())["body_bytes"]
            for path in runtime.settings.raw_dir.glob("**/p*.manifest.json")
        )
        assert raw_bytes > 1024**2, "the retained full source must remain byte-heavy"
        assert encoded < raw_bytes // 10, (
            "unmodeled raw fields must not be duplicated into dlt rows"
        )
        assert all("payload" not in row for row in (*rows.events, *rows.markets))
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


@pytest.mark.parametrize(
    "table,factory,columns",
    [
        ("event_observations", event_resource, event_columns),
        ("market_observations", market_resource, market_columns),
    ],
)
def test_native_parquet_preserves_json_and_nullable_timestamp_schema(
    tmp_path: Path, table, factory, columns
) -> None:
    from datetime import timedelta, timezone

    from oddsfox_catalogue.load.runner import _run_resource
    from oddsfox_catalogue.load.runtime import confined_runtime

    settings = make_settings(tmp_path)
    received = FIXED_NOW.replace(microsecond=123456).astimezone(
        timezone(timedelta(hours=5, minutes=30))
    )
    nested = {
        "exact": "0.12345678901234567890123456789",
        "nullable": None,
        "active": None,
        "text": '雪\\\n"' * 32768,
        "labels": ["Yes", "No"],
    }
    payload = _normalized(table)
    payload.update(active=None, tags=[nested], description=nested["text"])
    row = {
        "observation_id": "first",
        "venue": "polymarket",
        "entity_id": "1",
        "batch_id": "batch",
        "page_id": "page",
        "observed_at": received,
        "source_updated_at": None,
        "endpoint": "/markets" if table == "market_observations" else "/events",
        "payload_hash": "same-hash",
        "normalized": payload,
    }
    if table == "market_observations":
        row.update(source_kind="market_direct", json_pointer="/markets/0")
    original = deepcopy(row)
    with Ledger(settings.ledger_path) as ledger, confined_runtime(settings.dlt_pipelines_dir):
        runtime = LoadRuntime(settings, ledger)
        _run_resource(runtime, factory([], materialize_only=True))
        first = _run_resource(runtime, factory([row]))
        formats = {
            job.job_file_info.file_format
            for package in first.load_packages
            for job in package.jobs["completed_jobs"]
        }
        assert "parquet" in formats
        assert "insert_values" not in formats
        assert all(
            codecs == {"ZSTD"} for codecs in _parquet_codecs(first, table + "_values").values()
        )
        _run_resource(runtime, factory([row]))
        _run_resource(
            runtime,
            factory([{**row, "observation_id": "second", "source_updated_at": received}]),
        )
    assert row == original and payload == original["normalized"]
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        schema = {
            name: (kind, nullable)
            for name, kind, nullable in connection.execute(
                "SELECT column_name,data_type,is_nullable FROM information_schema.columns "
                "WHERE table_schema='bronze' AND table_name=?",
                [table],
            ).fetchall()
            if not name.startswith("_dlt_")
        }
        types = {"text": "VARCHAR", "timestamp": "TIMESTAMP WITH TIME ZONE", "json": "JSON"}
        assert schema == {
            name: (types[spec["data_type"]], "YES") for name, spec in columns().items()
        }
        actual = connection.execute(
            f"SELECT observation_id,observed_at,source_updated_at,normalized "
            f"FROM bronze.{table} ORDER BY observation_id"
        ).fetchall()
        assert len(actual) == 2
        assert actual[0][:3] == ("first", received, None)
        assert actual[1][:3] == ("second", received, received)
        assert [json.loads(values[3]) for values in actual] == [payload, payload]
