"""Empty resources skip native writes only when schema, target and recovery state agree."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import World, make_event, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.limits import retained_bytes
from oddsfox_catalogue.load import runner
from oddsfox_catalogue.load.rows import PageContext, RowSet, rows_for_page
from oddsfox_catalogue.load.runtime import confined_runtime
from oddsfox_catalogue.load.source import (
    QUARANTINE_COLUMNS,
    event_resource,
    market_resource,
    quarantine_resource,
)
from oddsfox_catalogue.semantics import bounded_connection
from oddsfox_catalogue.warehouse_version import ensure_warehouse_contract

pytestmark = pytest.mark.timeout(60)


@pytest.fixture
def native(tmp_path):
    settings = make_settings(
        tmp_path,
        {
            "CATALOGUE_LOAD_DUCKDB_MEMORY_LIMIT": "32MB",
            "CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(128 * 1024**2),
            "CATALOGUE_CAPTURE_MAX_RETAINED_BYTES": str(128 * 1024**2),
            "CATALOGUE_LOAD_MAX_PAGES_PER_RUN": "1",
        },
    )
    with Ledger(settings.ledger_path) as ledger, confined_runtime(settings.dlt_pipelines_dir):
        ensure_warehouse_contract(settings, create=True)
        rt = runner.LoadRuntime(settings, ledger, now=lambda: FIXED_NOW)
        for factory in (event_resource, market_resource, quarantine_resource):
            runner._run_resource(rt, factory([], materialize_only=True))
        with bounded_connection(settings, read_only=False) as connection:
            rt.connection = connection
            yield rt
            rt.connection = None


def test_real_native_empty_tables_are_ready_and_preserve_schema(native):
    before = deepcopy(native.pipeline.default_schema.to_dict())
    assert list(runner._chunk_resources(native, RowSet())) == [None, None, None]
    assert native.pipeline.default_schema.to_dict() == before
    assert native.connection.execute(
        "SELECT table_name,table_type FROM information_schema.tables "
        "WHERE table_schema='bronze' AND table_name IN "
        "('event_observations','market_observations','quarantined_records') ORDER BY table_name"
    ).fetchall() == [
        ("event_observations", "VIEW"),
        ("market_observations", "VIEW"),
        ("quarantined_records", "BASE TABLE"),
    ]


@pytest.mark.parametrize("disk_state", ["missing", "corrupt"])
def test_cached_native_schema_cannot_hide_missing_or_corrupt_persisted_state(native, disk_state):
    schema = native.pipeline.default_schema
    name = native.pipeline.default_schema_name
    path = Path(native.pipeline.schemas.config.schema_volume_path) / f"{name}.schema.json"
    assert path.is_file() and not schema.is_modified
    # Warm the live storage cache before an independent disk change.
    assert name in native.pipeline.schemas
    assert runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)
    if disk_state == "missing":
        path.unlink()
        assert not native.pipeline.schemas.has_schema(name)
        assert not runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)
    else:
        path.write_bytes(b"{corrupt persisted native schema")
        with pytest.raises((runner.LoadBlocked, ValueError)):
            runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)


@pytest.mark.parametrize("state", ["new", "modified", "missing-table", "missing-schema"])
def test_unready_native_schema_never_skips_materialization(native, state):
    schema = native.pipeline.default_schema
    if state == "missing-schema":
        native.pipeline.default_schema_name = "not-persisted"
    elif state == "missing-table":
        schema.tables.pop("quarantined_records")
        native.pipeline.schemas.save_schema(schema)
        assert not schema.is_modified
    elif state == "new":
        schema._stored_version_hash = None
        assert schema.is_new
    else:
        schema.tables["quarantined_records"]["columns"]["payload"]["nullable"] = True
        assert schema.is_modified
    assert not runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)
    assert list(runner._chunk_resources(native, RowSet()))[-1] is not None


@pytest.mark.parametrize("drift", ["missing", "extra", "type", "nullable"])
def test_clean_but_incompatible_native_schema_blocks_empty_skip(native, drift):
    schema = native.pipeline.default_schema
    columns = schema.tables["quarantined_records"]["columns"]
    if drift == "missing":
        columns.pop("payload")
    elif drift == "extra":
        columns["unexpected"] = {"name": "unexpected", "data_type": "text", "nullable": True}
    else:
        columns["payload"]["data_type" if drift == "type" else "nullable"] = (
            "text" if drift == "type" else True
        )
    native.pipeline.schemas.save_schema(schema)
    assert not schema.is_new and not schema.is_modified
    with pytest.raises(runner.LoadBlocked, match="native schema"):
        runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)


@pytest.mark.parametrize(
    "drift", ["missing", "extra", "type", "timestamp", "nullable", "view", "absent"]
)
def test_physical_schema_drift_cannot_be_acknowledged_by_empty_skip(native, drift):
    if drift == "missing":
        sql = "ALTER TABLE bronze.quarantined_records DROP COLUMN payload"
    elif drift == "extra":
        sql = "ALTER TABLE bronze.quarantined_records ADD COLUMN unexpected VARCHAR"
    elif drift == "type":
        sql = "ALTER TABLE bronze.quarantined_records ALTER COLUMN payload TYPE VARCHAR"
    elif drift == "timestamp":
        sql = "ALTER TABLE bronze.quarantined_records ALTER COLUMN observed_at TYPE TIMESTAMP"
    elif drift == "nullable":
        sql = "ALTER TABLE bronze.quarantined_records ALTER COLUMN payload DROP NOT NULL"
    else:
        sql = "DROP TABLE bronze.quarantined_records"
    native.connection.execute(sql)
    if drift == "view":
        native.connection.execute("CREATE VIEW bronze.quarantined_records AS SELECT 1 AS wrong")
    if drift == "absent":
        assert not runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)
        assert list(runner._chunk_resources(native, RowSet()))[-1] is not None
    else:
        with pytest.raises(runner.LoadBlocked, match="physical"):
            runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)


@pytest.mark.parametrize("stage", ["extracted", "normalized"])
def test_real_pending_package_forces_native_recovery_before_empty_skip(native, stage):
    native.pipeline.extract(event_resource([], materialize_only=True))
    if stage == "normalized":
        native.pipeline.normalize()
        assert native.pipeline.list_normalized_load_packages()
    else:
        assert native.pipeline.list_extracted_load_packages()
    assert not runner._empty_table_ready(native, "quarantined_records", QUARANTINE_COLUMNS)
    first = next(runner._chunk_resources(native, RowSet()))
    assert first is not None
    runner._run_resource(native, first)
    assert not native.pipeline.list_extracted_load_packages()
    assert not native.pipeline.list_normalized_load_packages()
    assert list(runner._chunk_resources(native, RowSet())) == [None, None, None]
    assert native.connection.execute(
        "SELECT count(*) FROM bronze.event_observations"
    ).fetchone() == (0,)


def _empty_capture(rt):
    capture, _ = build_runtime(rt.settings.root, FakeGamma(World()))
    try:
        result = run_capture(capture, "selected", market_ids=["11", "12"])
        assert result.status == "captured"
        pages = rt.ledger.pending_load_pages(result.batch_id)
        assert len(pages) == 2 and all(page["http_status"] == 404 for page in pages)
        return result.batch_id, pages
    finally:
        capture.client.close()
        capture.ledger.close()


def test_ready_empty_chunk_uses_null_load_id_and_recovers_unacknowledged_checkpoint(
    native, monkeypatch
):
    _, pages = _empty_capture(native)
    original = native.ledger.complete_load_chunk

    def interrupted(**kwargs):
        assert kwargs["load_id"] is None and kwargs["quarantine"] == []
        assert set(kwargs["page_counts"].values()) == {(0, 0, 0)}
        raise RuntimeError("before durable checkpoint")

    monkeypatch.setattr(native.ledger, "complete_load_chunk", interrupted)
    monkeypatch.setattr(
        runner,
        "_run_resource",
        lambda *args: pytest.fail("ready empty resource wrote a new native load"),
    )
    with pytest.raises(RuntimeError, match="before durable checkpoint"):
        runner._load_chunk(native, pages, runner.LoadSummary())
    assert all(
        page["loaded_at"] is None for page in native.ledger.pages_for_batch(pages[0]["batch_id"])
    )
    monkeypatch.setattr(native.ledger, "complete_load_chunk", original)
    summary = runner.LoadSummary()
    runner._load_chunk(native, pages, summary)
    assert summary.pages_loaded == 2 and summary.load_ids == []
    assert (summary.event_rows, summary.market_rows, summary.quarantined) == (0, 0, 0)
    assert all(
        page["loaded_at"] and page["dlt_load_id"] is None
        for page in native.ledger.pages_for_batch(pages[0]["batch_id"])
    )


@pytest.mark.parametrize("allowance", ["temporary", "retained"])
def test_ready_empty_resources_still_obey_admission_and_keep_pages_pending(
    native, monkeypatch, allowance
):
    _, pages = _empty_capture(native)
    original = native.settings
    cap = (
        {"max_temp_bytes": runner.LOAD_FIXED_RESERVE - 1}
        if allowance == "temporary"
        else {"max_retained_bytes": retained_bytes(original) + runner.LOAD_FIXED_RESERVE - 1}
    )
    native.settings = replace(original, capture=replace(original.capture, **cap))
    monkeypatch.setattr(
        runner,
        "_run_resource",
        lambda *args: pytest.fail("exhausted empty chunk reached a native write"),
    )
    with pytest.raises(runner.LoadReservationExceeded):
        runner._load_chunk(native, pages, runner.LoadSummary())
    assert all(
        page["loaded_at"] is None for page in native.ledger.pages_for_batch(pages[0]["batch_id"])
    )
    native.settings = original
    summary = runner.LoadSummary()
    runner._load_chunk(native, pages, summary)
    assert summary.pages_loaded == 2 and not summary.load_ids


def test_nonempty_resources_always_run_even_when_the_targets_are_ready(native, monkeypatch):
    market = make_market("21", "Market?")
    rows = rows_for_page(
        PageContext("page", "batch", "/events/keyset", FIXED_NOW, "events"),
        [make_event("1", "Event", markets=[market]), {"title": "Missing id"}],
    )
    assert tuple(map(len, (rows.events, rows.markets, rows.quarantine))) == (1, 1, 1)
    monkeypatch.setattr(
        runner,
        "_empty_table_ready",
        lambda *args: pytest.fail("nonempty data consulted an empty-resource shortcut"),
    )
    resources = runner._chunk_resources(native, rows)
    for table in ("event_observations", "market_observations", "quarantined_records"):
        resource = next(resources)
        assert resource is not None
        info = runner._run_resource(native, resource)
        assert info.loads_ids
        assert native.connection.execute(f"SELECT count(*) FROM bronze.{table}").fetchone() == (1,)
    with pytest.raises(StopIteration):
        next(resources)


@pytest.mark.parametrize("view_state", ["missing", "altered"])
def test_actual_load_entrypoint_repairs_missing_views_and_rejects_altered_views(native, view_state):
    batch_id, pages = _empty_capture(native)
    native.connection.execute("DROP VIEW bronze.event_observations")
    if view_state == "altered":
        native.connection.execute("CREATE VIEW bronze.event_observations AS SELECT 1 AS invalid")
    native.connection.close()
    native.connection = None
    if view_state == "altered":
        with pytest.raises(ValueError, match="view does not match warehouse contract"):
            runner.load_pending(native, batch_id)
        assert all(page["loaded_at"] is None for page in native.ledger.pages_for_batch(batch_id))
    else:
        summary = runner.load_pending(native, batch_id)
        assert summary.pages_loaded == len(pages) and summary.batches_registered == [batch_id]
        assert all(page["dlt_load_id"] is None for page in native.ledger.pages_for_batch(batch_id))
        with bounded_connection(native.settings) as connection:
            assert connection.execute(
                "SELECT count(*) FROM bronze.event_observations"
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT count(*) FROM bronze.quarantined_records"
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT page_count,event_observation_count,market_observation_count,"
                "quarantine_count,load_id FROM bronze.batch_registry WHERE batch_id=?",
                [batch_id],
            ).fetchone() == (2, 0, 0, 0, None)
