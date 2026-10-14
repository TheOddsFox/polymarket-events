"""Physical rows are encoded once and checked against all retained evidence."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import duckdb
import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import World, make_event, make_market
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.load import runner, storage
from oddsfox_catalogue.load.rows import PageContext, RowSet, rows_for_page
from oddsfox_catalogue.load.source import event_columns, market_columns, stored_columns


@pytest.fixture(params=["events", "markets"])
def physical_case(request):
    entity = request.param
    logical = rows_for_page(
        PageContext("page", "batch", "/" + entity, FIXED_NOW, entity),
        [{"id": "1", "description": "雪", "closed": None}],
    )
    fields = storage.EVENT_FIELDS if entity == "events" else storage.MARKET_FIELDS
    row = storage.stored_row(getattr(logical, entity)[0], fields)
    return entity, fields, row


@pytest.fixture
def historical_guard(tmp_path, monkeypatch, physical_case):
    entity, fields, row = physical_case
    columns = event_columns() if entity == "events" else market_columns()
    table = entity[:-1] + "_observations_values"
    connection = duckdb.connect(config={"threads": 1})
    connection.execute("CREATE SCHEMA bronze")
    types = {"text": "VARCHAR", "timestamp": "TIMESTAMPTZ"}
    declarations = ", ".join(
        f'"{name}" {types[hint["data_type"]]}'
        for name, hint in stored_columns(columns, fields).items()
    )
    connection.execute(f"CREATE TABLE bronze.{table} ({declarations})")
    parameters = ", ".join("?" for _ in row)
    connection.execute(f"INSERT INTO bronze.{table} VALUES ({parameters})", list(row.values()))
    storage.ensure_observation_views(connection, "bronze")
    path = tmp_path / "warehouse.duckdb"
    path.touch()
    runtime = SimpleNamespace(
        settings=SimpleNamespace(warehouse_path=path, load=SimpleNamespace(dataset_name="bronze")),
        connection=None,
    )
    queries = []

    class ObservedConnection:
        def execute(self, query, params=None):
            queries.append((query, params))
            return (
                connection.execute(query, params)
                if params is not None
                else connection.execute(query)
            )

    @contextmanager
    def bounded(settings, *, connection):
        yield ObservedConnection()

    monkeypatch.setattr(runner, "bounded_connection", bounded)
    try:
        yield runtime, connection, queries, table, RowSet(**{entity: [row]})
    finally:
        connection.close()


def test_historical_match_uses_one_bound_query(historical_guard):
    runtime, _, queries, table, rows = historical_guard
    runner._check_existing_observations(runtime, rows)
    matches = [(sql, params) for sql, params in queries if "WHERE observation_id IN" in sql]
    assert len(matches) == 1
    assert f"FROM bronze.{table}" in matches[0][0]
    incoming = rows.events or rows.markets
    assert matches[0][1] == [[incoming[0]["observation_id"]]]
    assert "normalized" not in matches[0][0]
    assert not any("LIMIT 1" in sql or "WHERE FALSE" in sql for sql, _ in queries)


@pytest.mark.parametrize(
    "field",
    ["venue", "entity_id", "batch_id", "page_id", "endpoint", "payload_hash", "n_description"],
)
def test_retained_evidence_conflicts_are_rejected(historical_guard, field):
    runtime, connection, _, table, rows = historical_guard
    value = '"different"' if field.startswith("n_") else "different"
    connection.execute(f'UPDATE bronze.{table} SET "{field}" = ?', [value])
    with pytest.raises(runner.LoadBlocked, match="conflicting persisted observation"):
        runner._check_existing_observations(runtime, rows)


def test_json_formatting_is_semantic_and_unicode_exact(historical_guard):
    runtime, connection, _, table, rows = historical_guard
    connection.execute(f"UPDATE bronze.{table} SET n_description = ?", [' "\\u96ea" '])
    runner._check_existing_observations(runtime, rows)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "{"])
def test_invalid_retained_json_is_rejected(historical_guard, value):
    runtime, connection, _, table, rows = historical_guard
    connection.execute(f"UPDATE bronze.{table} SET n_description = ?", [value])
    with pytest.raises((ValueError, runner.LoadBlocked)):
        runner._check_existing_observations(runtime, rows)


def test_no_match_still_binds_every_physical_column(historical_guard):
    runtime, connection, _, table, rows = historical_guard
    connection.execute(f"ALTER TABLE bronze.{table} DROP COLUMN payload_hash")
    (rows.events or rows.markets)[0]["observation_id"] = "new-id"
    with pytest.raises(duckdb.BinderException, match="payload_hash"):
        runner._check_existing_observations(runtime, rows)


def test_altered_logical_view_is_rejected(historical_guard):
    runtime, connection, _, table, rows = historical_guard
    logical = table.removesuffix("_values")
    connection.execute(f"CREATE OR REPLACE VIEW bronze.{logical} AS SELECT 1 AS invalid")
    with pytest.raises(ValueError, match="view does not match"):
        runner._check_existing_observations(runtime, rows)


def test_chunk_encodes_each_field_once_and_passes_exact_physical_rows(tmp_path, monkeypatch):
    world = World()
    world.add_event(make_event("1", "雪", markets=[make_market("2", "Market?")]))
    capture, _ = build_runtime(tmp_path, FakeGamma(world))
    try:
        summary = run_capture(capture, "selected", event_ids=["1"])
        runtime = runner.LoadRuntime(capture.settings, capture.ledger)
        pages = capture.ledger.pending_load_pages(summary.batch_id)
        calls = []
        original = storage.canonical_json

        def encode(value):
            calls.append(value)
            return original(value)

        monkeypatch.setattr(storage, "canonical_json", encode)
        rows, counts, sizes = runner._rows_for_chunk(runtime, pages)
        assert len(calls) == len(rows.events) * len(storage.EVENT_FIELDS) + len(rows.markets) * len(
            storage.MARKET_FIELDS
        )
        assert all("normalized" not in row for row in rows.events + rows.markets)
        expected = deepcopy(rows)
        resources = runner._chunk_resources(runtime, rows)
        for index, relation in enumerate((rows.events, rows.markets, rows.quarantine)):
            resource = next(resources)
            if relation:
                assert list(resource) == relation
            else:
                assert resource.compute_table_schema()["write_disposition"] == "append"
            assert sizes[index] == sum(runner._encoded_row_bytes(row) for row in relation)
        assert rows == expected
        assert len(calls) == len(rows.events) * len(storage.EVENT_FIELDS) + len(rows.markets) * len(
            storage.MARKET_FIELDS
        )
        assert sum(sum(count) for count in counts.values()) == sum(
            map(len, (rows.events, rows.markets, rows.quarantine))
        )
    finally:
        capture.ledger.close()


@pytest.mark.parametrize(
    "field", ["observed_at", "source_updated_at", "source_kind", "json_pointer"]
)
def test_remaining_provenance_fields_are_compared(historical_guard, field):
    runtime, connection, _, table, rows = historical_guard
    if field in {"source_kind", "json_pointer"} and rows.events:
        return
    value = FIXED_NOW + timedelta(seconds=1) if field.endswith("_at") else "changed"
    connection.execute(f'UPDATE bronze.{table} SET "{field}" = ?', [value])
    with pytest.raises(runner.LoadBlocked, match="conflicting persisted observation"):
        runner._check_existing_observations(runtime, rows)


def test_exact_json_numbers_compare_semantically_without_losing_precision(historical_guard):
    runtime, connection, _, table, rows = historical_guard
    incoming = (rows.events or rows.markets)[0]
    incoming["n_liquidity"] = "0.12345678901234567890123456789"
    connection.execute(
        f"UPDATE bronze.{table} SET n_liquidity = ?", ["0.123456789012345678901234567890"]
    )
    runner._check_existing_observations(runtime, rows)
    connection.execute(
        f"UPDATE bronze.{table} SET n_liquidity = ?", ["0.12345678901234567890123456788"]
    )
    with pytest.raises(runner.LoadBlocked, match="conflicting persisted observation"):
        runner._check_existing_observations(runtime, rows)


def test_reset_load_budget_always_measures_fresh_combined_usage(monkeypatch):
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            capture=SimpleNamespace(max_retained_bytes=100, max_temp_bytes=50)
        ),
        storage_snapshot=(1, 1),
    )
    readings = iter(
        [
            SimpleNamespace(retained_bytes=20, temporary_bytes=10),
            SimpleNamespace(retained_bytes=30, temporary_bytes=15),
        ]
    )
    monkeypatch.setattr(runner, "measure_storage", lambda settings: next(readings))
    runner._reset_load_budget(runtime)
    assert runtime.storage_snapshot == (20, 10)
    runner._reset_load_budget(runtime)
    assert runtime.storage_snapshot == (30, 15)
