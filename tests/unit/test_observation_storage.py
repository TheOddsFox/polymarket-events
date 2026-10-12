"""Lossless physical compression and logical-view recovery boundaries."""

import json
from datetime import UTC, datetime

import duckdb
import pytest

from oddsfox_catalogue.ids import canonical_json
from oddsfox_catalogue.load.rows import PageContext, rows_for_page
from oddsfox_catalogue.load.source import event_columns, market_columns, stored_columns
from oddsfox_catalogue.load.storage import (
    EVENT_FIELDS,
    MARKET_FIELDS,
    ensure_observation_views,
    stored_row,
)


@pytest.mark.parametrize("entity,fields", [("events", EVENT_FIELDS), ("markets", MARKET_FIELDS)])
def test_fixed_fields_match_normalization_and_preserve_exact_values(entity, fields):
    record = {
        "id": "1",
        "version": "v2",
        "positionIds": ["1", "9999999999999999999999999"],
        "outcomes": ["雪", "No"],
        "active": False,
        "closed": None,
        "events": [],
        "description": '\x00"\\雪' * 2000,
        "volume": "0.12345678901234567890123456789",
    }
    context = PageContext("page", "batch", "/" + entity, datetime.now(UTC), entity)
    row = getattr(rows_for_page(context, [record]), entity)[0]
    columns = market_columns() if entity == "markets" else event_columns()
    physical = stored_row(row, fields)
    reconstructed = {key: json.loads(physical["n_" + key]) for key in fields}
    assert canonical_json(reconstructed) == canonical_json(row["normalized"])
    assert row["normalized"].keys() == set(fields)
    with duckdb.connect(config={"memory_limit": "64MB", "threads": 1}) as connection:
        connection.execute("CREATE SCHEMA bronze")
        types = {"text": "VARCHAR", "timestamp": "TIMESTAMPTZ"}
        definitions = ", ".join(
            f'"{key}" {types[hint["data_type"]]}'
            for key, hint in stored_columns(columns, fields).items()
        )
        table = entity[:-1] + "_observations"
        connection.execute(f"CREATE TABLE bronze.{table}_values ({definitions})")
        parameters = ", ".join("?" for _ in physical)
        connection.execute(
            f"INSERT INTO bronze.{table}_values VALUES ({parameters})", list(physical.values())
        )
        ensure_observation_views(connection, "bronze")
        ensure_observation_views(connection, "bronze")
        actual = connection.execute(f"SELECT normalized FROM bronze.{table}").fetchone()[0]
        assert canonical_json(json.loads(actual)) == canonical_json(row["normalized"])
        connection.execute(f"DROP VIEW bronze.{table}")
        ensure_observation_views(connection, "bronze")
        assert connection.execute(f"SELECT count(*) FROM bronze.{table}").fetchone() == (1,)
        connection.execute(f"CREATE OR REPLACE VIEW bronze.{table} AS SELECT 1 AS normalized")
        with pytest.raises(ValueError, match="view does not match"):
            ensure_observation_views(connection, "bronze")
        assert connection.execute(
            "SELECT count(*) FROM duckdb_views() WHERE temporary AND view_name='_catalogue_expected_observation_view'"
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    "malformed",
    [
        {},
        {key: None for key in EVENT_FIELDS if key != "title"},
        {**dict.fromkeys(EVENT_FIELDS), "extra": None},
        None,
    ],
)
def test_storage_schema_drift_is_rejected(malformed):
    with pytest.raises(ValueError, match="frozen storage schema"):
        stored_row({"normalized": malformed}, EVENT_FIELDS)
