"""Batched extraction must retain the original 47-column SQL projection."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from jinja2 import StrictUndefined, Template

from oddsfox_catalogue.resources import dbt_project_dir

REFERENCE = Path(__file__).resolve().parents[1] / "fixtures/dbt/market_stage_individual_paths.sql"


def _record():
    return {
        "question": 'Quoted "question" \\ line\n雪',
        "slug": "distinct-slug",
        "description": "Distinct description",
        "condition_id": "0x" + "01" * 32,
        "source_market_version": "v2",
        "protocol": "polymarket_v2",
        "tick_size": "0.000000000000000001",
        "minimum_order_size": "5.000000000000000002",
        "volume": "123456789123456789.000000000000000003",
        "liquidity": "0.123456789123456789123456789",
        "identity_error": "distinct identity reason",
        "membership_mode": "explicit",
        "enclosing_event_id": "11",
        "semantic_hash": "ab" * 32,
        "active": True,
        "closed": False,
        "archived": True,
        "resolved": False,
        "neg_risk": True,
        "enable_order_book": False,
        "accepting_orders": True,
        "usable": False,
        "tags_present": True,
        "created_at": "2026-10-01T01:02:03.123456Z",
        "start_at": "2026-10-02T02:03:04+02:00",
        "end_at": "2026-10-03T03:04:05-03:00",
        "close_at": "2026-10-04T04:05:06Z",
        "resolved_at": "2026-10-05T05:06:07Z",
        "outcomes": [{"outcome_index": 1, "outcome_label": 'Escaped "Yes"', "extra": [[1, None]]}],
        "raw_outcomes": ["Yes", "No", None],
        "raw_clob_token_ids": [None, "101"],
        "raw_position_ids": [["nested"], "202"],
        "tags": [{"id": "tag-1", "label": "tag label"}],
        "memberships": [{"event_id": "11", "inferred": False}],
    }


def _render(text):
    def source(schema, name):
        assert (schema, name) == ("bronze", "market_observations")
        return "market_observations"

    def ref(name):
        assert name == "stg_gamma__batches"
        return "batches"

    return Template(text, undefined=StrictUndefined).render(
        source=source, ref=ref, config=lambda **kwargs: ""
    )


@pytest.fixture(scope="module")
def queries():
    return (
        _render(REFERENCE.read_text()),
        _render(
            dbt_project_dir()
            .joinpath("models/staging/stg_gamma__market_observations.sql")
            .read_text()
        ),
    )


@pytest.fixture
def connection():
    with duckdb.connect(":memory:", config={"threads": 1, "memory_limit": "32MiB"}) as con:
        con.execute("SET TimeZone = 'UTC'")
        con.execute(
            "CREATE TABLE market_observations (observation_id VARCHAR, venue VARCHAR, "
            "entity_id VARCHAR, batch_id VARCHAR, page_id VARCHAR, endpoint VARCHAR, "
            "observed_at TIMESTAMPTZ, source_updated_at TIMESTAMPTZ, payload_hash VARCHAR, "
            "source_kind VARCHAR, json_pointer VARCHAR, normalized VARCHAR)"
        )
        con.execute("CREATE TABLE batches (batch_id VARCHAR, batch_loaded_at TIMESTAMPTZ)")
        con.execute("INSERT INTO batches VALUES ('batch-1', '2026-10-10T11:00:00Z')")
        yield con


def _insert(connection, normalized, *, observation_id="observation-1", batch_id="batch-1"):
    connection.execute(
        "INSERT INTO market_observations VALUES "
        "(?, 'polymarket', '201', ?, 'page-1', '/markets/201', '2026-10-10T10:00:00Z', "
        "'2026-10-09T09:00:00Z', 'source-hash', 'market_direct', '/markets/0', ?)",
        [observation_id, batch_id, normalized],
    )


def _equivalent(connection, queries):
    outputs = []
    for query in queries:
        cursor = connection.execute(f"SELECT * FROM ({query}) ORDER BY observation_id")
        schema = [(name, str(kind)) for name, kind, *_ in cursor.description]
        outputs.append((schema, cursor.fetchall()))
    assert outputs[0] == outputs[1]
    assert len(outputs[0][0]) == 47
    return [
        dict(zip([name for name, _ in outputs[0][0]], row, strict=True)) for row in outputs[0][1]
    ]


def test_all_columns_preserve_types_exact_values_and_observation_grain(connection, queries):
    record = _record()
    for observation_id in ("observation-1", "observation-2"):
        _insert(connection, json.dumps(record), observation_id=observation_id)
    _insert(connection, json.dumps(record), observation_id="unregistered", batch_id="not-loaded")
    rows = _equivalent(connection, queries)
    assert [row["observation_id"] for row in rows] == ["observation-1", "observation-2"]
    for row in rows:
        assert row["question"] == record["question"]
        assert row["tick_size"] == record["tick_size"]
        assert row["minimum_order_size"] == record["minimum_order_size"]
        assert row["volume"] == record["volume"]
        assert row["liquidity"] == record["liquidity"]
        assert row["start_at"] == datetime(2026, 10, 2, 0, 3, 4, tzinfo=UTC)
        assert row["end_at"] == row["end_date"] == datetime(2026, 10, 3, 6, 4, 5, tzinfo=UTC)
        assert json.loads(row["outcomes_json"]) == record["outcomes"]
        assert json.loads(row["raw_clob_token_ids"]) == [None, "101"]
        assert json.loads(row["raw_position_ids"]) == [["nested"], "202"]


@pytest.mark.parametrize("key", list(_record()))
@pytest.mark.parametrize("absent", [True, False], ids=["missing-key", "json-null"])
def test_missing_and_explicit_null_fields_retain_individual_path_semantics(
    connection, queries, key, absent
):
    record = _record()
    if absent:
        del record[key]
    else:
        record[key] = None
    _insert(connection, json.dumps(record))
    row = _equivalent(connection, queries)[0]
    if key == "raw_clob_token_ids":
        assert row[key] is None if absent else row[key] == "null"


@pytest.mark.parametrize("normalized", [None, "null", "{}"])
def test_sql_null_json_null_and_empty_documents_preserve_null_outputs(
    connection, queries, normalized
):
    _insert(connection, normalized)
    row = _equivalent(connection, queries)[0]
    assert all(value is None for value in list(row.values())[12:])


def test_raw_scientific_numbers_retain_individual_path_rendering(connection, queries):
    record = _record()
    record["volume"] = "scientific-number-placeholder"
    normalized = json.dumps(record).replace(
        '"scientific-number-placeholder"', "1.2345678901234567890123456789e-17"
    )
    _insert(connection, normalized)
    row = _equivalent(connection, queries)[0]
    # Normalizers emit financial strings; numeric JSON retains DuckDB's existing rendering.
    assert row["volume"] == "1.2345678901234568e-17"


@pytest.mark.parametrize(
    ("normalized", "exception", "message"),
    [
        ("{not-json", duckdb.InvalidInputException, "Malformed JSON"),
        (json.dumps({"active": "not-boolean"}), duckdb.ConversionException, "BOOL"),
        (json.dumps({"closed": [True]}), duckdb.ConversionException, "BOOL"),
        (json.dumps({"created_at": "not-a-date"}), duckdb.ConversionException, "timestamp"),
        (json.dumps({"end_at": {"year": 2026}}), duckdb.ConversionException, "timestamp"),
    ],
)
def test_malformed_json_and_invalid_casts_fail_for_both_queries(
    connection, queries, normalized, exception, message
):
    _insert(connection, normalized)
    for query in queries:
        with pytest.raises(exception, match=message):
            connection.execute(query).fetchall()
