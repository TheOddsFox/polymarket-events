"""Encoded resources preserve the existing physical storage contract."""

from datetime import UTC, datetime

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from oddsfox_catalogue.load import source
from oddsfox_catalogue.load.rows import PageContext, rows_for_page
from oddsfox_catalogue.load.storage import EVENT_FIELDS, MARKET_FIELDS, stored_row


@pytest.fixture(params=["events", "markets"])
def resource_case(request):
    entity = request.param
    context = PageContext("page", "batch", "/" + entity, datetime.now(UTC), entity)
    record = {
        "id": "1",
        "description": '\x00"\\雪',
        "active": False,
        "closed": None,
        "volume": "0.12345678901234567890123456789",
    }
    row = getattr(rows_for_page(context, [record]), entity)[0]
    if entity == "events":
        return row, EVENT_FIELDS, source.event_resource, source._stored_event_resource
    return row, MARKET_FIELDS, source.market_resource, source._stored_market_resource


def test_encoded_resource_preserves_rows_and_logical_resource_contract(resource_case, monkeypatch):
    row, fields, logical_factory, physical_factory = resource_case
    physical = stored_row(row, fields)
    logical = logical_factory([row])
    expected_schema = logical.compute_table_schema()
    assert list(logical) == [physical]

    def unexpected_encoding(*args, **kwargs):
        pytest.fail("physical resources must not encode rows again")

    monkeypatch.setattr(source, "stored_rows", unexpected_encoding)
    encoded = physical_factory(iter([physical]))
    assert encoded.compute_table_schema() == expected_schema
    assert list(encoded) == [physical]
    assert expected_schema["write_disposition"] == "merge"
    assert expected_schema["x-merge-strategy"] == "insert-only"
    assert expected_schema["columns"]["observation_id"]["primary_key"]
    assert expected_schema["schema_contract"] == source.SCHEMA_CONTRACT


def test_encoded_empty_materialization_matches_logical_resource(resource_case):
    _, _, logical_factory, physical_factory = resource_case
    logical = logical_factory([], materialize_only=True)
    encoded = physical_factory([], materialize_only=True)
    assert encoded.compute_table_schema() == logical.compute_table_schema()
    schema = encoded.compute_table_schema()
    assert schema["write_disposition"] == "append"
    assert not schema["columns"]["observation_id"].get("primary_key")
    assert list(encoded) == list(logical)


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_logical_resource_rejects_nonfinite_values(resource_case, bad_value):
    row, fields, logical_factory, _ = resource_case
    row["normalized"][fields[0]] = bad_value
    with pytest.raises(ResourceExtractionError) as raised:
        list(logical_factory([row]))
    assert isinstance(raised.value.__cause__, ValueError)


@pytest.mark.parametrize("change", ["missing", "extra"])
def test_logical_resource_still_rejects_frozen_field_drift(resource_case, change):
    row, fields, logical_factory, _ = resource_case
    if change == "missing":
        del row["normalized"][fields[0]]
    else:
        row["normalized"]["unexpected"] = None
    with pytest.raises(ResourceExtractionError, match="frozen storage schema") as raised:
        list(logical_factory([row]))
    assert isinstance(raised.value.__cause__, ValueError)
