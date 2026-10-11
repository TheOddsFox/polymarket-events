"""Exact admission counts survive the faster standard-library JSON encoder."""

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from oddsfox_catalogue.ids import canonical_json
from oddsfox_catalogue.load.runner import _encoded_row_bytes


def streamed_size(row):
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=lambda value: (
            value.isoformat() if isinstance(value, datetime) else json.loads(canonical_json(value))
        ),
    )
    return sum(len(part.encode()) for part in encoder.iterencode(row)) + 1


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"null": None, "true": True, "false": False},
        {"ints": [0, -1, 2**256], "floats": [0.0, -0.0, 1e-30, 1e30]},
        {"escaped": '\x00\n\t\\"', "unicode": "£雪🦊"},
        {"a": [{"z": [None, True, {"label": "雪"}]}], "b": []},
        {"decimal": Decimal("0.12345678901234567890123456789")},
        {"decimal": Decimal("0.0100"), "negative_zero": Decimal("-0")},
        {"date": datetime(2026, 10, 11, 12, 0, 1, 123456, tzinfo=UTC)},
        {"date": datetime(2026, 10, 11, tzinfo=timezone(timedelta(hours=2)))},
        {"large": "雪" * 100_000},
    ],
)
def test_exact_admission_count_matches_streamed_encoding(row):
    assert _encoded_row_bytes(row) == streamed_size(row)


def test_financial_and_timestamp_representations_and_newline():
    row = {"price": Decimal("0.123456789012345678901"), "time": datetime(2026, 10, 11, tzinfo=UTC)}
    expected = b'{"price":"0.123456789012345678901","time":"2026-10-11T00:00:00+00:00"}\n'
    assert _encoded_row_bytes(row) == len(expected)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_numbers_remain_rejected(value):
    for counter in (_encoded_row_bytes, streamed_size):
        with pytest.raises(ValueError):
            counter({"value": value})


@pytest.mark.parametrize("value", [object(), {1, 2}, Decimal("NaN"), Decimal("Infinity")])
def test_unsupported_values_remain_rejected(value):
    for counter in (_encoded_row_bytes, streamed_size):
        with pytest.raises(TypeError):
            counter({"value": value})


def test_circular_values_remain_rejected():
    row = {}
    row["cycle"] = row
    for counter in (_encoded_row_bytes, streamed_size):
        with pytest.raises(ValueError, match="Circular"):
            counter(row)


def test_excessive_nesting_remains_rejected():
    nested = []
    for _ in range(10_000):
        nested = [nested]
    for counter in (_encoded_row_bytes, streamed_size):
        with pytest.raises(RecursionError):
            counter({"nested": nested})
