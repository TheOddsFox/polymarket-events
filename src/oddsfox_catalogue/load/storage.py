"""Compress normalized fields independently; expose the original logical JSON rows."""

from collections.abc import Iterable, Iterator
from typing import Any

from duckdb import DuckDBPyConnection

from oddsfox_catalogue.ids import canonical_json

EVENT_FIELDS = (
    "active",
    "archived",
    "closed",
    "created_at",
    "description",
    "end_date",
    "liquidity",
    "neg_risk",
    "open_interest",
    "semantic_hash",
    "series",
    "series_present",
    "slug",
    "source_updated_at",
    "start_date",
    "tags",
    "tags_present",
    "ticker",
    "title",
    "volume",
)
MARKET_FIELDS = (
    "accepting_orders",
    "active",
    "archived",
    "capture_id",
    "close_at",
    "closed",
    "condition_id",
    "created_at",
    "description",
    "enable_order_book",
    "enclosing_event_id",
    "end_at",
    "identity_error",
    "json_pointer",
    "liquidity",
    "market_id",
    "membership_mode",
    "memberships",
    "minimum_order_size",
    "neg_risk",
    "observation_id",
    "outcomes",
    "page_id",
    "payload_sha256",
    "protocol",
    "question",
    "raw_clob_token_ids",
    "raw_outcomes",
    "raw_position_ids",
    "received_at",
    "resolved",
    "resolved_at",
    "semantic_hash",
    "slug",
    "source_kind",
    "source_market_version",
    "source_updated_at",
    "start_at",
    "tags",
    "tags_present",
    "tick_size",
    "usable",
    "venue",
    "volume",
)


def stored_row(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    normalized = row["normalized"]
    if not isinstance(normalized, dict) or normalized.keys() != set(fields):
        raise ValueError("normalized observation does not match the frozen storage schema")
    return {
        **{key: value for key, value in row.items() if key != "normalized"},
        **{f"n_{key}": canonical_json(normalized[key]) for key in fields},
    }


def stored_rows(
    rows: Iterable[dict[str, Any]], fields: tuple[str, ...]
) -> Iterator[dict[str, Any]]:
    for row in rows:
        yield stored_row(row, fields)


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ensure_observation_views(connection: DuckDBPyConnection, dataset: str) -> None:
    """Repair absent views after a partial load; refuse altered definitions."""
    from oddsfox_catalogue.load.source import event_columns, market_columns

    for table, fields, columns in (
        ("event_observations", EVENT_FIELDS, event_columns()),
        ("market_observations", MARKET_FIELDS, market_columns()),
    ):
        physical = table + "_values"
        exists = connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema=? AND table_name=? AND table_type='BASE TABLE'",
            [dataset, physical],
        ).fetchone()[0]
        if not exists:
            continue
        envelope = ", ".join(_identifier(name) for name in columns if name != "normalized")
        values = ", ".join(f"'{key}', {_identifier('n_' + key)}::JSON" for key in fields)
        query = (
            f"SELECT {envelope}, json_object({values}) AS normalized "
            f"FROM {_identifier(dataset)}.{_identifier(physical)}"
        )
        # Let DuckDB canonicalize SQL rather than comparing whitespace or cast spelling.
        expected = "_catalogue_expected_observation_view"
        connection.execute(f"CREATE OR REPLACE TEMP VIEW {expected} AS {query}")
        try:
            expected_sql = (
                connection.execute(
                    "SELECT sql FROM duckdb_views() WHERE temporary AND view_name=?", [expected]
                )
                .fetchone()[0]
                .split(" AS ", 1)[1]
            )
            actual = connection.execute(
                "SELECT sql FROM duckdb_views() WHERE NOT temporary AND schema_name=? "
                "AND view_name=?",
                [dataset, table],
            ).fetchone()
            if actual is None:
                connection.execute(
                    f"CREATE VIEW {_identifier(dataset)}.{_identifier(table)} AS {query}"
                )
            elif actual[0].split(" AS ", 1)[1] != expected_sql:
                raise ValueError("normalized observation view does not match warehouse contract")
        finally:
            connection.execute(f"DROP VIEW {expected}")
