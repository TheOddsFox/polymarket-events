"""Read-only probes of the DuckDB warehouse used by the capture layer.

Capture needs one fact from the warehouse: which events were open at the last
loaded state. Daily refresh re-fetches those IDs if they are missing from the
open-event scan, which is how closures are caught. Each connection is opened
read-only and closed before returning, so nothing holds the database while a
load or dbt build runs.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.semantics import bounded_connection


class BaselineMissing(RuntimeError):
    """The warehouse has no open-event baseline yet. Run bootstrap first."""


@contextmanager
def _connection(warehouse: Path, settings: Settings | None = None):
    if settings is not None:
        with bounded_connection(settings, warehouse) as connection:
            yield connection
    else:
        with duckdb.connect(
            str(warehouse),
            read_only=True,
            config={
                "memory_limit": "2GB",
                "threads": 1,
                "max_temp_directory_size": "0B",
            },
        ) as connection:
            yield connection


def read_open_event_ids(warehouse: Path) -> set[str]:
    if not warehouse.exists():
        raise BaselineMissing(f"no warehouse at {warehouse}; run `catalogue bootstrap` first")
    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        rows = connection.execute(
            "SELECT event_id FROM core.events_current WHERE closed = false AND venue = 'polymarket'"
        ).fetchall()
    except duckdb.Error as exc:
        raise BaselineMissing(f"no core.events_current in {warehouse}: {exc}") from exc
    finally:
        connection.close()
    return {str(row[0]) for row in rows}


def _read_refresh_ids(warehouse: Path, entity: str, settings: Settings | None = None) -> set[str]:
    if not warehouse.exists():
        raise BaselineMissing("daily refresh requires a built warehouse baseline")
    try:
        with _connection(warehouse, settings) as connection:
            return {
                str(row[0])
                for row in connection.execute(
                    f"SELECT {entity}_id FROM core.{entity}s_current WHERE venue = 'polymarket' AND closed IS NOT TRUE AND archived IS NOT TRUE"
                ).fetchall()
            }
    except duckdb.Error as exc:
        raise BaselineMissing(
            "daily refresh requires complete event and market baseline relations"
        ) from exc


def read_refresh_event_ids(warehouse: Path, *, settings: Settings | None = None) -> set[str]:
    """Records whose lifecycle remains open or unknown need direct reobservation."""
    return _read_refresh_ids(warehouse, "event", settings)


def read_refresh_market_ids(warehouse: Path, *, settings: Settings | None = None) -> set[str]:
    return _read_refresh_ids(warehouse, "market", settings)
