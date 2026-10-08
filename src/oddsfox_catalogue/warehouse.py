"""Read-only probes of the DuckDB warehouse used by the capture layer.

Capture needs one fact from the warehouse: which events were open at the last
loaded state. Daily refresh re-fetches those IDs if they are missing from the
open-event scan, which is how closures are caught. Each connection is opened
read-only and closed before returning, so nothing holds the database while a
load or dbt build runs.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


class BaselineMissing(RuntimeError):
    """The warehouse has no open-event baseline yet. Run bootstrap first."""


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
