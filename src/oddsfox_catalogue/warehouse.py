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


def last_two_open_event_counts(warehouse: Path) -> tuple[int, int] | None:
    """``(previous, latest)`` open-event counts from ``marts.catalogue_snapshots``.

    Returns None when the warehouse, the snapshot table, or two snapshots do not exist yet.
    The ordering matches the ``assert_open_events_not_dropping`` test.
    """
    if not warehouse.exists():
        return None
    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        rows = connection.execute(
            "SELECT open_events FROM marts.catalogue_snapshots ORDER BY captured_at DESC LIMIT 2"
        ).fetchall()
    except duckdb.Error:
        return None
    finally:
        connection.close()
    if len(rows) < 2:
        return None
    latest, previous = int(rows[0][0]), int(rows[1][0])
    return previous, latest


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
