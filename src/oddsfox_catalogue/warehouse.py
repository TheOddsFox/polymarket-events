"""Read-only probes of the DuckDB warehouse used by the capture layer.

Capture needs one fact from the warehouse: which events were open at the last
loaded state. Daily refresh re-fetches those IDs if they are missing from the
open-event scan, which is how closures are caught. Each connection is opened
read-only and closed before returning, so nothing holds the database while a
load or dbt build runs.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class BaselineMissing(RuntimeError):
    """The warehouse has no open-event baseline yet. Run bootstrap first."""


def last_two_open_event_counts(warehouse: Path) -> tuple[int, int] | None:
    """``(previous, latest)`` open-event counts from ``marts.catalogue_snapshots``.

    Returns None when the warehouse, the snapshot table, or two snapshots do not exist yet.
    Any other failure (the file is held by another connection, is not a DuckDB file, or the
    snapshot table has a different shape) also returns None, but logs a warning first. The
    warn-band check is advisory; the ``assert_open_events_not_dropping`` test still enforces
    the error limit inside the dbt build. The ordering matches that test.
    """
    if not warehouse.exists():
        return None
    try:
        connection = duckdb.connect(str(warehouse), read_only=True)
    except duckdb.Error as exc:
        logger.warning("open-event drop check skipped: cannot open %s: %s", warehouse, exc)
        return None
    try:
        rows = connection.execute(
            "SELECT open_events FROM marts.catalogue_snapshots ORDER BY captured_at DESC LIMIT 2"
        ).fetchall()
    except duckdb.CatalogException:
        # The first build has not created the snapshot table yet.
        return None
    except duckdb.Error as exc:
        logger.warning("open-event drop check skipped: snapshot query failed: %s", exc)
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
