"""Historical-range age is independent of visible entity freshness."""

from datetime import UTC, datetime, timedelta

import duckdb

from oddsfox_catalogue.current_reconcile import freshness
from oddsfox_catalogue.current_store import initialize, set_state


def test_unstarted_and_starved_ranges_are_visible_without_entities():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    with duckdb.connect() as conn:
        initialize(conn)
        set_state(
            conn, "discovery", {"lanes": {k: {"frontier": 200} for k in ("events", "markets")}}
        )
        set_state(
            conn,
            "reconcile",
            {
                "windows": {
                    "events": {
                        "next": 101,
                        "high": 200,
                        "started_at": (now - timedelta(days=31)).timestamp(),
                    }
                }
            },
        )
        result = freshness(conn, now=now)
        assert result["cold_overdue_count"] == 0
        assert result["cold_ranges"]["events"] == {
            "age_s": 31 * 86400,
            "overdue": True,
            "unstarted": False,
        }
        assert result["cold_ranges"]["markets"] == {
            "age_s": None,
            "overdue": None,
            "unstarted": True,
        }
        set_state(
            conn,
            "reconcile",
            {
                "windows": {
                    "events": {
                        "next": 1,
                        "cycle": (now - timedelta(days=5)).timestamp(),
                    }
                }
            },
        )
        assert freshness(conn, now=now)["cold_ranges"]["events"] == {
            "age_s": 5 * 86400,
            "overdue": False,
            "unstarted": False,
        }
