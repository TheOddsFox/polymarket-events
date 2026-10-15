"""Exact outcome no-ops must preserve receipt, ownership and transaction behavior."""

import re
from datetime import UTC, datetime

import duckdb
import pytest
from test_current_store import db as db
from test_current_store import packet

from oddsfox_catalogue import current_store as store


class RecordedConnection:
    def __init__(self, connection, *, fail_commit=False):
        self.connection = connection
        self.queries = []
        self.fail_commit = fail_commit

    def execute(self, query, parameters=None):
        self.queries.append(query)
        if self.fail_commit and query.strip().upper() == "COMMIT":
            self.fail_commit = False
            raise duckdb.OutOfMemoryException("synthetic late SQL allocation failure")
        return (
            self.connection.execute(query, parameters)
            if parameters is not None
            else self.connection.execute(query)
        )

    def executemany(self, query, parameters):
        self.queries.append(query)
        return self.connection.executemany(query, parameters)

    @property
    def written_tables(self):
        writes = set()
        for query in self.queries:
            match = re.match(
                r'\s*(?:INSERT(?:\s+OR\s+(?:REPLACE|IGNORE))?\s+INTO|UPDATE|DELETE\s+FROM)\s+"?(\w+)',
                query,
                re.IGNORECASE,
            )
            if match:
                writes.add(match[1].lower())
        return writes


def snapshot(connection):
    return {
        table: connection.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
        for (table,) in connection.execute("SHOW TABLES").fetchall()
    }


@pytest.mark.parametrize("native", [False, True])
def test_exact_outcome_noop_renews_receipts_progress_and_membership(db, native):
    store.apply(db, packet(), {"cursor": "old"})
    original = db.execute("SELECT * FROM outcomes ORDER BY outcome_index").fetchall()
    db.execute("INSERT INTO _parent_repairs VALUES ('1')")
    recorded = RecordedConnection(db)
    result = store.apply(recorded, packet(2, native=native), {"cursor": "new"})
    assert "outcomes" not in recorded.written_tables
    assert result["changed"] is False
    assert result["generation"] == 1
    assert db.execute("SELECT * FROM outcomes ORDER BY outcome_index").fetchall() == original
    stamp = datetime(2026, 1, 2, tzinfo=UTC)
    assert db.execute("SELECT kind,receipt,response FROM _receipts ORDER BY kind").fetchall() == [
        ("event", stamp, "2"),
        ("market", stamp, "2"),
    ]
    assert db.execute(
        "SELECT receipt,response,unresolved,latest_receipt,latest_response FROM _membership"
    ).fetchone() == (stamp, "2", False, stamp, "2")
    assert db.execute("SELECT * FROM _parent_repairs").fetchall() == []
    assert store.get_state(db, "cursor") == "new"
    assert store.validate_store(db)["valid"]


def test_outcome_label_change_rewrites_served_mapping(db):
    store.apply(db, packet())
    changed = packet(2)
    changed.outcomes[0]["outcome_label"] = 'Yes "雪"\n'
    recorded = RecordedConnection(db)
    result = store.apply(recorded, changed)
    assert "outcomes" in recorded.written_tables
    assert result["changed"] is True
    assert result["generation"] == 2
    assert db.execute("SELECT outcome_label FROM outcomes ORDER BY outcome_index").fetchall() == [
        ('Yes "雪"\n',),
        ("2",),
    ]
    assert store.validate_store(db)["valid"]


def test_optional_native_id_null_transitions_are_not_skipped(db):
    store.apply(db, packet())
    for day, positions in ((2, ("111", "112")), (3, (None, None))):
        changed = packet(day, native=True)
        for outcome, position in zip(changed.outcomes, positions, strict=True):
            outcome["position_id"] = position
        recorded = RecordedConnection(db)
        result = store.apply(recorded, changed)
        assert "outcomes" in recorded.written_tables
        assert result["changed"] is True
        assert result["generation"] == day
        assert db.execute("SELECT position_id FROM outcomes ORDER BY outcome_index").fetchall() == [
            (position,) for position in positions
        ]
        assert store.validate_store(db)["valid"]


def test_nullable_market_change_and_empty_membership_still_apply(db):
    store.apply(db, packet())
    changed = packet(2)
    changed.markets[0]["question"] = None
    changed.memberships = {"1": []}
    recorded = RecordedConnection(db)
    result = store.apply(recorded, changed)
    assert {"markets", "event_markets"} <= recorded.written_tables
    assert "outcomes" not in recorded.written_tables
    assert result["changed"] is True
    assert result["generation"] == 2
    assert db.execute("SELECT question FROM markets").fetchone() == (None,)
    assert db.execute("SELECT * FROM event_markets").fetchall() == []
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (2,)
    assert store.validate_store(db)["valid"]


def test_other_owner_conflict_invalidates_unchanged_incumbent_and_requires_native(db):
    store.apply(db, packet())
    recorded = RecordedConnection(db)
    store.apply(recorded, packet(2, market="3"))
    assert "outcomes" in recorded.written_tables
    assert db.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", False),
        ("3", False),
    ]
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    store.apply(db, packet(3))
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    store.apply(db, packet(4, market="3", assets=("103", "104"), native=True))
    assert db.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", False),
        ("3", True),
    ]
    store.apply(db, packet(5, native=True))
    assert db.execute(
        "SELECT market_id,asset_id FROM outcomes ORDER BY market_id,asset_id"
    ).fetchall() == [
        ("2", "101"),
        ("2", "102"),
        ("3", "103"),
        ("3", "104"),
    ]
    assert store.validate_store(db)["valid"]


def test_late_sql_oom_rolls_back_every_table_and_retry_matches_clean_apply(db):
    initial = packet()
    progress = {"cursor": "old", "fetch_pending": {"remaining": ["frozen"]}}
    store.apply(db, initial, progress)
    db.execute("INSERT INTO _parent_repairs VALUES ('1')")
    before = snapshot(db)
    changed = packet(2, assets=("103", "104"), native=True)
    changed.events[0]["title"] = "Changed"
    changed.memberships = {"1": []}
    changed.parent_event_ids = {"77"}
    renewed = {"cursor": "new", "fetch_pending": None, "new_progress": {"done": True}}
    recorded = RecordedConnection(db, fail_commit=True)
    with pytest.raises(duckdb.OutOfMemoryException, match="synthetic late SQL"):
        store.apply(recorded, changed, renewed)
    assert {
        "events",
        "markets",
        "event_markets",
        "outcomes",
        "_identity",
        "_identity_claims",
        "_receipts",
        "_membership",
        "_parent_repairs",
        "sync_state",
    } <= recorded.written_tables
    assert recorded.queries[-1] == "ROLLBACK"
    assert snapshot(db) == before
    retried = store.apply(db, changed, renewed)
    with duckdb.connect(":memory:") as clean:
        store.initialize(clean)
        store.apply(clean, initial, progress)
        clean.execute("INSERT INTO _parent_repairs VALUES ('1')")
        assert store.apply(clean, changed, renewed) == retried
        assert snapshot(db) == snapshot(clean)
    assert store.validate_store(db)["valid"]
