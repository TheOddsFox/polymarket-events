"""Exact outcome no-ops must preserve receipt, ownership and transaction behavior."""

import json
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
        self.updated_market_rows = []

    def execute(self, query, parameters=None):
        self.queries.append(query)
        if self.fail_commit and query.strip().upper() == "COMMIT":
            self.fail_commit = False
            raise duckdb.OutOfMemoryException("synthetic late SQL allocation failure")
        result = (
            self.connection.execute(query, parameters)
            if parameters is not None
            else self.connection.execute(query)
        )
        if re.match(r"\s*UPDATE\s+markets\b", query, re.IGNORECASE):
            self.updated_market_rows.append(result.fetchone()[0])
        return result

    def executemany(self, query, parameters):
        self.queries.append(query)
        return self.connection.executemany(query, parameters)

    @property
    def mutations(self):
        writes = []
        for query in self.queries:
            match = re.match(
                r'\s*(INSERT(?:\s+OR\s+(?:REPLACE|IGNORE))?\s+INTO|UPDATE|DELETE\s+FROM)\s+"?(\w+)',
                query,
                re.IGNORECASE,
            )
            if match:
                writes.append((match[1].upper(), match[2].lower()))
        return writes

    @property
    def written_tables(self):
        return {table for _, table in self.mutations}


def assert_no_wide_rewrites(recorded):
    protected = {"events", "markets", "event_markets", "outcomes", "_identity", "_identity_claims"}
    for operation, table in recorded.mutations:
        if table in protected:
            assert (operation, table) == ("UPDATE", "markets")
    assert all(count == 0 for count in recorded.updated_market_rows)


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
    assert_no_wide_rewrites(recorded)
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
    for candidate, accepted in db.execute("SELECT candidate,accepted FROM _identity").fetchall():
        assert json.loads(candidate) == changed.outcomes
        assert json.loads(accepted) == changed.outcomes
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
        candidate, accepted = db.execute("SELECT candidate,accepted FROM _identity").fetchone()
        assert json.loads(candidate) == changed.outcomes
        assert json.loads(accepted) == changed.outcomes
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
    changed.markets[0]["question"] = "Changed question"
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


def test_explicit_null_in_full_identity_json_is_preserved_without_domain_change(db):
    store.apply(db, packet())
    refreshed = packet(2)
    for outcome in refreshed.outcomes:
        outcome["position_id"] = None
    recorded = RecordedConnection(db)
    result = store.apply(recorded, refreshed)
    assert "_identity" in recorded.written_tables
    assert "outcomes" not in recorded.written_tables
    assert result["changed"] is False
    candidate, accepted = db.execute("SELECT candidate,accepted FROM _identity").fetchone()
    assert json.loads(candidate) == refreshed.outcomes
    assert json.loads(accepted) == refreshed.outcomes
    assert store.validate_store(db)["valid"]


def test_owner_claim_repair_preserves_other_market_and_other_venue(db):
    store.apply(db, packet())
    store.apply(db, packet(market="3", assets=("201", "202")))
    foreign = packet()
    for row in [*foreign.events, *foreign.markets, *foreign.outcomes]:
        row["venue"] = "other-venue"
    store.apply(db, foreign)
    protected_claims = db.execute(
        "SELECT * FROM _identity_claims WHERE venue<>'polymarket' OR market_id<>'2' ORDER BY ALL"
    ).fetchall()
    protected_identities = db.execute(
        "SELECT * FROM _identity WHERE venue<>'polymarket' OR market_id<>'2' ORDER BY ALL"
    ).fetchall()
    expected = db.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall()
    db.execute(
        "INSERT INTO _identity_claims VALUES ('polymarket','2','candidate','ctf_token','999')"
    )
    store.apply(db, packet(2))
    assert db.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall() == expected
    assert (
        db.execute(
            "SELECT * FROM _identity_claims WHERE venue<>'polymarket' OR market_id<>'2' ORDER BY ALL"
        ).fetchall()
        == protected_claims
    )
    assert (
        db.execute(
            "SELECT * FROM _identity WHERE venue<>'polymarket' OR market_id<>'2' ORDER BY ALL"
        ).fetchall()
        == protected_identities
    )
    assert store.validate_store(db)["valid"]


@pytest.mark.parametrize("order", ["older-time", "equal", "earlier-id", "later-id"])
def test_response_timestamp_and_id_order_survives_write_suppression(db, order):
    current = packet(2)
    current.response_id = "b"
    store.apply(db, current)
    before = snapshot(db)
    incoming = packet(1 if order == "older-time" else 2)
    incoming.response_id = {"older-time": "z", "equal": "b", "earlier-id": "a", "later-id": "c"}[
        order
    ]
    incoming.events[0]["title"] = None
    incoming.markets[0]["question"] = None
    incoming.outcomes[0]["outcome_label"] = "Changed label"
    incoming.memberships = {"1": []}
    recorded = RecordedConnection(db)
    result = store.apply(recorded, incoming)
    if order == "later-id":
        assert result["changed"] is True
        assert result["generation"] == 2
        assert db.execute("SELECT title FROM events").fetchone() == (None,)
        assert db.execute("SELECT question FROM markets").fetchone() == (None,)
        assert db.execute("SELECT * FROM event_markets").fetchall() == []
        assert db.execute(
            "SELECT outcome_label FROM outcomes WHERE outcome_index=1"
        ).fetchone() == ("Changed label",)
        assert db.execute("SELECT DISTINCT response FROM _receipts").fetchall() == [("c",)]
    else:
        assert result["changed"] is False
        assert_no_wide_rewrites(recorded)
        assert snapshot(db) == before
    assert store.validate_store(db)["valid"]


@pytest.mark.parametrize("shape", ["absent", "unknown", "empty"])
def test_membership_omission_remains_distinct_from_complete_empty_response(db, shape):
    store.apply(db, packet())
    db.execute("INSERT INTO _parent_repairs VALUES ('1')")
    refreshed = packet(2)
    refreshed.memberships = {"absent": {}, "unknown": {"1": None}, "empty": {"1": []}}[shape]
    recorded = RecordedConnection(db)
    result = store.apply(recorded, refreshed)
    assert ("event_markets" in recorded.written_tables) == (shape == "empty")
    assert "outcomes" not in recorded.written_tables
    assert result["changed"] == (shape == "empty")
    assert db.execute("SELECT market_id FROM event_markets").fetchall() == (
        [] if shape == "empty" else [("2",)]
    )
    stamp = datetime(2026, 1, 2 if shape == "empty" else 1, tzinfo=UTC)
    latest = datetime(2026, 1, 1 if shape == "absent" else 2, tzinfo=UTC)
    assert db.execute("SELECT receipt,latest_receipt,unresolved FROM _membership").fetchone() == (
        stamp,
        latest,
        shape == "unknown",
    )
    assert db.execute("SELECT * FROM _parent_repairs").fetchall() == (
        [] if shape == "empty" else [("1",)]
    )


def test_complete_empty_membership_noop_still_renews_receipt_and_clears_repair(db):
    first, second = packet(), packet(2)
    first.memberships = second.memberships = {"1": []}
    store.apply(db, first)
    db.execute("INSERT INTO _parent_repairs VALUES ('1')")
    recorded = RecordedConnection(db)
    result = store.apply(recorded, second)
    assert_no_wide_rewrites(recorded)
    assert result["changed"] is False
    assert db.execute("SELECT receipt,latest_receipt,unresolved FROM _membership").fetchone() == (
        datetime(2026, 1, 2, tzinfo=UTC),
        datetime(2026, 1, 2, tzinfo=UTC),
        False,
    )
    assert db.execute("SELECT * FROM _parent_repairs").fetchall() == []
