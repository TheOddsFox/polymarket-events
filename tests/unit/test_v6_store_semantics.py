"""Cross-surface semantic regressions for the V6 store candidates."""

from copy import deepcopy
from datetime import UTC, datetime

import duckdb
import pytest

from fakes.reference_apply import reference_apply
from fakes.world import make_event, make_market
from oddsfox_catalogue import current_reconcile, current_store
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_rows import project_response

TABLES = (
    "events",
    "markets",
    "event_markets",
    "outcomes",
    "_receipts",
    "_membership",
    "_identity",
    "_identity_claims",
    "_parent_repairs",
    "_reconcile_attempts",
    "sync_state",
)


def _snapshot(conn):
    return {
        name: conn.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall() for name in TABLES
    }


@pytest.fixture
def pair():
    reference, production = duckdb.connect(), duckdb.connect()
    for conn in (reference, production):
        current_store.initialize(conn)
        current_reconcile._initialize(conn)
    yield reference, production
    reference.close()
    production.close()


def _both(pair, response, progress=None):
    reference, production = pair
    assert current_store.apply(production, response, progress) == reference_apply(
        reference, response, progress
    )
    assert _snapshot(production) == _snapshot(reference)


def test_successful_membership_and_latest_observation_remain_distinct_through_absence(pair):
    _, conn = pair
    seed = project_response(
        [make_event("1", "original", markets=[make_market("10", "market")])],
        "events",
        received_at="2026-01-01T00:00:00.123456Z",
        response_id="seed",
    )
    _both(pair, seed, {"cursor": 1})
    _both(
        pair,
        ResponseRows("2026-01-03T02:00:00.123456+02:00", "incomplete", memberships={"1": None}),
    )
    # This complete observation is older than the latest incomplete observation,
    # but newer than the last successful membership. Neither receipt may replace the other.
    _both(
        pair,
        ResponseRows("2026-01-02T00:00:00.654321Z", "complete", memberships={"1": []}),
    )
    assert conn.execute(
        "SELECT receipt, response, unresolved, latest_receipt, latest_response FROM _membership"
    ).fetchone() == (
        datetime(2026, 1, 2, microsecond=654321, tzinfo=UTC),
        "complete",
        True,
        datetime(2026, 1, 3, microsecond=123456, tzinfo=UTC),
        "incomplete",
    )
    assert conn.execute("SELECT count(*) FROM event_markets").fetchone()[0] == 0
    generation = current_store.generation(conn)
    _both(
        pair,
        ResponseRows(
            "2026-01-04T00:00:00Z",
            "native-404",
            absent_event_ids={"1", "999"},
            absent_market_ids={"10", "999"},
        ),
        {"cursor": 2},
    )
    assert current_store.generation(conn) == generation
    assert conn.execute("SELECT id FROM _receipts WHERE id='999'").fetchall() == []
    assert conn.execute(
        "SELECT receipt, latest_receipt, unresolved FROM _membership"
    ).fetchone() == (
        datetime(2026, 1, 4, tzinfo=UTC),
        datetime(2026, 1, 3, microsecond=123456, tzinfo=UTC),
        True,
    )


def test_conflict_loads_outside_owner_and_preserves_duplicate_claim_multisets(pair):
    _, conn = pair
    source = [make_market("10", "incoming", seed=10), make_market("99", "outside", seed=99)]
    seed = project_response(
        source,
        "markets",
        received_at="2026-01-01T00:00:00Z",
        response_id="seed",
        native_market_ids={"10", "99"},
    )
    other = deepcopy(seed)
    other.response_id = "other-venue"
    other.markets = [row for row in other.markets if row["market_id"] == "99"]
    other.outcomes = [row for row in other.outcomes if row["market_id"] == "99"]
    for row in (*other.markets, *other.outcomes):
        row["venue"] = "other"
    _both(pair, seed)
    _both(pair, other)
    for database in pair:
        database.execute(
            "INSERT INTO _identity_claims SELECT * FROM _identity_claims "
            "WHERE venue='polymarket' AND market_id='99' AND role='accepted'"
        )
    before_other = conn.execute(
        "SELECT usable,identity_error FROM markets WHERE venue='other' AND market_id='99'"
    ).fetchone()
    stolen = project_response(
        [make_market("10", "changed identity", seed=99)],
        "markets",
        received_at="2026-01-02T00:00:00Z",
        response_id="conflicting-candidate",
    )
    _both(pair, stolen)
    assert conn.execute(
        "SELECT usable,identity_error FROM markets WHERE venue='polymarket' AND market_id='99'"
    ).fetchone() == (False, "asset_ownership_conflict")
    assert (
        conn.execute(
            "SELECT usable,identity_error FROM markets WHERE venue='other' AND market_id='99'"
        ).fetchone()
        == before_other
    )
    assert (
        conn.execute(
            "SELECT count(*) FROM _identity_claims "
            "WHERE venue='polymarket' AND market_id='99' AND role='accepted'"
        ).fetchone()[0]
        == 4
    )
    before = _snapshot(conn)
    stale = deepcopy(seed)
    stale.response_id = "older"
    _both(pair, stale)
    assert _snapshot(conn) == before


@pytest.mark.parametrize("failure", ["second_receipt_chunk", "commit"])
def test_late_write_failures_restore_every_durable_surface_and_connection_can_retry(pair, failure):
    _, conn = pair
    seed = ResponseRows(
        "2026-01-01T00:00:00Z",
        "seed",
        events=[{"venue": "polymarket", "event_id": "1", "title": "old"}],
        memberships={"1": []},
    )
    current_store.apply(conn, seed, {"cursor": 1, "full_pass": {"cursor_after": 1}})
    current_store.record_attempts(conn, "events", ["1"], datetime(2026, 1, 1, tzinfo=UTC))
    before = _snapshot(conn)
    events = [
        {"venue": "polymarket", "event_id": str(index), "title": "new"} for index in range(1, 1002)
    ]
    response = ResponseRows(
        "2026-01-02T00:00:00Z", "replacement", events=events, memberships={"1": None}
    )
    progress = {"cursor": 1001, "full_pass": {"cursor_after": 1001}}
    attempts = (
        "events",
        [str(index) for index in range(1, 1002)],
        datetime(2026, 1, 2, tzinfo=UTC),
    )

    class FailingConnection:
        receipt_chunks = 0
        saw_pending_attempts = False

        def execute(self, query, parameters=None):
            if query.lstrip().startswith("INSERT OR REPLACE INTO _receipts"):
                self.receipt_chunks += 1
                if failure == "second_receipt_chunk" and self.receipt_chunks == 2:
                    raise RuntimeError("second receipt chunk failed")
            if query == "COMMIT" and failure == "commit":
                self.saw_pending_attempts = (
                    conn.execute(
                        "SELECT count(*) FROM _reconcile_attempts WHERE last_attempt=?",
                        [attempts[2]],
                    ).fetchone()[0]
                    == 1001
                )
                raise RuntimeError("commit failed")
            return conn.execute(query, parameters or [])

    failing = FailingConnection()
    with pytest.raises(RuntimeError, match="failed"):
        current_store.apply(failing, response, progress, attempts=attempts)
    if failure == "commit":
        assert failing.saw_pending_attempts
    else:
        assert failing.receipt_chunks == 2
    assert _snapshot(conn) == before
    current_store.apply(conn, response, progress, attempts=attempts)
    assert current_store.get_state(conn, "cursor") == 1001
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone()[0] == 1001
