"""Application invariants that narrowed reads and receipt transport must preserve."""

from copy import deepcopy
from datetime import UTC, datetime

import duckdb
import pytest
from test_current_store import packet

from fakes.reference_apply import reference_apply
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_contract import ResponseRows


def _snapshot(conn):
    return {
        table: conn.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
        for (table,) in conn.execute("SHOW TABLES").fetchall()
    }


@pytest.fixture
def pair():
    reference, current = duckdb.connect(), duckdb.connect()
    for conn in (reference, current):
        store.initialize(conn)
        conn.execute(
            "CREATE TABLE _reconcile_attempts (kind VARCHAR, id VARCHAR, "
            "last_attempt TIMESTAMPTZ, PRIMARY KEY(kind,id))"
        )
    try:
        yield reference, current
    finally:
        reference.close()
        current.close()


def _apply_pair(pair, response, progress=None, attempts=None):
    reference, current = pair
    original = deepcopy(response)
    expected = reference_apply(reference, response, progress)
    if attempts:
        store.record_attempts(reference, *attempts)
    actual = store.apply(current, response, progress, attempts=attempts)
    assert response == original
    assert actual == expected
    assert _snapshot(current) == _snapshot(reference)
    return actual


def _membership(day, members):
    return ResponseRows(
        received_at=f"2026-01-{day:02d}T00:00:00Z",
        response_id=f"membership-{day}",
        memberships={"1": members},
    )


def test_membership_only_key_preserves_newer_observation_without_public_event(pair):
    reference, current = pair
    _apply_pair(pair, _membership(1, ["2"]))
    _apply_pair(pair, _membership(4, None))
    for conn in pair:
        conn.execute("INSERT INTO _parent_repairs VALUES ('1')")

    # A complete response may renew membership while an even newer incomplete
    # observation continues to mark it unresolved.
    _apply_pair(pair, _membership(3, ["3"]), {"cursor": "membership-only"})
    assert current.execute("SELECT * FROM events").fetchall() == []
    assert current.execute("SELECT * FROM event_markets").fetchall() == [("polymarket", "1", "3")]
    assert current.execute(
        "SELECT receipt,response,unresolved,latest_receipt,latest_response FROM _membership"
    ).fetchone() == (
        datetime(2026, 1, 3, tzinfo=UTC),
        "membership-3",
        True,
        datetime(2026, 1, 4, tzinfo=UTC),
        "membership-4",
    )
    assert current.execute("SELECT * FROM _parent_repairs").fetchall() == []

    # Absence cannot renew a membership whose public event never existed.
    before = current.execute("SELECT * FROM _membership").fetchall()
    _apply_pair(
        pair,
        ResponseRows(
            received_at="2026-01-05T00:00:00Z",
            response_id="absent-orphan",
            absent_event_ids={"1"},
        ),
    )
    assert current.execute("SELECT * FROM _membership").fetchall() == before
    assert _snapshot(current) == _snapshot(reference)


def test_confirmed_absence_preserves_latest_unresolved_state_and_unknown_ids(pair):
    _apply_pair(pair, packet())
    _apply_pair(pair, _membership(4, None))
    _, current = pair
    generation = store.generation(current)
    event_receipt = current.execute(
        "SELECT receipt,response FROM _receipts WHERE kind='event'"
    ).fetchone()

    for day in (3, 2):
        _apply_pair(
            pair,
            ResponseRows(
                received_at=f"2026-01-{day:02d}T00:00:00Z",
                response_id=f"absent-{day}",
                absent_event_ids={"1", "404"},
                absent_market_ids={"2", "999"},
            ),
            {"full_pass": {"cursor_after": day}},
            ("events", ["1"], datetime(2026, 1, day, tzinfo=UTC)),
        )

    assert store.generation(current) == generation
    assert (
        current.execute("SELECT receipt,response FROM _receipts WHERE kind='event'").fetchone()
        == event_receipt
    )
    assert current.execute(
        "SELECT receipt,response,unresolved,latest_receipt,latest_response FROM _membership"
    ).fetchone() == (
        datetime(2026, 1, 3, tzinfo=UTC),
        "absent-3",
        True,
        datetime(2026, 1, 4, tzinfo=UTC),
        "membership-4",
    )
    assert current.execute(
        "SELECT kind,id,response FROM _receipts ORDER BY kind,id"
    ).fetchall() == [("event", "1", "1"), ("market", "2", "absent-3")]


def test_competing_identity_without_public_market_still_reserves_assets(pair):
    _apply_pair(pair, packet(native=True))
    for conn in pair:
        conn.execute("DELETE FROM outcomes WHERE market_id='2'")
        conn.execute("DELETE FROM markets WHERE market_id='2'")

    _apply_pair(pair, packet(2, market="3", native=True))
    _, current = pair
    assert current.execute("SELECT market_id,usable FROM markets").fetchall() == [("3", False)]
    assert current.execute(
        "SELECT market_id,invalidated_response FROM _identity ORDER BY market_id"
    ).fetchall() == [("2", "2"), ("3", "2")]
    assert current.execute(
        "SELECT market_id,role,count(*) FROM _identity_claims GROUP BY ALL ORDER BY ALL"
    ).fetchall() == [
        ("2", "accepted", 2),
        ("2", "candidate", 2),
        ("3", "candidate", 2),
    ]
    assert current.execute("SELECT * FROM outcomes").fetchall() == []


@pytest.mark.parametrize("incoming", ["none", "unrelated", "shared"])
def test_absence_only_identity_cannot_reenter_the_conflict_graph(pair, incoming):
    _apply_pair(pair, packet(native=True))
    _apply_pair(pair, packet(2, market="3", native=True))
    _, current = pair
    invalidations = current.execute(
        "SELECT market_id,invalidated_at,invalidated_response FROM _identity ORDER BY market_id"
    ).fetchall()
    generation = store.generation(current)

    if incoming == "unrelated":
        response = packet(3, market="4", assets=("104", "105"), native=True)
    elif incoming == "shared":
        response = packet(3, market="3", native=True)
    else:
        response = ResponseRows(received_at="2026-01-03T00:00:00Z", response_id="absent-2")
    response.absent_market_ids = {"2"}
    _apply_pair(pair, response, {"cursor": "absence-settled"})

    # An absent market participates only in receipt renewal. Its former claims
    # must not pull an otherwise unrelated ownership graph into this response.
    expected = (
        [(row[0], datetime(2026, 1, 3, tzinfo=UTC), "3") for row in invalidations]
        if incoming == "shared"
        else invalidations
    )
    assert (
        current.execute(
            "SELECT market_id,invalidated_at,invalidated_response FROM _identity "
            "WHERE market_id IN ('2','3') ORDER BY market_id"
        ).fetchall()
        == expected
    )
    assert all(row[1:] == (datetime(2026, 1, 2, tzinfo=UTC), "2") for row in invalidations)
    assert store.generation(current) == generation + int(incoming == "unrelated")
    assert current.execute(
        "SELECT receipt,response FROM _receipts WHERE kind='market' AND id='2'"
    ).fetchone() == (datetime(2026, 1, 3, tzinfo=UTC), response.response_id)


def test_nullable_stored_event_is_present_and_renewal_is_not_a_domain_change(pair):
    event = {"venue": "polymarket", "event_id": "99999999999999999999"}
    _apply_pair(
        pair,
        ResponseRows(received_at="2026-01-01T00:00:00Z", response_id="first", events=[event]),
    )
    result = _apply_pair(
        pair,
        ResponseRows(received_at="2026-01-02T00:00:00Z", response_id="second", events=[event]),
    )
    assert result["changed"] is False
    assert result["generation"] == 1
    assert pair[1].execute("SELECT title,slug,active,closed,archived FROM events").fetchone() == (
        None,
        None,
        None,
        None,
        None,
    )


def test_exact_ids_namespaces_and_microsecond_receipts_survive_replay(pair):
    key = "99999999999999999999"
    response = ResponseRows(
        received_at="2026-01-02T05:30:00.123456+05:30",
        response_id="receipt-雪\n",
        events=[
            {"venue": "polymarket", "event_id": key, "title": None},
            {"venue": "other-雪", "event_id": key, "title": "other"},
        ],
    )
    _apply_pair(pair, response)
    _apply_pair(pair, response)
    earlier_id = deepcopy(response)
    earlier_id.response_id = "receipt-a"
    earlier_id.events[0]["title"] = "must remain stale"
    _apply_pair(pair, earlier_id)

    assert pair[1].execute(
        "SELECT venue,id,receipt,response FROM _receipts ORDER BY venue"
    ).fetchall() == [
        ("other-雪", key, datetime(2026, 1, 2, 0, 0, 0, 123456, tzinfo=UTC), "receipt-雪\n"),
        ("polymarket", key, datetime(2026, 1, 2, 0, 0, 0, 123456, tzinfo=UTC), "receipt-雪\n"),
    ]


@pytest.mark.parametrize("count", [0, 1, 999, 1000, 1001])
def test_receipt_renewal_across_real_chunk_boundaries_keeps_exact_rows(count):
    with duckdb.connect() as conn:
        store.initialize(conn)
        events = [
            {"venue": "polymarket" if index % 2 else "other", "event_id": str(index + 1)}
            for index in range(count)
        ]
        for day in (1, 2):
            result = store.apply(
                conn,
                ResponseRows(
                    received_at=f"2026-01-{day:02d}T00:00:00.123456Z",
                    response_id=f"renew-{day}",
                    events=events,
                ),
                {"cursor": day},
            )
            assert result["generation"] == bool(count)
            assert result["changed"] == bool(count and day == 1)
        assert conn.execute("SELECT count(*) FROM events").fetchone() == (count,)
        receipts = conn.execute("SELECT kind,venue,id,receipt,response FROM _receipts").fetchall()
        assert set(receipts) == {
            (
                "event",
                event["venue"],
                event["event_id"],
                datetime(2026, 1, 2, 0, 0, 0, 123456, tzinfo=UTC),
                "renew-2",
            )
            for event in events
        }
        assert store.get_state(conn, "cursor") == 2


@pytest.mark.parametrize("fault", ["second-receipt-chunk", "commit"])
def test_late_failure_restores_every_table_including_attempts_and_checkpoints(pair, fault):
    _, current = pair
    old_progress = {"full_pass": {"cursor_after": 17}, "fetch_pending": {"raw_id": "old"}}
    _apply_pair(
        pair,
        packet(),
        old_progress,
        ("events", ["1"], datetime(2026, 1, 1, tzinfo=UTC)),
    )
    before = _snapshot(current)
    change = packet(2)
    change.events.extend(
        {"venue": "polymarket", "event_id": str(index), "title": "new"}
        for index in range(2000, 3000)
    )

    class FailingConnection:
        def __init__(self, inner):
            self.inner = inner
            self.receipt_chunks = 0

        def execute(self, query, parameters=None):
            normalized = " ".join(query.upper().split())
            if "INSERT OR REPLACE INTO _RECEIPTS" in normalized:
                self.receipt_chunks += 1
                if fault == "second-receipt-chunk" and self.receipt_chunks == 2:
                    raise duckdb.InvalidInputException("injected later receipt chunk failure")
            if fault == "commit" and normalized == "COMMIT":
                raise duckdb.InvalidInputException("injected commit failure")
            return (
                self.inner.execute(query)
                if parameters is None
                else self.inner.execute(query, parameters)
            )

        def __getattr__(self, name):
            return getattr(self.inner, name)

    faulty = FailingConnection(current)
    with pytest.raises(duckdb.InvalidInputException, match="injected"):
        store.apply(
            faulty,
            change,
            {"full_pass": {"cursor_after": 3000}, "fetch_pending": None},
            attempts=("events", ["1", "2000"], datetime(2026, 1, 2, tzinfo=UTC)),
        )
    assert faulty.receipt_chunks == 2
    assert _snapshot(current) == before
