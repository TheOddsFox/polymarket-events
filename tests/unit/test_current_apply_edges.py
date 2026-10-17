"""Set-based apply edges the oracle comparison does not reach: write chunking, absence, atomicity."""

from datetime import UTC, datetime

import duckdb
import pytest

from fakes.reference_apply import reference_apply
from oddsfox_catalogue import current_store
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import apply, get_state, initialize


def _condition(market_id):
    return f"0x{int(market_id):064x}"


def _market(market_id, *, usable=True):
    return dict(
        venue="polymarket",
        market_id=market_id,
        question=f"Q {market_id}",
        slug=market_id,
        condition_id=_condition(market_id),
        source_market_version="v1",
        protocol="ctf",
        active=True,
        closed=False,
        archived=False,
        usable=usable,
        identity_error=None if usable else "invalid_identity",
    )


def _outcomes(market_id, assets):
    return [
        dict(
            venue="polymarket",
            market_id=market_id,
            condition_id=_condition(market_id),
            outcome_index=index,
            outcome_label=str(index),
            asset_kind="ctf_token",
            asset_id=asset,
            clob_token_id=asset,
            position_id=None,
        )
        for index, asset in enumerate(assets, start=1)
    ]


def _event(event_id, *, title=None):
    return dict(
        venue="polymarket",
        event_id=event_id,
        title=title or f"Event {event_id}",
        slug=event_id,
        active=True,
        closed=False,
        archived=False,
    )


def _assets(market_id):
    return (f"{market_id}1", f"{market_id}2")


def _pair():
    left, right = duckdb.connect(), duckdb.connect()
    initialize(left)
    initialize(right)
    return left, right


def _dump(conn):
    dumped = {}
    for table in (
        "events",
        "markets",
        "event_markets",
        "outcomes",
        "_receipts",
        "_membership",
        "_identity",
        "_identity_claims",
        "_parent_repairs",
        "sync_state",
    ):
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        dumped[table] = sorted(
            tuple(value.astimezone(UTC) if isinstance(value, datetime) else value for value in row)
            for row in rows
        )
    return dumped


def _sequence():
    """Several responses that cross every batched statement, with conflicts and absence."""
    first = ResponseRows(
        received_at="2026-03-01T00:00:00Z",
        response_id="day1",
        events=[_event(str(i)) for i in range(1, 8)],
        markets=[_market(str(m)) for m in range(101, 108)],
        outcomes=[o for m in range(101, 108) for o in _outcomes(str(m), _assets(m))],
        # Overlapping membership: several events share markets, one event is incomplete.
        memberships={
            **{str(i): [str(100 + i), str(101 + i)] for i in range(1, 7)},
            "7": None,
        },
        native_market_ids={str(m) for m in range(101, 108)},
        parent_event_ids={"7"},
    )
    renamed = ResponseRows(
        received_at="2026-03-02T00:00:00Z",
        response_id="day2",
        events=[_event("2", title="Renamed"), _event("3", title="Renamed too")],
        markets=[_market("101", usable=False), _market("106")],
        # Market 106 claims 101's first asset: an ownership conflict on both markets.
        outcomes=_outcomes("106", ("1011", "1062")),
        memberships={"2": ["102"], "3": ["103", "104"]},
        native_market_ids=set(),
    )
    absent = ResponseRows(
        received_at="2026-03-03T00:00:00Z",
        response_id="day3",
        absent_event_ids={"5", "999"},
        absent_market_ids={"104", "999"},
        native_market_ids={"106"},
    )
    # A membership whose event row is not in this response (venue defaults to polymarket).
    orphan = ResponseRows(
        received_at="2026-03-04T00:00:00Z",
        response_id="day4",
        markets=[_market("120")],
        outcomes=_outcomes("120", ("1201", "1202")),
        memberships={"77": ["120"]},
        native_market_ids={"120"},
    )
    stale_replay = ResponseRows(
        received_at="2026-03-01T00:00:00Z",
        response_id="day1",
        events=[_event("1", title="stale")],
        memberships={"1": []},
    )
    return [first, renamed, absent, orphan, stale_replay]


@pytest.mark.parametrize("chunk", [1, 2, 3])
def test_chunked_statements_match_the_oracle_at_every_boundary(monkeypatch, chunk):
    """Tiny chunks exercise every batched select and write loop across many boundaries."""
    monkeypatch.setattr(current_store, "_WRITE_CHUNK", chunk)
    left, right = _pair()
    try:
        for index, response in enumerate(_sequence(), start=1):
            progress = {"cursor": index}
            assert apply(right, response, progress) == reference_apply(left, response, progress)
            assert _dump(right) == _dump(left)
    finally:
        left.close()
        right.close()


def test_absence_confirmation_is_a_receipt_not_a_data_change():
    left, right = _pair()
    try:
        seed = ResponseRows(
            received_at="2026-03-01T00:00:00Z",
            response_id="seed",
            events=[_event("1")],
            markets=[_market("10")],
            outcomes=_outcomes("10", _assets("10")),
            memberships={"1": ["10"]},
            native_market_ids={"10"},
        )
        apply(right, seed)
        reference_apply(left, seed)
        before = _dump(right)
        generation = get_state(right, "generation")

        absent = ResponseRows(
            received_at="2026-03-05T00:00:00Z",
            response_id="absent",
            absent_event_ids={"1", "404"},
            absent_market_ids={"10", "999"},
        )
        result = apply(right, absent)
        assert reference_apply(left, absent) == result
        assert result["changed"] is False
        assert get_state(right, "generation") == generation
        assert _dump(right) == _dump(left)
        after = _dump(right)
        assert {k: v for k, v in after.items() if k not in {"_receipts", "_membership"}} == {
            k: v for k, v in before.items() if k not in {"_receipts", "_membership"}
        }
        receipt = right.execute("SELECT receipt FROM _membership WHERE event_id='1'").fetchone()[0]
        assert receipt.isoformat().startswith("2026-03-05")
        # Unknown IDs are not invented: a 404 for a missing entity leaves no row or receipt.
        assert right.execute("SELECT count(*) FROM events WHERE event_id='404'").fetchone() == (0,)
        assert right.execute("SELECT count(*) FROM markets WHERE market_id='999'").fetchone() == (
            0,
        )
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("fault", ["attempts-statement", "commit"])
def test_failed_apply_rolls_back_rows_progress_generation_and_attempts(fault):
    """The attempt stamp commits with its data; a fault at either point leaves no trace."""
    conn = duckdb.connect()
    try:
        initialize(conn)
        conn.execute(
            "CREATE TABLE _reconcile_attempts (kind VARCHAR, id VARCHAR, "
            "last_attempt TIMESTAMPTZ, PRIMARY KEY(kind, id))"
        )
        seed = ResponseRows(
            received_at="2026-03-01T00:00:00Z",
            response_id="seed",
            events=[_event("1", title="old")],
            markets=[_market("10")],
            outcomes=_outcomes("10", _assets("10")),
            memberships={"1": ["10"]},
        )
        apply(conn, seed, {"cursor": "old"})
        before = _dump(conn)
        attempts_before = conn.execute("SELECT * FROM _reconcile_attempts").fetchall()

        change = ResponseRows(
            received_at="2026-03-02T00:00:00Z",
            response_id="change",
            events=[_event("1", title="new"), _event("2")],
            markets=[_market("10"), _market("11")],
            outcomes=_outcomes("10", ("1011", "1012")) + _outcomes("11", _assets("11")),
            memberships={"1": ["10", "11"], "2": ["11"]},
        )
        when = datetime(2026, 3, 2, tzinfo=UTC)

        class Fault:
            def __init__(self, inner):
                self.inner = inner

            def execute(self, query, params=None):
                normalized = " ".join(query.upper().split())
                if fault == "attempts-statement" and "_RECONCILE_ATTEMPTS" in normalized:
                    raise RuntimeError("injected attempt failure")
                if fault == "commit" and normalized == "COMMIT":
                    raise RuntimeError("injected commit failure")
                return (
                    self.inner.execute(query)
                    if params is None
                    else self.inner.execute(query, params)
                )

            def __getattr__(self, name):
                return getattr(self.inner, name)

        with pytest.raises(RuntimeError, match="injected"):
            apply(Fault(conn), change, {"cursor": "new"}, attempts=("events", ["1", "2"], when))
        assert _dump(conn) == before
        assert conn.execute("SELECT * FROM _reconcile_attempts").fetchall() == attempts_before
        assert get_state(conn, "cursor") == "old"
        assert get_state(conn, "generation") == 1
        # The same response applies cleanly afterwards, proving the rollback left no residue.
        apply(conn, change, {"cursor": "new"}, attempts=("events", ["1", "2"], when))
        assert get_state(conn, "generation") == 2
        assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (2,)
    finally:
        conn.close()
