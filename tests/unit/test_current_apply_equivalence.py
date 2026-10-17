"""The set-based apply must match the frozen row-at-a-time oracle."""

import random
from datetime import UTC, datetime

import duckdb

from fakes.reference_apply import reference_apply
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import apply, initialize, record_attempts

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
    "sync_state",
)


def _condition(seed):
    return "0x" + f"{seed % 16:x}" * 64


def _market(venue, market_id, assets, *, usable=True, day=1):
    return dict(
        venue=venue,
        market_id=market_id,
        question=f"Q {market_id}",
        slug=market_id,
        condition_id=_condition(int(market_id)),
        source_market_version="v1",
        protocol="ctf",
        active=day % 2 == 0,
        closed=False,
        archived=False,
        usable=usable,
        identity_error=None if usable else "invalid_identity",
    )


def _outcomes(venue, market_id, assets):
    return [
        dict(
            venue=venue,
            market_id=market_id,
            condition_id=_condition(int(market_id)),
            outcome_index=index,
            outcome_label=str(index),
            asset_kind="ctf_token",
            asset_id=asset,
            clob_token_id=asset,
            position_id=None,
        )
        for index, asset in enumerate(assets, start=1)
    ]


def _response(rng, day, *, conflict=False, truncated=False, absent=False, repair=False):
    venue = "polymarket"
    event_id = str(rng.randint(1, 4))
    market_id = str(rng.randint(10, 16))
    assets = (str(1000 + rng.randint(0, 8)), str(2000 + rng.randint(0, 8)))
    if conflict:
        assets = ("1001", "1002")
        market_id = str(10 + (day % 3))
    usable = rng.random() > 0.15
    events = [
        dict(
            venue=venue,
            event_id=event_id,
            title=f"Event {event_id}",
            slug=event_id,
            active=True,
            closed=day % 5 == 0,
            archived=False,
        )
    ]
    markets = [_market(venue, market_id, assets, usable=usable)]
    outcomes = _outcomes(venue, market_id, assets) if usable else []
    members = None if truncated else ([] if rng.random() < 0.1 else [market_id])
    native = {market_id} if rng.random() < 0.5 else set()
    return ResponseRows(
        received_at=f"2026-01-{day:02d}T00:00:00Z",
        response_id=f"{day}-{rng.randint(0, 3)}",
        events=events,
        markets=markets,
        outcomes=outcomes,
        memberships={event_id: members},
        native_market_ids=native,
        parent_event_ids={event_id} if repair else set(),
        absent_event_ids={event_id} if absent else set(),
        absent_market_ids={market_id} if absent else set(),
    )


def _dump(conn):
    dumped = {}
    for table in TABLES:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        description = [item[0] for item in conn.description]
        dumped[table] = sorted(
            tuple(value.astimezone(UTC) if isinstance(value, datetime) else value for value in row)
            for row in rows
        )
        dumped[table + ":columns"] = description
    dumped["generation"] = conn.execute(
        "SELECT value FROM sync_state WHERE key='generation'"
    ).fetchone()[0]
    return dumped


def _pair():
    left, right = duckdb.connect(), duckdb.connect()
    initialize(left)
    initialize(right)
    return left, right


def test_randomized_sequences_match_the_oracle():
    rng = random.Random(7)
    left, right = _pair()
    try:
        for day in range(1, 28):
            response = _response(
                rng,
                day,
                conflict=day in {8, 9, 14},
                truncated=day in {4, 15},
                absent=day in {18, 22},
                repair=day in {6, 19},
            )
            progress = {"cursor": day}
            when = datetime(2026, 1, day, 1, tzinfo=UTC)
            attempts = ("events", [response.events[0]["event_id"]], when)
            if day == 1:
                left.execute(
                    "CREATE TABLE _reconcile_attempts (kind VARCHAR, id VARCHAR, "
                    "last_attempt TIMESTAMPTZ, PRIMARY KEY(kind, id))"
                )
                right.execute(
                    "CREATE TABLE _reconcile_attempts (kind VARCHAR, id VARCHAR, "
                    "last_attempt TIMESTAMPTZ, PRIMARY KEY(kind, id))"
                )
            oracle = reference_apply(left, response, progress)
            record_attempts(left, *attempts)
            produced = apply(right, response, progress, attempts=attempts)
            assert produced == oracle
            assert _dump(left) == _dump(right)
    finally:
        left.close()
        right.close()


def test_replay_equal_stamp_and_older_stamp_match():
    left, right = _pair()
    try:
        first = _response(random.Random(1), 3, repair=True)
        reference_apply(left, first, {"cursor": "a"})
        apply(right, first, {"cursor": "a"})
        again = ResponseRows(**{**first.__dict__})
        reference_apply(left, again)
        apply(right, again)
        older = ResponseRows(
            received_at="2026-01-01T00:00:00Z",
            response_id="0",
            events=list(first.events),
            markets=list(first.markets),
            outcomes=list(first.outcomes),
            memberships=dict(first.memberships),
            native_market_ids=set(first.native_market_ids),
        )
        assert reference_apply(left, older) == apply(right, older)
        assert _dump(left) == _dump(right)
    finally:
        left.close()
        right.close()


class _Counting:
    def __init__(self, conn):
        self.conn = conn
        self.calls = 0

    def execute(self, *args, **kwargs):
        self.calls += 1
        return self.conn.execute(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.conn, name)


def _wide(count):
    events, markets, outcomes, members = [], [], [], {}
    for index in range(count):
        event_id, market_id = str(index + 1), str(10_000 + index)
        events.append(
            dict(
                venue="polymarket",
                event_id=event_id,
                title=event_id,
                active=True,
                closed=False,
                archived=False,
            )
        )
        markets.append(_market("polymarket", market_id, (market_id,)))
        outcomes.extend(_outcomes("polymarket", market_id, (str(index + 1), str(index + 5000))))
        members[event_id] = [market_id]
    return ResponseRows(
        received_at="2026-02-01T00:00:00Z",
        response_id="wide",
        events=events,
        markets=markets,
        outcomes=outcomes,
        memberships=members,
        native_market_ids={row["market_id"] for row in markets},
    )


def test_statement_count_does_not_grow_with_row_count():
    small, wide = duckdb.connect(), duckdb.connect()
    initialize(small)
    initialize(wide)
    counted_small, counted_wide = _Counting(small), _Counting(wide)
    try:
        apply(counted_small, _wide(30))
        apply(counted_wide, _wide(300))
    finally:
        small.close()
        wide.close()
    assert counted_wide.calls <= 80
    assert counted_wide.calls <= counted_small.calls + 15
