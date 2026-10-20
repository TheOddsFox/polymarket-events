"""Adversarial semantic checks for outcome grouping and claim-map refactors.

These cases deliberately compare the production writer with the frozen
row-at-a-time implementation, rather than asserting implementation details.
"""

from datetime import UTC, datetime

import duckdb
import pytest

from fakes.reference_apply import reference_apply
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import apply, get_state, initialize

_TABLES = (
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


def _market(venue, market_id, title=None, *, protocol="ctf"):
    condition = "0x" + f"{int(market_id) % (1 << 16):064x}"
    return {
        "venue": venue,
        "market_id": market_id,
        "question": title or f"Question {market_id}",
        "slug": market_id,
        "condition_id": condition,
        "source_market_version": "v1",
        "protocol": protocol,
        "active": True,
        "closed": False,
        "archived": False,
        "usable": True,
        "identity_error": None,
    }


def _outcome(venue, market_id, index, asset, *, kind="ctf_token", label=None):
    return {
        "venue": venue,
        "market_id": market_id,
        "condition_id": _market(venue, market_id)["condition_id"],
        "outcome_index": index,
        "outcome_label": label or f"Outcome {index}",
        "asset_kind": kind,
        "asset_id": asset,
        "clob_token_id": asset if kind == "ctf_token" else None,
        "position_id": asset if kind == "poly_v2_position" else None,
    }


def _response(day, response_id, *, markets=(), outcomes=(), events=(), memberships=None, native=()):
    return ResponseRows(
        received_at=f"2026-04-{day:02d}T00:00:00Z",
        response_id=response_id,
        events=list(events),
        markets=list(markets),
        outcomes=list(outcomes),
        memberships=dict(memberships or {}),
        native_market_ids=set(native),
    )


def _dump(conn):
    result = {}
    for table in _TABLES:
        result[table] = conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
    return result


def _pair():
    oracle, production = duckdb.connect(), duckdb.connect()
    initialize(oracle)
    initialize(production)
    return oracle, production


def _apply_both(oracle, production, response, progress=None):
    expected = reference_apply(oracle, response, progress)
    actual = apply(production, response, progress)
    assert actual == expected
    assert _dump(production) == _dump(oracle)
    return actual


def test_interleaved_duplicate_market_rows_and_stale_outcomes_match_oracle():
    """Outcome association is exact by namespace and retains first-market semantics."""
    oracle, production = _pair()
    try:
        event_rows = [
            {
                "venue": "polymarket",
                "event_id": "e1",
                "title": "P",
                "slug": "p",
                "active": True,
                "closed": False,
                "archived": False,
            },
            {
                "venue": "other",
                "event_id": "e1",
                "title": "O",
                "slug": "o",
                "active": True,
                "closed": False,
                "archived": False,
            },
        ]
        initial = _response(
            2,
            "initial",
            events=event_rows,
            # Duplicate market ID in one response: the first row wins, while
            # outcomes are interleaved and remain associated with their venue.
            markets=[
                _market("polymarket", "10", "first P"),
                _market("other", "10", "first O"),
                _market("polymarket", "10", "ignored duplicate P"),
            ],
            outcomes=[
                _outcome("other", "10", 2, "shared"),
                _outcome("polymarket", "10", 2, "p2"),
                _outcome("other", "10", 1, "other-only"),
                _outcome("polymarket", "10", 1, "p1"),
            ],
            memberships={"e1": ["10"]},
            native={"10"},
        )
        _apply_both(oracle, production, initial, {"cursor": 1})
        assert production.execute(
            "SELECT venue, question FROM markets ORDER BY venue"
        ).fetchall() == [("other", "first O"), ("polymarket", "first P")]
        assert production.execute(
            "SELECT venue, asset_id FROM outcomes ORDER BY venue, outcome_index"
        ).fetchall() == [
            ("other", "other-only"),
            ("other", "shared"),
            ("polymarket", "p1"),
            ("polymarket", "p2"),
        ]

        # A later receipt is authoritative. Replaying an older outcome list
        # must not disturb either market's normalized rows or claim graph.
        stale = _response(
            1,
            "older-response-id",
            markets=[_market("polymarket", "10", "stale")],
            outcomes=[_outcome("polymarket", "10", 1, "wrong")],
            native={"10"},
        )
        before = _dump(production)
        _apply_both(oracle, production, stale)
        assert _dump(production) == before
    finally:
        oracle.close()
        production.close()


@pytest.mark.parametrize("market_order", [("10", "20"), ("20", "10")])
def test_sequential_claim_transfer_preserves_namespaces_and_competing_roles(market_order):
    """Transfers are evaluated sequentially; same strings in other namespaces do not collide."""
    oracle, production = _pair()
    try:
        # A shared asset ID is legal across venues and asset kinds. The second
        # polymarket owner is deliberately represented in both claim roles.
        seed = _response(
            1,
            "seed",
            markets=[
                _market("polymarket", "10"),
                _market("polymarket", "20"),
                _market("other", "10"),
                _market("polymarket", "30", protocol="poly_v2"),
            ],
            outcomes=[
                _outcome("polymarket", "10", 1, "same"),
                _outcome("polymarket", "20", 1, "old20"),
                _outcome("other", "10", 1, "same"),
                _outcome("polymarket", "30", 1, "same", kind="poly_v2_position"),
            ],
            native={"10", "20", "30"},
        )
        _apply_both(oracle, production, seed)

        # A non-native conflicting update creates a retained candidate while
        # the accepted old identity remains claimed. This exercises both claim
        # roles for one owner plus a competing owner on the exact namespace key.
        candidate = _response(
            2,
            "candidate",
            markets=[_market("polymarket", "20")],
            outcomes=[_outcome("polymarket", "20", 1, "same")],
            native=(),
        )
        _apply_both(oracle, production, candidate)
        roles = production.execute(
            "SELECT role, asset_id FROM _identity_claims "
            "WHERE venue='polymarket' AND market_id='20' ORDER BY role, asset_id"
        ).fetchall()
        assert roles == [
            ("accepted", "old20"),
            ("candidate", "same"),
        ]

        # Transfer the exact shared asset through the two markets in each
        # order. The oracle captures the established sequential semantics.
        transfer_outcomes = {
            "10": [_outcome("polymarket", "10", 1, "free")],
            "20": [_outcome("polymarket", "20", 1, "same")],
        }
        transfer = _response(
            3,
            "transfer-" + "-".join(market_order),
            markets=[_market("polymarket", market_id) for market_id in market_order],
            outcomes=[row for market_id in market_order for row in transfer_outcomes[market_id]],
            native={"10", "20"},
        )
        _apply_both(oracle, production, transfer)
        assert _dump(production) == _dump(oracle)
        if market_order[0] == "10":
            # Releasing the old owner first lets the later native confirmation
            # repair market 20's previously disputed candidate identity.
            assert production.execute(
                "SELECT usable, identity_error FROM markets "
                "WHERE venue='polymarket' AND market_id='20'"
            ).fetchone() == (True, None)

        # An empty replacement keeps the previously retained candidate map;
        # its claims remain alongside the accepted map for reconciliation.
        empty = _response(
            4,
            "empty-replacement",
            markets=[_market("polymarket", "20")],
            outcomes=(),
            native={"20"},
        )
        _apply_both(oracle, production, empty)
        remaining = production.execute(
            "SELECT role, asset_id FROM _identity_claims "
            "WHERE venue='polymarket' AND market_id='20' ORDER BY role, asset_id"
        ).fetchall()
        assert remaining == [
            ("accepted", "same" if market_order[0] == "10" else "old20"),
            ("candidate", "same"),
        ]
    finally:
        oracle.close()
        production.close()


def test_failed_late_apply_restores_receipts_memberships_generation_and_checkpoint(monkeypatch):
    """A failure after data writes rolls back every durable part of the response."""
    from oddsfox_catalogue import current_store

    conn = duckdb.connect()
    try:
        initialize(conn)
        seed = _response(
            1,
            "seed",
            events=[
                {
                    "venue": "polymarket",
                    "event_id": "e",
                    "title": "old",
                    "slug": "e",
                    "active": True,
                    "closed": False,
                    "archived": False,
                }
            ],
            markets=[_market("polymarket", "10")],
            outcomes=[_outcome("polymarket", "10", 1, "old")],
            memberships={"e": ["10"]},
            native={"10"},
        )
        apply(conn, seed, {"cursor": 1, "pass": "old"})
        before = _dump(conn)
        original = current_store._write_values
        calls = 0

        def fail_after_first_write(connection, sql, rows, width):
            nonlocal calls
            calls += 1
            original(connection, sql, rows, width)
            if calls == 2:
                raise RuntimeError("injected late write failure")

        monkeypatch.setattr(current_store, "_write_values", fail_after_first_write)
        replacement = _response(
            2,
            "replacement",
            events=[
                {
                    "venue": "polymarket",
                    "event_id": "e",
                    "title": "new",
                    "slug": "e",
                    "active": True,
                    "closed": False,
                    "archived": False,
                }
            ],
            markets=[_market("polymarket", "10"), _market("polymarket", "11")],
            outcomes=[
                _outcome("polymarket", "10", 1, "new"),
                _outcome("polymarket", "11", 1, "other"),
            ],
            memberships={"e": ["10", "11"]},
            native={"10", "11"},
        )
        with pytest.raises(RuntimeError, match="late write failure"):
            apply(conn, replacement, {"cursor": 2, "pass": "new"})
        assert _dump(conn) == before
        assert get_state(conn, "cursor") == 1
        assert get_state(conn, "pass") == "old"
        assert get_state(conn, "generation") == 1
        assert conn.execute(
            "SELECT receipt, response FROM _membership WHERE event_id='e'"
        ).fetchone() == (datetime(2026, 4, 1, tzinfo=UTC), "seed")
    finally:
        conn.close()


def test_equal_outcome_indices_keep_stable_candidate_order_and_match_oracle():
    """Conflicting claims keep duplicate-index candidates durable in stable order."""
    oracle, production = _pair()
    try:
        seed = _response(
            1,
            "seed",
            markets=[_market("polymarket", "10")],
            outcomes=[
                _outcome("polymarket", "10", 1, "first"),
                _outcome("polymarket", "10", 2, "second"),
            ],
            native={"10"},
        )
        _apply_both(oracle, production, seed)
        before = _dump(production)
        ambiguous = _response(
            2,
            "duplicate-index",
            markets=[_market("polymarket", "11")],
            outcomes=[
                _outcome("polymarket", "11", 1, "first"),
                _outcome("polymarket", "11", 1, "second"),
            ],
            native=(),
        )
        _apply_both(oracle, production, ambiguous)
        assert _dump(production) != before
        import json

        candidate = production.execute(
            "SELECT candidate FROM _identity WHERE venue='polymarket' AND market_id='11'"
        ).fetchone()[0]
        assert [row["asset_id"] for row in json.loads(candidate)] == ["first", "second"]
    finally:
        oracle.close()
        production.close()
