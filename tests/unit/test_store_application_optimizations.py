"""Behavioral checks for the isolated current-store optimization candidates."""

from datetime import UTC, datetime

import duckdb

from fakes.reference_apply import reference_apply
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import apply, initialize

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


def _condition(market_id):
    return "0x" + f"{int(market_id) % 16:x}" * 64


def _market(venue, market_id):
    return {
        "venue": venue,
        "market_id": market_id,
        "question": f"Question {market_id}",
        "slug": f"market-{market_id}",
        "condition_id": _condition(market_id),
        "source_market_version": "v1",
        "protocol": "ctf",
        "active": True,
        "closed": False,
        "archived": False,
        "usable": True,
        "identity_error": None,
    }


def _outcome(venue, market_id, index, asset_kind, asset_id):
    return {
        "venue": venue,
        "market_id": market_id,
        "condition_id": _condition(market_id),
        "outcome_index": index,
        "outcome_label": f"outcome-{index}",
        "asset_kind": asset_kind,
        "asset_id": asset_id,
        "clob_token_id": asset_id if asset_kind == "ctf_token" else None,
        "position_id": asset_id if asset_kind == "position" else None,
    }


def _response(day, response_id, markets_and_outcomes):
    events, markets, outcomes, memberships = [], [], [], {}
    native = set()
    for venue, market_id, assets in markets_and_outcomes:
        event_id = f"{venue}-event-{market_id}"
        events.append(
            {
                "venue": venue,
                "event_id": event_id,
                "title": event_id,
                "slug": event_id,
                "active": True,
                "closed": False,
                "archived": False,
            }
        )
        markets.append(_market(venue, market_id))
        # Deliberately interleave markets and reverse outcome-index order.
        outcomes.extend(
            _outcome(venue, market_id, index, kind, asset) for index, kind, asset in assets
        )
        memberships[event_id] = [market_id]
        native.add(market_id)
    return ResponseRows(
        received_at=f"2026-09-{day:02d}T00:00:00Z",
        response_id=response_id,
        events=events,
        markets=markets,
        outcomes=outcomes,
        memberships=memberships,
        native_market_ids=native,
    )


def _snapshot(conn):
    snapshot = {}
    for table in TABLES:
        rows = conn.execute(f'SELECT * FROM "{table}"').fetchall()
        names = [column[0] for column in conn.description]
        snapshot[table] = sorted(
            tuple(value.astimezone(UTC) if isinstance(value, datetime) else value for value in row)
            for row in rows
        )
        snapshot[f"{table}:columns"] = names
    return snapshot


def test_interleaved_shared_and_namespaced_claims_match_row_oracle():
    reference, optimized = duckdb.connect(), duckdb.connect()
    initialize(reference)
    initialize(optimized)
    try:
        initial = _response(
            1,
            "seed",
            [
                ("polymarket", "1", [(2, "ctf_token", "shared"), (1, "ctf_token", "a")]),
                ("other-venue", "1", [(2, "ctf_token", "shared"), (1, "position", "a")]),
                ("polymarket", "2", [(2, "ctf_token", "b"), (1, "ctf_token", "shared")]),
            ],
        )
        assert reference_apply(reference, initial) == apply(optimized, initial)
        assert _snapshot(reference) == _snapshot(optimized)

        # Update one owner while its competing owner is outside this response.
        # The accepted mapping continues to reserve its former claims until the
        # native confirmation is applied, and the later old receipt is ignored.
        changed = _response(
            2,
            "native-repair",
            [("polymarket", "1", [(2, "ctf_token", "new-a"), (1, "ctf_token", "new-b")])],
        )
        assert reference_apply(reference, changed) == apply(optimized, changed)
        assert _snapshot(reference) == _snapshot(optimized)

        stale = _response(
            1,
            "older",
            [("polymarket", "1", [(2, "ctf_token", "shared"), (1, "ctf_token", "a")])],
        )
        assert reference_apply(reference, stale) == apply(optimized, stale)
        assert _snapshot(reference) == _snapshot(optimized)
    finally:
        reference.close()
        optimized.close()
