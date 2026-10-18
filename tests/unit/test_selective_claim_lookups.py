"""Selective claim queries retain exact ownership namespaces and multiplicity."""

import duckdb
import pytest

from oddsfox_catalogue import current_store
from oddsfox_catalogue.current_rows import project_response


def connection():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE _identity_claims (venue VARCHAR, market_id VARCHAR, role VARCHAR, "
        "asset_kind VARCHAR, asset_id VARCHAR)"
    )
    return conn


def test_asset_lookup_filters_each_1000_key_chunk_and_keeps_duplicate_owners_and_roles():
    conn = connection()
    asset = "0x" + "f" * 64
    keys = (
        [("v1", "token", asset)]
        + [("v1", "token", f"filler-{index}") for index in range(999)]
        + [("v2", "token", asset)]
    )
    claims = [
        ("v1", "00000000000000000001", "candidate", "token", asset),
        ("v1", "00000000000000000001", "candidate", "token", asset),
        ("v1", "00000000000000000001", "accepted", "token", asset),
        ("v2", "00000000000000000002", "accepted", "token", asset),
        ("v3", "00000000000000000003", "candidate", "token", asset),
        ("v1", "00000000000000000004", "candidate", "other", asset),
    ]
    conn.executemany("INSERT INTO _identity_claims VALUES (?,?,?,?,?)", claims)

    result = current_store._select_claims_by_assets(conn, keys)

    assert sorted(result) == sorted(claims[:4])
    conn.close()


def test_market_lookup_filters_venue_before_accumulating_each_chunk():
    conn = connection()
    market = "98765432109876543210"
    keys = [("v1", f"filler-{index}") for index in range(999)] + [
        ("v1", market),
        ("v2", market),
    ]
    claims = [
        ("v1", market, "candidate", "token", "1"),
        ("v1", market, "candidate", "token", "1"),
        ("v1", market, "accepted", "token", "2"),
        ("v2", market, "accepted", "token", "3"),
        ("v3", market, "candidate", "token", "4"),
    ]
    conn.executemany("INSERT INTO _identity_claims VALUES (?,?,?,?,?)", claims)

    result = current_store._select_claims_by_markets(conn, keys)

    assert sorted(result) == sorted(claims[:4])
    conn.close()


def test_claim_lookups_accept_absent_and_exact_long_string_keys():
    conn = connection()
    market = "9" * 20
    asset = "1" + "0" * 77
    conn.execute(
        "INSERT INTO _identity_claims VALUES (?,?,?,?,?)",
        ["venue", market, "accepted", "position", asset],
    )

    assert current_store._select_claims_by_assets(
        conn, [("venue", "position", asset), ("venue", "position", "missing")]
    ) == [("venue", market, "accepted", "position", asset)]
    assert current_store._select_claims_by_markets(
        conn, [("venue", market), ("venue", "missing")]
    ) == [("venue", market, "accepted", "position", asset)]
    assert current_store._select_claims_by_markets(conn, [("other", market)]) == []
    conn.close()


def test_claim_lookup_failure_rolls_back_receipts_domain_and_generation():
    conn = duckdb.connect(":memory:")
    current_store.initialize(conn)
    response = project_response(
        {
            "id": "9" * 20,
            "version": "v1",
            "conditionId": "0x" + "a" * 64,
            "outcomes": ["Yes", "No"],
            "clobTokenIds": ["1" + "0" * 77, "2" + "0" * 77],
        },
        "markets",
        received_at="2026-10-14T00:00:00Z",
        response_id="claim-lookup-rollback",
        native_market_ids=["9" * 20],
    )

    class FailingLookup:
        def execute(self, query, parameters=None):
            if "FROM _identity_claims" in query and query.startswith("SELECT"):
                raise RuntimeError("synthetic claim lookup failure")
            return conn.execute(query, parameters or [])

        def executemany(self, query, parameters):
            return conn.executemany(query, parameters)

    with pytest.raises(RuntimeError, match="synthetic claim lookup failure"):
        current_store.apply(FailingLookup(), response)

    assert current_store.generation(conn) == 0
    for table in (
        "events",
        "markets",
        "outcomes",
        "event_markets",
        "_receipts",
        "_identity",
        "_identity_claims",
    ):
        assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
    conn.close()
