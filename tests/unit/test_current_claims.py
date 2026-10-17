"""Claim multiplicity, retained ownership and corruption checks without a claim PK."""

import json
from datetime import UTC, datetime

import duckdb
import pytest

from oddsfox_catalogue import current_publish
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response


def response(day=1, key="2", assets=("11", "12"), *, native=False):
    return project_response(
        {
            "id": key,
            "version": "v1",
            "conditionId": "0x" + "a" * 64,
            "outcomes": ["Yes", "No"],
            "clobTokenIds": list(assets),
        },
        "markets",
        received_at=f"2026-10-{day:02d}T00:00:00Z",
        response_id=f"response-{day}",
        native_market_ids=[key] if native else (),
    )


def claims(conn):
    return conn.execute(
        """SELECT market_id,role,asset_id,count(*) FROM _identity_claims
        GROUP BY ALL ORDER BY market_id,role,asset_id"""
    ).fetchall()


@pytest.fixture
def conn():
    with duckdb.connect(":memory:") as conn:
        store.initialize(conn)
        yield conn


def test_repeated_same_maps_keep_one_claim_per_role_and_asset(conn):
    for day in (1, 2, 2, 1, 3):
        store.apply(conn, response(day))
        assert claims(conn) == [
            ("2", "accepted", "11", 1),
            ("2", "accepted", "12", 1),
            ("2", "candidate", "11", 1),
            ("2", "candidate", "12", 1),
        ]
        candidate, accepted, invalidated = conn.execute(
            "SELECT candidate,accepted,invalidated_at FROM _identity"
        ).fetchone()
        assert candidate == accepted
        assert invalidated is None
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (2,)
        assert store.validate_store(conn)["valid"]


def test_candidate_change_and_malformed_map_keep_accepted_reservation_until_native(conn):
    store.apply(conn, response())
    store.apply(conn, response(2, assets=("21", "22")))
    store.apply(conn, response(3, assets=("bad", "22")))
    assert claims(conn) == [
        ("2", "accepted", "11", 1),
        ("2", "accepted", "12", 1),
        ("2", "candidate", "21", 1),
        ("2", "candidate", "22", 1),
    ]
    candidate, accepted, invalidated = conn.execute(
        "SELECT candidate,accepted,invalidated_at FROM _identity WHERE market_id='2'"
    ).fetchone()
    assert [row["asset_id"] for row in json.loads(candidate)] == ["21", "22"]
    assert [row["asset_id"] for row in json.loads(accepted)] == ["11", "12"]
    assert invalidated == datetime(2026, 10, 2, tzinfo=UTC)
    store.apply(conn, response(4, key="3", native=True))
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", False),
        ("3", False),
    ]
    assert conn.execute(
        "SELECT asset_id FROM _identity_claims WHERE market_id='2' AND role='accepted' ORDER BY asset_id"
    ).fetchall() == [("11",), ("12",)]
    store.apply(conn, response(5, assets=("21", "22"), native=True))
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", True),
        ("3", False),
    ]
    store.apply(conn, response(6, key="3", native=True))
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", True),
        ("3", True),
    ]
    assert claims(conn) == [
        ("2", "accepted", "21", 1),
        ("2", "accepted", "22", 1),
        ("2", "candidate", "21", 1),
        ("2", "candidate", "22", 1),
        ("3", "accepted", "11", 1),
        ("3", "accepted", "12", 1),
        ("3", "candidate", "11", 1),
        ("3", "candidate", "12", 1),
    ]
    assert store.validate_store(conn)["valid"]


@pytest.mark.parametrize("gate", ["validate", "publish"])
@pytest.mark.parametrize("role", ["candidate", "accepted"])
def test_duplicate_claim_corruption_is_rejected_before_publication(tmp_path, gate, role):
    settings = CurrentSettings(tmp_path / "current")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
        store.apply(conn, response())
        current_publish.publish(settings, conn)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    with connect_store(settings) as conn:
        conn.execute(
            "INSERT INTO _identity_claims SELECT * FROM _identity_claims WHERE role=? LIMIT 1",
            [role],
        )
        assert conn.execute("SELECT count(*) FROM _identity_claims").fetchone() == (5,)
        if gate == "validate":
            with pytest.raises(CurrentError, match="integrity|duplicate"):
                store.validate_store(conn)
        else:
            with pytest.raises(CurrentError, match="integrity|duplicate"):
                current_publish.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert current_publish.verify_current(settings)["valid"]


@pytest.mark.parametrize("column", range(5), ids=["venue", "market", "role", "kind", "asset"])
def test_claim_null_fields_are_rejected_immediately(conn, column):
    values = ["polymarket", "2", "candidate", "clob_token", "11"]
    values[column] = None
    with pytest.raises(duckdb.ConstraintException, match="NOT NULL"):
        conn.execute("INSERT INTO _identity_claims VALUES (?,?,?,?,?)", values)
    assert claims(conn) == []


@pytest.mark.parametrize("role", ["", "Candidate", "owner"])
def test_claim_unknown_roles_are_rejected_immediately(conn, role):
    with pytest.raises(duckdb.ConstraintException, match="CHECK"):
        conn.execute(
            "INSERT INTO _identity_claims VALUES ('polymarket','2',?,'clob_token','11')", [role]
        )
    assert claims(conn) == []
