"""Heap-backed event links retain uniqueness, replacement and atomic progress."""

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue import current_publish
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import CurrentError, projection
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.fingerprints import semantic_fingerprint


def market(key):
    return {
        "id": str(key),
        "version": "v1",
        "conditionId": f"0x{int(key):064x}",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": [str(int(key) * 10 + i) for i in (1, 2)],
    }


def event(keys, key="1"):
    return {"id": key, "title": f"event:{key}", "markets": [market(mid) for mid in keys]}


def response(records, day=1):
    return project_response(
        records, "events", received_at=f"2026-10-{day:02d}T00:00:00Z", response_id=f"response:{day}"
    )


def links(conn):
    return conn.execute(
        "SELECT venue,event_id,market_id,count(*) FROM event_markets GROUP BY ALL ORDER BY ALL"
    ).fetchall()


def snapshot(conn):
    return {
        name: conn.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall()
        for (name,) in conn.execute("SHOW TABLES").fetchall()
    }


@pytest.fixture
def settings(tmp_path):
    settings = CurrentSettings(tmp_path / "current")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
    return settings


def test_duplicate_children_repeated_identical_and_newer_complete_do_not_accumulate(settings):
    with connect_store(settings) as conn:
        for day in (1, 1, 2, 3, 2):
            store.apply(conn, response([event([2, 2, 3, 3])], day))
            assert links(conn) == [("polymarket", "1", "2", 1), ("polymarket", "1", "3", 1)]
            assert store.generation(conn) == 1
            assert store.validate_store(conn)["valid"]
        assert conn.execute("SELECT receipt FROM _membership").fetchone() == (
            datetime(2026, 10, 3, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    "kind", ["replacement", "empty", "missing", "null", "truncated", "bad-child", "nonlist"]
)
def test_complete_replaces_links_and_incomplete_preserves_last_complete(settings, kind):
    with connect_store(settings) as conn:
        store.apply(conn, response([event([2, 3])]))
        incoming = event([3])
        if kind == "empty":
            incoming["markets"] = []
        elif kind == "missing":
            incoming.pop("markets")
        elif kind == "null":
            incoming["markets"] = None
        elif kind == "truncated":
            incoming["hasMore"] = True
        elif kind == "bad-child":
            incoming["markets"].append({"id": True})
        elif kind == "nonlist":
            incoming["markets"] = {}
        store.apply(conn, response([incoming], 2))
        complete = kind in {"replacement", "empty"}
        expected = [] if kind == "empty" else [("polymarket", "1", "3", 1)]
        if not complete:
            expected.insert(0, ("polymarket", "1", "2", 1))
        assert links(conn) == expected
        receipt, latest, unresolved = conn.execute(
            "SELECT receipt,latest_receipt,unresolved FROM _membership"
        ).fetchone()
        assert receipt == datetime(2026, 10, 2 if complete else 1, tzinfo=UTC)
        assert latest == datetime(2026, 10, 2, tzinfo=UTC)
        assert unresolved is not complete
        assert conn.execute("SELECT market_id FROM markets ORDER BY market_id").fetchall() == [
            ("2",),
            ("3",),
        ]
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (4,)
        assert store.validate_store(conn)["valid"]


def test_older_complete_response_cannot_undo_newer_empty_membership(settings):
    with connect_store(settings) as conn:
        store.apply(conn, response([event([2])]))
        store.apply(conn, response([event([])], 3))
        store.apply(conn, response([event([2])], 2))
        assert links(conn) == []
        assert conn.execute("SELECT receipt,unresolved FROM _membership").fetchone() == (
            datetime(2026, 10, 3, tzinfo=UTC),
            False,
        )
        assert store.generation(conn) == 2


def test_same_market_can_belong_to_two_events_without_false_duplicate_detection(settings):
    with connect_store(settings) as conn:
        store.apply(conn, response([event([2]), event([2], "4")]))
        assert links(conn) == [("polymarket", "1", "2", 1), ("polymarket", "4", "2", 1)]
        assert store.validate_store(conn)["valid"]
        current_publish.publish(settings, conn)
        assert current_publish.verify_current(settings)["counts"]["event_markets"] == 2
        store.apply(conn, response([event([])], 2))
        assert links(conn) == [("polymarket", "4", "2", 1)]
        assert conn.execute("SELECT count(*) FROM markets").fetchone() == (1,)
        assert store.validate_store(conn)["valid"]


def test_rehashed_release_still_rejects_duplicate_public_link_key(settings):
    with connect_store(settings) as conn:
        store.apply(conn, response([event([2])]))
        release = Path(current_publish.publish(settings, conn)["path"])
        conn.execute("INSERT INTO event_markets SELECT * FROM event_markets")
        file = release / "event_markets.parquet"
        current_publish._export(conn, "event_markets", file)
        manifest_path = release / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["relations"]["event_markets"] = {
            **semantic_fingerprint(
                conn, f"SELECT {projection('event_markets')} FROM event_markets"
            ),
            **current_publish._physical(file),
        }
        manifest["counts"]["event_markets"] = 2
        manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(CurrentError, match="duplicate event_markets"):
        current_publish.verify_release(release, settings=settings)


@pytest.mark.parametrize("column", range(3), ids=["venue", "event", "market"])
def test_link_null_key_fields_are_rejected_immediately(settings, column):
    values = ["polymarket", "1", "2"]
    values[column] = None
    with connect_store(settings) as conn:
        with pytest.raises(duckdb.ConstraintException, match="NOT NULL"):
            conn.execute("INSERT INTO event_markets VALUES (?,?,?)", values)
        assert links(conn) == []


def test_previous_store_root_is_rejected_without_automatic_migration(tmp_path):
    root = tmp_path / "old"
    root.mkdir()
    stamp = root / "catalogue.json"
    stamp.write_text(json.dumps({"contract": "oddsfox.polymarket.catalogue_store.v3"}))
    database = root / "catalogue.duckdb"
    database.write_bytes(b"preserve previous store")
    before = {path.name: path.read_bytes() for path in root.iterdir()}
    with pytest.raises(CurrentError, match="fresh"):
        validate_root(root, initialize=True)
    assert {path.name: path.read_bytes() for path in root.iterdir()} == before


@pytest.mark.parametrize("fault", ["after-replacement", "commit"])
def test_replacement_failure_rolls_back_links_membership_progress_and_generation(settings, fault):
    with connect_store(settings) as conn:
        store.apply(conn, response([event([2])]), {"cursor": "old"})
        before = snapshot(conn)

        class FaultConnection:
            def execute(self, query, params=None):
                normalized = " ".join(query.upper().split())
                if (
                    fault == "after-replacement"
                    and normalized.startswith("INSERT OR REPLACE INTO _MEMBERSHIP")
                ) or (fault == "commit" and normalized == "COMMIT"):
                    assert links(conn) == [("polymarket", "1", "3", 1)]
                    if fault == "commit":
                        assert store.get_state(conn, "cursor") == "new"
                    raise RuntimeError("link transaction interrupted")
                return conn.execute(query) if params is None else conn.execute(query, params)

            def __getattr__(self, name):
                return getattr(conn, name)

        with pytest.raises(RuntimeError, match="link transaction interrupted"):
            store.apply(FaultConnection(), response([event([3])], 2), {"cursor": "new"})
        assert snapshot(conn) == before
    with connect_store(settings) as conn:
        assert snapshot(conn) == before
        store.apply(conn, response([event([3])], 2), {"cursor": "new"})
        assert links(conn) == [("polymarket", "1", "3", 1)]
        assert store.get_state(conn, "cursor") == "new"
        assert store.generation(conn) == 2
        assert store.validate_store(conn)["valid"]
