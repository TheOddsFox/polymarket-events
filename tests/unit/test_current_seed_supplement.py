"""Only required missing markets may supplement immutable Parquet seed rows."""

import hashlib

import duckdb
import pytest
from test_current_import import add_event_page, release_fixture

from oddsfox_catalogue import current_import as importer
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings
from oddsfox_catalogue.current_contract import CurrentError


def market(mid="9", **changes):
    return {
        "id": mid,
        "question": "nested",
        "version": "v2",
        "outcomes": ["Yes", "No"],
        "positionIds": [str(2**256 - 1), str(2**256 - 2)],
        "volume": {"ignored": "bad financial data"},
        **changes,
    }


def setup(tmp_path, records=None):
    release, checksum, conn = release_fixture(tmp_path, usable=True)
    raw = tmp_path / "raw"
    if records is not None:
        checksum = add_event_page(release, raw, records)
    settings = CurrentSettings(tmp_path / "current")
    args = dict(manifest_sha256=checksum, raw_root=raw)
    assert importer.import_release(settings, conn, release, **args)["phase"] == "evidence"
    return settings, conn, release, args


def finish(seed):
    settings, conn, release, args = seed
    assert importer.import_release(settings, conn, release, **args)["phase"] == "verify"
    assert store.generation(conn) == 0
    assert store.get_state(conn, "seed") is None
    return importer.import_release(settings, conn, release, **args)


def mock_pages(monkeypatch, pages):
    def read(*args):
        for day, response_id, records in pages:
            receipt = f"2026-10-{day}T00:00:00Z"
            args[-1](records, receipt, response_id)
            yield importer._membership_response(
                records, received_at=receipt, response_id=response_id
            )

    monkeypatch.setattr(importer, "_event_pages", read)


@pytest.mark.parametrize("mid", ["3", str(2**64 + 1)])
def test_inventoried_supplement_preserves_baseline_receipts_and_frontiers(tmp_path, mid):
    seed = setup(
        tmp_path,
        [
            {
                "id": "1",
                "title": {"ignored": "bad retained baseline metadata"},
                "markets": [{"id": "2", "question": {}, "conditionId": {}}, market(mid)],
            }
        ],
    )
    _, conn, _, _ = seed
    baseline = {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
        for table in ("events", "markets", "outcomes", "_receipts", "_identity", "_identity_claims")
    }
    result = finish(seed)
    for table, expected in baseline.items():
        predicate = (
            ""
            if table == "events"
            else ("WHERE id IN ('1','2')" if table == "_receipts" else "WHERE market_id='2'")
        )
        assert (
            conn.execute(f"SELECT * FROM {table} {predicate} ORDER BY ALL").fetchall() == expected
        )
    assert conn.execute(
        "SELECT market_id FROM event_markets ORDER BY market_id"
    ).fetchall() == sorted([("2",), (mid,)])
    assert conn.execute("SELECT usable FROM markets WHERE market_id=?", [mid]).fetchone() == (True,)
    assert conn.execute(
        "SELECT asset_id FROM outcomes WHERE market_id=? ORDER BY outcome_index", [mid]
    ).fetchall() == [(str(2**256 - 1),), (str(2**256 - 2),)]
    assert result["seed"]["frontiers"] == {"events": 3, "markets": 4}
    assert result["seed"]["supplement"] == {
        "policy": "verified-event-required-markets-v1",
        "markets": 1,
        "market_ids_sha256": hashlib.sha256((mid + "\n").encode()).hexdigest(),
    }
    assert (
        conn.execute("SELECT receipt::DATE,response FROM _receipts WHERE id=?", [mid])
        .fetchone()[1]
        .startswith("legacy-event:")
    )
    assert store.validate_store(conn)["valid"]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("invalid", [False, True])
def test_latest_receipt_page_tie_and_invalid_winner_are_order_independent(
    tmp_path, monkeypatch, reverse, invalid
):
    seed = setup(tmp_path)
    pages = [
        ("13", "legacy-event:z", [{"id": "1", "markets": [market(question="older")]}]),
        ("14", "legacy-event:a", [{"id": "1", "markets": [market(question="loser")]}]),
        (
            "14",
            "legacy-event:z",
            [
                {
                    "id": "1",
                    "markets": [
                        market(question=None, positionIds=None)
                        if invalid
                        else market(question="winner")
                    ],
                }
            ],
        ),
    ]
    mock_pages(monkeypatch, pages[::-1] if reverse else pages)
    finish(seed)
    conn = seed[1]
    assert conn.execute("SELECT question,usable FROM markets WHERE market_id='9'").fetchone() == (
        None if invalid else "winner",
        not invalid,
    )
    assert conn.execute("SELECT response FROM _receipts WHERE id='9'").fetchone() == (
        "legacy-event:z",
    )
    if invalid:
        assert conn.execute("SELECT count(*) FROM outcomes WHERE market_id='9'").fetchone() == (0,)
        assert conn.execute(
            "SELECT invalidated_at IS NOT NULL FROM _identity WHERE market_id='9'"
        ).fetchone() == (True,)


def test_historical_or_incomplete_only_children_are_not_backfilled(tmp_path, monkeypatch):
    seed = setup(tmp_path)
    mock_pages(
        monkeypatch,
        [
            ("12", "legacy-event:old", [{"id": "1", "markets": [market("8")]}]),
            ("13", "legacy-event:complete", [{"id": "1", "markets": [market("9")]}]),
            (
                "14",
                "legacy-event:incomplete",
                [{"id": "1", "markets": [market("10")], "hasMore": True}],
            ),
        ],
    )
    finish(seed)
    assert seed[1].execute("SELECT market_id FROM markets ORDER BY market_id").fetchall() == [
        ("2",),
        ("9",),
    ]
    assert seed[1].execute("SELECT unresolved FROM _membership").fetchone() == (True,)


def test_same_response_retained_conflicts_roll_back_and_retry(tmp_path, monkeypatch):
    seed = setup(tmp_path)
    mock_pages(
        monkeypatch,
        [
            (
                "13",
                "legacy-event:a",
                [
                    {
                        "id": "1",
                        "markets": [market(), market(question="conflict")],
                    }
                ],
            )
        ],
    )
    with pytest.raises(CurrentError, match="conflicting market 9"):
        finish(seed)
    conn = seed[1]
    assert store.get_state(conn, "seed_pending")["stage"] == "evidence"
    assert conn.execute("SELECT market_id FROM markets").fetchall() == [("2",)]
    assert conn.execute("SELECT count(*) FROM event_markets").fetchone() == (0,)
    mock_pages(monkeypatch, [("13", "legacy-event:a", [{"id": "1", "markets": [market()]}])])
    assert finish(seed)["valid"]


def test_supplement_collision_quarantines_all_owners_preserving_claims(tmp_path, monkeypatch):
    seed = setup(tmp_path)
    child = market(
        version="v1", conditionId="0x" + "1" * 64, clobTokenIds=["123", "456"], positionIds=None
    )
    mock_pages(monkeypatch, [("13", "legacy-event:a", [{"id": "1", "markets": [child]}])])
    finish(seed)
    conn = seed[1]
    assert conn.execute(
        "SELECT market_id,usable,identity_error FROM markets ORDER BY market_id"
    ).fetchall() == [
        ("2", False, "asset_ownership_conflict"),
        ("9", False, "asset_ownership_conflict"),
    ]
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    assert conn.execute(
        "SELECT count(*) FROM _identity_claims WHERE asset_id='123'"
    ).fetchone() == (4,)
    assert conn.execute(
        "SELECT count(*) FROM _identity WHERE accepted IS NOT NULL AND invalidated_at IS NOT NULL"
    ).fetchone() == (2,)


def test_packed_keys_keep_uint64_boundaries_and_canonical_ids_exact():
    with duckdb.connect() as conn:
        store.initialize(conn)
        values = ["1", str(2**64 - 1), str(2**64), str(2**64 + 1), "99999999999999999999"]
        conn.executemany(
            "INSERT INTO markets (venue,market_id) VALUES ('polymarket',?)", [(v,) for v in values]
        )
        keys = importer._seed_market_keys(conn)
        assert sum(len(bucket) for bucket in keys.values()) == len(values)
        assert all(bucket.itemsize == 8 for bucket in keys.values())
        assert all(importer._seed_has_market(keys, value) for value in values)
        assert not any(
            importer._seed_has_market(keys, value)
            for value in ["2", str(2**64 - 2), str(2**64 + 2), str(3 * 2**64)]
        )
        with pytest.raises(CurrentError, match="canonical decimal"):
            importer._seed_has_market(keys, "01")


def test_old_private_protocol_requires_fresh_root(tmp_path):
    seed = setup(tmp_path)
    settings, conn, release, args = seed
    pending = store.get_state(conn, "seed_pending")
    store.set_state(conn, "seed_pending", {**pending, "version": 1})
    with pytest.raises(CurrentError, match="fresh current root"):
        importer.import_release(settings, conn, release, **args)


def test_late_evidence_failure_rolls_back_supplement_rows_and_claims(tmp_path, monkeypatch):
    seed = setup(tmp_path, [{"id": "1", "markets": [market()]}])
    original = importer._finish_memberships

    def fail(conn, **kwargs):
        original(conn, **kwargs)
        assert conn.execute("SELECT count(*) FROM markets WHERE market_id='9'").fetchone() == (1,)
        raise RuntimeError("late supplemental failure")

    monkeypatch.setattr(importer, "_finish_memberships", fail)
    with pytest.raises(CurrentError, match="late supplemental failure"):
        finish(seed)
    conn = seed[1]
    for table in ("markets", "outcomes", "_identity", "_identity_claims"):
        assert conn.execute(f"SELECT count(*) FROM {table} WHERE market_id='9'").fetchone() == (0,)
    assert conn.execute("SELECT count(*) FROM _receipts WHERE id='9'").fetchone() == (0,)
    assert store.generation(conn) == 0
    assert store.get_state(conn, "seed_pending")["stage"] == "evidence"
    monkeypatch.setattr(importer, "_finish_memberships", original)
    assert finish(seed)["valid"]


def test_irrelevant_and_superseded_malformed_retained_fields_are_ignored(tmp_path, monkeypatch):
    seed = setup(tmp_path)
    mock_pages(
        monkeypatch,
        [
            (
                "11",
                "legacy-event:history",
                [{"id": "1", "markets": [market("8", question={}), market("9", question={})]}],
            ),
            (
                "12",
                "legacy-event:incomplete",
                [{"id": "1", "markets": [market("10", question={})], "hasMore": True}],
            ),
            ("13", "legacy-event:winner", [{"id": "1", "markets": [market("9")]}]),
        ],
    )
    assert finish(seed)["valid"]
    assert seed[1].execute(
        "SELECT market_id,question FROM markets ORDER BY market_id"
    ).fetchall() == [("2", "seed market"), ("9", "nested")]


def test_latest_required_malformed_retained_fields_fail_atomically(tmp_path, monkeypatch):
    seed = setup(tmp_path)
    mock_pages(
        monkeypatch,
        [
            ("12", "legacy-event:old", [{"id": "1", "markets": [market()]}]),
            ("13", "legacy-event:winner", [{"id": "1", "markets": [market(question={})]}]),
        ],
    )
    with pytest.raises(CurrentError, match="question has an invalid retained field type"):
        finish(seed)
    assert seed[1].execute("SELECT market_id FROM markets").fetchall() == [("2",)]
    assert store.get_state(seed[1], "seed_pending")["stage"] == "evidence"


def test_unusable_baseline_and_supplement_preserve_meaningful_identity_errors(
    tmp_path, monkeypatch
):
    seed = setup(tmp_path)
    conn = seed[1]
    conn.execute("UPDATE markets SET usable=false,identity_error='baseline reason'")
    conn.execute("DELETE FROM outcomes")
    mock_pages(
        monkeypatch,
        [("13", "legacy-event:winner", [{"id": "1", "markets": [market(positionIds=None)]}])],
    )
    assert finish(seed)["valid"]
    assert conn.execute("SELECT identity_error FROM markets WHERE market_id='2'").fetchone() == (
        "baseline reason",
    )
    assert conn.execute("SELECT identity_error FROM markets WHERE market_id='9'").fetchone() == (
        "v2 market lacks positionIds",
    )


def test_identity_phase_failure_keeps_private_evidence_for_retry(tmp_path, monkeypatch):
    seed = setup(tmp_path, [{"id": "1", "markets": [market()]}])
    settings, conn, release, args = seed
    assert importer.import_release(settings, conn, release, **args)["phase"] == "verify"
    tables = ("markets", "outcomes", "_identity", "_identity_claims", "_receipts")
    before = {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() for table in tables
    }
    original = importer._identity_checks

    def fail(conn):
        original(conn)
        conn.execute("UPDATE markets SET usable=false WHERE market_id='9'")
        raise RuntimeError("identity verification failure")

    monkeypatch.setattr(importer, "_identity_checks", fail)
    with pytest.raises(CurrentError, match="identity verification failure"):
        importer.import_release(settings, conn, release, **args)
    assert store.get_state(conn, "seed_pending")["stage"] == "verify"
    assert store.generation(conn) == 0
    assert store.get_state(conn, "seed") is None
    assert before == {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() for table in tables
    }
    monkeypatch.setattr(importer, "_identity_checks", original)

    def no_replay(*args):
        raise AssertionError("phase C replayed raw pages")

    monkeypatch.setattr(importer, "_event_pages", no_replay)
    assert importer.import_release(settings, conn, release, **args)["valid"]
