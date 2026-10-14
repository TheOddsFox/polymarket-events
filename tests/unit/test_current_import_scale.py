"""Identity seed ranges preserve exact maps, ownership and all-or-nothing recovery."""

import hashlib
import json

import pytest
from test_current_import import descriptor, release_fixture

from oddsfox_catalogue import current_import as importer
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, memory_connection, validate_root
from oddsfox_catalogue.current_contract import SCHEMAS, CurrentError
from oddsfox_catalogue.fingerprints import semantic_fingerprint

MARKETS = 50_001
SHARED_ASSET = "340282366920938463463374607431768211507"


@pytest.fixture(scope="module")
def scale_release(tmp_path_factory):
    directory = tmp_path_factory.mktemp("identity-seed-ranges")
    release, _, conn = release_fixture(directory)
    try:
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET threads=2")
        conn.execute(
            "CREATE TEMP TABLE seed_markets AS SELECT * FROM read_parquet(?) LIMIT 0",
            [str(release / "markets.parquet")],
        )
        conn.execute(
            """INSERT INTO seed_markets
            (venue,market_id,condition_id,usable,last_observed_at,observation_id)
            SELECT 'polymarket',id::VARCHAR,'condition:'||id,id<>100002,
                '2026-10-12T00:00:00Z'::TIMESTAMPTZ,'observation:'||id
            FROM range(100000,150001) ids(id)"""
        )
        conn.execute(
            "CREATE TEMP TABLE seed_outcomes AS SELECT * FROM read_parquet(?) LIMIT 0",
            [str(release / "outcomes.parquet")],
        )
        conn.execute(
            """INSERT INTO seed_outcomes
            (venue,market_id,condition_id,outcome_index,outcome_label,asset_kind,
                asset_id,clob_token_id,position_id)
            SELECT 'polymarket',id::VARCHAR,
                CASE WHEN id=100003 THEN NULL ELSE 'condition:'||id END,
                idx,CASE WHEN idx=0 THEN 'Yes α' WHEN id=100003 THEN NULL ELSE 'No' END,
                'clob_token',asset,asset,NULL
            FROM (SELECT id,idx,
                CASE WHEN id IN (100000,150000) AND idx=0 THEN ?
                ELSE (id*10+idx)::VARCHAR END asset
                FROM range(100000,150001) ids(id) CROSS JOIN range(2) outcomes(idx)
                WHERE id<>100001) ORDER BY idx DESC,id DESC""",
            [SHARED_ASSET],
        )
        manifest = json.loads((release / "release.json").read_text())
        for name in ("markets", "outcomes"):
            path = release / f"{name}.parquet"
            conn.execute(f"COPY seed_{name} TO ? (FORMAT PARQUET)", [str(path)])
            manifest["tables"][name] = {
                **descriptor(path),
                **semantic_fingerprint(conn, f"SELECT * FROM seed_{name}"),
            }
        (release / "release.json").write_text(json.dumps(manifest))
        checksum = hashlib.sha256((release / "release.json").read_bytes()).hexdigest()
    finally:
        conn.close()
    return release, checksum, directory / "raw"


@pytest.fixture
def target(tmp_path):
    settings = CurrentSettings(tmp_path / "current")
    validate_root(settings.root, initialize=True)
    with memory_connection(settings) as conn:
        store.initialize(conn)
        assert conn.execute("SELECT current_setting('threads')").fetchone() == (2,)
        assert conn.execute("SELECT current_setting('memory_limit')").fetchone() == ("1.8 GiB",)
        yield settings, conn


class IdentityBatches:
    def __init__(self, conn, *, fail_second=False):
        self.conn = conn
        self.fail_second = fail_second
        self.rows_per_batch = []

    def execute(self, query, params=None):
        identity_insert = query.split(None, 3)[:3] == ["INSERT", "INTO", "_identity"]
        if identity_insert:
            before = self.conn.execute("SELECT count(*) FROM _identity").fetchone()[0]
            if self.fail_second and self.rows_per_batch:
                assert before == 50_000
                raise RuntimeError("second identity range failed")
        result = self.conn.execute(query) if params is None else self.conn.execute(query, params)
        if identity_insert:
            after = self.conn.execute("SELECT count(*) FROM _identity").fetchone()[0]
            self.rows_per_batch.append(after - before)
        return result

    def __getattr__(self, name):
        return getattr(self.conn, name)


def expected_map(key):
    return [
        {
            "venue": "polymarket",
            "market_id": str(key),
            "condition_id": None if key == 100003 else f"condition:{key}",
            "outcome_index": index,
            "outcome_label": "Yes α" if index == 0 else None if key == 100003 else "No",
            "asset_kind": "clob_token",
            "asset_id": SHARED_ASSET
            if key in (100000, 150000) and index == 0
            else str(key * 10 + index),
            "clob_token_id": SHARED_ASSET
            if key in (100000, 150000) and index == 0
            else str(key * 10 + index),
            "position_id": None,
        }
        for index in range(2)
    ]


def test_identity_seed_crosses_range_boundary_without_losing_maps_or_conflicts(
    target, scale_release
):
    settings, conn = target
    release, checksum, raw = scale_release
    batches = IdentityBatches(conn)
    result = importer.import_release(
        settings, batches, release, manifest_sha256=checksum, raw_root=raw
    )
    assert result["valid"]
    assert batches.rows_per_batch == [50_000, 1]
    assert conn.execute("SELECT count(*) FROM _identity").fetchone() == (MARKETS,)
    assert conn.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (MARKETS - 4,)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (2 * (MARKETS - 4),)
    assert conn.execute("SELECT count(*) FROM _identity_claims").fetchone() == (4 * (MARKETS - 2),)
    for key in (100000, 100003, 149999, 150000):
        candidate, accepted, invalidated, response = conn.execute(
            "SELECT candidate,accepted,invalidated_at,invalidated_response FROM _identity WHERE market_id=?",
            [str(key)],
        ).fetchone()
        assert json.loads(candidate) == json.loads(accepted) == expected_map(key)
        if key in (100000, 150000):
            assert invalidated.isoformat() == "2026-10-12T00:00:00+00:00"
            assert response == f"legacy-baseline:observation:{key}"
        else:
            assert invalidated is response is None
    assert conn.execute(
        "SELECT market_id,identity_error FROM markets WHERE NOT usable ORDER BY market_id"
    ).fetchall() == [
        ("100000", "asset_ownership_conflict"),
        ("100001", "invalid_identity"),
        ("100002", "invalid_identity"),
        ("150000", "asset_ownership_conflict"),
    ]
    assert conn.execute(
        "SELECT candidate,accepted,invalidated_response FROM _identity WHERE market_id IN ('100001','100002') ORDER BY market_id"
    ).fetchall() == [
        (None, None, "legacy-baseline:observation:100001"),
        (None, None, "legacy-baseline:observation:100002"),
    ]
    assert store.validate_store(conn)["valid"]


def assert_pristine(conn):
    for table in (
        *SCHEMAS,
        "_receipts",
        "_identity",
        "_identity_claims",
        "_membership",
        "_parent_repairs",
    ):
        assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,), table
    assert store.generation(conn) == 0
    for state in ("seed", "events_frontier", "markets_frontier"):
        assert store.get_state(conn, state) is None


def test_later_identity_batch_failure_rolls_back_whole_seed_and_allows_retry(target, scale_release):
    settings, conn = target
    release, checksum, raw = scale_release
    batches = IdentityBatches(conn, fail_second=True)
    with pytest.raises(CurrentError, match="identity state failed: second identity range failed"):
        importer.import_release(settings, batches, release, manifest_sha256=checksum, raw_root=raw)
    assert batches.rows_per_batch == [50_000]
    assert_pristine(conn)
    assert importer.import_release(settings, conn, release, manifest_sha256=checksum, raw_root=raw)[
        "valid"
    ]
    assert conn.execute("SELECT count(*) FROM _identity").fetchone() == (MARKETS,)


@pytest.mark.parametrize("filename", ["events.parquet", "release.json"])
def test_consumed_seed_source_mutation_before_commit_rolls_back(tmp_path, monkeypatch, filename):
    release, checksum, conn = release_fixture(tmp_path, usable=True)
    settings = CurrentSettings(tmp_path / "current")
    original = importer._event_pages

    def mutate_after_pages(*args):
        yield from original(*args)
        with (release / filename).open("ab") as handle:
            handle.write(b" ")

    monkeypatch.setattr(importer, "_event_pages", mutate_after_pages)
    try:
        with pytest.raises(CurrentError, match="seed event evidence failed"):
            importer.import_release(
                settings, conn, release, manifest_sha256=checksum, raw_root=tmp_path / "raw"
            )
        assert_pristine(conn)
    finally:
        conn.close()
