from __future__ import annotations

import importlib.util
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import duckdb

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-full-pass.py"
SPEC = importlib.util.spec_from_file_location("full_pass_claim_diagnostics", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_fixture_contains_duplicate_competing_and_namespace_collisions():
    legacy_rows = benchmark.claim_fixture_rows(0, 8)
    rows = benchmark.claim_fixture_rows(0, 8, adversarial=True)

    assert legacy_rows[0] != legacy_rows[4]
    assert all(row[0] == "polymarket" for row in legacy_rows)
    assert rows[0] == rows[4]
    assert rows[0][4] == rows[1][4] == rows[2][4] == rows[3][4]
    assert {row[2] for row in rows[:4]} == {"candidate", "accepted"}
    assert {(row[0], row[3]) for row in rows[:4]} == {
        ("polymarket", "clob"),
        ("polymarket", "position"),
        ("other-venue", "clob"),
    }


def test_actual_selective_claim_queries_match_exact_namespace_oracle():
    from oddsfox_catalogue.current_store import (
        _select_claims_by_assets,
        _select_claims_by_markets,
        initialize,
    )

    conn = duckdb.connect(":memory:")
    try:
        initialize(conn)
        benchmark.populate_claim_fixture(conn, 32, batch_size=32, adversarial=True)

        asset_keys = [("polymarket", "clob", "7"), ("polymarket", "position", "7")]
        market_keys = [("polymarket", "10000000000000000001")]
        asset_expected, market_expected = benchmark._claim_full_scan_oracle(
            conn, asset_keys=asset_keys, market_keys=market_keys
        )

        asset_rows = _select_claims_by_assets(conn, asset_keys)
        market_rows = _select_claims_by_markets(conn, market_keys)
        assert Counter(map(tuple, asset_rows)) == Counter(map(tuple, asset_expected))
        assert Counter(map(tuple, market_rows)) == Counter(map(tuple, market_expected))
        assert len(asset_rows) == 4  # duplicate preserved; wrong venue excluded
        assert {row[2] for row in asset_rows} == {"candidate", "accepted"}

        unindexed = benchmark._claim_lookup_diagnostics(conn, 32)
        assert unindexed["asset_lookup"]["rows"] == unindexed["expected_rows"]["asset"]
        assert unindexed["market_lookup"]["rows"] == unindexed["expected_rows"]["market"]
        benchmark.build_claim_fixture_indexes(conn)
        indexed = benchmark._claim_lookup_diagnostics(conn, 32)
        assert indexed["expected_rows"]["asset"] >= 4
        assert indexed["expected_rows"]["market"] >= 1
        assert len(indexed["asset_query_plan"]) > 0
        assert len(indexed["market_query_plan"]) > 0
        assert indexed["asset_lookup"]["rows"] == indexed["expected_rows"]["asset"]
        assert indexed["market_lookup"]["rows"] == indexed["expected_rows"]["market"]
    finally:
        conn.close()


def test_claim_lookup_diagnostic_oracle_preserves_duplicate_multiplicity():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            "CREATE TABLE _identity_claims(venue VARCHAR, market_id VARCHAR, role VARCHAR, "
            "asset_kind VARCHAR, asset_id VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)",
            [
                benchmark.claim_fixture_rows(0, 5, adversarial=True)[index]
                for index in (0, 1, 2, 3, 4)
            ],
        )
        expected, _ = benchmark._claim_full_scan_oracle(
            conn, asset_keys=[("polymarket", "clob", "7")]
        )
        assert (
            Counter(map(tuple, expected))[benchmark.claim_fixture_rows(0, 1, adversarial=True)[0]]
            == 2
        )
        assert len(expected) == 3
    finally:
        conn.close()


def test_diagnostic_resource_metrics_are_labeled_as_sampled_or_lifetime(tmp_path):
    catalogue = tmp_path / "catalogue"
    temporary = tmp_path / "temporary"
    catalogue.mkdir()
    temporary.mkdir()
    (catalogue / "catalogue.duckdb").write_bytes(b"db")
    (catalogue / "catalogue.duckdb.wal").write_bytes(b"wal")
    (temporary / "spill.tmp").write_bytes(b"tmp")

    storage = benchmark._claim_storage_metrics(catalogue, temporary)
    rss = benchmark._claim_rss_metrics(1234)

    assert storage["database_bytes"] == 2
    assert storage["wal_bytes"] == 3
    assert storage["temporary_bytes"] == 3
    assert "sampled" in storage["measurement"]
    assert rss["worker_lifetime_peak_rss_bytes"] == 1234
    assert "not an isolated" in rss["rss_measurement"]


def test_bounded_write_diagnostics_restore_exact_rows_before_and_after_indexes():
    from oddsfox_catalogue.current_store import initialize

    conn = duckdb.connect(":memory:")
    try:
        initialize(conn)
        benchmark.populate_claim_fixture(conn, 64, batch_size=64, adversarial=True)
        original = Counter(map(tuple, conn.execute("SELECT * FROM _identity_claims").fetchall()))

        before_indexes = benchmark._claim_write_diagnostics(conn, 64, adversarial=True)
        assert set(before_indexes) == {
            "bounded_replacement_seconds",
            "conflicting_owner_insert_delete_seconds",
            "asset_deletion_restore_seconds",
        }
        assert (
            Counter(map(tuple, conn.execute("SELECT * FROM _identity_claims").fetchall()))
            == original
        )

        benchmark.build_claim_fixture_indexes(conn)
        after_indexes = benchmark._claim_write_diagnostics(conn, 64, adversarial=True)
        assert set(after_indexes) == set(before_indexes)
        assert (
            Counter(map(tuple, conn.execute("SELECT * FROM _identity_claims").fetchall()))
            == original
        )
    finally:
        conn.close()


def test_diagnostic_store_closes_database_under_writer_lock():
    events = []

    class Settings:
        run_lock_path = Path("run.lock")

    @contextmanager
    def locker(path):
        assert path == Settings.run_lock_path
        events.append("lock-enter")
        try:
            yield
        finally:
            events.append("lock-exit")

    @contextmanager
    def connector(_settings):
        events.append("database-enter")
        try:
            yield object()
        finally:
            events.append("database-close")

    with benchmark._claim_diagnostic_store(
        Settings(), connector, locker, lambda: events.append("post-close-sample")
    ):
        events.append("body")

    assert events == [
        "lock-enter",
        "database-enter",
        "body",
        "database-close",
        "post-close-sample",
        "lock-exit",
    ]
