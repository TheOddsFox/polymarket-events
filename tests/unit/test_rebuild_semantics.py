"""Semantic SHA comparisons cover duplicate rows and schema, with isolated recovery paths."""

from dataclasses import replace

import duckdb
import pytest

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.rebuild import VERIFIED_TABLES, fingerprint_table, scratch_settings


def test_fingerprint_preserves_duplicate_rows_schema_and_exact_decimal_values():
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE SCHEMA core")
        connection.execute(
            "CREATE TABLE core.example (id VARCHAR, value DECIMAL(38,18), loaded_at TIMESTAMP, _dlt_id VARCHAR)"
        )
        connection.execute(
            "INSERT INTO core.example VALUES ('a', 0.123456789012345678, now(), 'first')"
        )
        original = fingerprint_table(connection, "core.example")
        connection.execute(
            "UPDATE core.example SET loaded_at = TIMESTAMP '2000-01-01', _dlt_id = 'different'"
        )
        assert fingerprint_table(connection, "core.example") == original
        connection.execute("INSERT INTO core.example SELECT * FROM core.example")
        duplicate = fingerprint_table(connection, "core.example")
        assert duplicate["rows"] == 2
        assert duplicate["semantic_sha256"] != original["semantic_sha256"]
        connection.execute("DELETE FROM core.example")
        connection.execute(
            "INSERT INTO core.example VALUES ('a', 0.123456789012345679, now(), 'first')"
        )
        assert (
            fingerprint_table(connection, "core.example")["semantic_sha256"]
            != original["semantic_sha256"]
        )
        connection.execute("ALTER TABLE core.example ALTER value TYPE VARCHAR")
        assert fingerprint_table(connection, "core.example")["schema"] != original["schema"]
        assert fingerprint_table(connection, "core.missing") is None


def test_rebuild_boundary_includes_staging_metrics_quarantine_and_relationships():
    assert {
        "bronze.quarantined_records",
        "staging.stg_gamma__market_event_refs",
        "history.market_metrics",
        "history.event_metrics",
        "core.quarantine_market_outcomes",
        "core.market_event_bridge",
        "history.market_history",
    } <= set(VERIFIED_TABLES)


def test_scratch_paths_and_quality_are_isolated_without_ambient_overrides(tmp_path, monkeypatch):
    source = Settings(tmp_path / "source")
    source = replace(source, quality=replace(source.quality, open_events_drop_warn_pct=7.0))
    monkeypatch.setenv("CATALOGUE_PATHS_DATA_DIR", str(source.data_dir))
    monkeypatch.setenv("CATALOGUE_PATHS_STATE_DIR", str(source.state_dir))
    scratch = scratch_settings(source, tmp_path / "scratch")
    assert scratch.raw_dir == source.raw_dir
    assert scratch.data_dir != source.data_dir
    assert scratch.state_dir != source.state_dir
    assert scratch.warehouse_path != source.warehouse_path
    assert scratch.run_lock_path != source.run_lock_path
    assert scratch.published_dir != source.published_dir
    assert scratch.quality == source.quality


def test_scratch_rejects_symlink_ancestors(tmp_path):
    (tmp_path / "alias").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        scratch_settings(Settings(tmp_path / "source"), tmp_path / "alias/new")
