"""Semantic SHA comparisons cover duplicate rows and schema, with isolated recovery paths."""

from dataclasses import replace

import duckdb
import pytest

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.rebuild import (
    VERIFIED_TABLES,
    _combined_limits,
    fingerprint_table,
    scratch_settings,
)


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


@pytest.mark.parametrize("nested", [False, True])
def test_combined_rebuild_limit_counts_complete_source_and_scratch_once(tmp_path, nested):
    source = Settings(tmp_path / "source")
    source = replace(source, capture=replace(source.capture, max_retained_bytes=9))
    source.state_dir.mkdir(parents=True)
    source.data_dir.mkdir()
    (source.root / "operator-evidence").write_bytes(b"123")
    (source.state_dir / "checkpoint").write_bytes(b"12")
    (source.data_dir / "capture").write_bytes(b"1")
    scratch = scratch_settings(
        source, source.state_dir / "rebuild" / "attempt" if nested else tmp_path / "scratch"
    )
    scratch.root.mkdir(parents=True)
    (scratch.root / "rebuilt-evidence").write_bytes(b"123")
    _combined_limits(source, scratch)
    limited = replace(source, capture=replace(source.capture, max_retained_bytes=8))
    with pytest.raises(ValueError, match="combined retained"):
        _combined_limits(limited, scratch)


@pytest.mark.parametrize("relative", [False, True])
def test_combined_rebuild_limit_keeps_external_configured_paths(tmp_path, relative):
    source = Settings(tmp_path / "source")
    source.root.mkdir()
    (source.root / "operator-evidence").write_bytes(b"123")
    data, state = tmp_path / "external-data", tmp_path / "external-state"
    data.mkdir()
    state.mkdir()
    (data / "capture").write_bytes(b"12")
    (state / "checkpoint").write_bytes(b"12")
    warehouse = tmp_path / "external.duckdb"
    warehouse.write_bytes(b"12345")
    warehouse.with_name(warehouse.name + ".wal").write_bytes(b"1")
    source = replace(
        source,
        paths=replace(
            source.paths,
            data_dir="../external-data" if relative else str(data),
            state_dir="../external-state" if relative else str(state),
            warehouse_file="../external.duckdb" if relative else str(warehouse),
        ),
        capture=replace(source.capture, max_retained_bytes=17),
    )
    scratch = scratch_settings(source, tmp_path / "scratch")
    if relative:
        scratch = replace(scratch, root=source.root / ".." / "scratch")
    scratch.root.mkdir()
    (scratch.root / "rebuilt").write_bytes(b"1234")
    _combined_limits(source, scratch)
    limited = replace(source, capture=replace(source.capture, max_retained_bytes=16))
    with pytest.raises(ValueError, match="combined retained"):
        _combined_limits(limited, scratch)


def test_combined_rebuild_rejects_source_alias_before_parent_normalization(tmp_path):
    source = Settings(tmp_path / "source")
    source.root.mkdir()
    target = tmp_path / "elsewhere" / "nested"
    target.mkdir(parents=True)
    (tmp_path / "alias").symlink_to(target, target_is_directory=True)
    source = replace(source, paths=replace(source.paths, data_dir="../alias/../external"))
    scratch = scratch_settings(source, tmp_path / "scratch")
    with pytest.raises(RequestBudgetExceeded, match="symlink"):
        _combined_limits(source, scratch)
