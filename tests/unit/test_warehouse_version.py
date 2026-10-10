"""Version checks never retrofit a populated legacy warehouse."""

import hashlib

import duckdb
import pytest

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.warehouse_version import WarehouseVersionError, ensure_warehouse_contract


def test_fresh_stamp_and_read_only_verification(tmp_path):
    settings = Settings(root=tmp_path)
    ensure_warehouse_contract(settings)
    assert not settings.warehouse_path.exists()
    ensure_warehouse_contract(settings, create=True)
    original = hashlib.sha256(settings.warehouse_path.read_bytes()).hexdigest()
    ensure_warehouse_contract(settings)
    assert hashlib.sha256(settings.warehouse_path.read_bytes()).hexdigest() == original


def test_legacy_data_is_rejected_without_migration(tmp_path):
    settings = Settings(root=tmp_path)
    settings.warehouse_path.parent.mkdir(parents=True)
    with duckdb.connect(str(settings.warehouse_path)) as c:
        c.execute("CREATE TABLE old_state AS SELECT 42 AS preserved")
    original = settings.warehouse_path.read_bytes()
    with pytest.raises(WarehouseVersionError, match="fresh"):
        ensure_warehouse_contract(settings, create=True)
    assert settings.warehouse_path.read_bytes() == original


def test_changed_contract_and_unreadable_warehouse_fail_closed(tmp_path):
    settings = Settings(root=tmp_path)
    ensure_warehouse_contract(settings, create=True)
    with duckdb.connect(str(settings.warehouse_path)) as c:
        c.execute("UPDATE catalogue_meta.contract SET normalization_revision = 'future'")
    with pytest.raises(WarehouseVersionError, match="incompatible"):
        ensure_warehouse_contract(settings)
    settings.warehouse_path.write_bytes(b"invalid database")
    with pytest.raises(WarehouseVersionError, match="unreadable"):
        ensure_warehouse_contract(settings)


def test_symlink_warehouse_rejected_before_open(tmp_path):
    settings = Settings(root=tmp_path)
    outside = tmp_path / "outside.duckdb"
    with duckdb.connect(str(outside)) as connection:
        connection.execute("create table evidence as select 1 as n")
    before = outside.read_bytes()
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    settings.warehouse_path.symlink_to(outside)
    with pytest.raises(WarehouseVersionError, match="symlink"):
        ensure_warehouse_contract(settings, create=True)
    assert outside.read_bytes() == before
