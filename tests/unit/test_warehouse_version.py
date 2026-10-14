"""Version checks never retrofit a populated legacy warehouse."""

import hashlib

import duckdb
import pytest

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.certification import prepare_dbt_execution, read_build_validity
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import CONTRACT, NORMALIZATION_REVISION
from oddsfox_catalogue.dbt_runner import run_dbt
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.warehouse_version import (
    WAREHOUSE_CONTRACT,
    WarehouseVersionError,
    ensure_warehouse_contract,
)


def test_fresh_stamp_and_read_only_verification(tmp_path):
    settings = Settings(root=tmp_path)
    ensure_warehouse_contract(settings)
    assert not settings.warehouse_path.exists()
    ensure_warehouse_contract(settings, create=True)
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute("SELECT * FROM catalogue_meta.contract").fetchall() == [
            (WAREHOUSE_CONTRACT, NORMALIZATION_REVISION)
        ]
    assert WAREHOUSE_CONTRACT == "oddsfox.polymarket.warehouse.v5"
    assert CONTRACT == "oddsfox.polymarket.catalogue.v2" and CONTRACT != WAREHOUSE_CONTRACT
    original = hashlib.sha256(settings.warehouse_path.read_bytes()).hexdigest()
    ensure_warehouse_contract(settings)
    assert hashlib.sha256(settings.warehouse_path.read_bytes()).hexdigest() == original


@pytest.mark.parametrize("create", [False, True])
def test_legacy_reader_rejects_new_stamp_without_changing_warehouse(tmp_path, monkeypatch, create):
    settings = Settings(root=tmp_path)
    ensure_warehouse_contract(settings, create=True)
    before = settings.warehouse_path.read_bytes()
    # The former reader accepts catalogue.v2 plus normalization 2; emulate that exact stamp check.
    monkeypatch.setattr("oddsfox_catalogue.warehouse_version.WAREHOUSE_CONTRACT", CONTRACT)
    with pytest.raises(WarehouseVersionError, match="incompatible"):
        ensure_warehouse_contract(settings, create=create)
    assert settings.warehouse_path.read_bytes() == before


@pytest.mark.parametrize("entry", ["load", "parse", "build", "run", "prepare-build"])
def test_old_layout_is_rejected_before_certification_or_artifact_mutation(
    tmp_path, monkeypatch, entry
):
    settings = Settings(root=tmp_path)
    settings.warehouse_path.parent.mkdir(parents=True)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA catalogue_meta")
        connection.execute(
            "CREATE TABLE catalogue_meta.contract (contract VARCHAR, normalization_revision VARCHAR)"
        )
        connection.execute(
            "INSERT INTO catalogue_meta.contract VALUES (?, ?)", [CONTRACT, NORMALIZATION_REVISION]
        )
        connection.execute(
            "CREATE TABLE retained_legacy_evidence AS SELECT 'unmodeled raw source' AS payload"
        )
    with Ledger(settings.ledger_path) as ledger:
        ledger.set_build_validity(
            "valid",
            "2026-10-10T00:00:00Z",
            payload_json='{"legacy":"receipt"}',
            payload_sha256="a" * 64,
        )
    warehouse = settings.warehouse_path.read_bytes()
    certificate = read_build_validity(settings)
    before_files = {path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file()}

    def forbidden(*args, **kwargs):
        pytest.fail("an incompatible warehouse reached dbt parsing or execution")

    monkeypatch.setattr("oddsfox_catalogue.dbt_runner.run_dbt_at", forbidden)
    monkeypatch.setattr("oddsfox_catalogue.certification._parse_required_nodes", forbidden)
    with pytest.raises(WarehouseVersionError, match="incompatible"):
        if entry == "load":
            with Ledger(settings.ledger_path) as ledger:
                load_pending(LoadRuntime(settings, ledger))
        elif entry == "prepare-build":
            prepare_dbt_execution(settings, settings.state_dir / "dbt" / "target")
        else:
            run_dbt(settings, [entry])
    assert settings.warehouse_path.read_bytes() == warehouse
    assert read_build_validity(settings) == certificate
    assert {
        path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file()
    } == before_files


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
