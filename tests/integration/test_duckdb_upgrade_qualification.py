"""Small offline fixtures for the DuckDB storage upgrade qualification tool."""

import importlib.util
import json
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from filelock import FileLock

from fakes.world import make_event, make_market
from oddsfox_catalogue import current_publish, current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_sync import _write_response

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "qualify-duckdb-upgrade.py"
SPEC = importlib.util.spec_from_file_location("duckdb_upgrade_qualification", SCRIPT)
qualify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(qualify)


@pytest.mark.parametrize("engine", [(1, 5), (2, 0)])
def test_qualification_connections_use_application_order_policy_and_limits(
    tmp_path, monkeypatch, engine
):
    monkeypatch.setattr(qualify, "_engine_version_pair", lambda: engine)
    settings = CurrentSettings(
        tmp_path,
        limits=CurrentLimits(duckdb_memory_bytes=64 * 1024**2, max_temp_bytes=32 * 1024**2),
    )
    config = qualify._connection_config(settings)
    with duckdb.connect(":memory:", config=config) as connection:
        assert connection.execute(
            "SELECT current_setting('preserve_insertion_order')"
        ).fetchone() == (False,)
        assert connection.execute("SELECT current_setting('threads')").fetchone() == (2,)
        assert connection.execute("SELECT current_setting('memory_limit')").fetchone() == (
            "64.0 MiB",
        )
        assert connection.execute(
            "SELECT current_setting('max_temp_directory_size')"
        ).fetchone() == ("32.0 MiB",)
    assert ("async_threads" in config) == (engine >= (2, 0))


@pytest.fixture
def source_root(tmp_path):
    root = tmp_path / "source"
    settings = CurrentSettings(
        root, limits=CurrentLimits(max_temp_bytes=1024, free_reserve_bytes=1)
    )
    validate_root(root, initialize=True)
    (root / "config.toml").write_text(
        "[catalogue]\nmax_retained_bytes = 1073741824\n"
        "max_temp_bytes = 1024\nfree_reserve_bytes = 1\n"
    )
    raw_id, pending_raw_id = "a" * 32, "f" * 32
    raw_dir = root / "raw" / raw_id
    pending_raw_dir = root / "raw" / pending_raw_id
    raw_dir.mkdir(parents=True)
    pending_raw_dir.mkdir(parents=True)
    (raw_dir / "body.json").write_text("synthetic private response")
    with duckdb.connect(str(root / "catalogue.duckdb")) as connection:
        current_store.initialize(connection)
        market = make_market(
            "11",
            "Synthetic exact-ID market?",
            version="v1",
            event_stub={"id": "1"},
            seed=11,
        )
        event = make_event("1", "synthetic event", markets=[market])
        response = project_response(
            event,
            "events",
            received_at="2026-10-01T00:00:00Z",
            response_id=raw_id,
            native_market_ids={"11"},
        )
        current_store.apply(
            connection,
            response,
            {
                "qualification_progress": {
                    "frontier": "115792089237316195423570985008687907853269984665640564039457584007913129639935",
                    "raw_id": raw_id,
                }
            },
        )
        connection.execute(
            "CREATE TABLE _qualification_private "
            "(signed_id BIGINT PRIMARY KEY, exact_asset_id DECIMAL(38,0), raw_ref VARCHAR)"
        )
        connection.execute(
            "INSERT INTO _qualification_private VALUES (?, ?, ?)",
            [-9223372036854775808, 10**36 + 7, raw_id],
        )
        connection.execute(
            "CREATE INDEX _qualification_private_raw_ref ON _qualification_private(raw_ref)"
        )
        connection.execute(
            "CREATE VIEW _qualification_private_view AS "
            "SELECT signed_id,exact_asset_id FROM _qualification_private"
        )
        connection.execute(
            "COMMENT ON TABLE _qualification_private IS 'synthetic private table metadata'"
        )
        connection.execute(
            "COMMENT ON COLUMN _qualification_private.raw_ref IS 'synthetic private column metadata'"
        )
        connection.execute(
            "COMMENT ON VIEW _qualification_private_view IS 'synthetic private view metadata'"
        )
        connection.execute("CREATE MACRO _qualification_increment(x) AS x + 1")
        connection.execute("CREATE SEQUENCE _qualification_sequence START 5")

        pending_request = {"endpoint": "/events/2", "params": {}}
        pending_response = SimpleNamespace(
            body=json.dumps(make_event("2", "pending synthetic event")).encode(),
            received_at=datetime(2026, 10, 2, tzinfo=UTC),
            status=200,
        )
        _write_response(settings, pending_raw_dir, pending_response, pending_request)
        pending = {
            "identity": {
                "lane": "events",
                "ids": [2],
                "native": True,
                "progress": {},
                "policy": current_sync._pending_policy(connection),
                "certificate": None,
                "passes": [None],
                "api_certificate_version": current_sync._api_certificate_version(connection),
            },
            "remaining": [pending_request],
            "raw_id": pending_raw_id,
            "absence": False,
        }
        current_store.set_state(connection, "fetch_pending", pending)
        current_sync.validate_fetch_pending(settings, connection, pending)
        current_publish.publish(settings, connection)
        current_publish.publish(settings, connection, force=True)
    return root


def test_inventory_hashes_private_state_and_emits_only_nonvalue_evidence(source_root, tmp_path):
    before = qualify._tree_fingerprint(qualify._source_layout(source_root))
    output = tmp_path / "inventory.json"

    report = qualify.inventory(source_root, output)

    after = qualify._tree_fingerprint(qualify._source_layout(source_root))
    text = output.read_text()
    assert report["source"]["table_count"] >= 11
    assert report["source"]["row_count"] >= 2
    assert report["source"]["schema_object_counts"]["views"] >= 1
    assert report["source"]["schema_object_counts"]["macros"] >= 1
    assert report["source"]["schema_object_counts"]["sequences"] >= 1
    assert report["source"]["schema_object_counts"]["indexes"] >= 1
    assert report["source"]["schema_object_counts"]["constraints"] >= 1
    assert report["source"]["schema_objects_sha256"]
    assert report["source"]["previous_release_present"] is True
    assert report["source"]["previous_release_valid"] is True
    assert before == after
    assert "synthetic private response" not in text
    assert (
        "115792089237316195423570985008687907853269984665640564039457584007913129639935" not in text
    )
    assert "-9223372036854775808" not in text
    for token_id in json.loads(
        make_market(
            "11", "Synthetic exact-ID market?", version="v1", event_stub={"id": "1"}, seed=11
        )["clobTokenIds"]
    ):
        assert token_id not in text


def test_table_hash_preserves_lexical_json_values():
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE TABLE private_json(value JSON)")
        connection.execute("INSERT INTO private_json VALUES ('{\"n\":1.00}'::JSON)")
        _, first, _ = qualify._table_semantics(
            connection, connection.execute("SELECT current_database()").fetchone()[0]
        )

        connection.execute("UPDATE private_json SET value='{\"n\":1e0}'::JSON")
        _, second, _ = qualify._table_semantics(
            connection, connection.execute("SELECT current_database()").fetchone()[0]
        )

    assert first[0]["sha256"] != second[0]["sha256"]


@pytest.mark.parametrize("storage_version", ["latest", "v1.5.0"])
def test_convert_copies_root_and_preserves_all_table_and_object_inventories(
    source_root, tmp_path, storage_version
):
    destination = tmp_path / f"converted-{storage_version.replace('.', '-')}"
    output = tmp_path / f"{destination.name}.json"
    before = qualify._tree_fingerprint(qualify._source_layout(source_root))

    report = qualify.convert(source_root, destination, storage_version, output)

    assert report["equal"] == {
        "source_unchanged": True,
        "sidefiles": True,
        "tables_and_rows": True,
        "schema_objects": True,
        "current_release_valid": True,
        "previous_release_valid": True,
    }
    assert report["storage_version_requested"] == storage_version
    assert report["source"]["database_bytes"] > 0
    assert report["target"]["database_bytes"] > 0
    assert report["conversion_elapsed_s"] >= 0
    assert report["peak_rss_bytes"] > 0
    assert (
        destination / "raw" / ("a" * 32) / "body.json"
    ).read_text() == "synthetic private response"
    pending_raw = destination / "raw" / ("f" * 32)
    assert (pending_raw / "p000001.json.gz").exists()
    assert json.loads((pending_raw / "p000001.manifest.json").read_text())["request"] == {
        "endpoint": "/events/2",
        "params": {},
    }
    pointer = json.loads((destination / "published" / "current.json").read_text())
    assert pointer["previous_release_id"]
    assert len(list((destination / "published" / "releases").iterdir())) == 2
    assert stat.S_IMODE(destination.stat().st_mode) == 0o700
    assert stat.S_IMODE((destination / "raw").stat().st_mode) == 0o700
    assert stat.S_IMODE((destination / "catalogue.duckdb").stat().st_mode) == 0o600
    after = qualify._tree_fingerprint(qualify._source_layout(source_root))
    assert before == after
    with duckdb.connect(str(destination / "catalogue.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT * FROM _qualification_private").fetchone() == (
            -9223372036854775808,
            10**36 + 7,
            "a" * 32,
        )
        assert (
            connection.execute(
                "SELECT comment FROM duckdb_tables() WHERE table_name='_qualification_private'"
            ).fetchone()[0]
            == "synthetic private table metadata"
        )
        assert (
            connection.execute(
                "SELECT comment FROM duckdb_columns() "
                "WHERE table_name='_qualification_private' AND column_name='raw_ref'"
            ).fetchone()[0]
            == "synthetic private column metadata"
        )
        assert (
            connection.execute(
                "SELECT comment FROM duckdb_views() WHERE view_name='_qualification_private_view'"
            ).fetchone()[0]
            == "synthetic private view metadata"
        )
        raw_ref = connection.execute(
            "SELECT value FROM sync_state WHERE key='fetch_pending'"
        ).fetchone()[0]
        pending = json.loads(raw_ref)
        assert pending["raw_id"] == "f" * 32
        assert pending["remaining"] == [{"endpoint": "/events/2", "params": {}}]
        progress = connection.execute(
            "SELECT value FROM sync_state WHERE key='qualification_progress'"
        ).fetchone()[0]
        assert json.loads(progress)["raw_id"] == "a" * 32
        actual_ids = connection.execute(
            "SELECT clob_token_id,asset_id FROM outcomes ORDER BY outcome_index"
        ).fetchall()
        expected_ids = json.loads(
            make_market(
                "11", "Synthetic exact-ID market?", version="v1", event_stub={"id": "1"}, seed=11
            )["clobTokenIds"]
        )
        assert actual_ids == [(value, value) for value in expected_ids]
        current_sync.validate_fetch_pending(
            CurrentSettings(
                destination, limits=CurrentLimits(max_temp_bytes=1024, free_reserve_bytes=1)
            ),
            connection,
            pending,
        )


def test_lock_does_not_truncate_and_remains_compatible_with_filelock(source_root, tmp_path):
    lock = source_root / ".catalogue.lock"
    lock.write_bytes(b"qualification lock sentinel")
    before = lock.read_bytes()
    with qualify._catalogue_lock(lock):
        assert lock.read_bytes() == before

    with (
        FileLock(str(lock)),
        pytest.raises(RuntimeError, match="another catalogue run holds"),
        qualify._catalogue_lock(lock),
    ):
        pass


def test_hardlinked_lock_is_rejected_without_changing_database(source_root, tmp_path):
    lock = source_root / ".catalogue.lock"
    database = source_root / "catalogue.duckdb"
    database_before = database.read_bytes()
    lock.hardlink_to(database)
    output = tmp_path / "hardlink.json"

    with pytest.raises(ValueError, match="multiple hard links"):
        qualify.inventory(source_root, output)

    assert database.read_bytes() == database_before
    assert not output.exists()


def test_inventory_rejects_nonempty_temporary_directory_without_writing_report(
    source_root, tmp_path
):
    temporary = source_root / "tmp"
    temporary.mkdir()
    (temporary / "unfinished.spill").write_text("spill")
    output = tmp_path / "rejected.json"

    with pytest.raises(ValueError, match="tmp directory is not empty"):
        qualify.inventory(source_root, output)

    assert not output.exists()


def test_inventory_rejects_symlink_without_following_it(source_root, tmp_path):
    outside = tmp_path / "outside.json"
    outside.write_text("private")
    (source_root / "external.json").symlink_to(outside)
    output = tmp_path / "symlink.json"

    with pytest.raises(ValueError, match="contains a symlink"):
        qualify.inventory(source_root, output)

    assert outside.read_text() == "private"
    assert not output.exists()


def test_conversion_rejects_existing_destination(source_root, tmp_path):
    destination = tmp_path / "existing"
    destination.mkdir()
    output = tmp_path / "existing.json"

    with pytest.raises(ValueError, match="destination must not exist"):
        qualify.convert(source_root, destination, "latest", output)

    assert not output.exists()


@pytest.mark.parametrize("legacy_marker", ["ledger.sqlite", "warehouse", ".state/ledger.sqlite"])
def test_conversion_rejects_destination_nested_in_legacy_root_before_copy_or_report(
    source_root, tmp_path, legacy_marker
):
    legacy_root = tmp_path / "legacy-root"
    marker = legacy_root / legacy_marker
    if legacy_marker == "warehouse":
        marker.mkdir(parents=True)
        preserved = marker / "legacy-data"
    else:
        marker.parent.mkdir(parents=True, exist_ok=True)
        preserved = marker
    preserved.write_bytes(b"legacy marker sentinel")
    marker_bytes = preserved.read_bytes()
    destination = legacy_root / "destination"
    output = tmp_path / "conversion.json"
    source_before = qualify._tree_fingerprint(qualify._source_layout(source_root))

    with pytest.raises(ValueError):
        qualify.convert(source_root, destination, "v1.5.0", output)

    assert not destination.exists()
    assert not output.exists()
    assert preserved.read_bytes() == marker_bytes
    assert qualify._tree_fingerprint(qualify._source_layout(source_root)) == source_before


def test_conversion_rejects_trigger_before_copy_or_report(source_root, tmp_path):
    with duckdb.connect(str(source_root / "catalogue.duckdb")) as connection:
        connection.execute("CREATE TABLE _qualification_trigger_log(value INTEGER)")
        connection.execute(
            "CREATE TRIGGER _qualification_trigger AFTER INSERT ON _qualification_private "
            "FOR EACH ROW INSERT INTO _qualification_trigger_log VALUES (NEW.signed_id)"
        )

    destination = tmp_path / "trigger-source-copy"
    output = tmp_path / "trigger-source.json"
    with pytest.raises(ValueError, match="database triggers"):
        qualify.convert(source_root, destination, "v1.5.0", output)

    assert not destination.exists()
    assert not output.exists()


def test_cli_reports_fail_closed_validation_error(source_root, tmp_path):
    wal = source_root / "catalogue.duckdb.wal"
    wal.write_bytes(b"pending WAL")
    output = tmp_path / "cli.json"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "inventory",
            "--root",
            str(source_root),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "has a WAL" in result.stderr
    assert not output.exists()
