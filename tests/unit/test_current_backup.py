import gzip
import json
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.current_backup import create_backup, prune, restore_backup, verify_backup
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_publish import publish, verify_current
from oddsfox_catalogue.current_store import get_state, initialize, set_state


@pytest.fixture
def settings(tmp_path):
    settings = CurrentSettings(tmp_path / "root")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        initialize(conn)
        publish(settings, conn)
    return settings


def test_pending_state_and_older_publication_restore(settings, tmp_path):
    identifier = "a" * 32
    raw = settings.raw_dir / identifier
    raw.mkdir(parents=True)
    body = b'{"id":"1","markets":[]}'
    write_page(
        raw,
        1,
        body,
        {
            "request": {"endpoint": "/events/1", "params": {}},
            "status": 200,
            "received_at": "2026-10-14T00:00:00+00:00",
        },
    )
    with connect_store(settings) as conn:
        set_state(
            conn,
            "fetch_pending",
            {
                "raw_id": identifier,
                "remaining": [{"endpoint": "/events/1", "params": {}}],
                "identity": {
                    "lane": "events",
                    "ids": [1],
                    "native": True,
                    "progress": {},
                    "policy": current_sync.POLICY,
                    "certificate": None,
                    "passes": [None],
                },
            },
        )
        set_state(conn, "generation", 1)
    backup = create_backup(settings)
    assert verify_backup(backup["path"])["valid"]
    destination = tmp_path / "restored"
    restore_backup(backup["path"], destination)
    restored = CurrentSettings(destination)
    with connect_store(restored, read_only=True) as conn:
        assert get_state(conn, "fetch_pending")["raw_id"] == identifier
        result = verify_current(restored, connection=conn)
        assert result["published_generation"] == 0
        assert result["store_generation"] == 1
    assert (destination / "raw" / identifier / "p000001.json.gz").read_bytes() == gzip.compress(
        body, compresslevel=6, mtime=0
    )

    def no_http(*args, **kwargs):
        pytest.fail("restored pending receipt must resume without HTTP")

    with connect_store(restored) as conn:
        current_sync.fetch_apply(
            restored, conn, SimpleNamespace(get=no_http), "events", [1], native=True
        )
        assert get_state(conn, "fetch_pending") is None
        assert conn.execute("SELECT event_id FROM events").fetchall() == [("1",)]


def test_corruption_rejected_before_destination_creation(settings, tmp_path):
    path = Path(create_backup(settings)["path"])
    (path / "catalogue.duckdb").write_bytes(b"broken")
    destination = tmp_path / "restored"
    with pytest.raises(CurrentError):
        restore_backup(path, destination)
    assert not destination.exists()


def test_inventory_traversal_and_existing_empty_destination(settings, tmp_path):
    path = Path(create_backup(settings)["path"])
    destination = tmp_path / "restored"
    destination.mkdir()
    with pytest.raises(CurrentError, match="must not exist"):
        restore_backup(path, destination)
    value = json.loads((path / "backup.json").read_text())
    value["files"]["../outside"] = {"bytes": 0, "sha256": "bad"}
    (path / "backup.json").write_text(json.dumps(value))
    with pytest.raises(CurrentError):
        verify_backup(path)


def test_symlink_destination_ancestor_rejected(settings, tmp_path):
    path = create_backup(settings)["path"]
    (tmp_path / "link").symlink_to(settings.root, target_is_directory=True)
    with pytest.raises(CurrentError, match="symlink"):
        restore_backup(path, tmp_path / "link" / "restore")


def test_prune_age_cap_and_pinning(settings):
    for identifier in ("a" * 32, "b" * 32):
        directory = settings.raw_dir / identifier
        directory.mkdir(parents=True)
        (directory / "p000001.json.gz").write_bytes(b"1234")
    with connect_store(settings) as conn:
        set_state(conn, "fetch_pending", {"raw_id": "a" * 32})
    constrained = replace(settings, limits=replace(settings.limits, max_raw_bytes=4))
    prune(constrained, now=time.time() + 8 * 86400)
    assert (settings.raw_dir / ("a" * 32)).exists()
    assert not (settings.raw_dir / ("b" * 32)).exists()
    constrained = replace(settings, limits=replace(settings.limits, max_raw_bytes=3))
    with pytest.raises(CurrentError, match="pinned raw"):
        prune(constrained)


def test_failed_backup_preserves_good_and_admission(settings, monkeypatch):
    good = create_backup(settings)["path"]
    monkeypatch.setattr(
        "oddsfox_catalogue.current_backup.verify_backup",
        lambda *a, **kw: (_ for _ in ()).throw(CurrentError("failed")),
    )
    with pytest.raises(CurrentError, match="failed"):
        create_backup(settings)
    assert Path(good).exists()
    assert len(list(settings.backups_dir.iterdir())) == 1
    constrained = replace(settings, limits=replace(settings.limits, max_retained_bytes=1))
    with pytest.raises(CurrentError, match="retained"):
        create_backup(constrained)


def test_legacy_destination_and_unowned_raw_fail_closed(settings, tmp_path):
    backup = create_backup(settings)["path"]
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    (legacy / "ledger.sqlite").touch()
    with pytest.raises(CurrentError, match="legacy"):
        restore_backup(backup, legacy / "new")
    assert not (legacy / "new").exists()
    settings.raw_dir.mkdir()
    (settings.raw_dir / "unknown").write_bytes(b"1234")
    constrained = replace(settings, limits=replace(settings.limits, max_raw_bytes=3))
    with pytest.raises(CurrentError, match="unowned"):
        prune(constrained)
    assert (settings.raw_dir / "unknown").exists()


def test_prune_only_owned_candidates_and_preserves_publication(settings):
    backup = Path(create_backup(settings)["path"])
    stage = settings.backups_dir / (".staging-" + "f" * 32)
    stage.mkdir()
    (stage / "incomplete").touch()
    unknown = settings.backups_dir / "operator-data"
    unknown.mkdir()
    prune(settings)
    assert not stage.exists()
    assert unknown.exists()
    assert backup.exists()
    assert verify_current(settings)["valid"]


def test_raw_only_retention_skips_all_release_and_backup_verification(settings, monkeypatch):
    backup = Path(create_backup(settings)["path"])
    stage = settings.backups_dir / (".staging-" + "f" * 32)
    stage.mkdir()

    def unexpected(*args, **kwargs):
        raise AssertionError("refresh retention must not verify snapshots or backups")

    monkeypatch.setattr("oddsfox_catalogue.current_backup.verify_current", unexpected)
    monkeypatch.setattr("oddsfox_catalogue.current_backup.verify_backup", unexpected)
    monkeypatch.setattr("oddsfox_catalogue.current_backup._physical", unexpected)
    directory = settings.raw_dir / ("a" * 32)
    directory.mkdir(parents=True)
    (directory / "p000001.json.gz").write_bytes(b"old")
    result = prune(settings, now=time.time() + 8 * 86400, raw_only=True)
    assert result["deleted"] == 1
    assert backup.exists()
    assert stage.exists()
    assert not directory.exists()


def test_failed_backup_keeps_last_success_state(settings, monkeypatch):
    result = create_backup(settings)
    with connect_store(settings, read_only=True) as conn:
        assert get_state(conn, "last_backup") == result

    def fail(*args, **kwargs):
        raise CurrentError("candidate failed")

    monkeypatch.setattr("oddsfox_catalogue.current_backup.verify_backup", fail)
    with pytest.raises(CurrentError, match="candidate failed"):
        create_backup(settings)
    with connect_store(settings, read_only=True) as conn:
        assert get_state(conn, "last_backup") == result


@pytest.mark.parametrize("corruption", ["gzip", "manifest", "request", "status", "payload"])
def test_existing_pending_corruption_rejected_before_backup_promotion(
    settings, monkeypatch, corruption
):
    def crash(*args, **kwargs):
        raise RuntimeError("crash after raw")

    monkeypatch.setattr(current_store, "apply", crash)
    response = SimpleNamespace(
        status=200,
        json={"id": "1", "markets": []},
        body=b'{"id":"1","markets":[]}',
        received_at=datetime.now(UTC),
    )
    with connect_store(settings) as conn:
        with pytest.raises(RuntimeError, match="crash after raw"):
            current_sync.fetch_apply(
                settings,
                conn,
                SimpleNamespace(get=lambda *args: response),
                "events",
                [1],
                native=True,
            )
        identifier = get_state(conn, "fetch_pending")["raw_id"]
    directory = settings.raw_dir / identifier
    manifest_path = directory / "p000001.manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if corruption == "gzip":
        (directory / "p000001.json.gz").write_bytes(b"corrupt")
    elif corruption == "manifest":
        manifest_path.write_text("broken")
    elif corruption == "request":
        manifest["request"]["endpoint"] = "/events/2"
        manifest_path.write_text(json.dumps(manifest))
    elif corruption == "status":
        manifest["status"] = 500
        manifest_path.write_text(json.dumps(manifest))
    else:
        write_page(
            directory,
            1,
            b'{"id":"2","markets":[]}',
            {"request": manifest["request"], "status": 200, "received_at": manifest["received_at"]},
        )
    with pytest.raises(CurrentError):
        create_backup(settings)
    assert not settings.backups_dir.exists()


def test_inventory_rehash_cannot_hide_corrupt_pending_body(settings, tmp_path):
    from oddsfox_catalogue.current_publish import _physical

    identifier = "c" * 32
    directory = settings.raw_dir / identifier
    write_page(
        directory,
        1,
        b'{"id":"1","markets":[]}',
        {
            "request": {"endpoint": "/events/1", "params": {}},
            "status": 200,
            "received_at": datetime.now(UTC).isoformat(),
        },
    )
    with connect_store(settings) as conn:
        set_state(
            conn,
            "fetch_pending",
            {
                "raw_id": identifier,
                "remaining": [{"endpoint": "/events/1", "params": {}}],
                "identity": {
                    "lane": "events",
                    "ids": [1],
                    "native": True,
                    "progress": {},
                    "policy": current_sync.POLICY,
                    "certificate": None,
                    "passes": [None],
                },
            },
        )
    backup = Path(create_backup(settings)["path"])
    name = f"raw/{identifier}/p000001.json.gz"
    (backup / name).write_bytes(b"corrupt")
    manifest = json.loads((backup / "backup.json").read_text())
    manifest["files"][name] = _physical(backup / name)
    (backup / "backup.json").write_text(json.dumps(manifest))
    with pytest.raises(CurrentError, match="pending response"):
        verify_backup(backup)
    with pytest.raises(CurrentError, match="pending response"):
        restore_backup(backup, tmp_path / "restore")
    assert not (tmp_path / "restore").exists()


def test_native_pending_404_is_recoverable(settings, tmp_path):
    identifier = "d" * 32
    write_page(
        settings.raw_dir / identifier,
        1,
        b'{"error":"not found"}',
        {
            "request": {"endpoint": "/events/1", "params": {}},
            "status": 404,
            "received_at": datetime.now(UTC).isoformat(),
        },
    )
    with connect_store(settings) as conn:
        set_state(
            conn,
            "fetch_pending",
            {
                "raw_id": identifier,
                "remaining": [{"endpoint": "/events/1", "params": {}}],
                "identity": {
                    "lane": "events",
                    "ids": [1],
                    "native": True,
                    "progress": {},
                    "policy": current_sync.POLICY,
                    "certificate": None,
                    "passes": [None],
                },
            },
        )
    backup = create_backup(settings)
    destination = tmp_path / "restore"
    restore_backup(backup["path"], destination)
    with connect_store(CurrentSettings(destination)) as conn:
        current_sync.fetch_apply(
            CurrentSettings(destination),
            conn,
            SimpleNamespace(get=lambda *args: pytest.fail("HTTP")),
            "events",
            [1],
            native=True,
        )
        assert get_state(conn, "fetch_pending") is None
