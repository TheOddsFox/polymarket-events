"""Compressed databases remain bounded, inventoried and independently recoverable."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_publish, current_store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import CurrentError


@pytest.fixture
def settings(tmp_path):
    settings = CurrentSettings(tmp_path / "source")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_publish.publish(settings, conn)
    return settings


def manifest(path):
    return json.loads((path / "backup.json").read_text())


def save(path, value):
    (path / "backup.json").write_text(json.dumps(value))


def rehash_stored(path, value):
    value["files"]["catalogue.duckdb"].update(
        {
            "stored_bytes": (path / "catalogue.duckdb.gz").stat().st_size,
            "stored_sha256": hashlib.sha256(
                (path / "catalogue.duckdb.gz").read_bytes()
            ).hexdigest(),
        }
    )
    save(path, value)


def test_compressed_database_roundtrip_and_private_verification_reads_evidence_directly(
    settings, tmp_path, monkeypatch
):
    result = backup.create_backup(settings)
    path = Path(result["path"])
    value = manifest(path)
    descriptor = value["files"]["catalogue.duckdb"]
    assert set(descriptor) == {"bytes", "sha256", "encoding", "stored_bytes", "stored_sha256"}
    assert descriptor["encoding"] == "gzip"
    assert not (path / "catalogue.duckdb").exists()
    assert gzip.decompress((path / "catalogue.duckdb.gz").read_bytes())
    assert result["stored_bytes"] < result["bytes"]
    before = {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()}
    original = backup._validate
    seen = []

    def validate(private, deadline, *, evidence_root=None):
        assert evidence_root == path
        assert set(p.name for p in private.root.iterdir()) == {"catalogue.json", "catalogue.duckdb"}
        assert not (private.root / "published").exists()
        seen.append(private)
        return original(private, deadline, evidence_root=evidence_root)

    monkeypatch.setattr(backup, "_validate", validate)
    assert backup.verify_backup(path)["valid"]
    assert seen
    assert before == {
        str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()
    }
    destination = tmp_path / "restored"
    assert backup.restore_backup(path, destination)["valid"]
    assert backup._physical(destination / "catalogue.duckdb") == {
        key: descriptor[key] for key in ("bytes", "sha256")
    }
    assert current_publish.verify_current(CurrentSettings(destination))["valid"]


@pytest.mark.parametrize(
    "corruption",
    [
        "truncated",
        "crc",
        "expanded",
        "logical-hash",
        "stored-hash",
        "unknown-codec",
        "unknown-field",
        "bool-bytes",
        "plain-extra",
    ],
)
def test_malformed_compressed_database_rejected_before_restore_creation(
    settings, tmp_path, corruption
):
    path = Path(backup.create_backup(settings)["path"])
    value = manifest(path)
    descriptor = value["files"]["catalogue.duckdb"]
    compressed = path / "catalogue.duckdb.gz"
    data = compressed.read_bytes()
    if corruption == "truncated":
        compressed.write_bytes(data[:-4])
        rehash_stored(path, value)
    elif corruption == "crc":
        compressed.write_bytes(data[:-8] + bytes([data[-8] ^ 1]) + data[-7:])
        rehash_stored(path, value)
    elif corruption == "expanded":
        descriptor["bytes"] -= 1
    elif corruption == "logical-hash":
        descriptor["sha256"] = "0" * 64
    elif corruption == "stored-hash":
        descriptor["stored_sha256"] = "0" * 64
    elif corruption == "unknown-codec":
        descriptor["encoding"] = "zip"
    elif corruption == "unknown-field":
        descriptor["file"] = "elsewhere.gz"
    elif corruption == "bool-bytes":
        descriptor["bytes"] = True
    else:
        (path / "catalogue.duckdb").write_bytes(b"extra")
    save(path, value)
    destination = tmp_path / "restored"
    with pytest.raises(CurrentError):
        backup.restore_backup(path, destination)
    assert not destination.exists()


def test_expansion_bound_is_checked_before_offending_chunk_is_written(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    body = b"x" * (1024**2 + 1)
    (source / "catalogue.duckdb.gz").write_bytes(gzip.compress(body))
    descriptor = {"encoding": "gzip", "bytes": 1024**2, "sha256": hashlib.sha256(body).hexdigest()}
    with pytest.raises(CurrentError, match="expanded size"):
        backup._copy(source, destination, {"catalogue.duckdb": descriptor}, None)
    assert (destination / "catalogue.duckdb").stat().st_size == 1024**2


@pytest.mark.parametrize("duplicate", ["top", "descriptor"])
def test_duplicate_manifest_keys_are_rejected(settings, duplicate):
    path = Path(backup.create_backup(settings)["path"])
    value = (path / "backup.json").read_text()
    if duplicate == "top":
        value = value.replace('"contract":', '"contract":"untrusted","contract":', 1)
    else:
        value = value.replace('"bytes":', '"bytes":0,"bytes":', 1)
    (path / "backup.json").write_text(value)
    with pytest.raises(CurrentError, match="manifest"):
        backup.verify_backup(path)


def test_no_publication_is_recoverable(tmp_path):
    settings = CurrentSettings(tmp_path / "source")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        current_store.initialize(conn)
    result = backup.create_backup(settings)
    assert set(manifest(Path(result["path"]))["files"]) == {"catalogue.json", "catalogue.duckdb"}
    assert backup.restore_backup(result["path"], tmp_path / "restore")["valid"]


def test_stale_current_and_previous_publications_share_private_connection(settings, monkeypatch):
    with connect_store(settings) as conn:
        current_store.set_state(conn, "generation", 1)
        current_publish.publish(settings, conn)
        current_store.set_state(conn, "generation", 2)
    path = Path(backup.create_backup(settings)["path"])
    original = backup._verify_release_on_connection
    seen = []

    def verify(release, conn, *, bind_store):
        seen.append((id(conn), bind_store))
        return original(release, conn, bind_store=bind_store)

    monkeypatch.setattr(backup, "_verify_release_on_connection", verify)
    assert backup.verify_backup(path)["valid"]
    assert len(seen) == 2
    assert len({identifier for identifier, _ in seen}) == 1
    assert not any(bound for _, bound in seen)


def test_failed_compression_preserves_previous_backup(settings, monkeypatch):
    good = backup.create_backup(settings)

    def fail(*args, **kwargs):
        raise CurrentError("compression failure")

    monkeypatch.setattr(backup, "_compress_database", fail)
    with pytest.raises(CurrentError, match="compression failure"):
        backup.create_backup(settings)
    assert [p.name for p in settings.backups_dir.iterdir()] == [Path(good["path"]).name]
    with connect_store(settings, read_only=True) as conn:
        assert current_store.get_state(conn, "last_backup") == good


def test_failed_restore_cleans_destination_and_stamp(settings, tmp_path, monkeypatch):
    path = backup.create_backup(settings)["path"]
    destination = tmp_path / "restore"
    original = backup._copy

    def fail(source, target, inventory, deadline):
        original(source, target, inventory, deadline)
        if target == destination:
            assert not (target / "catalogue.json").exists()
            raise CurrentError("restore failure")

    monkeypatch.setattr(backup, "_copy", fail)
    with pytest.raises(CurrentError, match="restore failure"):
        backup.restore_backup(path, destination)
    assert not destination.exists()


def test_source_changes_during_compression_prevent_promotion(settings, monkeypatch):
    config = settings.root / "config.toml"
    config.write_text("# original\n")
    good = backup.create_backup(settings)
    original = backup._compress_database

    def compress(*args, **kwargs):
        result = original(*args, **kwargs)
        config.write_text("# changed\n")
        return result

    monkeypatch.setattr(backup, "_compress_database", compress)
    with pytest.raises(CurrentError, match="source changed"):
        backup.create_backup(settings)
    assert [p.name for p in settings.backups_dir.iterdir()] == [Path(good["path"]).name]


def test_backup_changes_during_private_validation_are_rejected(settings, monkeypatch):
    (settings.root / "config.toml").write_text("# original\n")
    path = Path(backup.create_backup(settings)["path"])
    original = backup._validate

    def validate(*args, **kwargs):
        result = original(*args, **kwargs)
        (path / "config.toml").write_text("# modified\n")
        return result

    monkeypatch.setattr(backup, "_validate", validate)
    with pytest.raises(CurrentError, match="inventory"):
        backup.verify_backup(path)


def test_compression_deadline_is_checked_between_chunks(tmp_path, monkeypatch):
    source = tmp_path / "database"
    source.write_bytes(b"x" * 1024**2)
    destination = tmp_path / "destination"
    destination.mkdir()
    expected = backup._physical(source)
    monkeypatch.setattr(backup.time, "monotonic", lambda: 2)
    with pytest.raises(CurrentError, match="deadline"):
        backup._compress_database(
            source, destination, expected, 1, settings=CurrentSettings(destination)
        )


def test_rehashed_future_publication_cannot_claim_private_store_state(settings):
    path = Path(backup.create_backup(settings)["path"])
    value = manifest(path)
    pointer_path = path / "published/current.json"
    pointer = json.loads(pointer_path.read_text())
    release_path = path / "published/releases" / pointer["release_id"] / "manifest.json"
    release = json.loads(release_path.read_text())
    release["generation"] = 1
    release_path.write_text(json.dumps(release))
    pointer["manifest_sha256"] = backup._physical(release_path)["sha256"]
    pointer_path.write_text(json.dumps(pointer))
    for file in (release_path, pointer_path):
        value["files"][str(file.relative_to(path))] = backup._physical(file)
    save(path, value)
    with pytest.raises(CurrentError, match="generation mismatch"):
        backup.verify_backup(path)


def test_compressed_output_fits_headroom_smaller_than_database(settings):
    from dataclasses import replace

    good = backup.create_backup(settings)
    with connect_store(settings) as conn:
        files = backup._files(settings, conn)
        conn.execute("CHECKPOINT")
    other_bytes = sum(
        (settings.root / name).stat().st_size for name in files if name != "catalogue.duckdb"
    )
    compressed_bytes = manifest(Path(good["path"]))["files"]["catalogue.duckdb"]["stored_bytes"]
    headroom = other_bytes + compressed_bytes + 65536 + 16384
    assert headroom < settings.store_path.stat().st_size
    ceiling = backup.directory_bytes(settings.root) + headroom
    constrained = replace(settings, limits=replace(settings.limits, max_retained_bytes=ceiling))
    result = backup.create_backup(constrained)
    assert result["valid"]
    assert Path(good["path"]).is_dir()
    assert Path(result["path"]).is_dir()
    assert result["stored_bytes"] <= headroom


def test_admission_failure_after_encoded_writes_removes_only_candidate(settings, monkeypatch):
    good = backup.create_backup(settings)
    original_admit = backup.admit
    writes = []

    class Compressor:
        def compress(self, block):
            return b"x" * 17

        def flush(self):
            return b"x" * 17

    def admit(current, *, additional_bytes=0):
        staged = list(settings.backups_dir.glob(".staging-*/catalogue.duckdb.gz"))
        if staged:
            writes.append(staged[0].stat().st_size)
            if len(writes) == 3:
                raise CurrentError("encoded output allowance exhausted")
        return original_admit(current, additional_bytes=additional_bytes)

    monkeypatch.setattr(backup.zlib, "compressobj", lambda **kwargs: Compressor())
    monkeypatch.setattr(backup, "admit", admit)
    with pytest.raises(CurrentError, match="encoded output allowance"):
        backup.create_backup(settings)
    assert writes == [0, 17, 34]
    assert [p.name for p in settings.backups_dir.iterdir()] == [Path(good["path"]).name]
    with connect_store(settings, read_only=True) as conn:
        assert current_store.get_state(conn, "last_backup") == good
