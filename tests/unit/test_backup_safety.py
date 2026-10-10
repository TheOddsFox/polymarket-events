"""Backup trust boundaries are checked before a restoration can create its destination."""

import json
import os
from dataclasses import replace

import pytest

from oddsfox_catalogue.backup import MANIFEST, create_backup, restore_backup, verify_backup
from oddsfox_catalogue.config import Settings, load_settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


@pytest.fixture
def backup(tmp_path):
    settings = Settings(tmp_path / "source")
    settings.raw_dir.mkdir(parents=True)
    (settings.raw_dir / "evidence").write_bytes(b"immutable")
    settings = replace(settings, quality=replace(settings.quality, open_events_drop_warn_pct=7.0))
    return create_backup(settings)


def change_manifest(backup, mutate):
    path = backup / MANIFEST
    value = json.loads(path.read_bytes())
    mutate(value)
    path.write_text(json.dumps(value))


def test_restore_preserves_evidence_and_effective_settings_in_portable_paths(backup, tmp_path):
    destination = tmp_path / "restored"
    assert restore_backup(backup, destination) == destination
    restored = load_settings(root=destination, env={})
    assert restored.quality.open_events_drop_warn_pct == 7.0
    assert (restored.raw_dir / "evidence").read_bytes() == b"immutable"
    assert restored.state_dir == destination / ".state"
    assert restored.warehouse_path == destination / "data/warehouse/catalogue.duckdb"
    assert (backup / "raw/evidence").read_bytes() == b"immutable"
    assert verify_backup(backup) == []


@pytest.mark.parametrize(
    "name",
    [
        "../outside",
        "/absolute",
        "raw/../outside",
        "raw\\bad",
        "raw//bad",
        "raw/./bad",
        "unexpected/a",
    ],
)
def test_unsafe_inventory_is_rejected_before_destination_creation(backup, tmp_path, name):
    change_manifest(
        backup, lambda value: value["files"].update({name: value["files"]["raw/evidence"]})
    )
    destination = tmp_path / "new"
    assert verify_backup(backup)
    with pytest.raises(ValueError):
        restore_backup(backup, destination)
    assert not destination.exists()


@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "extra_file",
        "extra_directory",
        "size",
        "checksum",
        "boolean_size",
        "directory_type",
        "version",
        "settings",
        "allowance",
    ],
)
def test_inventory_types_sizes_checksums_and_settings_fail_closed(backup, tmp_path, kind):
    if kind == "missing":
        (backup / "raw/evidence").unlink()
    elif kind == "extra_file":
        (backup / "surprise").write_bytes(b"x")
    elif kind == "extra_directory":
        (backup / "raw/surprise").mkdir()
    elif kind == "size":
        (backup / "raw/evidence").write_bytes(b"changed length")
    elif kind == "checksum":
        (backup / "raw/evidence").write_bytes(b"different")
    elif kind == "boolean_size":
        change_manifest(backup, lambda value: value["files"]["raw/evidence"].update(bytes=True))
    elif kind == "directory_type":
        (backup / "metadata").rmdir()
        (backup / "metadata").write_bytes(b"x")
    elif kind == "version":
        change_manifest(backup, lambda value: value.update(backup_version=True))
    elif kind == "settings":
        change_manifest(backup, lambda value: value["settings"]["capture"].update(workers="1"))
    elif kind == "allowance":
        change_manifest(
            backup, lambda value: value["settings"]["capture"].update(max_retained_bytes=1)
        )
    destination = tmp_path / "new"
    assert verify_backup(backup)
    with pytest.raises((ValueError, OSError)):
        restore_backup(backup, destination)
    assert not destination.exists()


@pytest.mark.parametrize("kind", ["file_link", "directory_link", "fifo", "ancestor_link"])
def test_links_and_special_files_are_rejected_without_following_them(backup, tmp_path, kind):
    if kind == "file_link":
        (backup / "raw/evidence").unlink()
        (backup / "raw/evidence").symlink_to(tmp_path / "outside")
    elif kind == "directory_link":
        (backup / "metadata").rmdir()
        (backup / "metadata").symlink_to(tmp_path, target_is_directory=True)
    elif kind == "fifo":
        os.mkfifo(backup / "raw/fifo")
    elif kind == "ancestor_link":
        link = tmp_path / "alias"
        link.symlink_to(backup.parent, target_is_directory=True)
        backup = link / backup.name
    assert verify_backup(backup)
    with pytest.raises(ValueError):
        restore_backup(backup, tmp_path / "new")


def test_existing_empty_destination_is_never_merged_or_overwritten(backup, tmp_path):
    destination = tmp_path / "existing"
    destination.mkdir()
    with pytest.raises(FileExistsError, match="fresh destination"):
        restore_backup(backup, destination)
    assert list(destination.iterdir()) == []


def test_restore_rejects_destination_symlink_ancestor(backup, tmp_path):
    (tmp_path / "alias").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        restore_backup(backup, tmp_path / "alias/new")
    assert not (tmp_path / "new").exists()


def test_manifest_duplicate_keys_are_not_silently_overwritten(backup):
    path = backup / MANIFEST
    text = path.read_text()
    path.write_text(
        text.replace('"backup_version": 2,', '"backup_version": 1, "backup_version": 2,')
    )
    assert any("duplicate" in problem for problem in verify_backup(backup))


def test_backup_rejects_source_symlink_and_does_not_commit_a_manifest(tmp_path):
    settings = Settings(tmp_path)
    settings.raw_dir.mkdir(parents=True)
    (settings.raw_dir / "link").symlink_to(tmp_path / "outside")
    with pytest.raises((ValueError, RequestBudgetExceeded), match="symlink"):
        create_backup(settings)
    assert not list(settings.data_dir.glob("backups/*/backup.json"))


def test_failed_fresh_restore_preserves_partial_evidence_without_overwrite(
    backup, tmp_path, monkeypatch
):
    import oddsfox_catalogue.backup as module

    original = module._stream_file

    def fail_copy(path, *, destination=None, **kwargs):
        if destination is not None:
            raise OSError("injected copying failure")
        return original(path, destination=destination, **kwargs)

    monkeypatch.setattr(module, "_stream_file", fail_copy)
    destination = tmp_path / "new"
    with pytest.raises(OSError, match="injected"):
        restore_backup(backup, destination)
    assert destination.is_dir()
    with pytest.raises(FileExistsError):
        restore_backup(backup, destination)


def test_backup_does_not_commit_when_source_changes_during_copy(tmp_path, monkeypatch):
    import oddsfox_catalogue.backup as module

    settings = Settings(tmp_path)
    settings.raw_dir.mkdir(parents=True)
    evidence = settings.raw_dir / "evidence"
    evidence.write_bytes(b"before")
    original = module._stream_file

    def mutate_source(path, *, destination=None, **kwargs):
        result = original(path, destination=destination, **kwargs)
        if path == evidence and destination is not None:
            evidence.write_bytes(b"after!")
        return result

    monkeypatch.setattr(module, "_stream_file", mutate_source)
    with pytest.raises(ValueError, match="source changed"):
        create_backup(settings)
    assert not list(settings.data_dir.glob("backups/*/backup.json"))


def test_backup_never_writes_into_immutable_source_evidence(tmp_path):
    settings = Settings(tmp_path)
    settings.raw_dir.mkdir(parents=True)
    with pytest.raises(ValueError, match="inside source evidence"):
        create_backup(settings, dest_root=settings.raw_dir / "backups")
    assert list(settings.raw_dir.iterdir()) == []


def test_restoration_never_writes_inside_the_verified_backup(backup):
    with pytest.raises(ValueError, match="inside its backup"):
        restore_backup(backup, backup / "new")
    assert verify_backup(backup) == []
