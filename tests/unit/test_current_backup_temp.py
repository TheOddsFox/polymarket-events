"""Private verification ownership and aggregate admission, without a database."""

import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from filelock import FileLock

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.runlock import RunBusy


@pytest.fixture
def temporary(monkeypatch, tmp_path):
    root = tmp_path / "temporary"
    root.mkdir()
    monkeypatch.setattr(backup, "_verification_temp", lambda: root)
    return root


@pytest.fixture
def verification(monkeypatch, temporary, tmp_path):
    source = tmp_path / "backup"
    source.mkdir()
    value = {"files": {"catalogue.duckdb": {"bytes": 20}, "catalogue.json": {"bytes": 5}}}
    monkeypatch.setattr(backup, "_inventory", lambda *args: value)
    settings_type = backup.CurrentSettings
    limits = replace(settings_type(source).limits, max_temp_bytes=100, free_reserve_bytes=0)
    monkeypatch.setattr(
        backup,
        "CurrentSettings",
        lambda path, **kwargs: settings_type(path, limits=kwargs.get("limits", limits)),
    )
    monkeypatch.setattr(backup.shutil, "disk_usage", lambda path: SimpleNamespace(free=1000))
    observed = []

    def copy(source, root, inventory, deadline):
        (root / "catalogue.duckdb").write_bytes(b"x" * 20)
        (root / "catalogue.json").write_bytes(b"x" * 5)

    def validate(settings, deadline, **kwargs):
        observed.append((settings.root, settings.limits.max_temp_bytes))
        return {"valid": True}, list(value["files"])

    monkeypatch.setattr(backup, "_copy", copy)
    monkeypatch.setattr(backup, "_validate", validate)
    return source, observed


def test_standalone_and_inherited_cleanup(verification):
    source, observed = verification
    assert backup.verify_backup(source)["valid"]
    assert not observed[-1][0].exists()
    base = backup._verification_base()
    assert list(base.iterdir()) == [base / "admission.lock"]
    with backup._verification_workspace() as workspace:
        assert backup.verify_backup(source, _workspace=workspace)["valid"]
        assert workspace.exists()
        assert list(workspace.iterdir()) == []
    assert not workspace.exists()


def test_stale_bytes_reduce_spill_and_reject_next_copy(verification):
    source, observed = verification
    with backup._verification_workspace() as stale:
        residue = stale / "interrupted.duckdb"
        residue.write_bytes(b"x" * 30)
        backup.verify_backup(source)
        assert observed[-1][1] == 45
        residue.write_bytes(b"x" * 75)
        with pytest.raises(CurrentError, match="temporary allowance"):
            backup.verify_backup(source)
        assert residue.stat().st_size == 75


def test_cleanup_only_exact_invocation(temporary):
    with backup._verification_workspace() as sibling:
        evidence = sibling / "evidence"
        evidence.write_text("keep")
        with backup._verification_workspace() as owned:
            (owned / "copy").write_text("remove")
        assert not owned.exists()
        assert evidence.read_text() == "keep"


@pytest.mark.parametrize("kind", ["unexpected", "symlink", "special"])
def test_reject_unsafe_base_contents(verification, kind):
    source, _ = verification
    base = backup._verification_base()
    child = base / ("unexpected" if kind == "unexpected" else "invocation-" + "a" * 32)
    if kind == "symlink":
        child.symlink_to(source, target_is_directory=True)
    elif kind == "special":
        os.mkfifo(child)
    else:
        child.mkdir(mode=0o700)
    with pytest.raises(CurrentError):
        backup.verify_backup(source)
    assert child.exists()


def test_reject_nested_special_file(verification):
    source, _ = verification
    with backup._verification_workspace() as stale:
        os.mkfifo(stale / "special")
        with pytest.raises(CurrentError):
            backup.verify_backup(source)


def test_old_flat_artifact_preserved(temporary):
    artifact = temporary / "catalogue-verify-old"
    artifact.mkdir()
    with pytest.raises(CurrentError, match=str(artifact)), backup._verification_workspace():
        pytest.fail("legacy artifact admitted")
    assert artifact.exists()


def test_lock_contends_across_instances(verification):
    source, _ = verification
    base = backup._verification_base()
    with FileLock(str(base / "admission.lock"), mode=0o600), pytest.raises(RunBusy):
        backup.verify_backup(source)


def test_workspace_must_be_direct_private_owned_child(verification, tmp_path):
    source, _ = verification
    unrelated = tmp_path / "invocation-" / ("a" * 32)
    unrelated.mkdir(parents=True)
    with pytest.raises(CurrentError, match="unrecognized"):
        backup.verify_backup(source, _workspace=unrelated)
    with backup._verification_workspace() as workspace:
        workspace.chmod(0o755)
        try:
            with pytest.raises(CurrentError, match="unsafe"):
                backup.verify_backup(source, _workspace=workspace)
        finally:
            workspace.chmod(0o700)


def test_reject_base_symlink_or_public_permissions(temporary, tmp_path):
    base = temporary / f"oddsfox-catalogue-verification-{os.getuid()}"
    base.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(CurrentError):
        backup._verification_base()
    base.unlink()
    base.mkdir(mode=0o755)
    base.chmod(0o755)
    with pytest.raises(CurrentError, match="unsafe"):
        backup._verification_base()


def test_directory_owner_required(temporary, monkeypatch):
    with backup._verification_workspace() as workspace:
        actual_uid = os.getuid()
        with monkeypatch.context() as patch:
            patch.setattr(backup.os, "getuid", lambda: actual_uid + 1)
            with pytest.raises(CurrentError, match="unsafe"):
                backup._private_directory(workspace)


@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo"])
def test_legacy_artifact_ownership_filter(temporary, monkeypatch, kind):
    foreign = temporary / "catalogue-verify-foreign"
    if kind == "directory":
        foreign.mkdir()
    elif kind == "symlink":
        foreign.symlink_to(temporary / "missing-target")
    else:
        os.mkfifo(foreign)
    original_lstat = Path.lstat

    def ownership(path, *args, **kwargs):
        info = original_lstat(path, *args, **kwargs)
        return SimpleNamespace(st_uid=os.getuid() + 1) if path == foreign else info

    monkeypatch.setattr(Path, "lstat", ownership)
    assert backup._verification_base().is_dir()
    assert os.path.lexists(foreign)
    own = temporary / "catalogue-verify-own-unsafe"
    own.symlink_to(temporary / "missing-own-target")
    with pytest.raises(CurrentError, match=str(own)):
        backup._verification_base()
    assert os.path.lexists(own)
    assert os.path.lexists(foreign)
