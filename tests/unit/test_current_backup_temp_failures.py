"""Resource ownership is tested with real processes and byte-sized inventories."""

import multiprocessing
import os
import signal
from contextlib import suppress
from dataclasses import replace
from pathlib import Path

import pytest

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.runlock import RunBusy
from oddsfox_catalogue.signals import Terminated

_FILES = {"catalogue.duckdb": {"bytes": 8}, "catalogue.json": {"bytes": 4}}


def _byte_verifier(patch, temporary, *, budget=64, ready=None, release=None):
    """Keep real workspace, admission, copy lifetime and cleanup; omit SQL/content checks."""
    patch.setattr(backup.tempfile, "gettempdir", lambda: str(temporary))
    limits = replace(CurrentLimits(), max_temp_bytes=budget, free_reserve_bytes=0)
    patch.setattr(
        backup,
        "CurrentSettings",
        lambda root, **kwargs: CurrentSettings(root, **({"limits": limits} | kwargs)),
    )
    patch.setattr(backup, "_inventory", lambda *_: {"files": _FILES})

    def copy(source, destination, files, deadline):
        for name, item in files.items():
            (destination / name).write_bytes(b"x" * item["bytes"])

    def validate(settings, deadline, **kwargs):
        if ready is not None:
            ready.set()
            assert release.wait(30), "test did not release semantic verification"
        return {"valid": True, "spill_allowance": settings.limits.max_temp_bytes}, set(_FILES)

    patch.setattr(backup, "_copy", copy)
    patch.setattr(backup, "_validate", validate)


def _verify_in_process(temporary, source, ready, release, workspace=None, done=None):
    with pytest.MonkeyPatch.context() as patch:
        _byte_verifier(patch, temporary, ready=ready, release=release)
        backup.verify_backup(source, _workspace=workspace)
    if done is not None:
        done.set()


def _workspace_owner(temporary, source, ready, release, done, metadata):
    with pytest.MonkeyPatch.context() as patch:
        _byte_verifier(patch, temporary)
        with backup._verification_workspace() as workspace:
            child = multiprocessing.get_context("spawn").Process(
                target=_verify_in_process,
                args=(temporary, source, ready, release, workspace, done),
            )
            child.start()
            metadata.send((child.pid, str(workspace)))
            metadata.close()
            child.join(30)


@pytest.fixture
def byte_verifier(tmp_path, monkeypatch):
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    source = tmp_path / "backup-one"
    source.mkdir()
    _byte_verifier(monkeypatch, temporary)
    return temporary, source


@pytest.mark.parametrize("headroom", [0, 1], ids=["no-spill-room", "one-byte-spill-room"])
def test_killed_standalone_verifier_leftovers_are_charged_on_retry(
    byte_verifier, monkeypatch, headroom
):
    temporary, source = byte_verifier
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    child = context.Process(target=_verify_in_process, args=(temporary, source, ready, release))
    child.start()
    try:
        assert ready.wait(10), "verifier never reached its private copy"
        base = backup._verification_base()
        (orphan,) = base.glob("invocation-*")
        assert backup._verification_bytes(base) == 12
        child.kill()
        child.join(10)
        assert not child.is_alive()
        assert child.exitcode == -signal.SIGKILL
        before = {
            path.relative_to(orphan): path.read_bytes()
            for path in orphan.rglob("*")
            if path.is_file()
        }
        _byte_verifier(monkeypatch, temporary, budget=24 + headroom)
        if headroom:
            result = backup.verify_backup(source)
            assert result["valid"]
            assert result["spill_allowance"] == 1
        else:
            monkeypatch.setattr(backup, "_copy", lambda *_: pytest.fail("copy exceeded shared cap"))
            with pytest.raises(CurrentError, match="temporary allowance"):
                backup.verify_backup(source)
        assert list(base.glob("invocation-*")) == [orphan]
        assert {
            path.relative_to(orphan): path.read_bytes()
            for path in orphan.rglob("*")
            if path.is_file()
        } == before
        assert backup._verification_bytes(base) == 12
    finally:
        if child.is_alive():
            child.kill()
        child.join(10)


def test_semantic_interruption_removes_owned_copy_and_releases_admission(
    byte_verifier, monkeypatch
):
    temporary, source = byte_verifier

    def interrupted(*args, **kwargs):
        raise Terminated(signal.SIGTERM)

    monkeypatch.setattr(backup, "_validate", interrupted)
    with pytest.raises(Terminated):
        backup.verify_backup(source)
    base = backup._verification_base()
    assert not list(base.glob("invocation-*"))
    _byte_verifier(monkeypatch, temporary)
    assert backup.verify_backup(source)["valid"]


def test_parent_death_preserves_live_orphan_lock_across_catalogue_roots(byte_verifier, tmp_path):
    temporary, source = byte_verifier
    other_source = tmp_path / "backup-two"
    other_source.mkdir()
    context = multiprocessing.get_context("spawn")
    ready, release, done = context.Event(), context.Event(), context.Event()
    receive, send = context.Pipe(duplex=False)
    owner = context.Process(
        target=_workspace_owner,
        args=(temporary, source, ready, release, done, send),
    )
    orphan_pid = None
    owner.start()
    send.close()
    try:
        assert receive.poll(10), "owner never launched verifier"
        orphan_pid, workspace = receive.recv()
        workspace = Path(workspace)
        assert ready.wait(10), "orphan never reached semantic verification"
        owner.kill()
        owner.join(10)
        assert not owner.is_alive()
        with pytest.raises(RunBusy, match="another backup verification"):
            backup.verify_backup(other_source)
        assert workspace.exists()
        assert backup._verification_bytes(backup._verification_base()) == 12
        release.set()
        assert done.wait(10), "orphan failed to finish after its parent died"
        assert backup.verify_backup(other_source)["valid"]
        # Only the dead owner could remove this invocation; no other run may adopt it.
        assert workspace.exists()
        assert not list(workspace.iterdir())
    finally:
        release.set()
        if owner.is_alive():
            owner.kill()
        owner.join(10)
        if orphan_pid is not None and not done.wait(5):
            with suppress(ProcessLookupError):
                os.kill(orphan_pid, signal.SIGKILL)
        receive.close()


@pytest.mark.parametrize("location", ["sibling", "arbitrary", "symlink"])
def test_inherited_workspace_must_be_a_private_direct_child(
    byte_verifier, tmp_path, monkeypatch, location
):
    _, source = byte_verifier
    base = backup._verification_base()
    outside = tmp_path / "operator-directory"
    outside.mkdir(mode=0o700)
    sentinel = outside / "keep"
    sentinel.write_bytes(b"unrelated")
    if location == "sibling":
        workspace = base.parent / ("invocation-" + "a" * 32)
        workspace.mkdir(mode=0o700)
    elif location == "symlink":
        workspace = base / ("invocation-" + "a" * 32)
        workspace.symlink_to(outside, target_is_directory=True)
    else:
        workspace = outside
    monkeypatch.setattr(
        backup, "_inventory", lambda *_: pytest.fail("unconfined workspace reached inventory")
    )
    with pytest.raises(CurrentError):
        backup.verify_backup(source, _workspace=workspace)
    assert sentinel.read_bytes() == b"unrelated"
    assert outside.is_dir()
