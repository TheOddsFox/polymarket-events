"""Canonical backup capacity excludes a confined off-root verification copy."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_publish, current_store
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.limits import directory_bytes


@pytest.fixture
def setup(tmp_path, monkeypatch):
    temporary = tmp_path / "verification"
    temporary.mkdir()
    monkeypatch.setattr(backup.tempfile, "gettempdir", lambda: str(temporary))
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.apply(
            conn,
            project_response(
                [{"id": "1", "title": "capacity", "markets": []}],
                "events",
                received_at="2026-10-14T00:00:00Z",
                response_id="capacity",
            ),
        )
        current_publish.publish(settings, conn)
    return settings, temporary


def source_size(settings):
    with connect_store(settings) as conn:
        files = backup._files(settings, conn)
        conn.execute("CHECKPOINT")
    return sum((settings.root / name).stat().st_size for name in files)


def physical_snapshot(root):
    return {
        str(path.relative_to(root)): None if path.is_dir() else backup._physical(path)
        for path in root.rglob("*")
    }


def test_eighth_daily_backup_fits_one_stage_budget_and_restores_verified_copy(
    setup, tmp_path, monkeypatch
):
    settings, temporary = setup
    clock = [datetime(2026, 10, 7, tzinfo=UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]

    monkeypatch.setattr(backup, "datetime", Clock)
    previous = []
    for _ in range(7):
        previous.append(backup.create_backup(settings))
        clock[0] += timedelta(days=1)
    assert len(list(settings.backups_dir.iterdir())) == 7
    size = source_size(settings)
    retained = directory_bytes(settings.root)
    ceiling = retained + size + 65536
    assert retained + 2 * size + 65536 > ceiling
    constrained = replace(settings, limits=replace(settings.limits, max_retained_bytes=ceiling))
    original_validate, original_temporary = backup._validate, backup.tempfile.TemporaryDirectory
    validations, allocations = [], []

    def validate(private, deadline):
        validations.append(private)
        return original_validate(private, deadline)

    def allocate(*args, **kwargs):
        allocations.append(kwargs.get("dir"))
        return original_temporary(*args, **kwargs)

    monkeypatch.setattr(backup, "_validate", validate)
    monkeypatch.setattr(backup.tempfile, "TemporaryDirectory", allocate)
    eighth = backup.create_backup(constrained)
    assert eighth["bytes"] == size
    assert directory_bytes(settings.root) <= ceiling
    assert len(list(settings.backups_dir.iterdir())) == 8
    assert all(Path(item["path"]).is_dir() for item in previous)
    assert backup.verify_backup(eighth["path"])["valid"]
    destination = tmp_path / "restored"
    assert backup.restore_backup(eighth["path"], destination)["valid"]
    restored = CurrentSettings(destination)
    with connect_store(restored, read_only=True) as conn:
        assert current_store.validate_store(conn)["valid"]
        assert conn.execute("SELECT event_id,title FROM events").fetchall() == [("1", "capacity")]
        assert current_publish.verify_current(restored)["counts"]["events"] == 1
    assert len(validations) == len(allocations) == 3
    assert all(Path(directory) == temporary.resolve() for directory in allocations)
    assert all(private.root.is_relative_to(temporary.resolve()) for private in validations)
    assert all(
        private.limits.max_temp_bytes + size == CurrentLimits().max_temp_bytes
        for private in validations
    )
    backup.prune(constrained)
    assert len(list(settings.backups_dir.iterdir())) == 7
    assert not Path(previous[0]["path"]).exists()
    assert Path(eighth["path"]).exists()
    with connect_store(constrained, read_only=True) as conn:
        assert current_store.get_state(conn, "last_backup") == eighth


@pytest.mark.parametrize("operation", ["create", "verify"])
@pytest.mark.parametrize(
    "location",
    [
        "canonical",
        "canonical-descendant",
        "ledger",
        "warehouse",
        "state-ledger",
        "alias",
        "alias-descendant",
    ],
)
def test_unsafe_verification_base_rejected_before_copy_or_staging(
    setup, tmp_path, monkeypatch, operation, location
):
    settings, _ = setup
    good = backup.create_backup(settings)
    if location.startswith("canonical"):
        unsafe = (
            settings.root if location == "canonical" else settings.root / "nested" / "temporary"
        )
    elif location.startswith("alias"):
        alias = tmp_path / "alias"
        alias.symlink_to(settings.root, target_is_directory=True)
        unsafe = alias if location == "alias" else alias / "nested" / "temporary"
    else:
        legacy = tmp_path / "legacy"
        legacy.mkdir()
        marker = {
            "ledger": "ledger.sqlite",
            "warehouse": "warehouse",
            "state-ledger": ".state/ledger.sqlite",
        }[location]
        path = legacy / marker
        path.parent.mkdir(parents=True, exist_ok=True)
        if location == "warehouse":
            path.mkdir()
        else:
            path.touch()
        unsafe = legacy / "nested" / "temporary"
    unsafe.mkdir(parents=True, exist_ok=True)
    before = physical_snapshot(settings.root)

    def forbidden(*args, **kwargs):
        pytest.fail("unsafe temporary base reached copying, staging or database access")

    with monkeypatch.context() as patch:
        patch.setattr(backup.tempfile, "gettempdir", lambda: str(unsafe))
        patch.setattr(backup.tempfile, "TemporaryDirectory", forbidden)
        patch.setattr(backup, "_copy", forbidden)
        patch.setattr(backup, "connect_store", forbidden)
        with pytest.raises(CurrentError, match="temporary directory.*outside"):
            if operation == "create":
                backup.create_backup(settings)
            else:
                backup.verify_backup(good["path"])
    assert physical_snapshot(settings.root) == before
    with connect_store(settings, read_only=True) as conn:
        assert current_store.get_state(conn, "last_backup") == good
    assert backup.verify_backup(good["path"])["valid"]


@pytest.mark.parametrize("topology", ["same-filesystem", "separate-filesystem"])
@pytest.mark.parametrize("headroom", [-1, 0], ids=["one-byte-short", "at-reserve"])
def test_offroot_verification_free_space_reserve_is_checked_on_temporary_base(
    setup, monkeypatch, topology, headroom
):
    settings, temporary = setup
    good = backup.create_backup(settings)
    limits = CurrentLimits()
    free = limits.free_reserve_bytes + limits.max_temp_bytes + headroom
    checked = []

    def usage(path):
        path = Path(path).resolve()
        checked.append(path)
        on_temporary = path.is_relative_to(temporary.resolve())
        return SimpleNamespace(
            free=free if on_temporary or topology == "same-filesystem" else 2 * free
        )

    monkeypatch.setattr(backup.shutil, "disk_usage", usage)
    if headroom < 0:
        with monkeypatch.context() as patch:
            patch.setattr(
                backup, "_copy", lambda *args: pytest.fail("copy before free-space guard")
            )
            with pytest.raises(CurrentError, match="verification free-space reserve"):
                backup.verify_backup(good["path"])
    else:
        assert backup.verify_backup(good["path"])["valid"]
    assert temporary.resolve() in checked


@pytest.mark.parametrize("headroom", [-1, 0], ids=["one-byte-short", "at-reserve"])
def test_canonical_stage_free_space_reserve_remains_enforced(setup, monkeypatch, headroom):
    settings, temporary = setup
    good = backup.create_backup(settings)
    size = source_size(settings)
    free = settings.limits.free_reserve_bytes + size + 65536 + headroom

    def usage(path):
        private = Path(path).resolve().is_relative_to(temporary.resolve())
        return SimpleNamespace(
            free=2 * (CurrentLimits().free_reserve_bytes + CurrentLimits().max_temp_bytes)
            if private
            else free
        )

    monkeypatch.setattr(backup.shutil, "disk_usage", usage)
    if headroom < 0:
        with monkeypatch.context() as patch:
            patch.setattr(
                backup, "_copy", lambda *args: pytest.fail("copy before canonical free-space guard")
            )
            with pytest.raises(CurrentError, match="free-space reserve"):
                backup.create_backup(settings)
        with connect_store(settings, read_only=True) as conn:
            assert current_store.get_state(conn, "last_backup") == good
    else:
        assert backup.create_backup(settings)["valid"]


@pytest.mark.parametrize("excess", [0, 1])
def test_private_copy_cannot_consume_entire_eight_gib_temporary_budget(setup, monkeypatch, excess):
    settings, _ = setup
    good = backup.create_backup(settings)
    monkeypatch.setattr(
        backup,
        "_inventory",
        lambda *args: {
            "files": {"catalogue.duckdb": {"bytes": CurrentLimits().max_temp_bytes + excess}}
        },
    )
    monkeypatch.setattr(backup, "_copy", lambda *args: pytest.fail("copy above temporary cap"))
    with pytest.raises(CurrentError, match="temporary allowance"):
        backup.verify_backup(good["path"])
