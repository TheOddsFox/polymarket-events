"""Retention preserves the durable latest backup, independent of UUID order."""

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_publish import publish
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


@pytest.fixture
def catalogue(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
        publish(settings, conn)
    temporary = tmp_path / "verification"
    temporary.mkdir()
    monkeypatch.setattr(backup.tempfile, "gettempdir", lambda: str(temporary))
    return settings


def set_latest(settings, value):
    with connect_store(settings) as conn:
        store.set_state(conn, "last_backup", value)


def archive(settings, name):
    path = settings.backups_dir / name
    path.mkdir(parents=True)
    (path / "sentinel").write_bytes(name.encode())
    return path


def verified_archives(monkeypatch, *, corrupt=()):
    visited = []

    def verify(path):
        path = Path(path)
        visited.append(path)
        if path in corrupt:
            raise CurrentError("injected corrupt backup")
        return {"valid": True, "path": str(path)}

    monkeypatch.setattr(backup, "verify_backup", verify)
    return visited


def test_latest_same_day_backup_survives_lower_random_uuid(catalogue, monkeypatch):
    settings = catalogue

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 14, tzinfo=UTC)

    identifiers = iter(["f" * 32, "b" * 32, "1" * 32, "c" * 32])
    monkeypatch.setattr(backup, "datetime", Clock)
    monkeypatch.setattr(backup, "uuid4", lambda: SimpleNamespace(hex=next(identifiers)))
    first = backup.create_backup(settings)
    latest = backup.create_backup(settings)
    assert Path(latest["path"]).name < Path(first["path"]).name

    backup.prune(settings)

    assert Path(latest["path"]).is_dir()
    assert not Path(first["path"]).exists()
    assert backup.verify_backup(latest["path"])["valid"]
    with connect_store(settings, read_only=True) as conn:
        assert store.get_state(conn, "last_backup") == latest


@pytest.mark.parametrize("latest_day", ["20260901", "20261101"])
def test_latest_wins_over_date_order_and_seven_day_retention(catalogue, monkeypatch, latest_day):
    settings = catalogue
    others = [archive(settings, f"202610{day:02d}-" + "f" * 32) for day in range(2, 11)]
    latest = archive(settings, latest_day + "-" + "1" * 32)
    state = {"path": str(latest), "created_at": "irrelevant to retention"}
    set_latest(settings, state)
    visited = verified_archives(monkeypatch)

    result = backup.prune(settings)

    assert set(visited) == {*others, latest}
    assert len(visited) == 10
    assert set(settings.backups_dir.iterdir()) == {latest, *sorted(others, reverse=True)[:6]}
    assert result["deleted"] == 3
    assert (latest / "sentinel").read_bytes() == latest.name.encode()
    with connect_store(settings, read_only=True) as conn:
        assert store.get_state(conn, "last_backup") == state


@pytest.mark.parametrize("damage", ["missing", "corrupt", "file", "symlink", "fifo"])
def test_unavailable_owned_latest_fails_before_any_archive_deletion(
    catalogue, tmp_path, monkeypatch, damage
):
    settings = catalogue
    others = [archive(settings, f"202610{day:02d}-" + "f" * 32) for day in range(2, 10)]
    latest = settings.backups_dir / ("20261001-" + "1" * 32)
    set_latest(settings, {"path": str(latest)})
    if damage == "corrupt":
        archive(settings, latest.name)
    elif damage == "file":
        latest.write_bytes(b"not a backup directory")
    elif damage == "symlink":
        target = tmp_path / "outside"
        target.mkdir()
        (target / "sentinel").write_bytes(b"must survive")
        latest.symlink_to(target, target_is_directory=True)
    elif damage == "fifo":
        backup.os.mkfifo(latest)
    verified_archives(monkeypatch, corrupt={latest})

    with pytest.raises((CurrentError, RequestBudgetExceeded)):
        backup.prune(settings)

    assert all((path / "sentinel").read_bytes() == path.name.encode() for path in others)
    if damage == "symlink":
        assert latest.is_symlink()
        assert (target / "sentinel").read_bytes() == b"must survive"
    if damage == "file":
        assert latest.read_bytes() == b"not a backup directory"
    if damage == "fifo":
        assert latest.lstat()


def test_missing_backups_directory_does_not_hide_missing_owned_latest(catalogue):
    settings = catalogue
    latest = settings.backups_dir / ("20261014-" + "1" * 32)
    assert not settings.backups_dir.exists()
    set_latest(settings, {"path": str(latest)})
    with pytest.raises(CurrentError):
        backup.prune(settings)
    assert not settings.backups_dir.exists()


@pytest.mark.parametrize(
    "metadata",
    [
        "traversal",
        "dot",
        "trailing-slash",
        "double-slash",
        "relative",
        "uppercase",
        "short",
        "nested",
        "other-local",
        "missing-path",
        "null-path",
        "list",
        "string",
    ],
)
def test_unsafe_or_malformed_latest_metadata_fails_closed(catalogue, monkeypatch, metadata):
    settings = catalogue
    paths = [archive(settings, "20261014-" + digit * 32) for digit in ("f", "1")]
    name = paths[1].name
    value = {
        "traversal": {"path": f"{settings.backups_dir}/../backups/{name}"},
        "dot": {"path": f"{settings.backups_dir}/./{name}"},
        "trailing-slash": {"path": str(paths[1]) + "/"},
        "double-slash": {"path": f"{settings.backups_dir}//{name}"},
        "relative": {"path": f"backups/{name}"},
        "uppercase": {"path": str(settings.backups_dir / ("20261014-" + "A" * 32))},
        "short": {"path": str(settings.backups_dir / ("20261014-" + "1" * 31))},
        "nested": {"path": str(settings.backups_dir / "nested" / name)},
        "other-local": {"path": str(settings.root / "other" / name)},
        "missing-path": {},
        "null-path": {"path": None},
        "list": [],
        "string": str(paths[1]),
    }[metadata]
    set_latest(settings, value)
    verified_archives(monkeypatch)

    with pytest.raises(CurrentError):
        backup.prune(settings)

    assert all((path / "sentinel").read_bytes() == path.name.encode() for path in paths)


@pytest.mark.parametrize("foreign_name", ["20261014-" + "a" * 32, "unrelated-foreign-name"])
def test_foreign_latest_from_restored_root_is_never_resolved_or_statted(
    catalogue, monkeypatch, foreign_name
):
    settings = catalogue
    paths = [archive(settings, "20261014-" + digit * 32) for digit in ("f", "1")]
    foreign_root = settings.root.with_name(settings.root.name + "-old")
    foreign = foreign_root / "backups" / foreign_name
    set_latest(settings, {"path": str(foreign)})
    visited = verified_archives(monkeypatch)
    original_stat, original_resolve = Path.stat, Path.resolve

    def checked_stat(path, *args, **kwargs):
        assert not str(path).startswith(str(foreign_root))
        return original_stat(path, *args, **kwargs)

    def checked_resolve(path, *args, **kwargs):
        assert not str(path).startswith(str(foreign_root))
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", checked_stat)
    monkeypatch.setattr(Path, "resolve", checked_resolve)
    backup.prune(settings)
    assert set(visited) == set(paths)
    assert set(settings.backups_dir.iterdir()) == {paths[0]}


def test_unverified_nonlatest_artifact_is_preserved(catalogue, monkeypatch):
    settings = catalogue
    invalid = archive(settings, "20261014-" + "f" * 32)
    latest = archive(settings, "20261014-" + "1" * 32)
    set_latest(settings, {"path": str(latest)})
    visited = verified_archives(monkeypatch, corrupt={invalid})
    backup.prune(settings)
    assert set(visited) == {invalid, latest}
    assert set(settings.backups_dir.iterdir()) == {invalid, latest}


def test_raw_only_pruning_skips_latest_metadata_and_backup_verification(catalogue, monkeypatch):
    settings = catalogue
    latest = archive(settings, "20261014-" + "1" * 32)
    set_latest(settings, {"path": ["invalid metadata deliberately ignored"]})
    raw = settings.raw_dir / ("a" * 32)
    raw.mkdir(parents=True)
    (raw / "p000001.json.gz").write_bytes(b"expired")
    original_get = backup.get_state

    def checked_get(conn, key, *args, **kwargs):
        assert key != "last_backup", "raw-only pruning loaded full-backup ownership state"
        return original_get(conn, key, *args, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("raw-only pruning verified publication or backups")

    monkeypatch.setattr(backup, "get_state", checked_get)
    monkeypatch.setattr(backup, "verify_backup", forbidden)
    monkeypatch.setattr(backup, "verify_current", forbidden)
    result = backup.prune(settings, now=backup.time.time() + 8 * 86400, raw_only=True)
    assert result["deleted"] == 1
    assert not raw.exists()
    assert (latest / "sentinel").read_bytes() == latest.name.encode()
