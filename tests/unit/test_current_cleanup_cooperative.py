"""Deadline-aware publication maintenance remains resumable and observable."""

from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_store import initialize


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1),
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(str(settings.store_path))
    initialize(conn)
    yield settings, conn
    conn.close()


def _artifact(directory, name, contents=b"data"):
    path = directory / name
    path.mkdir(parents=True)
    (path / "payload").write_bytes(contents)
    return path


class _TrackedScandir:
    def __init__(self, path, closed):
        self._entries = _real_scandir(path)
        self._closed = closed

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self._closed.append(True)
        return self._entries.__exit__(*exc_info)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._entries)


_real_scandir = publisher.os.scandir


def test_release_cleanup_stops_at_cutoff_and_resumes_next_invocation(catalogue, monkeypatch):
    settings, _conn = catalogue
    releases = settings.published_dir / "releases"
    releases.mkdir(parents=True)
    current = _artifact(releases, "a" * 32)
    first = _artifact(releases, "b" * 32, b"one")
    second = _artifact(releases, "c" * 32, b"two")
    now = [0.0]
    monkeypatch.setattr(publisher.time, "monotonic", lambda: now[0])
    real_rmtree = publisher.shutil.rmtree

    def remove_one(path):
        real_rmtree(path)
        now[0] = 15.0

    monkeypatch.setattr(publisher.shutil, "rmtree", remove_one)
    progress = {"deleted_count": 0, "deleted_bytes": 0}
    deferred = publisher._cleanup_cooperative(settings, {current.name}, 15.0, progress)

    assert deferred
    assert progress["deleted_count"] == 1
    assert progress["deleted_bytes"] == 3
    assert current.is_dir()
    assert not (first.is_dir() and second.is_dir())
    assert first.exists() != second.exists()

    monkeypatch.setattr(publisher.shutil, "rmtree", real_rmtree)
    progress = {"deleted_count": 0, "deleted_bytes": 0}
    assert not publisher._cleanup_cooperative(settings, {current.name}, None, progress)
    assert progress == {"deleted_count": 1, "deleted_bytes": 3}
    assert current.is_dir()


def test_raw_cleanup_keeps_successful_counts_when_later_delete_fails(catalogue, monkeypatch):
    settings, _conn = catalogue
    settings.raw_dir.mkdir()
    first = _artifact(settings.raw_dir, "1" * 32, b"one")
    second = _artifact(settings.raw_dir, "2" * 32, b"two")
    real_rmtree = publisher.shutil.rmtree
    calls = 0

    def fail_second(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("second deletion failed")
        real_rmtree(path)

    monkeypatch.setattr(publisher.shutil, "rmtree", fail_second)
    result = {}
    publisher._retain_raw(settings, set(), result)

    assert result["raw_retention"] == {"deleted_count": 1, "deleted_bytes": 3}
    assert result["retention_warning"] == "second deletion failed"
    assert first.exists() != second.exists()


def test_cleanup_cutoff_before_work_reports_deferred_without_deleting(catalogue):
    settings, _conn = catalogue
    releases = settings.published_dir / "releases"
    releases.mkdir(parents=True)
    old = _artifact(releases, "d" * 32)
    progress = {"deleted_count": 0, "deleted_bytes": 0}

    assert publisher._cleanup_cooperative(settings, set(), 0.0, progress)
    assert progress == {"deleted_count": 0, "deleted_bytes": 0}
    assert old.is_dir()


def test_optional_enumeration_uses_scandir_instead_of_path_iterdir(catalogue, monkeypatch):
    settings, _conn = catalogue
    settings.raw_dir.mkdir()
    raw_id = "a" * 32
    _artifact(settings.raw_dir, raw_id)
    releases = settings.published_dir / "releases"
    _artifact(releases, "b" * 32)
    closed = []
    monkeypatch.setattr(publisher.os, "scandir", lambda path: _TrackedScandir(path, closed))
    monkeypatch.setattr(
        Path,
        "iterdir",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("iterdir called")),
    )

    assert [(name, path.name) for name, path in publisher._iter_raw_directories(settings)] == [
        (raw_id, raw_id)
    ]
    assert not publisher._cleanup_cooperative(
        settings, {"b" * 32}, None, {"deleted_count": 0, "deleted_bytes": 0}
    )
    assert closed and all(closed)


@pytest.mark.parametrize("cleanup", ["release", "raw"])
def test_scandir_closes_when_cutoff_arrives_after_open(catalogue, monkeypatch, cleanup):
    settings, _conn = catalogue
    directory = settings.published_dir / "releases" if cleanup == "release" else settings.raw_dir
    directory.mkdir(parents=True)
    _artifact(directory, "c" * 32)
    now = [0.0]
    closed = []

    def open_scandir(path):
        scan = _TrackedScandir(path, closed)
        now[0] = 15.0
        return scan

    monkeypatch.setattr(publisher.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(publisher.os, "scandir", open_scandir)
    if cleanup == "release":
        deferred = publisher._cleanup_cooperative(
            settings, set(), 15.0, {"deleted_count": 0, "deleted_bytes": 0}
        )
    else:
        deferred = publisher._prune_unreferenced_raw(settings, set(), 15.0)["deferred"]

    assert deferred
    assert closed == [True]


@pytest.mark.parametrize("cleanup", ["release", "raw"])
def test_scandir_closes_when_entry_validation_fails(catalogue, monkeypatch, cleanup):
    settings, _conn = catalogue
    directory = settings.published_dir / "releases" if cleanup == "release" else settings.raw_dir
    directory.mkdir(parents=True)
    name = "d" * 32
    _artifact(directory, name)
    closed = []
    real_safe_path = publisher.safe_path

    def reject_entry(path):
        if Path(path).name == name:
            raise OSError("injected validation failure")
        return real_safe_path(path)

    monkeypatch.setattr(publisher.os, "scandir", lambda path: _TrackedScandir(path, closed))
    monkeypatch.setattr(publisher, "safe_path", reject_entry)
    with pytest.raises(OSError, match="injected validation failure"):
        if cleanup == "release":
            publisher._cleanup_cooperative(
                settings, set(), None, {"deleted_count": 0, "deleted_bytes": 0}
            )
        else:
            publisher._prune_unreferenced_raw(settings, set())
    assert closed == [True]


@pytest.mark.parametrize("cleanup", ["release", "raw"])
def test_fsync_failure_does_not_mask_operator_termination(catalogue, monkeypatch, cleanup):
    settings, _conn = catalogue
    directory = settings.published_dir / "releases" if cleanup == "release" else settings.raw_dir
    directory.mkdir(parents=True)
    _artifact(directory, "1" * 32)
    _artifact(directory, "2" * 32)
    real_rmtree = publisher.shutil.rmtree
    calls = 0

    def delete_then_terminate(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise KeyboardInterrupt
        real_rmtree(path)

    monkeypatch.setattr(publisher.shutil, "rmtree", delete_then_terminate)
    monkeypatch.setattr(
        publisher, "_fsync_dir", lambda _path: (_ for _ in ()).throw(OSError("fsync failed"))
    )
    with pytest.raises(KeyboardInterrupt):
        if cleanup == "release":
            publisher._cleanup_cooperative(
                settings, set(), None, {"deleted_count": 0, "deleted_bytes": 0}
            )
        else:
            publisher._prune_unreferenced_raw(
                settings, set(), None, {"deleted_count": 0, "deleted_bytes": 0}
            )


def test_noop_publication_reports_cleanup_phases_and_defers_raw_work(catalogue, monkeypatch):
    settings, conn = catalogue
    first = publisher.publish(settings, conn)
    garbage = _artifact(settings.raw_dir, "e" * 32)
    monkeypatch.setattr(publisher.time, "monotonic", lambda: 100.0)
    result = publisher.publish(settings, conn, deadline=101.0)

    assert result["noop"] is True
    assert garbage.is_dir()
    assert result["raw_retention"] == {"deleted_count": 0, "deleted_bytes": 0}
    assert "deferred" in result["retention_warning"]
    assert set(result["publication_timings"]) == {
        "pre_commit_s",
        "release_cleanup_s",
        "raw_cleanup_s",
    }
    assert result["publication_timings"]["pre_commit_s"] >= 0
    assert result["publication_timings"]["release_cleanup_s"] >= 0
    assert result["publication_timings"]["raw_cleanup_s"] >= 0
    assert publisher.verify_current(settings, connection=conn)["release_id"] == first["release_id"]
