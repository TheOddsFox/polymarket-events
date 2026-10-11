"""Storage scans must account for every path without weakening failure handling."""

import errno
import os
from pathlib import Path

import pytest

from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.limits import directory_bytes


@pytest.fixture(autouse=True)
def descriptor_support():
    if (
        not hasattr(os, "fwalk")
        or os.stat not in os.supports_dir_fd
        or os.listdir not in os.supports_fd
    ):
        pytest.skip("descriptor-relative directory accounting is unavailable")


def patch_operation(monkeypatch, name, replacement):
    monkeypatch.setattr(os, name, replacement)
    support = "supports_dir_fd" if name == "stat" else "supports_fd"
    monkeypatch.setattr(os, support, {*getattr(os, support), replacement})


def test_regular_roots_missing_roots_and_hard_links_are_counted_per_path(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    payload = root / "payload"
    payload.write_bytes(b"12345")
    os.link(payload, root / "other-name")
    assert directory_bytes(payload) == 5
    assert directory_bytes(tmp_path / "missing") == 0
    assert directory_bytes(root) == 10


@pytest.mark.parametrize("omitted", ["file", "directory", "symlink", "special"])
def test_traversal_classification_cannot_omit_storage_entries(tmp_path, monkeypatch, omitted):
    root = tmp_path / "root"
    root.mkdir()
    entry = root / "omitted"
    if omitted == "file":
        entry.write_bytes(b"12345")
    elif omitted == "directory":
        entry.mkdir()
        (entry / "payload").write_bytes(b"12345")
    elif omitted == "symlink":
        entry.symlink_to(tmp_path / "missing")
    else:
        os.mkfifo(entry)
    original = os.fwalk

    def incomplete_walk(*args, **kwargs):
        for current, directories, files, descriptor in original(*args, **kwargs):
            if Path(current) == root:
                directories[:] = []
                files[:] = []
            yield current, directories, files, descriptor

    monkeypatch.setattr(os, "fwalk", incomplete_walk)
    if omitted in {"file", "directory"}:
        assert directory_bytes(root) == 5
    else:
        with pytest.raises(RequestBudgetExceeded, match="symlink or special"):
            directory_bytes(root)


@pytest.mark.parametrize("initial_kind", ["file", "directory"])
def test_file_directory_swaps_use_the_current_no_follow_classification(
    tmp_path, monkeypatch, initial_kind
):
    root = tmp_path / "root"
    root.mkdir()
    entry = root / "entry"
    if initial_kind == "file":
        entry.write_bytes(b"old")
    else:
        entry.mkdir()
    original = os.fwalk

    def swap_walk(*args, **kwargs):
        for point in original(*args, **kwargs):
            if Path(point[0]) == root:
                if initial_kind == "file":
                    entry.unlink()
                    entry.mkdir()
                    (entry / "payload").write_bytes(b"12345")
                else:
                    entry.rmdir()
                    entry.write_bytes(b"12345")
            yield point

    monkeypatch.setattr(os, "fwalk", swap_walk)
    assert directory_bytes(root) == 5


@pytest.mark.parametrize("restored", [False, True])
def test_a_directory_skipped_after_symlink_replacement_is_rejected(tmp_path, monkeypatch, restored):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "child"
    child.mkdir()
    (child / "payload").write_bytes(b"12345")
    stash = tmp_path / "stash"
    original = os.fwalk

    def swap_walk(*args, **kwargs):
        try:
            for point in original(*args, **kwargs):
                yield point
                if Path(point[0]) == root:
                    child.rename(stash)
                    child.symlink_to(stash, target_is_directory=True)
        finally:
            if restored and child.is_symlink():
                child.unlink()
                stash.rename(child)

    monkeypatch.setattr(os, "fwalk", swap_walk)
    with pytest.raises(RequestBudgetExceeded, match="symlink|changed|skipped"):
        directory_bytes(root)


def test_a_replaced_directory_cannot_be_counted_as_the_original_identity(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "child"
    child.mkdir()
    (child / "payload").write_bytes(b"12345")
    original = os.fwalk

    def replace_walk(*args, **kwargs):
        for point in original(*args, **kwargs):
            yield point
            if Path(point[0]) == root:
                child.rename(tmp_path / "original-child")
                child.mkdir()
                (child / "replacement").write_bytes(b"1")

    monkeypatch.setattr(os, "fwalk", replace_walk)
    with pytest.raises(RequestBudgetExceeded, match="changed|skipped"):
        directory_bytes(root)


@pytest.mark.parametrize("partial", [False, True])
def test_missing_root_during_iteration_preserves_only_already_counted_bytes(
    tmp_path, monkeypatch, partial
):
    root = tmp_path / "root"
    root.mkdir()
    (root / "payload").write_bytes(b"12345")
    original = os.fwalk

    def disappear_walk(*args, **kwargs):
        with_closing = original(*args, **kwargs)
        try:
            if partial:
                yield next(with_closing)
            root.rename(tmp_path / "gone")
            raise FileNotFoundError("temporary root disappeared")
        finally:
            with_closing.close()

    monkeypatch.setattr(os, "fwalk", disappear_walk)
    assert directory_bytes(root) == (5 if partial else 0)


@pytest.mark.parametrize("operation", ["stat", "listdir", "walk"])
@pytest.mark.parametrize("error_type", [PermissionError, OSError])
def test_nonmissing_errors_propagate_and_close_traversal_descriptors(
    tmp_path, monkeypatch, operation, error_type
):
    root = tmp_path / "root"
    root.mkdir()
    (root / "payload").write_bytes(b"12345")
    original_walk = os.fwalk
    descriptors = []
    closed = []
    failure = error_type(errno.EACCES if error_type is PermissionError else errno.EIO, "unreadable")

    def tracked_walk(*args, **kwargs):
        entries = original_walk(*args, **kwargs)
        try:
            for point in entries:
                descriptors.append(point[3])
                if operation == "walk":
                    kwargs["onerror"](failure)
                yield point
        finally:
            entries.close()
            closed.append(True)

    monkeypatch.setattr(os, "fwalk", tracked_walk)
    if operation != "walk":
        original_operation = getattr(os, operation)

        def fail_operation(path, *args, **kwargs):
            if (operation == "listdir" and isinstance(path, int)) or (
                operation == "stat" and path == "payload" and kwargs.get("dir_fd") is not None
            ):
                raise failure
            return original_operation(path, *args, **kwargs)

        patch_operation(monkeypatch, operation, fail_operation)
    with pytest.raises(error_type, match="unreadable") as raised:
        directory_bytes(root)
    assert raised.value is failure
    assert closed == [True]
    assert descriptors
    for descriptor in descriptors:
        with pytest.raises(OSError) as invalid:
            os.fstat(descriptor)
        assert invalid.value.errno == errno.EBADF


@pytest.mark.parametrize("unavailable", ["fwalk", "stat", "listdir"])
def test_unsupported_platform_uses_the_existing_path_scanner(tmp_path, monkeypatch, unavailable):
    root = tmp_path / "root"
    root.mkdir()
    (root / "payload").write_bytes(b"12345")
    if unavailable == "fwalk":
        monkeypatch.delattr(os, "fwalk")
    else:
        support = "supports_dir_fd" if unavailable == "stat" else "supports_fd"
        monkeypatch.setattr(os, support, getattr(os, support) - {getattr(os, unavailable)})
    assert directory_bytes(root) == 5
    (root / "unsafe").symlink_to(tmp_path / "missing")
    with pytest.raises(RequestBudgetExceeded, match="symlink"):
        directory_bytes(root)
