"""Portable contracts for the native Windows filesystem branches."""

from pathlib import Path, PureWindowsPath

import pytest

from oddsfox_catalogue import current_config, limits
from oddsfox_catalogue.capture import writer
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


def test_windows_path_rejects_ads_and_ambiguous_components():
    for value in (r"C:\catalogue\state.db:secret", r"C:\catalogue\raw ", r"C:\catalogue\x."):
        with pytest.raises(CurrentError):
            current_config._validate_windows_path(PureWindowsPath(value))


def test_windows_path_allows_drive_and_ordinary_components():
    current_config._validate_windows_path(PureWindowsPath(r"C:\catalogue\raw\page.json"))


def test_safe_path_rejects_reparse_ancestors(tmp_path, monkeypatch):
    path = tmp_path / "root" / "child"
    monkeypatch.setattr(
        current_config, "is_link_or_reparse", lambda item: Path(item) == path.parent
    )
    with pytest.raises(CurrentError, match="symlink"):
        current_config.safe_path(path)


def test_atomic_write_pins_and_flushes_after_regular_file_write(tmp_path, monkeypatch):
    target = tmp_path / "value.json"
    calls = []

    class Pin:
        def __enter__(self):
            calls.append("pin")

        def __exit__(self, *_args):
            calls.append("unpin")

    original_open = writer.open_regular_nofollow

    def open_file(path, flags, mode=0o600):
        calls.append("open")
        return original_open(path, flags, mode)

    monkeypatch.setattr(writer, "pinned_path", lambda path: Pin())
    monkeypatch.setattr(writer, "open_regular_nofollow", open_file)
    monkeypatch.setattr(writer, "_fsync_dir", lambda path: calls.append("flush"))

    writer.atomic_write_bytes(target, b"payload")

    assert target.read_bytes() == b"payload"
    assert calls == ["pin", "unpin", "pin", "open", "unpin", "flush"]


def test_regular_reader_uses_the_portable_nofollow_opener(tmp_path, monkeypatch):
    target = tmp_path / "payload"
    target.write_bytes(b"payload")
    original_open = writer.open_regular_nofollow
    opened = []

    def open_file(path, flags, mode=0o600):
        opened.append((Path(path), flags))
        return original_open(path, flags, mode)

    monkeypatch.setattr(writer, "open_regular_nofollow", open_file)

    assert writer.read_regular_bytes(target, max_bytes=20) == b"payload"
    assert opened[0][0] == target
    assert opened[0][1] & writer.os.O_RDONLY == writer.os.O_RDONLY
    assert opened[0][1] & getattr(writer.os, "O_NONBLOCK", 0) == getattr(writer.os, "O_NONBLOCK", 0)


def test_initialize_preflights_before_creating_root(tmp_path, monkeypatch):
    root = tmp_path / "new" / "catalogue"
    calls = []

    class Pin:
        def __enter__(self):
            calls.append("pin")

        def __exit__(self, *_args):
            calls.append("unpin")

    monkeypatch.setattr(
        current_config,
        "require_durability",
        lambda path: calls.append(("preflight", Path(path))),
    )
    monkeypatch.setattr(current_config, "pinned_path", lambda _path: Pin())
    monkeypatch.setattr(
        current_config, "atomic_write_bytes", lambda path, _data: calls.append(("write", path))
    )
    monkeypatch.setattr(current_config, "sync_directory", lambda path: calls.append(("sync", path)))

    current_config.validate_root(root, initialize=True)

    assert calls[0] == ("preflight", tmp_path)
    assert calls[1] == "pin"
    assert calls[3][0] == "write"
    assert calls[4][0] == "sync"


def test_windows_directory_accounting_counts_regular_files(tmp_path):
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "one").write_bytes(b"123")
    (nested / "two").write_bytes(b"45")

    assert limits._directory_bytes_windows(root) == 5


def test_windows_directory_accounting_rejects_reparse_entry(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    link = root / "junction-like"
    link.mkdir()
    (link / "payload").write_bytes(b"123")
    original = limits.is_link_or_reparse
    monkeypatch.setattr(
        limits, "is_link_or_reparse", lambda path: Path(path) == link or original(path)
    )

    with pytest.raises(RequestBudgetExceeded, match="reparse"):
        limits._directory_bytes_windows(root)


def test_windows_directory_accounting_rejects_replaced_directory(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "child"
    child.mkdir()
    (child / "old").write_bytes(b"123")
    swapped = False

    class Pin:
        def __init__(self, path):
            self.path = Path(path)

        def __enter__(self):
            nonlocal swapped
            if self.path == child and not swapped:
                swapped = True
                child.rename(tmp_path / "old-child")
                child.mkdir()
                (child / "new").write_bytes(b"1")

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(limits, "pinned_path", lambda path: Pin(path))

    with pytest.raises(RequestBudgetExceeded, match="changed"):
        limits._directory_bytes_windows(root)
