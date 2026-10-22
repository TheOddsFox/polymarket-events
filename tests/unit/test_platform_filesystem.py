import signal
from pathlib import Path

import pytest

from oddsfox_catalogue import current_config, platform_support
from oddsfox_catalogue.capture import writer
from oddsfox_catalogue.current_contract import CurrentError


def test_operator_signals_cover_mac_termination_and_optional_interrupt():
    assert signal.SIGTERM in platform_support.operator_signals()
    assert signal.SIGINT not in platform_support.operator_signals()
    assert signal.SIGINT in platform_support.operator_signals(include_interrupt=True)
    if hasattr(signal, "SIGHUP"):
        assert signal.SIGHUP in platform_support.operator_signals()


def test_safe_path_rejects_symlink_ancestors(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(CurrentError, match="symlink"):
        current_config.safe_path(link / "child")


def test_atomic_write_fsyncs_file_before_replace_and_directory(tmp_path, monkeypatch):
    target = tmp_path / "value.json"
    calls = []
    original_open = writer.open_regular_nofollow
    original_replace = writer.replace_file
    original_fsync = writer.os.fsync

    def open_file(path, flags, mode=0o600):
        calls.append("open")
        return original_open(path, flags, mode)

    def flush(fd):
        calls.append("file-fsync")
        original_fsync(fd)

    def replace(source, destination):
        calls.append("replace")
        original_replace(source, destination)

    monkeypatch.setattr(writer, "open_regular_nofollow", open_file)
    monkeypatch.setattr(writer.os, "fsync", flush)
    monkeypatch.setattr(writer, "replace_file", replace)
    monkeypatch.setattr(writer, "_fsync_dir", lambda _path: calls.append("dir-fsync"))

    writer.atomic_write_bytes(target, b"payload")

    assert target.read_bytes() == b"payload"
    assert calls == ["open", "file-fsync", "replace", "dir-fsync"]


def test_regular_reader_uses_nofollow_opener(tmp_path, monkeypatch):
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


def test_regular_reader_rejects_symlink_target(tmp_path):
    target = tmp_path / "payload"
    target.write_bytes(b"preserve")
    link = tmp_path / "link"
    link.symlink_to(target)

    with pytest.raises(ValueError, match="symlink"):
        writer.read_regular_bytes(link, max_bytes=20)
    assert target.read_bytes() == b"preserve"
