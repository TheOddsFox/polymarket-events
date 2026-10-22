"""Mac filesystem safety contracts retained when retiring the Windows backend."""

import os

import pytest

from oddsfox_catalogue import platform_support
from oddsfox_catalogue.capture import writer


def test_failed_directory_flush_is_visible_after_atomic_replacement(tmp_path, monkeypatch):
    target = tmp_path / "existing.json"
    target.write_bytes(b"kept")

    def refuse(_path):
        raise PermissionError("directory flush failed")

    monkeypatch.setattr(writer, "_fsync_dir", refuse)
    with pytest.raises(PermissionError, match="directory flush"):
        writer.atomic_write_bytes(target, b"replacement")

    assert target.read_bytes() == b"replacement"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing.json"]


@pytest.mark.parametrize("failure", [OSError("disk failure"), KeyboardInterrupt()])
def test_directory_sync_closes_descriptor_without_masking_failure(tmp_path, monkeypatch, failure):
    opened = []
    original_open = os.open

    def track_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    def fail_sync(_descriptor):
        raise failure

    monkeypatch.setattr(platform_support.os, "open", track_open)
    monkeypatch.setattr(platform_support.os, "fsync", fail_sync)

    with pytest.raises(type(failure)) as raised:
        platform_support.sync_directory(tmp_path)

    assert raised.value is failure
    assert len(opened) == 1
    with pytest.raises(OSError):
        os.fstat(opened[0])


def test_atomic_write_rejects_symlink_temporary_file_without_truncating_target(tmp_path):
    outside = tmp_path / "outside"
    outside.write_bytes(b"must survive")
    destination = tmp_path / "receipt.json"
    destination.write_bytes(b"old receipt")
    destination.with_name(destination.name + ".tmp").symlink_to(outside)

    with pytest.raises(OSError):
        writer.atomic_write_bytes(destination, b"replacement")

    assert outside.read_bytes() == b"must survive"
    assert destination.read_bytes() == b"old receipt"
