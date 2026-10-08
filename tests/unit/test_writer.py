import gzip
import os
from pathlib import Path

import pytest

from oddsfox_catalogue.capture.writer import (
    atomic_write_bytes,
    manifest_path,
    read_body,
    read_manifest,
    verify_page,
    write_page,
)


def _fields(seq: int) -> dict:
    return {"page_id": f"p{seq}", "terminal": False, "record_count": 1}


def test_write_page_round_trips_and_verifies(tmp_path: Path) -> None:
    body = b'{"events":[{"id":"1"}]}'
    result = write_page(tmp_path, 1, body, _fields(1))
    manifest = read_manifest(manifest_path(tmp_path, 1))
    assert manifest is not None
    assert verify_page(tmp_path, manifest)
    assert read_body(tmp_path, manifest) == body
    assert result.body_sha256 == manifest["body_sha256"]


def test_gzip_output_is_byte_stable(tmp_path: Path) -> None:
    body = b'{"events":[]}'
    first = write_page(tmp_path / "a", 1, body, _fields(1))
    second = write_page(tmp_path / "b", 1, body, _fields(1))
    assert first.gz_sha256 == second.gz_sha256
    assert gzip.decompress(first.gz_path.read_bytes()) == body


def test_corrupt_gzip_fails_verification(tmp_path: Path) -> None:
    write_page(tmp_path, 1, b'{"events":[]}', _fields(1))
    manifest = read_manifest(manifest_path(tmp_path, 1))
    assert manifest is not None
    (tmp_path / manifest["file"]).write_bytes(b"garbage")
    assert verify_page(tmp_path, manifest) is False


def test_missing_gz_fails_verification(tmp_path: Path) -> None:
    write_page(tmp_path, 1, b"{}", _fields(1))
    manifest = read_manifest(manifest_path(tmp_path, 1))
    assert manifest is not None
    (tmp_path / manifest["file"]).unlink()
    assert verify_page(tmp_path, manifest) is False


def test_manifest_checksum_mismatch_fails_verification(tmp_path: Path) -> None:
    write_page(tmp_path, 1, b"{}", _fields(1))
    manifest = read_manifest(manifest_path(tmp_path, 1))
    assert manifest is not None
    tampered = dict(manifest, body_sha256="0" * 64)
    assert verify_page(tmp_path, tampered) is False


def test_atomic_write_leaves_no_temp_file(tmp_path: Path) -> None:
    target = tmp_path / "x.json"
    atomic_write_bytes(target, b"hello")
    assert target.read_bytes() == b"hello"
    assert not (tmp_path / "x.json.tmp").exists()


def test_failed_write_keeps_previous_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "x.json"
    atomic_write_bytes(target, b"old")

    def boom(*_: object, **__: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        atomic_write_bytes(target, b"new")
    assert target.read_bytes() == b"old"
