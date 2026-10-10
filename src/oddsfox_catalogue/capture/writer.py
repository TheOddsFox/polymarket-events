"""Durable raw storage: gzip page bodies plus JSON manifests.

Write order for every file: write ``<name>.tmp``, fsync it, ``os.replace`` it
into place, then fsync the parent directory. A page counts as durable only
when its manifest exists and both checksums verify.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oddsfox_catalogue.ids import canonical_json, sha256_bytes

MANIFEST_VERSION = 1
MAX_BODY_BYTES = 16 * 1024**2
MAX_MANIFEST_BYTES = 1024**2


def atomic_write_bytes(path: Path, data: bytes) -> None:
    for ancestor in path.parents:
        if ancestor.is_symlink():
            raise ValueError("output path contains a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def page_stem(seq: int) -> str:
    return f"p{seq:06d}"


def gz_path(scan_dir: Path, seq: int) -> Path:
    return scan_dir / f"{page_stem(seq)}.json.gz"


def manifest_path(scan_dir: Path, seq: int) -> Path:
    return scan_dir / f"{page_stem(seq)}.manifest.json"


def scan_marker_path(scan_dir: Path) -> Path:
    return scan_dir / "_scan.json"


def batch_marker_path(batch_dir: Path) -> Path:
    return batch_dir / "_batch.json"


@dataclass(frozen=True)
class PageWrite:
    gz_path: Path
    manifest_path: Path
    body_sha256: str
    gz_sha256: str


def write_page(
    scan_dir: Path,
    seq: int,
    body: bytes,
    manifest_fields: dict[str, Any],
) -> PageWrite:
    """Persist one page body and its manifest. The gzip header carries no timestamp."""
    compressed = gzip.compress(body, compresslevel=6, mtime=0)
    body_digest = sha256_bytes(body)
    gz_digest = sha256_bytes(compressed)
    gz = gz_path(scan_dir, seq)
    atomic_write_bytes(gz, compressed)

    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "seq": seq,
        "file": gz.name,
        "body_sha256": body_digest,
        "gz_sha256": gz_digest,
        "body_bytes": len(body),
        **manifest_fields,
    }
    manifest_file = manifest_path(scan_dir, seq)
    atomic_write_bytes(manifest_file, (canonical_json(manifest) + "\n").encode("utf-8"))
    return PageWrite(gz, manifest_file, body_digest, gz_digest)


def read_regular_bytes(path: Path, *, max_bytes: int, trusted_root: Path | None = None) -> bytes:
    """Read a bounded regular file without following a symlink or leaving its root."""
    path = Path(os.path.abspath(path))
    if trusted_root is not None:
        root = Path(os.path.abspath(trusted_root))
        if not path.is_relative_to(root):
            raise ValueError("raw path leaves trusted root")
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise ValueError("raw path contains a symlink")
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("raw path is not a regular file")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("raw path is not a regular file")
        if info.st_size > max_bytes:
            raise ValueError("raw file exceeds byte limit")
        data = handle.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise ValueError("raw file exceeds byte limit")
    return data


def read_manifest(path: Path, *, trusted_root: Path | None = None) -> dict[str, Any] | None:
    try:
        data = json.loads(
            read_regular_bytes(path, max_bytes=MAX_MANIFEST_BYTES, trusted_root=trusted_root)
        )
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def verify_page(
    scan_dir: Path,
    manifest: dict[str, Any],
    *,
    trusted_root: Path | None = None,
    max_body_bytes: int = MAX_BODY_BYTES,
) -> bool:
    """True only if the gz file exists and matches both recorded checksums."""
    try:
        read_body(scan_dir, manifest, trusted_root=trusted_root, max_body_bytes=max_body_bytes)
    except (OSError, ValueError, EOFError, KeyError):
        return False
    return True


def read_body(
    scan_dir: Path,
    manifest: dict[str, Any],
    *,
    trusted_root: Path | None = None,
    max_body_bytes: int = MAX_BODY_BYTES,
) -> bytes:
    """Return the verified raw body for a page. Raises if the checksum fails."""
    if (
        isinstance(max_body_bytes, bool)
        or not isinstance(max_body_bytes, int)
        or not 0 < max_body_bytes <= MAX_BODY_BYTES
    ):
        raise ValueError("raw body byte limit must be within 16 MiB")
    name = manifest.get("file")
    if (
        not isinstance(name, str)
        or Path(name).name != name
        or not name.endswith(".json.gz")
        or "\\" in name
    ):
        raise ValueError("unsafe raw payload path")
    size = manifest.get("body_bytes")
    if (
        type(manifest.get("manifest_version")) is not int
        or manifest.get("manifest_version") != MANIFEST_VERSION
        or isinstance(size, bool)
        or not isinstance(size, int)
        or not 0 <= size <= max_body_bytes
    ):
        raise ValueError("unsupported raw manifest or body byte limit")
    gz = read_regular_bytes(
        scan_dir / name, max_bytes=max_body_bytes + 1024**2, trusted_root=trusted_root or scan_dir
    )
    if sha256_bytes(gz) != manifest.get("gz_sha256"):
        raise ValueError("raw gzip checksum mismatch")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(gz)) as handle:
            body = handle.read(min(size, max_body_bytes) + 1)
    except (OSError, EOFError) as exc:
        raise ValueError("invalid raw gzip") from exc
    if len(body) != size or sha256_bytes(body) != manifest.get("body_sha256"):
        raise ValueError("raw body checksum or size mismatch")
    return body


def write_marker(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_bytes(path, (canonical_json(payload) + "\n").encode("utf-8"))
