"""Durable raw storage: gzip page bodies plus JSON manifests.

Write order for every file: write ``<name>.tmp``, fsync it, ``os.replace`` it
into place, then fsync the parent directory. A page counts as durable only
when its manifest exists and both checksums verify.
"""

from __future__ import annotations

import gzip
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oddsfox_catalogue.ids import canonical_json, sha256_bytes

MANIFEST_VERSION = 1


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as handle:
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


def read_manifest(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def verify_page(scan_dir: Path, manifest: dict[str, Any]) -> bool:
    """True only if the gz file exists and matches both recorded checksums."""
    gz = scan_dir / manifest["file"]
    try:
        compressed = gz.read_bytes()
    except OSError:
        return False
    if sha256_bytes(compressed) != manifest["gz_sha256"]:
        return False
    try:
        body = gzip.decompress(compressed)
    except (OSError, EOFError):
        return False
    return sha256_bytes(body) == manifest["body_sha256"] and len(body) == manifest["body_bytes"]


def read_body(scan_dir: Path, manifest: dict[str, Any]) -> bytes:
    """Return the verified raw body for a page. Raises if the checksum fails."""
    gz = (scan_dir / manifest["file"]).read_bytes()
    body = gzip.decompress(gz)
    if sha256_bytes(body) != manifest["body_sha256"]:
        raise ValueError(f"checksum mismatch for {manifest['file']}")
    return body


def write_marker(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_bytes(path, (canonical_json(payload) + "\n").encode("utf-8"))
