"""Read verified records back from the raw store, using the ledger as the index."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.writer import manifest_path, read_body, read_manifest, verify_page
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.paginators import unpack


class DurabilityError(RuntimeError):
    """A page the ledger considers fetched is missing or fails its checksum."""


def scan_dir_for(settings: Settings, observation_date: str, batch_id: str, scan_id: str) -> Path:
    return settings.raw_dir / observation_date / batch_id / scan_id


def iter_scan_pages(
    settings: Settings,
    batch: dict[str, Any],
    scan: dict[str, Any],
) -> Iterator[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Yield ``(manifest, records)`` for each durable page of one scan attempt, in order."""
    directory = scan_dir_for(
        settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
    )
    seq = 0
    while True:
        seq += 1
        path = manifest_path(directory, seq)
        if not path.exists():
            break
        manifest = read_manifest(path)
        if manifest is None or not verify_page(directory, manifest):
            raise DurabilityError(f"page {seq} of scan {scan['scan_id']} is not durable")
        if manifest["http_status"] != 200:
            # Recorded 404s carry no records; their bodies are evidence only.
            yield manifest, []
            continue
        body = read_body(directory, manifest)
        records, _ = unpack(json.loads(body), scan["record_key"])
        yield manifest, records
