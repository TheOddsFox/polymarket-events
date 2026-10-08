"""Record small live samples from the Gamma API for contract tests.

This needs network access to https://gamma-api.polymarket.com. It writes one
JSON file per endpoint under tests/fixtures/gamma/live/ together with a
manifest that records the request, the UTC time, and the body checksum.
Re-run it on a networked machine and commit the output; the payload contract
tests then run against real shapes instead of synthetic ones.

    uv run python tools/record_gamma_fixtures.py
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

BASE_URL = "https://gamma-api.polymarket.com"
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "fixtures" / "gamma" / "live"

REQUESTS: list[tuple[str, str, dict[str, object]]] = [
    (
        "events_keyset_open",
        "/events/keyset",
        {
            "limit": 3,
            "order": "id",
            "ascending": "true",
            "include_children": "true",
            "closed": "false",
        },
    ),
    (
        "events_keyset_closed",
        "/events/keyset",
        {
            "limit": 3,
            "order": "id",
            "ascending": "true",
            "include_children": "true",
            "closed": "true",
        },
    ),
    (
        "events_archived_offset",
        "/events",
        {"limit": 3, "order": "id", "ascending": "true", "archived": "true", "offset": 0},
    ),
    (
        "markets_keyset_open",
        "/markets/keyset",
        {"limit": 3, "order": "id", "ascending": "true", "closed": "false", "include_tag": "true"},
    ),
    (
        "markets_keyset_closed",
        "/markets/keyset",
        {"limit": 3, "order": "id", "ascending": "true", "closed": "true", "include_tag": "true"},
    ),
]


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, object]] = []
    with httpx.Client(base_url=BASE_URL, timeout=30.0, follow_redirects=True) as client:
        for name, path, params in REQUESTS:
            response = client.get(path, params=params)
            response.raise_for_status()
            body = response.content
            target = OUT / f"{name}.json"
            target.write_bytes(body)
            manifest.append(
                {
                    "name": name,
                    "endpoint": path,
                    "params": params,
                    "status": response.status_code,
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "bytes": len(body),
                    "observed_at": datetime.now(UTC).isoformat(timespec="seconds"),
                }
            )
            print(f"recorded {target.relative_to(ROOT)} ({len(body)} bytes)")

    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
