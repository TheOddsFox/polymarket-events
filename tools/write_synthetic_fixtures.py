"""Regenerate the synthetic Gamma fixtures under tests/fixtures/gamma/synthetic/.

Run with ``uv run python tools/write_synthetic_fixtures.py``. Output is
deterministic, so the committed files only change when the builders change.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from fakes.world import demo_world, event_stub  # noqa: E402

OUT = ROOT / "tests" / "fixtures" / "gamma" / "synthetic"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    world = demo_world()
    events = sorted(world.events.values(), key=lambda e: int(e["id"]))

    pages = {
        "events_keyset_open_p1.json": {
            "events": [e for e in events if not e["closed"]][:1],
            "next_cursor": "example-cursor-1",
        },
        "events_keyset_open_p2.json": {
            "events": [e for e in events if not e["closed"]][1:],
        },
        "events_keyset_closed.json": {
            "events": [e for e in events if e["closed"]],
        },
        "events_archived_offset.json": [
            {**events[0], "archived": True},
        ],
        "markets_keyset_open.json": {
            "markets": [m for m in world.markets.values() if not m["closed"]],
        },
        "event_by_id_101.json": events[0],
        "event_stub_reference.json": event_stub(events[0]),
    }
    for name, body in pages.items():
        path = OUT / name
        path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
