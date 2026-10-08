"""Child-process entry point for crash-recovery tests.

    python capture_child.py <root> <mode> <iso-now>

Runs one capture against the demo world. A ``CATALOGUE_FAULT`` value in the
environment makes the process exit with ``os._exit`` at that point, so the
parent test observes a real abrupt termination.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from fakes.fake_gamma import FakeGamma
from fakes.harness import build_runtime
from fakes.world import demo_world
from oddsfox_catalogue.capture.runner import run_capture


def main() -> int:
    root = Path(sys.argv[1])
    mode = sys.argv[2]
    now = datetime.fromisoformat(sys.argv[3])
    fake = FakeGamma(demo_world())
    runtime, _ = build_runtime(root, fake, now=now)
    try:
        summary = run_capture(runtime, mode)
        print(f"captured {summary.batch_id} status={summary.status}")
    finally:
        runtime.ledger.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
