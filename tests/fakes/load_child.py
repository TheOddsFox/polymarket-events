"""Child-process entry point for loader crash-recovery tests.

    python load_child.py <root>

Loads every pending page under ``root``. ``CATALOGUE_FAULT`` in the environment
makes the process exit with ``os._exit`` at the named point, so the parent test
sees a real abrupt termination mid-load.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from fakes.harness import FIXED_NOW
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.config import load_settings
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending


def main() -> int:
    root = Path(sys.argv[1])
    settings = load_settings(root=root, env=dict(os.environ))
    ledger = Ledger(settings.ledger_path)
    try:
        summary = load_pending(LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW))
        print(f"loaded pages={summary.pages_loaded} registered={summary.batches_registered}")
    finally:
        ledger.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
