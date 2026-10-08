"""Child-process entry point for publish crash tests.

    python publish_child.py <root>

Publishes one release under ``root``. ``CATALOGUE_FAULT`` makes the process exit with
``os._exit`` at the named point, which is what a kill -9 during publish looks like.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from fakes.harness import FIXED_NOW
from oddsfox_catalogue.config import load_settings
from oddsfox_catalogue.publish import publish_release


def main() -> int:
    settings = load_settings(root=Path(sys.argv[1]), env=dict(os.environ))
    info = publish_release(settings, now=FIXED_NOW, git_sha="testsha")
    print(info.release_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
