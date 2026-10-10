"""Crash injection hooks for recovery tests.

Set ``CATALOGUE_FAULT=<point>`` to make the process exit abruptly the first
time execution reaches that point. ``os._exit`` skips finally blocks and
atexit handlers, which is what a power loss or kill -9 would do.
"""

from __future__ import annotations

import os
import sys

FAULT_ENV = "CATALOGUE_FAULT"
CRASH_EXIT_CODE = 87

KNOWN_POINTS = frozenset(
    {
        "after_page_rename",
        "after_control_ledger_commit",
        "after_control_marker_commit",
        "after_ledger_commit",
        "mid_dlt_load",
        "before_registry",
        "mid_publish",
    }
)


def fault_point(name: str) -> None:
    if name not in KNOWN_POINTS:
        raise ValueError(f"unknown fault point {name!r}")
    if os.environ.get(FAULT_ENV) == name:
        sys.stderr.write(f"fault injected at {name}\n")
        sys.stderr.flush()
        os._exit(CRASH_EXIT_CODE)
