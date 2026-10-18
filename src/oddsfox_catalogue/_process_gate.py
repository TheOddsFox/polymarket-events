"""Private launch shim used to put Windows children in a Job before work starts."""

from __future__ import annotations

import argparse
import os
import subprocess


def wait_for_parent_gate(handle: int) -> bool:
    """Return true only when the parent explicitly releases the inherited pipe."""
    if os.name != "nt":
        raise OSError("the process gate is only available on Windows")
    import msvcrt

    descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY)
    try:
        return os.read(descriptor, 1) == b"\x01"
    finally:
        os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--gate-handle", type=int, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        return 2
    try:
        if not wait_for_parent_gate(args.gate_handle):
            return 125
        return subprocess.call(command)
    except OSError:
        return 126


if __name__ == "__main__":
    raise SystemExit(main())
