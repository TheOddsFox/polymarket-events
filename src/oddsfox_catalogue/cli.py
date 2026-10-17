"""Current Polymarket event and market catalogue."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from dataclasses import asdict
from pathlib import Path

from . import __version__
from .current_contract import CurrentError
from .runlock import RunBusy, run_lock
from .signals import SIGNALS, Terminated


def configure_logging():
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _install_termination_handlers():
    def handle(signum, _frame):
        SIGNALS.receive(signum)

    previous = {s: signal.signal(s, handle) for s in (signal.SIGHUP, signal.SIGTERM)}

    def restore():
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    return restore


def build_parser():
    parser = argparse.ArgumentParser(prog="catalogue", description=__doc__)
    parser.add_argument("--root", type=Path, help="explicit catalogue.v3 root")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version")
    config = sub.add_parser("config")
    config.add_subparsers(dest="config_command", required=True).add_parser("show")
    for name in ("init", "refresh", "publish", "status", "verify", "current"):
        sub.add_parser(name)
    reconcile = sub.add_parser("reconcile", help="bounded reconcile of known events and markets")
    reconcile.add_argument(
        "--full-pass",
        action="store_true",
        help="start a forced full pass, or resume an unfinished one",
    )
    probe = sub.add_parser("probe", help="bounded native/bulk provider compatibility checks")
    for entity in ("event", "market"):
        for status in ("open", "closed", "absent"):
            probe.add_argument(f"--{status}-{entity}-id", action="append", required=True)
    metadata = sub.add_parser("metadata", help="targeted metadata.v1 handoff")
    operations = metadata.add_subparsers(dest="metadata_command", required=True)
    refresh = operations.add_parser("refresh")
    refresh.add_argument("--market-id", action="append", required=True)
    refresh.add_argument("--output", type=Path, required=True)
    refresh.add_argument(
        "--catalogue-root",
        type=Path,
        help="v1 catalogue root; identity_history is empty when omitted",
    )
    refresh.add_argument("--max-output-bytes", type=int, default=128 * 1024**2)
    refresh.add_argument("--max-response-bytes", type=int, default=8 * 1024**2)
    refresh.add_argument("--max-requests", type=int, default=500)
    refresh.add_argument("--max-download-bytes", type=int, default=128 * 1024**2)
    return parser


def _metadata(args):
    from .current_config import load_current_settings
    from .ids import sha256_bytes
    from .metadata import GammaClient, refresh_metadata

    root = args.root
    if root is None and os.environ.get("CATALOGUE_ROOT"):
        root = Path(os.environ["CATALOGUE_ROOT"])
    if root is None:
        raise CurrentError("metadata refresh requires an explicit isolated --root")
    producer = load_current_settings(root)
    catalogue = load_current_settings(args.catalogue_root) if args.catalogue_root else None
    locations = [
        producer.root,
        args.output,
        producer.root / "metadata" / "raw",
        producer.run_lock_path,
    ]
    for location in locations:
        try:
            actual = location.resolve(strict=False)
        except RuntimeError as exc:
            raise CurrentError("invalid metadata.v1 isolation path") from exc
        if any((parent / "catalogue.json").exists() for parent in (actual, *actual.parents)):
            raise CurrentError("metadata.v1 requires isolated producer and output roots")
    client = None
    try:
        with run_lock(producer.run_lock_path):
            client = GammaClient(
                producer.gamma,
                max_body_bytes=args.max_response_bytes,
                max_requests=args.max_requests,
                max_download_bytes=args.max_download_bytes,
                max_duration_s=min(120, producer.limits.max_duration_s),
            )
            manifest = refresh_metadata(
                producer.gamma,
                catalogue,
                producer.root,
                args.market_id,
                args.output,
                client=client,
                max_output_bytes=args.max_output_bytes,
                max_body_bytes=args.max_response_bytes,
                max_requests=args.max_requests,
                max_download_bytes=args.max_download_bytes,
            )
        coverage = json.loads((args.output / "coverage.json").read_text())
        counts = {s: sum(r["status"] == s for r in coverage) for s in ("found", "absent", "failed")}
        print(
            json.dumps(
                {
                    "source_release_id": manifest["source_release_id"],
                    "manifest_sha256": sha256_bytes((args.output / "manifest.json").read_bytes()),
                    "path": str(args.output),
                    "requested": len(coverage),
                    "http_attempts": client.stats.requests if client else 0,
                    "downloaded_bytes": client.stats.downloaded_bytes if client else 0,
                    **counts,
                }
            )
        )
        return 2 if counts["failed"] else 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "message": str(exc),
                    "http_attempts": client.stats.requests if client else 0,
                    "downloaded_bytes": client.stats.downloaded_bytes if client else 0,
                }
            )
        )
        return 3
    finally:
        if client is not None:
            client.close()


def _current(args):
    from .current_config import load_current_settings, validate_root
    from .current_runtime import supervise

    settings = load_current_settings(args.root)
    if args.command == "config":
        print(json.dumps(asdict(settings), default=str, indent=2))
        return 0
    operation, options = args.command, {}
    if operation == "reconcile":
        options = {"full_pass": args.full_pass}
    if operation == "probe":
        options = {
            f"{entity}_ids": {
                s: getattr(args, f"{s}_{entity}_id") for s in ("open", "closed", "absent")
            }
            for entity in ("event", "market")
        }
    validate_root(settings.root, initialize=operation == "init")
    success, result = supervise(
        settings, operation, options, duration=settings.limits.max_duration_s
    )
    print(json.dumps(result, default=str, indent=2))
    return 0 if success else 3


def main(argv=None):
    configure_logging()
    restore = _install_termination_handlers()
    try:
        args = build_parser().parse_args(argv)
        if args.command == "version":
            print(__version__)
            return 0
        if args.command == "metadata":
            return _metadata(args)
        return _current(args)
    except Terminated as exc:
        print(f"terminated by {exc}", file=sys.stderr)
        return 128 + exc.signum
    except (RunBusy, ValueError, OSError) as exc:
        print(
            json.dumps(
                {
                    "status": "busy" if isinstance(exc, RunBusy) else "blocked",
                    "message": str(exc),
                    "http_attempts": 0,
                    "downloaded_bytes": 0,
                }
            )
        )
        return 3
    finally:
        restore()


if __name__ == "__main__":
    sys.exit(main())
