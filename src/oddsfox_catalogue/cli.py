"""Current Polymarket event and market catalogue."""

from __future__ import annotations

import argparse
import json
import logging
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
    for name in ("init", "refresh", "reconcile", "publish", "status", "verify", "current"):
        sub.add_parser(name)
    seed = sub.add_parser("import-release", help="verified v2 import into a fresh v3 root")
    seed.add_argument("release_path", type=Path)
    seed.add_argument("--manifest-sha256", required=True)
    seed.add_argument("--raw-root", type=Path, required=True)
    probe = sub.add_parser("probe", help="bounded native/bulk provider compatibility checks")
    for entity in ("event", "market"):
        for status in ("open", "closed", "absent"):
            probe.add_argument(f"--{status}-{entity}-id", action="append", required=True)
    backup = sub.add_parser("backup")
    operations = backup.add_subparsers(dest="backup_command", required=True)
    operations.add_parser("create")
    operations.add_parser("verify").add_argument("backup", type=Path)
    restore = operations.add_parser("restore")
    restore.add_argument("backup", type=Path)
    restore.add_argument("--destination", type=Path, required=True)
    metadata = sub.add_parser("metadata", help="unchanged targeted metadata.v1 handoffs")
    operations = metadata.add_subparsers(dest="metadata_command", required=True)
    for operation in ("refresh", "export"):
        command = operations.add_parser(operation)
        command.add_argument("--market-id", action="append", required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--max-output-bytes", type=int, default=128 * 1024**2)
        if operation == "refresh":
            command.add_argument("--max-response-bytes", type=int, default=8 * 1024**2)
            command.add_argument("--max-requests", type=int, default=500)
            command.add_argument("--max-download-bytes", type=int, default=128 * 1024**2)
    for name in ("capture", "load", "dbt", "ledger", "batch", "replay", "rebuild", "validate"):
        old = sub.add_parser(name, help="retired; use the matching legacy runtime for old roots")
        old.add_argument("legacy_args", nargs=argparse.REMAINDER)
    return parser


def _metadata(args):
    from .config import load_settings
    from .ids import sha256_bytes
    from .metadata import GammaClient, export_metadata, refresh_metadata

    settings = load_settings(root=args.root)
    locations = [settings.root, args.output]
    if args.metadata_command == "refresh":
        locations.extend([settings.data_dir / "metadata" / "raw", settings.run_lock_path])
    for location in locations:
        try:
            actual = location.resolve(strict=False)
        except RuntimeError as exc:
            raise CurrentError("invalid metadata.v1 isolation path") from exc
        if any((parent / "catalogue.json").exists() for parent in (actual, *actual.parents)):
            raise CurrentError("metadata.v1 requires isolated producer and output roots")
    if args.metadata_command == "refresh" and (
        settings.warehouse_path.exists() or settings.ledger_path.exists()
    ):
        raise CurrentError("preserve the legacy root; select an isolated metadata producer root")
    client = None
    try:
        if args.metadata_command == "refresh":
            with run_lock(settings.run_lock_path):
                client = GammaClient(
                    settings.gamma,
                    max_body_bytes=args.max_response_bytes,
                    max_requests=args.max_requests,
                    max_download_bytes=args.max_download_bytes,
                    max_duration_s=min(120, settings.capture.max_duration_s),
                )
                manifest = refresh_metadata(
                    settings,
                    args.market_id,
                    args.output,
                    client=client,
                    max_output_bytes=args.max_output_bytes,
                    max_body_bytes=args.max_response_bytes,
                    max_requests=args.max_requests,
                    max_download_bytes=args.max_download_bytes,
                )
        else:
            manifest = export_metadata(
                settings, args.market_id, args.output, max_output_bytes=args.max_output_bytes
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
    if operation == "backup":
        operation = {"create": "backup", "verify": "verify-backup", "restore": "restore"}[
            args.backup_command
        ]
        options = {
            key: getattr(args, key) for key in ("backup", "destination") if hasattr(args, key)
        }
    if operation == "import-release":
        options = {
            key: getattr(args, key) for key in ("release_path", "manifest_sha256", "raw_root")
        }
    if operation == "probe":
        options = {
            f"{entity}_ids": {
                s: getattr(args, f"{s}_{entity}_id") for s in ("open", "closed", "absent")
            }
            for entity in ("event", "market")
        }
    if operation in {"verify-backup", "restore"}:
        success, result = supervise(settings, operation, options, duration=4 * 3600)
    else:
        validate_root(settings.root, initialize=operation in {"init", "import-release"})
        duration = (
            4 * 3600
            if operation in {"import-release", "backup"}
            else settings.limits.max_duration_s
        )
        success, result = supervise(settings, operation, options, duration=duration)
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
        if args.command in {
            "capture",
            "load",
            "dbt",
            "ledger",
            "batch",
            "replay",
            "rebuild",
            "validate",
        }:
            raise CurrentError(
                "historical command retired: use its preserved runtime/root; "
                "catalogue.v3 uses refresh, reconcile, import-release and verify"
            )
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
