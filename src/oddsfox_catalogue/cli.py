"""`catalogue` command line entry point.

Subcommands are registered as they are implemented. Each handler receives the
parsed namespace and returns a process exit code.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from oddsfox_catalogue import __version__
from oddsfox_catalogue.backup import create_backup, verify_backup
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import (
    CaptureRuntime,
    abandon_batch,
    rebuild_from_raw,
    run_capture,
)
from oddsfox_catalogue.config import Settings, load_settings, settings_as_dict
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.ids import MODES
from oddsfox_catalogue.load.runner import LoadBlocked
from oddsfox_catalogue.pipeline import dbt_stage, load_stage, publish_stage, refresh
from oddsfox_catalogue.publish import PublishBlocked, current_release
from oddsfox_catalogue.rebuild import rebuild_and_verify
from oddsfox_catalogue.runlock import RunBusy, current_git_sha, run_lock
from oddsfox_catalogue.signals import SIGNALS, Terminated
from oddsfox_catalogue.warehouse import BaselineMissing, read_open_event_ids

Handler = Callable[[argparse.Namespace], int]


def _install_termination_handlers() -> Callable[[], None]:
    """Route SIGHUP and SIGTERM to ``SIGNALS``. Returns a restore function."""

    def handle(signum: int, _frame: object) -> None:
        SIGNALS.receive(signum)

    previous = {signum: signal.signal(signum, handle) for signum in (signal.SIGHUP, signal.SIGTERM)}

    def restore() -> None:
        for signum, previous_handler in previous.items():
            signal.signal(signum, previous_handler)

    return restore


def configure_logging() -> None:
    """Send INFO and above to stderr. Library calls stay quiet until the CLI starts.

    httpx logs the full request URL at INFO, and a keyset URL contains the page cursor.
    Keep that logger at WARNING so cursors stay out of the operator log.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


@contextmanager
def _capture_runtime(settings: Settings) -> Iterator[CaptureRuntime]:
    client = GammaClient(settings.gamma)
    ledger = Ledger(settings.ledger_path)
    runtime = CaptureRuntime(
        settings=settings,
        client=client,
        ledger=ledger,
        open_event_ids=lambda: read_open_event_ids(settings.warehouse_path),
        git_sha=current_git_sha(settings.root),
    )
    try:
        yield runtime
    finally:
        ledger.close()
        client.close()


def _cmd_version(_: argparse.Namespace) -> int:
    print(__version__)
    return 0


def _cmd_config_show(_: argparse.Namespace) -> int:
    settings = load_settings()
    print(json.dumps(settings_as_dict(settings), indent=2, sort_keys=True))
    return 0


def _cmd_capture(args: argparse.Namespace) -> int:
    settings = load_settings()
    with run_lock(settings.run_lock_path), _capture_runtime(settings) as runtime:
        summary = run_capture(runtime, args.mode)
    print(
        json.dumps(
            {
                "batch_id": summary.batch_id,
                "status": summary.status,
                "resumed": summary.resumed,
                "pages_written": summary.pages_written,
                "pages_adopted": summary.pages_adopted,
                "records": summary.records,
                "scans": len(summary.scans),
            },
            indent=2,
        )
    )
    return 0 if summary.status == "captured" else 2


def _cmd_ledger_rebuild(_: argparse.Namespace) -> int:
    settings = load_settings()
    with run_lock(settings.run_lock_path):
        if settings.ledger_path.exists():
            raise SystemExit(
                f"refusing to rebuild over existing {settings.ledger_path}; move it aside first"
            )
        ledger = Ledger(settings.ledger_path)
        try:
            counts = rebuild_from_raw(settings, ledger)
        finally:
            ledger.close()
    print(json.dumps(counts, indent=2))
    return 0


def _cmd_batch_abandon(args: argparse.Namespace) -> int:
    settings = load_settings()
    with run_lock(settings.run_lock_path):
        ledger = Ledger(settings.ledger_path)
        try:
            abandon_batch(ledger, args.batch_id, datetime.now(UTC), args.reason)
        finally:
            ledger.close()
    print(f"abandoned {args.batch_id}")
    return 0


def _cmd_load(args: argparse.Namespace) -> int:
    settings = load_settings()
    summary = load_stage(settings, batch_id=args.batch_id)
    print(json.dumps(asdict(summary), indent=2, default=str))
    return 0


def _cmd_dbt(args: argparse.Namespace) -> int:
    settings = load_settings()
    extra = list(args.dbt_args)
    if extra[:1] == ["--"]:
        extra = extra[1:]
    result = dbt_stage(settings, extra)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


def _cmd_publish(_: argparse.Namespace) -> int:
    settings = load_settings()
    release = publish_stage(settings)
    print(json.dumps({"release_id": release.release_id, "path": str(release.path)}, indent=2))
    return 0


def _cmd_refresh(args: argparse.Namespace) -> int:
    settings = load_settings()
    result = refresh(settings, args.mode)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") == "published" else 2


def _cmd_replay(_: argparse.Namespace) -> int:
    """Load pending pages, build dbt, and publish, without fetching from Gamma."""
    settings = load_settings()
    loaded = load_stage(settings)
    built = dbt_stage(settings, ["build"])
    if built.returncode != 0:
        sys.stderr.write(built.stdout[-4000:])
        return 2
    release = publish_stage(settings)
    print(
        json.dumps(
            {"loaded_pages": loaded.pages_loaded, "release_id": release.release_id}, indent=2
        )
    )
    return 0


def _cmd_validate(_: argparse.Namespace) -> int:
    settings = load_settings()
    result = dbt_stage(settings, ["test"])
    sys.stdout.write(result.stdout)
    return result.returncode


def _cmd_current(_: argparse.Namespace) -> int:
    settings = load_settings()
    pointer = current_release(settings)
    print(json.dumps(pointer, indent=2) if pointer else "no published release yet")
    return 0


def _cmd_backup_create(args: argparse.Namespace) -> int:
    settings = load_settings()
    with run_lock(settings.run_lock_path):
        dest = create_backup(settings, dest_root=args.dest)
    problems = verify_backup(dest)
    print(
        json.dumps({"backup": str(dest), "verified": not problems, "problems": problems}, indent=2)
    )
    return 0 if not problems else 4


def _cmd_backup_verify(args: argparse.Namespace) -> int:
    problems = verify_backup(args.path)
    print(json.dumps({"verified": not problems, "problems": problems}, indent=2))
    return 0 if not problems else 4


def _cmd_rebuild(_: argparse.Namespace) -> int:
    """Rebuild from raw in a scratch area and compare fingerprints with the live warehouse."""
    settings = load_settings()
    with run_lock(settings.run_lock_path):
        report = rebuild_and_verify(settings)
    print(
        json.dumps(
            {
                "matched": report.matched,
                "rebuilt_to": str(report.rebuilt_to),
                "tables": report.tables,
                "mismatches": report.mismatches,
            },
            indent=2,
            default=str,
        )
    )
    return 0 if report.matched else 4


def _cmd_status(_: argparse.Namespace) -> int:
    settings = load_settings()
    if not settings.ledger_path.exists():
        print("no ledger yet")
        return 0
    ledger = Ledger(settings.ledger_path)
    try:
        rows: list[dict[str, Any]] = [
            {
                "batch_id": b["batch_id"],
                "mode": b["mode"],
                "status": b["status"],
                "plan_stage": b["plan_stage"],
                "started_at": b["started_at"],
                "error": b["error"],
            }
            for b in ledger.list_batches()
        ]
    finally:
        ledger.close()
    print(json.dumps(rows, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalogue", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("version", help="print the package version").set_defaults(handler=_cmd_version)

    config = sub.add_parser("config", help="inspect configuration")
    config_sub = config.add_subparsers(dest="config_command", required=True)
    config_sub.add_parser("show", help="print effective settings").set_defaults(
        handler=_cmd_config_show
    )

    capture = sub.add_parser("capture", help="capture raw Gamma pages for one mode")
    capture.add_argument("--mode", choices=MODES, required=True)
    capture.set_defaults(handler=_cmd_capture)

    ledger = sub.add_parser("ledger", help="ledger maintenance")
    ledger_sub = ledger.add_subparsers(dest="ledger_command", required=True)
    ledger_sub.add_parser(
        "rebuild", help="rebuild the ledger from raw manifests (ledger must be absent)"
    ).set_defaults(handler=_cmd_ledger_rebuild)

    batch = sub.add_parser("batch", help="batch operations")
    batch_sub = batch.add_subparsers(dest="batch_command", required=True)
    abandon = batch_sub.add_parser("abandon", help="stop resuming a capturing batch")
    abandon.add_argument("batch_id")
    abandon.add_argument("--reason", default="abandoned by operator")
    abandon.set_defaults(handler=_cmd_batch_abandon)

    sub.add_parser("status", help="list batches and their states").set_defaults(handler=_cmd_status)

    load = sub.add_parser("load", help="load captured pages into bronze (idempotent)")
    load.add_argument("--batch-id", default=None, help="load only this batch")
    load.set_defaults(handler=_cmd_load)

    dbt = sub.add_parser("dbt", help="run a dbt command against the warehouse")
    dbt.add_argument("dbt_args", nargs=argparse.REMAINDER, help="arguments passed to dbt")
    dbt.set_defaults(handler=_cmd_dbt)

    sub.add_parser("publish", help="write a release if the dbt build passed").set_defaults(
        handler=_cmd_publish
    )
    sub.add_parser("current", help="show the current published release").set_defaults(
        handler=_cmd_current
    )

    refresh_parser = sub.add_parser(
        "refresh", help="capture, load, build, and publish in one stage order"
    )
    refresh_parser.add_argument("--mode", choices=MODES, required=True)
    refresh_parser.set_defaults(handler=_cmd_refresh)

    sub.add_parser(
        "replay", help="load, build, and publish from existing raw pages (no Gamma calls)"
    ).set_defaults(handler=_cmd_replay)
    sub.add_parser("validate", help="run dbt tests against the warehouse").set_defaults(
        handler=_cmd_validate
    )

    backup = sub.add_parser("backup", help="point-in-time backups")
    backup_sub = backup.add_subparsers(dest="backup_command", required=True)
    backup_create = backup_sub.add_parser("create", help="write a checksummed backup")
    backup_create.add_argument("--dest", type=Path, default=None, help="parent directory")
    backup_create.set_defaults(handler=_cmd_backup_create)
    backup_verify = backup_sub.add_parser("verify", help="re-hash a backup and list problems")
    backup_verify.add_argument("path", type=Path)
    backup_verify.set_defaults(handler=_cmd_backup_verify)

    rebuild = sub.add_parser(
        "rebuild", help="rebuild from raw pages and compare with the live warehouse"
    )
    rebuild.add_argument(
        "--verify", action="store_true", required=True, help="compare fingerprints (required)"
    )
    rebuild.set_defaults(handler=_cmd_rebuild)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    restore_signals = _install_termination_handlers()
    try:
        try:
            parser = build_parser()
            args = parser.parse_args(argv)
            handler: Handler = args.handler
            return handler(args)
        except Terminated as exc:
            # 128 plus the signal number, so a resume wrapper does not treat a kill as exit 1.
            # This covers a signal during argument parsing as well as during a stage.
            print(f"error: terminated by {exc}", file=sys.stderr)
            return 128 + exc.signum
        except (RunBusy, PublishBlocked, BaselineMissing, LoadBlocked) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 3
    finally:
        restore_signals()


if __name__ == "__main__":
    sys.exit(main())
