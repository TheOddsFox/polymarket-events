"""One finite, measured installed-CLI invocation; never schedules or automatically retries."""

import argparse
import json
import math
import os
import resource
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from oddsfox_catalogue.capture.writer import atomic_write_bytes
from oddsfox_catalogue.cli import _install_termination_handlers, build_parser
from oddsfox_catalogue.config import load_settings
from oddsfox_catalogue.limits import _union_bytes
from oddsfox_catalogue.rebuild import _runtime_revision, scratch_settings
from oddsfox_catalogue.signals import SIGNALS, Terminated

RETAINED_LIMIT = 256 * 1024**3
TEMP_LIMIT = 32 * 1024**3
FREE_RESERVE = 32 * 1024**3
LOG_LIMIT = 64 * 1024**2


def confined(path, workspace):
    path = Path(path)
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("acceptance path contains a symlink")
    path = Path(os.path.abspath(path))
    if not path.is_relative_to(workspace) or path == workspace:
        raise ValueError("acceptance path must be inside its workspace")
    return path


def storage(workspace, *, additional_bytes=0, temporary_paths=()):
    used = _union_bytes((workspace,))
    temporary = list(temporary_paths)
    for current, directories, _ in os.walk(workspace, followlinks=False):
        for name in directories:
            path = Path(current) / name
            if name == "dlt_pipelines" or (path.parent.name == ".state" and name in {"dlt", "tmp"}):
                temporary.append(path)
    temp = _union_bytes(tuple(temporary))
    disk = os.statvfs(workspace)
    free = disk.f_bavail * disk.f_frsize
    if used + additional_bytes > RETAINED_LIMIT:
        raise ValueError("combined acceptance retained storage allowance exceeded")
    if temp > TEMP_LIMIT:
        raise ValueError("combined acceptance temporary storage allowance exceeded")
    if free < FREE_RESERVE + additional_bytes:
        raise ValueError("acceptance filesystem free-space reserve exhausted")
    return {"retained_bytes": used, "temporary_bytes": temp, "filesystem_free_bytes": free}


def environment(root, arguments, *, load_pages=50):
    parsed = build_parser().parse_args(arguments)
    if type(load_pages) is not int or load_pages not in {50, 250}:
        raise ValueError("acceptance load pages must be 50 or 250")
    if load_pages == 250 and not (
        parsed.command == "load" and parsed.record_key == "markets" and parsed.batch_id
    ):
        raise ValueError("250-page acceptance requires an explicit market-page batch load")
    env = {k: v for k, v in os.environ.items() if not k.startswith("CATALOGUE_")}
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    # Synthetic sources require both explicit loopback settings, just as the existing smoke does.
    for key in ("CATALOGUE_GAMMA_BASE_URL", "CATALOGUE_GAMMA_ALLOW_LOOPBACK"):
        if all(
            name in os.environ
            for name in ("CATALOGUE_GAMMA_BASE_URL", "CATALOGUE_GAMMA_ALLOW_LOOPBACK")
        ):
            env[key] = os.environ[key]
    env.update(
        CATALOGUE_ROOT=str(root),
        CATALOGUE_CAPTURE_MAX_RETAINED_BYTES=str(RETAINED_LIMIT),
        CATALOGUE_CAPTURE_MAX_TEMP_BYTES=str(TEMP_LIMIT),
        CATALOGUE_CAPTURE_MAX_REQUESTS="25000",
        CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES=str(4 * 1024**3),
        CATALOGUE_CAPTURE_MAX_RESPONSE_BYTES=str(16 * 1024**2),
        CATALOGUE_CAPTURE_MAX_DURATION_S="14400",
        CATALOGUE_CAPTURE_WORKERS="1",
        CATALOGUE_LOAD_MAX_PAGES_PER_RUN=str(load_pages),
        CATALOGUE_LOAD_DUCKDB_MEMORY_LIMIT="2GB",
        CATALOGUE_LOAD_DUCKDB_THREADS="2",
        CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO="0.01",
        CATALOGUE_QUALITY_OPEN_EVENTS_DROP_WARN_PCT="5",
        CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT="10",
    )
    mode = parsed.mode if parsed.command == "capture" else None
    env["CATALOGUE_GAMMA_REQUESTS_PER_SECOND"] = "10" if mode in {"daily", "reconcile"} else "2"
    return env


def run(workspace, root, arguments, *, duration=14400, additional_bytes=0, load_pages=50):
    started = time.monotonic()
    if (
        isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration <= 0
        or duration > 14400
        or type(additional_bytes) is not int
        or additional_bytes < 0
    ):
        raise ValueError("acceptance duration/reservation must be finite and within its allowance")
    _union_bytes((workspace,))
    workspace = Path(os.path.abspath(workspace))
    if not workspace.is_dir():
        raise ValueError("acceptance requires an existing directory workspace")
    root = confined(root, workspace)
    if not arguments or arguments[0] not in {
        "capture",
        "ledger",
        "load",
        "dbt",
        "publish",
        "verify",
        "replay",
        "backup",
        "rebuild",
        "status",
        "current",
        "config",
    }:
        raise ValueError("unsupported acceptance command")
    try:
        parsed = build_parser().parse_args(arguments)
    except SystemExit as exc:
        raise ValueError("invalid acceptance catalogue arguments") from exc
    env = environment(root, arguments, load_pages=load_pages)
    for name in ("scratch", "dest", "destination", "release", "path"):
        if (value := getattr(parsed, name, None)) is not None:
            confined(value, workspace)
    settings = load_settings(root=root, env=env)
    temporary_paths = [settings.temporary_dir, settings.dlt_pipelines_dir]
    if parsed.command == "rebuild" and parsed.scratch is not None:
        rebuilt = scratch_settings(settings, parsed.scratch)
        temporary_paths.extend((rebuilt.temporary_dir, rebuilt.dlt_pipelines_dir))
    for path in (settings.data_dir, settings.state_dir, settings.warehouse_path, *temporary_paths):
        confined(path, workspace)
    root_bytes = _union_bytes((root,))
    if arguments[:2] == ["backup", "create"]:
        additional_bytes = max(additional_bytes, root_bytes + 128 * 1024**2)
    elif arguments[:2] == ["backup", "restore"]:
        additional_bytes = max(additional_bytes, _union_bytes((parsed.path,)) + 16384)
    before = storage(
        workspace, additional_bytes=additional_bytes, temporary_paths=tuple(temporary_paths)
    )
    temporary_allowance = (
        TEMP_LIMIT - before["temporary_bytes"] + _union_bytes(tuple(temporary_paths))
    )
    if temporary_allowance <= 0:
        raise ValueError("acceptance operation temporary allowance exhausted")
    env["CATALOGUE_CAPTURE_MAX_TEMP_BYTES"] = str(temporary_allowance)
    if arguments[:1] == ["publish"] or arguments[:2] == ["backup", "create"]:
        # Native publication/copy admission must reserve space occupied by other roots and logs.
        env["CATALOGUE_CAPTURE_MAX_RETAINED_BYTES"] = str(
            RETAINED_LIMIT - before["retained_bytes"] + root_bytes - 2 * LOG_LIMIT - 16384
        )
    settings = load_settings(root=root, env=env)
    for path in (settings.data_dir, settings.state_dir, settings.warehouse_path):
        confined(path, workspace)
    output = workspace / "completion-proof" / uuid.uuid4().hex
    output.mkdir(parents=True)
    report = {
        "revision": 1,
        "command": arguments,
        "root": str(root),
        "runtime": _runtime_revision(),
        "before": before,
        "max_retained_bytes": RETAINED_LIMIT,
        "max_temp_bytes": TEMP_LIMIT,
        "duration_limit_s": duration,
        "additional_bytes_reserved": additional_bytes,
        "operator_retained_allowance_bytes": settings.capture.max_retained_bytes,
        "operator_temporary_allowance_bytes": settings.capture.max_temp_bytes,
        "load_pages": settings.load.max_pages_per_run,
        "status": "running",
    }
    atomic_write_bytes(output / "report.json", (json.dumps(report, sort_keys=True) + "\n").encode())
    process, stopped = None, False
    try:
        with (
            (output / "stdout.log").open("xb") as stdout,
            (output / "stderr.log").open("xb") as stderr,
        ):
            if time.monotonic() - started >= duration:
                raise TimeoutError("acceptance invocation deadline exhausted before launch")
            with SIGNALS.hold() as startup:
                process = subprocess.Popen(
                    [sys.executable, "-m", "oddsfox_catalogue.cli", *arguments],
                    env=env,
                    stdout=stdout,
                    stderr=stderr,
                )
            if startup.signum is not None:
                raise Terminated(startup.signum)
            report["pid"] = process.pid
            atomic_write_bytes(
                output / "report.json", (json.dumps(report, sort_keys=True) + "\n").encode()
            )
            print(
                json.dumps(
                    {"report": str(output / "report.json"), "pid": process.pid, "status": "running"}
                ),
                flush=True,
            )
            next_measure = 0
            while process.poll() is None:
                if time.monotonic() - started >= duration:
                    raise TimeoutError("acceptance invocation deadline exhausted")
                if any(os.fstat(f.fileno()).st_size > LOG_LIMIT for f in (stdout, stderr)):
                    raise ValueError("acceptance diagnostics exceed their byte allowance")
                if time.monotonic() >= next_measure:
                    report["latest_storage"] = storage(
                        workspace, temporary_paths=tuple(temporary_paths)
                    )
                    atomic_write_bytes(
                        output / "report.json", (json.dumps(report, sort_keys=True) + "\n").encode()
                    )
                    next_measure = time.monotonic() + 30
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    continue
            if time.monotonic() - started >= duration:
                raise TimeoutError("acceptance invocation deadline exhausted")
            if any(os.fstat(f.fileno()).st_size > LOG_LIMIT for f in (stdout, stderr)):
                raise ValueError("acceptance diagnostics exceed their byte allowance")
            report["returncode"] = process.returncode
            report["after"] = storage(workspace, temporary_paths=tuple(temporary_paths))
            report["status"] = "passed" if process.returncode == 0 else "failed"
    except BaseException as exc:
        report["status"] = "failed"
        report["error_type"] = type(exc).__name__
        # Full diagnostics stay in the bounded CLI log; exceptions can carry credentials.
        if process is not None and process.poll() is None:
            try:
                process.send_signal(signal.SIGHUP)
                stopped = True
            except ProcessLookupError:
                pass
        raise
    finally:
        with SIGNALS.hold():
            if process is not None:
                process.wait()  # Reap supported graceful shutdown; never launch a competing writer.
                report["returncode"] = process.returncode
        report["stop_signal_sent"] = "SIGHUP" if stopped else None
        report["duration_s"] = time.monotonic() - started
        report["child_maxrss_native"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        atomic_write_bytes(
            output / "report.json", (json.dumps(report, sort_keys=True) + "\n").encode()
        )
        print(
            json.dumps({"report": str(output / "report.json"), "status": report["status"]}),
            flush=True,
        )
    return report.get("returncode", 3)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=14400)
    parser.add_argument("--reserve-bytes", type=int, default=0)
    parser.add_argument("--load-pages", type=int, choices=(50, 250), default=50)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    restore = _install_termination_handlers()
    try:
        return run(
            args.workspace,
            args.root,
            command,
            duration=args.duration,
            additional_bytes=args.reserve_bytes,
            load_pages=args.load_pages,
        )
    except (ValueError, OSError, TimeoutError):
        print("acceptance invocation failed; inspect its report and CLI logs", file=sys.stderr)
        return 3
    except Terminated as exc:
        return 128 + exc.signum
    finally:
        restore()


if __name__ == "__main__":
    sys.exit(main())
