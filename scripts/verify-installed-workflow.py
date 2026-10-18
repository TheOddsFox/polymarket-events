#!/usr/bin/env python3
"""Prove the audited wheel's full workflow outside the checkout, using only loopback fixtures."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import queue
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))

from fakes.current_gamma import CurrentGammaServer  # noqa: E402
from oddsfox_catalogue.current_config import CurrentLimits, safe_path  # noqa: E402
from oddsfox_catalogue.platform_support import (  # noqa: E402
    WindowsJob,
    is_link_or_reparse,
    operator_signals,
)
from oddsfox_catalogue.signals import SIGNALS, Terminated  # noqa: E402


def resource_inventory(root: Path) -> dict[str, dict[str, object]]:
    if os.name == "nt":
        safe_path(root)
    inventory = {}
    for path in sorted(root.rglob("*")):
        if os.name == "nt":
            safe_path(path)
        if is_link_or_reparse(path) or not (path.is_dir() or path.is_file()):
            raise ValueError("installed package resource is not a regular file or directory")
        if path.is_file():
            inventory[path.relative_to(root).as_posix()] = {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
    if not inventory or "__init__.py" not in inventory or "cli.py" not in inventory:
        raise ValueError("installed package is incomplete")
    return inventory


def freeze_resources(root: Path) -> None:
    if os.name == "nt":
        raise ValueError("Windows installed-resource freeze requires a restorable ACL snapshot")
    for path in root.rglob("*"):
        path.chmod(0o500 if path.is_dir() else 0o400)
    root.chmod(0o500)


def assert_resources_frozen(root: Path) -> None:
    if os.name == "nt":
        for path in (root, *root.rglob("*")):
            if path.is_dir():
                probe = path / f".codex-freeze-probe-{uuid.uuid4().hex}"
                try:
                    probe.open("xb").close()
                except PermissionError:
                    continue
                with suppress(FileNotFoundError):
                    probe.unlink()
                raise ValueError("installed package directory remained writable")
            try:
                path.open("ab").close()
            except PermissionError:
                continue
            raise ValueError("installed package file remained writable")
        return
    if any(stat.S_IMODE(path.stat().st_mode) & 0o222 for path in (root, *root.rglob("*"))):
        raise ValueError("installed package resources became writable")


def _icacls(*arguments: str) -> None:
    result = subprocess.run(["icacls", *arguments], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ValueError("Windows ACL operation failed; installed workflow proof is unsupported")


@contextlib.contextmanager
def frozen_resources(root: Path, acl_snapshot: Path):
    if os.name != "nt":
        modes = {path: stat.S_IMODE(path.stat().st_mode) for path in (root, *root.rglob("*"))}
        freeze_resources(root)
        try:
            yield
        finally:
            for path, mode in modes.items():
                with suppress(FileNotFoundError):
                    path.chmod(mode)
        return
    _icacls(str(root), "/save", str(acl_snapshot), "/T", "/C")
    if not acl_snapshot.is_file() or acl_snapshot.stat().st_size == 0:
        raise ValueError("Windows ACL snapshot is empty; installed workflow proof is unsupported")
    try:
        _icacls(str(root), "/deny", "*S-1-1-0:(WD,AD,DC,DE)", "/T", "/C")
        yield
    finally:
        _icacls(str(root.parent), "/restore", str(acl_snapshot), "/C")


def assert_accounting(report: dict, measured: dict, allowed_paths: set[str]) -> None:
    if report.get("status") != "passed" or report.get("accounting_complete") is not True:
        raise ValueError("installed workflow did not pass with complete accounting")
    if measured["exhausted"] or any(
        response["status"] != 200 for response in measured["responses"]
    ):
        raise ValueError("fixture returned an unexpected or exhausted response")
    for name in ("http_attempts", "downloaded_bytes"):
        if report.get(name) != measured[name]:
            raise ValueError(f"installed workflow {name} differs from fixture measurements")
    actual_paths = {urlsplit(response["path"]).path for response in measured["responses"]}
    if not actual_paths or not actual_paths <= allowed_paths:
        raise ValueError("unexpected source request outside the selected fixture scope")
    network_steps = (
        {"capture_1", "capture_2"} if report.get("workflow") == "catalogue" else {"refresh"}
    )
    network_attempts = network_bytes = 0
    for step in report["steps"]:
        result = step.get("result", {})
        attempts = result.get("http_attempts", result.get("requests", 0))
        downloaded = result.get("downloaded_bytes", 0)
        if step["name"] in network_steps:
            network_attempts += attempts
            network_bytes += downloaded
        elif attempts or downloaded:
            raise ValueError("offline workflow step reported network activity")
    if (
        network_attempts != measured["http_attempts"]
        or network_bytes != measured["downloaded_bytes"]
    ):
        raise ValueError("fixture observed source calls outside the acquisition steps")


def run_command(
    argv: list[str],
    cwd: Path,
    env: dict[str, str],
    log: Path,
    timeout=600,
    *,
    max_output_bytes: int | None = None,
) -> None:
    if max_output_bytes is not None and (type(max_output_bytes) is not int or max_output_bytes < 1):
        raise ValueError("command output allowance must be a positive integer")
    if os.name == "nt":
        _run_command_windows(argv, cwd, env, log, timeout, max_output_bytes)
        return
    with log.open("wb") as output:
        with SIGNALS.hold() as started:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdout=output if max_output_bytes is None else subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        try:
            if started.signum is not None:
                raise Terminated(started.signum)
            if max_output_bytes is not None:
                deadline, written = time.monotonic() + timeout, 0
                with selectors.DefaultSelector() as readable:
                    readable.register(process.stdout, selectors.EVENT_READ)
                    try:
                        while readable.get_map():
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                raise subprocess.TimeoutExpired(argv, timeout)
                            for key, _ in readable.select(remaining):
                                block = os.read(key.fd, 65536)
                                if not block:
                                    readable.unregister(key.fileobj)
                                    continue
                                output.write(block[: max_output_bytes - written])
                                written += len(block)
                                if written > max_output_bytes:
                                    raise ValueError(
                                        f"command output allowance exhausted; inspect {log}"
                                    )
                        code = process.wait(timeout=max(0, deadline - time.monotonic()))
                    finally:
                        process.stdout.close()
            else:
                code = process.wait(timeout=timeout)
        finally:
            with SIGNALS.hold() as stopped:

                def signal_group(signum):
                    running = process.poll() is None
                    try:
                        os.killpg(process.pid, signum)
                    except ProcessLookupError:
                        pass
                    except PermissionError:
                        if not running or process.poll() is None:
                            raise
                        with suppress(ProcessLookupError):
                            os.killpg(process.pid, signum)

                try:
                    signal_group(signal.SIGTERM)
                    with suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=5)
                finally:
                    try:
                        signal_group(signal.SIGKILL)
                    finally:
                        process.wait(timeout=5)
            if stopped.signum is not None:
                raise Terminated(stopped.signum)
    if code:
        with log.open("rb") as diagnostics:
            diagnostics.seek(0, os.SEEK_END)
            diagnostics.seek(max(0, diagnostics.tell() - 8192))
            tail = diagnostics.read(8192).lower()
        reason = ""
        if b"not found in the cache" in tail or b"needs to be downloaded from a registry" in tail:
            reason = "; diagnostic: offline_cache_miss"
        elif b"hash mismatch" in tail:
            reason = "; diagnostic: dependency_hash_mismatch"
        # Subprocess output may contain credentials; expose only fixed diagnostic codes.
        raise ValueError(f"command failed ({code}); inspect {log}{reason}")


def _run_command_windows(
    argv: list[str],
    cwd: Path,
    env: dict[str, str],
    log: Path,
    timeout: float,
    max_output_bytes: int | None,
) -> None:
    """Start through a gated shim so no target work can escape Job assignment."""
    import msvcrt

    command = list(argv)
    if not Path(command[0]).is_absolute():
        local = cwd / command[0]
        resolved = str(local.resolve()) if local.exists() else shutil.which(command[0])
        if resolved:
            command[0] = resolved

    gate_read, gate_write = os.pipe()
    gate_handle = msvcrt.get_osfhandle(gate_read)
    os.set_handle_inheritable(gate_handle, True)
    shim = REPO / "src" / "oddsfox_catalogue" / "_process_gate.py"
    child_env = dict(env)
    child_env["CATALOGUE_PROCESS_GATE_HANDLE"] = str(gate_handle)
    startup = subprocess.STARTUPINFO()
    startup.lpAttributeList = {"handle_list": [gate_handle]}
    reader_thread = None
    chunks: queue.Queue[bytes | None] | None = None
    failure: BaseException | None = None
    code: int | None = None
    process = None
    with log.open("wb") as output, WindowsJob() as job:
        try:
            with SIGNALS.hold() as started:
                process = subprocess.Popen(
                    [sys.executable, str(shim), "--gate-handle", str(gate_handle), "--", *command],
                    cwd=cwd,
                    env=child_env,
                    stdin=None,
                    stdout=output if max_output_bytes is None else subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                    startupinfo=startup,
                    creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
                )
                os.close(gate_read)
                gate_read = -1
                job.assign(process)
                if SIGNALS.pending is not None:
                    raise Terminated(SIGNALS.pending)
                os.write(gate_write, b"\x01")
                os.close(gate_write)
                gate_write = -1
            if started.signum is not None:
                raise Terminated(started.signum)
            if max_output_bytes is not None:
                chunks = queue.Queue(maxsize=2)

                def read_output() -> None:
                    try:
                        while block := process.stdout.read(65536):
                            chunks.put(block)
                    finally:
                        chunks.put(None)

                reader_thread = threading.Thread(target=read_output, daemon=True)
                reader_thread.start()
            deadline = time.monotonic() + timeout
            written = 0
            if chunks is None:
                code = process.wait(timeout=max(0, deadline - time.monotonic()))
            else:
                finished = False
                while not finished:
                    if time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired(argv, timeout)
                    try:
                        block = chunks.get(timeout=min(0.1, deadline - time.monotonic()))
                    except queue.Empty:
                        if (
                            process.poll() is not None
                            and reader_thread
                            and not reader_thread.is_alive()
                        ):
                            finished = True
                        continue
                    if block is None:
                        finished = True
                    else:
                        remaining = max_output_bytes - written
                        if len(block) > remaining:
                            output.write(block[:remaining])
                            written += remaining
                            raise ValueError(f"command output allowance exhausted; inspect {log}")
                        output.write(block)
                        written += len(block)
                code = process.wait(timeout=max(0, deadline - time.monotonic()))
        except BaseException as exc:
            failure = exc
        finally:
            if gate_read >= 0:
                os.close(gate_read)
            if gate_write >= 0:
                os.close(gate_write)
            if process is not None:
                with SIGNALS.hold() as stopped:
                    try:
                        if process.poll() is None and hasattr(signal, "CTRL_BREAK_EVENT"):
                            with suppress(OSError, ValueError):
                                process.send_signal(signal.CTRL_BREAK_EVENT)
                        with suppress(subprocess.TimeoutExpired):
                            process.wait(timeout=5)
                    finally:
                        try:
                            job.terminate()
                        finally:
                            try:
                                process.wait(timeout=5)
                            except subprocess.TimeoutExpired as exc:
                                if failure is None:
                                    failure = exc
                    if reader_thread is not None:
                        drain_until = time.monotonic() + 5
                        while reader_thread.is_alive() and time.monotonic() < drain_until:
                            with suppress(queue.Empty):
                                chunks.get(timeout=0.05)
                        reader_thread.join(timeout=5)
                    if process.stdout is not None:
                        process.stdout.close()
                if stopped.signum is not None:
                    failure = Terminated(stopped.signum)
            if chunks is not None:
                while True:
                    try:
                        block = chunks.get_nowait()
                    except queue.Empty:
                        break
                    if block:
                        continue
    if failure is not None:
        raise failure
    if code:
        with log.open("rb") as diagnostics:
            diagnostics.seek(0, os.SEEK_END)
            diagnostics.seek(max(0, diagnostics.tell() - 8192))
            tail = diagnostics.read(8192).lower()
        reason = ""
        if b"not found in the cache" in tail or b"needs to be downloaded from a registry" in tail:
            reason = "; diagnostic: offline_cache_miss"
        elif b"hash mismatch" in tail:
            reason = "; diagnostic: dependency_hash_mismatch"
        raise ValueError(f"command failed ({code}); inspect {log}{reason}")


def verify(output: Path) -> dict:
    if os.name == "nt":
        safe_path(output)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CATALOGUE_", "PYTHON")) and key != "VIRTUAL_ENV"
    }
    env.update(
        UV_LINK_MODE="copy",
        UV_OFFLINE="1",
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH="",
    )
    dist, runtime, venv = output / "dist", output / "runtime", output / "venv"
    runtime.mkdir()
    scripts = venv / ("Scripts" if os.name == "nt" else "bin")
    python = scripts / ("python.exe" if os.name == "nt" else "python")
    requirements = output / "requirements.txt"
    run_command(
        ["uv", "build", "--offline", "--python", sys.executable, "--out-dir", str(dist)],
        REPO,
        env,
        output / "build.log",
    )
    archives = sorted((*dist.glob("*.whl"), *dist.glob("*.tar.gz")))
    if len(archives) != 2:
        raise ValueError("build must produce one wheel and one sdist")
    run_command(
        [sys.executable, str(REPO / "scripts/verify-distribution.py"), *map(str, archives)],
        REPO,
        env,
        output / "distribution-audit.json",
    )
    run_command(
        [
            "uv",
            "export",
            "--locked",
            "--offline",
            "--no-dev",
            "--no-emit-project",
            "--output-file",
            str(requirements),
        ],
        REPO,
        env,
        output / "lock-export.log",
    )
    run_command(
        ["uv", "venv", "--offline", "--no-project", "--python", sys.executable, str(venv)],
        runtime,
        env,
        output / "venv.log",
    )
    run_command(
        [
            "uv",
            "pip",
            "install",
            "--offline",
            "--python",
            str(python),
            "--require-hashes",
            "-r",
            str(requirements),
        ],
        runtime,
        env,
        output / "install-dependencies.log",
    )
    wheel = next(path for path in archives if path.suffix == ".whl")
    run_command(
        ["uv", "pip", "install", "--offline", "--python", str(python), "--no-deps", str(wheel)],
        runtime,
        env,
        output / "install-wheel.log",
    )
    run_command(
        ["uv", "pip", "check", "--python", str(python)], runtime, env, output / "pip-check.log"
    )
    run_command(
        [
            str(python),
            "-c",
            "import importlib.util as u; "
            "assert all(u.find_spec(m) is None for m in ['dlt','dbt','dagster']); "
            "import oddsfox_catalogue; print(oddsfox_catalogue.__file__)",
        ],
        runtime,
        env,
        output / "installed-origin.log",
    )
    origin = Path((output / "installed-origin.log").read_text().strip())
    if os.name == "nt":
        safe_path(origin)
    if origin.is_relative_to(REPO) or not origin.is_relative_to(venv):
        raise ValueError("package was imported from checkout instead of installed wheel")
    package = origin.parent
    before = resource_inventory(package)
    with frozen_resources(package, output / "installed-package.acl"):
        root = runtime / "catalogue"
        steps = []
        executable = scripts / ("catalogue.exe" if os.name == "nt" else "catalogue")

        def call(name, arguments, *, source=None, selected_root=root):
            child_env = dict(env)
            child_env.update(
                CATALOGUE_GAMMA_BASE_URL=source or "http://127.0.0.1:9",
                CATALOGUE_GAMMA_ALLOW_LOOPBACK="1",
                CATALOGUE_GAMMA_MAX_RETRIES="0",
            )
            log = output / (name + ".json")
            run_command(
                [str(executable), "--root", str(selected_root), *map(str, arguments)],
                runtime,
                child_env,
                log,
                timeout=CurrentLimits().max_duration_s + 10,
            )
            result = json.loads(log.read_text())
            steps.append({"name": name, "result": result})
            return result

        call("init", ["init"])
        with CurrentGammaServer() as fixture:
            controls = [
                "probe",
                "--open-event-id",
                "1",
                "--closed-event-id",
                "2",
                "--absent-event-id",
                "999",
                "--open-market-id",
                "11",
                "--closed-market-id",
                "21",
                "--absent-market-id",
                "999",
            ]
            call("probe", controls, source=fixture.url)
            refresh = call("refresh", ["refresh"], source=fixture.url)
            call("verify", ["verify"])
            call("reconcile", ["reconcile"], source=fixture.url)
            call("full_pass", ["reconcile", "--full-pass"], source=fixture.url)
            bundle = runtime / "metadata-bundle"
            call(
                "metadata_refresh",
                [
                    "metadata",
                    "refresh",
                    "--market-id",
                    "11",
                    "--market-id",
                    "12",
                    "--output",
                    bundle,
                    "--catalogue-root",
                    root,
                ],
                source=fixture.url,
                selected_root=runtime / "metadata-producer",
            )
            measured = json.loads(json.dumps(fixture.stats))
        current = call("published", ["current"])
        if current.get("valid") is not True:
            raise ValueError("installed current pointer did not verify")
        attempts = sum(step["result"].get("http_attempts", 0) for step in steps)
        downloaded = sum(step["result"].get("downloaded_bytes", 0) for step in steps)
        if attempts != measured["http_attempts"] or downloaded != measured["downloaded_bytes"]:
            raise ValueError("installed request accounting differs from HTTP fixture")
        if refresh["publication"]["counts"] != {
            "events": 2,
            "markets": 3,
            "event_markets": 3,
            "outcomes": 6,
        }:
            raise ValueError("installed current catalogue contents differ")
        if resource_inventory(package) != before:
            raise ValueError("installed package resources changed during workflow")
        assert_resources_frozen(package)
        report = {
            "status": "passed",
            "workflow": "catalogue.v3",
            "accounting_complete": True,
            "http_attempts": attempts,
            "downloaded_bytes": downloaded,
            "steps": steps,
            "fixture": measured,
        }
        (output / "report.json").write_text(json.dumps(report, indent=2))
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="fresh evidence directory outside the checkout")
    args = parser.parse_args()
    output = (
        args.output.absolute()
        if args.output
        else Path(
            tempfile.mkdtemp(
                prefix="catalogue-installed-", dir=Path(tempfile.gettempdir()).resolve()
            )
        )
    )
    if output.is_relative_to(REPO):
        parser.error("installed proof must run outside the checkout")
    if args.output:
        if output.exists() or any(is_link_or_reparse(path) for path in (output, *output.parents)):
            parser.error("output must be fresh and confined by regular directories")
        output.mkdir(parents=True)
    started = time.monotonic()
    previous = {
        signum: signal.signal(signum, lambda signum, _frame: SIGNALS.receive(signum))
        for signum in operator_signals(include_interrupt=True)
    }
    code = 0
    try:
        report = verify(output)
    except (OSError, ValueError, subprocess.TimeoutExpired, KeyboardInterrupt, Terminated) as exc:
        report = {"status": "failed", "error": str(exc) or type(exc).__name__}
        code = (
            128 + exc.signum
            if isinstance(exc, Terminated)
            else 130
            if isinstance(exc, KeyboardInterrupt)
            else 1
        )
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    report.update(duration_seconds=round(time.monotonic() - started, 3), output=str(output))
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "report": str(output / "report.json"),
                "error": report.get("error"),
            },
            indent=2,
        )
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
