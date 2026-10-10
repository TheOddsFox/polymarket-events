#!/usr/bin/env python3
"""Prove the audited wheel's full workflow outside the checkout, using only loopback fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))

from fakes.fake_gamma import FakeGamma  # noqa: E402
from fakes.gamma_server import GammaServer  # noqa: E402
from fakes.world import demo_world  # noqa: E402
from oddsfox_catalogue.signals import SIGNALS, Terminated  # noqa: E402


def resource_inventory(root: Path) -> dict[str, dict[str, object]]:
    inventory = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise ValueError("installed dbt resource is not a regular file or directory")
        if path.is_file():
            inventory[path.relative_to(root).as_posix()] = {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
    if not inventory or "dbt_project.yml" not in inventory or "profiles.yml" not in inventory:
        raise ValueError("installed dbt project is incomplete")
    return inventory


def freeze_resources(root: Path) -> None:
    for path in root.rglob("*"):
        path.chmod(0o500 if path.is_dir() else 0o400)
    root.chmod(0o500)


def assert_resources_frozen(root: Path) -> None:
    if any(stat.S_IMODE(path.stat().st_mode) & 0o222 for path in (root, *root.rglob("*"))):
        raise ValueError("installed dbt resources became writable")


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


def run_command(argv: list[str], cwd: Path, env: dict[str, str], log: Path, timeout=600) -> None:
    with log.open("w") as output:
        with SIGNALS.hold() as started:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        try:
            if started.signum is not None:
                raise Terminated(started.signum)
            code = process.wait(timeout=timeout)
        finally:
            with SIGNALS.hold() as stopped:
                try:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGTERM)
                    with suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=5)
                finally:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
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


def verify(output: Path) -> dict:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CATALOGUE_", "PYTHON")) and key != "VIRTUAL_ENV"
    }
    env.update(UV_LINK_MODE="copy", UV_OFFLINE="1", PYTHONNOUSERSITE="1", PYTHONPATH="")
    dist = output / "dist"
    runtime = output / "runtime"
    runtime.mkdir()
    venv = output / "venv"
    python = venv / "bin/python"
    requirements = output / "requirements.txt"
    run_command(
        ["uv", "build", "--offline", "--python", sys.executable, "--out-dir", str(dist)],
        REPO,
        env,
        output / "build.log",
    )
    archives = sorted((*dist.glob("*.whl"), *dist.glob("*.tar.gz")))
    if len(archives) != 2 or sum(path.suffix == ".whl" for path in archives) != 1:
        raise ValueError("build did not produce exactly one wheel and one sdist")
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
        ["uv", "pip", "check", "--python", str(python)],
        runtime,
        env,
        output / "installed-dependencies.log",
    )
    probe = """
import json, sys
from pathlib import Path
import oddsfox_catalogue.cli as cli
import oddsfox_catalogue.smoke as smoke
from oddsfox_catalogue.resources import dbt_project_dir
Path(sys.argv[1]).write_text(json.dumps({'cli': cli.__file__, 'smoke': smoke.__file__, 'dbt': str(dbt_project_dir()), 'sys_path': sys.path}))
"""
    run_command(
        [str(python), "-I", "-c", probe, str(output / "installed-paths.json")],
        runtime,
        env,
        output / "installed-probe.log",
    )
    installed = json.loads((output / "installed-paths.json").read_text())
    if any(not Path(installed[key]).is_relative_to(venv) for key in ("cli", "smoke", "dbt")):
        raise ValueError("runtime imports did not originate from the installed wheel")
    if any(Path(path).is_relative_to(REPO) for path in installed["sys_path"] if path):
        raise ValueError("runtime import path exposes the source checkout")
    resources = Path(installed["dbt"])
    before = resource_inventory(resources)
    freeze_resources(resources)
    assert_resources_frozen(resources)
    run_command([str(venv / "bin/catalogue"), "--help"], runtime, env, output / "installed-cli.log")
    workflows = {}
    for workflow in ("catalogue", "metadata"):
        proof = output / workflow
        with GammaServer(FakeGamma(demo_world())) as source:
            proof_env = dict(
                env, CATALOGUE_GAMMA_BASE_URL=source.url, CATALOGUE_GAMMA_ALLOW_LOOPBACK="1"
            )
            argv = [
                str(python),
                "-I",
                "-m",
                "oddsfox_catalogue.smoke",
                "--workflow",
                workflow,
                "--output",
                str(proof),
            ]
            market_ids = ("5001", "5002", "6001") if workflow == "catalogue" else ("5001",)
            for market_id in market_ids:
                argv.extend(("--market-id", market_id))
            run_command(argv, runtime, proof_env, output / f"{workflow}-smoke.log", timeout=1830)
            report = json.loads((proof / "report.json").read_text())
            measured = source.accounting()
        allowed = {f"/markets/{market_id}" for market_id in market_ids}
        if workflow == "catalogue":
            allowed.update(("/events/101", "/events/202"))
        assert_accounting(report, measured, allowed)
        if workflow == "catalogue":
            required = {
                "raw_rebuild",
                "backup_create",
                "backup_verify",
                "backup_restore",
                "verify_restore",
                "injected_publication",
                "verify_after_failure",
                "offline_replay",
            }
            if not required <= {step["name"] for step in report["steps"]}:
                raise ValueError("installed catalogue proof omitted a required recovery step")
            if report.get("semantic_comparisons") != {
                "offline_replay": True,
                "raw_rebuild": True,
                "restore": True,
            } or report.get("compared_relations") != {
                "warehouse": 29,
                "published": 7,
                "warehouse_schemas": 30,
            }:
                raise ValueError("installed catalogue proof omitted complete semantic comparisons")
        workflows[workflow] = {"report": str(proof / "report.json"), "fixture": measured}
    after = resource_inventory(resources)
    assert_resources_frozen(resources)
    if before != after:
        raise ValueError("installed workflow changed the packaged dbt resources")
    return {
        "status": "passed",
        "installed": installed,
        "resources": before,
        "workflows": workflows,
        "distributions": [
            {
                "file": path.name,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in archives
        ],
        "distribution_audit": json.loads((output / "distribution-audit.json").read_text()),
    }


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
        if output.exists() or any(path.is_symlink() for path in (output, *output.parents)):
            parser.error("output must be fresh and confined by regular directories")
        output.mkdir(parents=True)
    started = time.monotonic()
    previous = {
        signum: signal.signal(signum, lambda signum, _frame: SIGNALS.receive(signum))
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
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
