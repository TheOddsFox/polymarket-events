"""Run one installed daily workflow against a pinned recorded loopback corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import sqlite3
import subprocess
import tempfile
import time
import uuid
from contextlib import closing
from pathlib import Path

from oddsfox_catalogue.capture.writer import read_regular_bytes

try:
    from tools.speed_corpus import RecordedGammaServer, _hash_file, _safe_path
except ModuleNotFoundError:
    from speed_corpus import RecordedGammaServer, _hash_file, _safe_path

LIMITS = {
    "max_requests": 25000,
    "max_download_bytes": 4 * 1024**3,
    "max_duration_s": 14400,
    "max_retained_bytes": 256 * 1024**3,
    "max_temp_bytes": 32 * 1024**3,
    "fixture_max_requests": 200000,
    "fixture_max_download_bytes": 32 * 1024**3,
}
CAP_ERRORS = {
    "RequestBudgetExceeded: Gamma request allowance exhausted",
    "RequestBudgetExceeded: Gamma download allowance exhausted",
    "RequestBudgetExceeded: Gamma download allowance exceeded",
    "RequestBudgetExceeded: Gamma invocation duration exhausted",
}
REPORT_LIMIT = 4 * 1024**2


def _json(path):
    path = _safe_path(path, exists=True)
    with path.open("rb") as handle:
        body = handle.read(REPORT_LIMIT + 1)
    if len(body) > REPORT_LIMIT:
        raise ValueError("benchmark metadata exceeds its bound")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("benchmark metadata must be an object")
    return value


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _descriptor(value):
    return (
        isinstance(value, dict)
        and set(value) == {"bytes", "sha256"}
        and type(value["bytes"]) is int
        and value["bytes"] > 0
        and isinstance(value["sha256"], str)
        and re.fullmatch(r"[a-f0-9]{64}", value["sha256"]) is not None
    )


def _machine():
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError):
        if platform.system() != "Darwin":
            raise ValueError("cannot pin physical memory") from None
        memory = int(subprocess.check_output(["/usr/sbin/sysctl", "-n", "hw.memsize"], timeout=10))
    return {
        "node": platform.node(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "physical_memory_bytes": memory,
    }


def _harness_sha256():
    directory = Path(__file__).resolve().parent
    return _digest(
        {
            name: _hash_file(directory / name)
            for name in ("speed_benchmark.py", "speed_corpus.py", "speed_oracle.py")
        }
    )


def _validate_experiment(value):
    shared_keys = {
        "lock_sha256",
        "dependencies_sha256",
        "effective_settings_sha256",
        "python_version",
        "machine",
        "harness_sha256",
        "seed_raw_inventory",
        "seed_coverage",
    }
    if (
        not isinstance(value, dict)
        or set(value) != {"revision", "shared", "variants"}
        or type(value["revision"]) is not int
        or value["revision"] != 1
    ):
        raise ValueError("invalid experiment revision or fields")
    shared, variants = value["shared"], value["variants"]
    if (
        not isinstance(shared, dict)
        or set(shared) != shared_keys
        or not isinstance(variants, dict)
        or set(variants) != {"baseline", "candidate"}
    ):
        raise ValueError("incomplete experiment pins")
    for key in (
        "lock_sha256",
        "dependencies_sha256",
        "effective_settings_sha256",
        "harness_sha256",
    ):
        if not isinstance(shared[key], str) or not re.fullmatch(r"[a-f0-9]{64}", shared[key]):
            raise ValueError("invalid experiment SHA-256")
    if not isinstance(shared["python_version"], str) or not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+", shared["python_version"]
    ):
        raise ValueError("invalid Python version pin")
    machine = shared["machine"]
    if (
        not isinstance(machine, dict)
        or set(machine)
        != {"node", "system", "release", "machine", "cpu_count", "physical_memory_bytes"}
        or any(
            not isinstance(machine[k], str) or not machine[k]
            for k in ("node", "system", "release", "machine")
        )
        or any(
            type(machine[k]) is not int or machine[k] < 1
            for k in ("cpu_count", "physical_memory_bytes")
        )
    ):
        raise ValueError("invalid machine pin")
    if not all(_descriptor(shared[key]) for key in ("seed_raw_inventory", "seed_coverage")):
        raise ValueError("invalid raw seed descriptors")
    for variant in variants.values():
        if (
            not isinstance(variant, dict)
            or set(variant) != {"runtime", "wheel_sha256"}
            or not _descriptor(variant["runtime"])
            or not isinstance(variant["wheel_sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", variant["wheel_sha256"])
        ):
            raise ValueError("invalid variant runtime or wheel pin")
    return value


def _probe(python, root):
    code = """
import hashlib, json, platform, sys
from importlib.metadata import distributions
from dataclasses import asdict
from pathlib import Path
from oddsfox_catalogue.acceptance import environment
from oddsfox_catalogue.config import PathSettings, load_settings
from oddsfox_catalogue.certification import capture_binding
from oddsfox_catalogue.rebuild import _runtime_revision
effective_env = environment(Path(sys.argv[1]), ['capture','--mode','daily'])
effective_env.update(CATALOGUE_GAMMA_BASE_URL='http://127.0.0.1:9', CATALOGUE_GAMMA_ALLOW_LOOPBACK='1')
settings = load_settings(root=Path(sys.argv[1]), env=effective_env)
if settings.paths != PathSettings():
    raise ValueError('benchmark requires default paths and installed dbt resources')
effective_settings = {name: asdict(getattr(settings, name)) for name in ('gamma','capture','load','quality','schedule')}
effective_settings['gamma'].update(base_url='recorded-fixture', allow_loopback=True)
effective_settings_sha256 = hashlib.sha256(json.dumps(effective_settings,sort_keys=True,separators=(',',':')).encode()).hexdigest()
binding = capture_binding(settings)
dependencies = sorted((d.metadata['Name'], d.version) for d in distributions())
print(json.dumps({'effective_settings': effective_settings, 'effective_settings_sha256': effective_settings_sha256, 'runtime': _runtime_revision(), 'python_version': platform.python_version(), 'dependencies': dependencies,
 'dependencies_sha256': hashlib.sha256(json.dumps(dependencies,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
 'seed_raw_inventory': binding['inventory'], 'seed_coverage': binding['coverage']}))
"""
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("CATALOGUE_") and k not in {"PYTHONPATH", "PYTHONHOME"}
    }
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        completed = subprocess.run(
            [str(python), "-I", "-c", code, str(root)],
            env=env,
            stdout=stdout,
            stderr=stderr,
            check=False,
        )
        stdout.seek(0)
        body = stdout.read(REPORT_LIMIT + 1)
    if completed.returncode or len(body) > REPORT_LIMIT:
        raise ValueError("installed runtime/raw-seed probe failed")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("invalid installed probe result")
    return value


def _observations_match(manifest, variant, observed):
    if variant not in {"baseline", "candidate"} or not isinstance(observed, dict):
        return False
    shared, expected = manifest["shared"], manifest["variants"][variant]
    return (
        all(observed.get(k) == v for k, v in shared.items())
        and observed.get("runtime") == expected["runtime"]
        and observed.get("wheel_sha256") == expected["wheel_sha256"]
        and isinstance(observed.get("dependencies"), list)
        and _digest(observed["dependencies"]) == shared["dependencies_sha256"]
        and isinstance(observed.get("effective_settings"), dict)
        and set(observed["effective_settings"])
        == {"gamma", "capture", "load", "quality", "schedule"}
        and all(
            isinstance(section, dict) and section
            for section in observed["effective_settings"].values()
        )
        and _digest(observed["effective_settings"]) == shared["effective_settings_sha256"]
    )


def _ledger(root):
    path = _safe_path(root / ".state" / "ledger.sqlite", exists=True)
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)


def _batches(root):
    with closing(_ledger(root)) as db:
        db.row_factory = sqlite3.Row
        return {
            row["batch_id"]: dict(row)
            for row in db.execute(
                "SELECT batch_id,mode,status,scope_json,error FROM batches ORDER BY batch_id"
            )
        }


def _capture_stage_id(root):
    with closing(_ledger(root)) as db:
        return db.execute("SELECT coalesce(max(rowid),0) FROM stage_runs").fetchone()[0]


def _cap_exhausted(root, batch, *, after=0):
    with closing(_ledger(root)) as db:
        row = db.execute(
            "SELECT rowid,status,error FROM stage_runs WHERE batch_id=? AND stage='capture:daily' ORDER BY rowid DESC LIMIT 1",
            (batch["batch_id"],),
        ).fetchone()
    return (
        batch["status"] == "capturing"
        and batch["error"] in CAP_ERRORS
        and row is not None
        and row[0] > after
        and row[1:] == ("failed", batch["error"])
    )


def _invoke(python, root, workspace, arguments, *, load_pages, env, expected_runtime=None):
    proof = workspace / "completion-proof"
    if proof.exists():
        _safe_path(proof / "placeholder", exists=False)
    before = set(proof.iterdir()) if proof.exists() else set()
    command = [
        str(python),
        "-m",
        "oddsfox_catalogue.acceptance",
        "--workspace",
        str(workspace),
        "--root",
        str(root),
        "--load-pages",
        str(load_pages),
        "--",
        *arguments,
    ]
    began = time.perf_counter()
    invocation = {"command": command, "returncode": None, "report": None}
    try:
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            completed = subprocess.run(command, env=env, stdout=stdout, stderr=stderr, check=False)
        invocation["returncode"] = completed.returncode
        elapsed = time.perf_counter() - began
        added = set(proof.iterdir()) - before if proof.exists() else set()
        if len(added) != 1:
            raise ValueError("invocation must produce exactly one new acceptance report")
        directory = added.pop()
        if not re.fullmatch(r"[a-f0-9]{32}", directory.name):
            raise ValueError("unexpected acceptance report directory")
        path = directory / "report.json"
        report = _json(path)
        invocation.update(report_path=str(path), report=report)
        duration = report.get("duration_s")
        if (
            report.get("status") not in {"passed", "failed"}
            or report.get("returncode") != completed.returncode
            or report.get("root") != str(root)
            or report.get("command") != arguments
            or not isinstance(report.get("runtime"), dict)
            or (expected_runtime is not None and report["runtime"] != expected_runtime)
            or isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(duration)
            or duration <= 0
            or report.get("max_retained_bytes") != LIMITS["max_retained_bytes"]
            or report.get("max_temp_bytes") != LIMITS["max_temp_bytes"]
            or report.get("duration_limit_s") != LIMITS["max_duration_s"]
            or report.get("load_pages") != load_pages
            or (completed.returncode == 0) != (report["status"] == "passed")
        ):
            raise ValueError("incomplete or inconsistent acceptance report")
        return {
            "command": command,
            "returncode": completed.returncode,
            "wall_s": elapsed,
            "report_path": str(path),
            "report": report,
        }
    except (ValueError, OSError) as exc:
        invocation["wall_s"] = time.perf_counter() - began
        exc.invocation = invocation
        raise


def run_variant(
    python: Path,
    root: Path,
    workspace: Path,
    corpus: Path,
    *,
    experiment: Path,
    variant: str,
    wheel: Path,
    lockfile: Path | None = None,
    max_capture_invocations=8,
) -> dict:
    """Measure the whole workflow; semantic proof remains a separate required gate."""
    python = Path(os.path.abspath(python))
    if not python.is_file() or not os.access(python, os.X_OK):
        raise ValueError("python must be an existing executable")
    workspace = Path(os.path.abspath(workspace))
    root = Path(os.path.abspath(root))
    if (
        not workspace.is_dir()
        or not root.is_dir()
        or root == workspace
        or not root.is_relative_to(workspace)
    ):
        raise ValueError("benchmark root must be an existing seed inside its workspace")
    _safe_path(root / "placeholder", exists=False)
    owner = json.loads(
        read_regular_bytes(
            root / ".state" / "pipeline-speed-owner.json", max_bytes=16384, trusted_root=root
        )
    )
    required_owner = {
        "purpose": "pipeline-speed-benchmark",
        "revision": 1,
        "baseline_commit": "bd5eb0c1af1e40295305b6558d9ea28d1f1ce5fa",
    }
    if (
        not isinstance(owner, dict)
        or any(owner.get(k) != v for k, v in required_owner.items())
        or type(owner["revision"]) is not int
    ):
        raise ValueError("benchmark requires an explicit pipeline-speed owned seed marker")
    if type(max_capture_invocations) is not int or not 1 <= max_capture_invocations <= 8:
        raise ValueError("capture invocation bound must be 1..8")
    seed = _batches(root)
    if not seed or not any(b["status"] in {"captured", "loaded"} for b in seed.values()):
        raise ValueError("benchmark requires an existing captured seed")
    workload = [
        {k: b[k] for k in ("batch_id", "mode", "status", "scope_json")} for b in seed.values()
    ]
    workload_pin = hashlib.sha256(json.dumps(workload, sort_keys=True).encode()).hexdigest()
    experiment = _safe_path(experiment, exists=True)
    if any((parent / ".git").exists() for parent in experiment.parents):
        raise ValueError("experiment manifest must be outside Git checkouts")
    if experiment.stat().st_mode & 0o222:
        raise ValueError("experiment manifest must be frozen read-only before timing")
    manifest = _validate_experiment(_json(experiment))
    if variant not in {"baseline", "candidate"}:
        raise ValueError("variant must be baseline or candidate")
    experiment_sha256 = _hash_file(experiment)
    lockfile = _safe_path(lockfile or Path(__file__).resolve().parents[1] / "uv.lock", exists=True)
    observed = {
        **_probe(python, root),
        "machine": _machine(),
        "harness_sha256": _harness_sha256(),
        "lock_sha256": _hash_file(lockfile),
        "wheel_sha256": _hash_file(_safe_path(wheel, exists=True)),
    }
    if not _observations_match(manifest, variant, observed):
        raise ValueError("experiment preflight pin mismatch")
    expected_runtime = manifest["variants"][variant]["runtime"]
    result = {
        "status": "stage_failed",
        "variant": variant,
        "experiment_sha256": experiment_sha256,
        "experiment": manifest,
        "experiment_content_sha256": _digest(manifest),
        "observed_pins": observed,
        "semantic_validated": False,
        "limits_verified": False,
        "limits": {**LIMITS, "max_capture_invocations": max_capture_invocations},
        "workload_pin": workload_pin,
        "corpus_sha256": _hash_file(_safe_path(corpus, exists=True)),
        "invocations": [],
        "python": str(python),
        "root": str(root),
    }
    began = time.perf_counter()
    batch_id = None
    with RecordedGammaServer(
        corpus,
        max_requests=LIMITS["fixture_max_requests"],
        max_download_bytes=LIMITS["fixture_max_download_bytes"],
    ) as fixture:
        result["delay_policy_sha256"] = fixture.manifest["delay_policy_sha256"]
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("CATALOGUE_") and k not in {"PYTHONPATH", "PYTHONHOME"}
        }
        env.update(CATALOGUE_GAMMA_BASE_URL=fixture.url, CATALOGUE_GAMMA_ALLOW_LOOPBACK="true")
        try:
            captured = False
            budget_stop = False
            for _ in range(max_capture_invocations):
                arguments = ["capture", "--mode", "daily"] + (
                    ["--resume", batch_id] if batch_id else []
                )
                stage_before = _capture_stage_id(root)
                invocation = _invoke(
                    python,
                    root,
                    workspace,
                    arguments,
                    load_pages=50,
                    env=env,
                    expected_runtime=expected_runtime,
                )
                result["invocations"].append(invocation)
                current = {
                    k: b
                    for k, b in _batches(root).items()
                    if k not in seed and b["mode"] == "daily"
                }
                if len(current) != 1 or (batch_id is not None and batch_id not in current):
                    raise ValueError("capture did not identify exactly one new daily batch")
                batch_id, batch = next(iter(current.items()))
                result["batch_id"] = batch_id
                if invocation["returncode"] == 0 and batch["status"] == "captured":
                    captured = True
                    break
                budget_stop = (
                    invocation["returncode"] == 3
                    and invocation["report"]["status"] == "failed"
                    and "error_type" not in invocation["report"]
                    and _cap_exhausted(root, batch, after=stage_before)
                )
                if not budget_stop:
                    break
            if captured:
                stages = [
                    (["load", "--batch-id", batch_id, "--record-key", "markets"], 250),
                    (["load", "--batch-id", batch_id], 50),
                    (["dbt", "--", "build"], 50),
                    (["publish"], 50),
                    (["verify"], 50),
                ]
                for arguments, load_pages in stages:
                    invocation = _invoke(
                        python,
                        root,
                        workspace,
                        arguments,
                        load_pages=load_pages,
                        env=env,
                        expected_runtime=expected_runtime,
                    )
                    result["invocations"].append(invocation)
                    if invocation["returncode"] != 0:
                        break
                else:
                    result["status"] = "passed"
            elif budget_stop:
                result["status"] = "capture_cap_exhausted"
            if (
                _hash_file(experiment) != experiment_sha256
                or _hash_file(lockfile) != observed["lock_sha256"]
                or _hash_file(wheel) != observed["wheel_sha256"]
                or _harness_sha256() != observed["harness_sha256"]
                or _machine() != observed["machine"]
            ):
                raise ValueError("experiment pins changed during workflow")
            result["limits_verified"] = bool(result["invocations"])
        except (ValueError, OSError, sqlite3.Error) as exc:
            if hasattr(exc, "invocation"):
                result["invocations"].append(exc.invocation)
            result["status"] = "invalid_run"
            result["error_type"] = type(exc).__name__
        finally:
            result["fixture"] = fixture.accounting()
            if result["fixture"]["errors"]:
                result["status"] = "invalid_fixture"
    result["wall_s"] = time.perf_counter() - began
    result["acceptance_duration_s"] = sum(
        i["report"]["duration_s"]
        for i in result["invocations"]
        if isinstance(i.get("report"), dict)
        and isinstance(i["report"].get("duration_s"), (int, float))
        and math.isfinite(i["report"]["duration_s"])
    )
    return result


def compare_runs(baseline, candidate, *, scaling_variability=0) -> dict:
    """Only independently validated, equally pinned workflows may produce a speed ratio."""
    invalid = {"status": "invalid_comparison", "ratio": None, "repeat_required": False}
    if (
        isinstance(scaling_variability, bool)
        or not isinstance(scaling_variability, (int, float))
        or not math.isfinite(scaling_variability)
        or scaling_variability < 0
    ):
        return invalid
    if baseline.get("variant") != "baseline" or candidate.get("variant") != "candidate":
        return invalid
    for run in (baseline, candidate):
        try:
            manifest = _validate_experiment(run.get("experiment"))
            if (
                not _observations_match(manifest, run["variant"], run.get("observed_pins"))
                or run.get("experiment_content_sha256") != _digest(manifest)
                or not re.fullmatch(r"[a-f0-9]{64}", run.get("experiment_sha256", ""))
            ):
                return invalid
            if not run.get("invocations") or any(
                not isinstance(i.get("report"), dict)
                or i["report"].get("runtime") != manifest["variants"][run["variant"]]["runtime"]
                for i in run["invocations"]
            ):
                return invalid
        except (ValueError, TypeError, KeyError, AttributeError):
            return invalid
        duration = run.get("wall_s")
        if (
            run.get("status") != "passed"
            or run.get("semantic_validated") is not True
            or run.get("limits_verified") is not True
            or isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(duration)
            or duration <= 0
        ):
            return invalid
    for pin in (
        "corpus_sha256",
        "delay_policy_sha256",
        "workload_pin",
        "limits",
        "experiment_sha256",
        "experiment",
        "experiment_content_sha256",
    ):
        if not baseline.get(pin) or baseline[pin] != candidate.get(pin):
            return invalid
    ratio = candidate["wall_s"] / baseline["wall_s"]
    if not math.isfinite(ratio):
        return invalid
    repeat = 0.45 <= ratio <= 0.55 or scaling_variability > 0.10
    return {
        "status": "repeat_required" if repeat else "passed" if ratio <= 0.50 else "target_missed",
        "ratio": ratio,
        "repeat_required": repeat,
        "target_met": ratio <= 0.50,
    }


def _write_output(path, result):
    path = _safe_path(path, exists=False)
    if any((parent / ".git").exists() for parent in (path.parent, *path.parents)):
        raise ValueError("benchmark output must be outside Git checkouts")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write((json.dumps(result, sort_keys=True, allow_nan=False) + "\n").encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("python", "root", "workspace", "corpus", "output", "experiment", "wheel"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--variant", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--lockfile", type=Path)
    args = parser.parse_args(argv)
    result = run_variant(
        args.python,
        args.root,
        args.workspace,
        args.corpus,
        experiment=args.experiment,
        variant=args.variant,
        wheel=args.wheel,
        lockfile=args.lockfile,
    )
    _write_output(args.output, result)
    return 0 if result["status"] == "passed" else 3


if __name__ == "__main__":
    raise SystemExit(main())
