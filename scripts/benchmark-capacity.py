#!/usr/bin/env python3
"""Finite installed-runtime characterization; never claims sustained acceptance."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import shutil
import signal
import sys
import time
from dataclasses import asdict
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))
from fakes.capacity_source import MAX_AUDIT_BYTES, CapacitySource  # noqa: E402
from oddsfox_catalogue.current_config import (  # noqa: E402
    CurrentLimits,
    load_current_settings,
    safe_path,
    validate_root,
)
from oddsfox_catalogue.signals import SIGNALS, Terminated  # noqa: E402


def harness(name):
    spec = importlib.util.spec_from_file_location(
        name.replace("-", "_"), REPO / "scripts" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


installed = harness("verify-installed-workflow")
accounting = harness("benchmark-current")
ANCILLARY_BYTES = 32 * 1024**2
REPORT_BYTES = 4 * 1024**2


def child_environment(url):
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("CATALOGUE_", "PYTHON")) and k != "VIRTUAL_ENV"
    }
    env.update(
        CATALOGUE_GAMMA_BASE_URL=url,
        CATALOGUE_GAMMA_ALLOW_LOOPBACK="1",
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
    )
    return env


def command_result(path):
    text = path.read_text()
    offset = 0
    for line in text.splitlines(keepends=True):
        if line.startswith("{"):
            try:
                value = json.loads(text[offset:])
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(value, dict):
                    return value
        offset += len(line)
    raise ValueError("canonical command did not return a final JSON object")


def disjoint(left, right):
    return left != right and left not in right.parents and right not in left.parents


def paths(args):
    seed, root, output = (safe_path(p) for p in (args.seed_root, args.root, args.output))
    accounts = [safe_path(p) for p in args.account_root]
    if not accounts or any(not p.is_dir() for p in accounts):
        raise ValueError("existing frozen accounting roots are required")
    if not disjoint(seed, root) or any(not disjoint(output, p) for p in [seed, root, *accounts]):
        raise ValueError("source, candidate and fresh output must be disjoint")
    if output.exists():
        raise ValueError("output must be fresh")
    validate_root(seed)
    validate_root(root)
    if not args.executable.is_file() or not os.access(args.executable, os.X_OK):
        raise ValueError("installed executable required")
    return seed, root, output, accounting.accounting_roots([seed, root, output, *accounts])


def capacity(roots, root, output):
    limits = CurrentLimits()
    usage = {str(p): accounting.aggregate_usage(p) for p in roots}
    total = sum(v["logical_owned_bytes"] for v in usage.values())
    candidate = accounting.aggregate_usage(root)["logical_owned_bytes"]
    # Reserve the entire remaining runtime allowance, plus bounded source audit.
    reserved = max(0, limits.max_retained_bytes - candidate) + MAX_AUDIT_BYTES + ANCILLARY_BYTES
    if candidate > limits.max_retained_bytes or total + reserved > 256 * 1024**3:
        raise ValueError("combined retained capacity exhausted")
    if shutil.disk_usage(output).free < limits.free_reserve_bytes + reserved:
        raise ValueError("free disk reserve unavailable")
    return {"roots": usage, "total_bytes": total, "reserved_bytes": reserved}


def log_budget(output):
    used = sum(p.stat().st_size for p in output.glob("*.log"))
    remaining = ANCILLARY_BYTES - 2 * REPORT_BYTES - used
    if remaining <= 0:
        raise ValueError("child log budget exhausted")
    return remaining


def snapshot(seed, root, temporary):
    """Independent full-cohort receipt comparison; never substitutes attempts for renewal."""
    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET threads=2")
        conn.execute("SET temp_directory=?", [str(temporary)])
        conn.execute("SET max_temp_directory_size='0B'")
        escaped = str(seed / "catalogue.duckdb").replace("'", "''")
        conn.execute(f"ATTACH '{escaped}' AS oracle (READ_ONLY)")
        result = {}
        for cold, label in ((False, "active"), (True, "cold")):
            where = f"(s.closed IS TRUE)={str(cold).lower()}"
            for kind, key, receipts, join, seed_join, valid in (
                (
                    "events",
                    "event_id",
                    "_membership",
                    "r.venue=s.venue AND r.event_id=s.event_id",
                    "b.venue=s.venue AND b.event_id=s.event_id",
                    "r.receipt IS NOT NULL AND NOT coalesce(r.unresolved,true)",
                ),
                (
                    "markets",
                    "market_id",
                    "_receipts",
                    "r.venue=s.venue AND r.id=s.market_id AND r.kind='market'",
                    "b.venue=s.venue AND b.id=s.market_id AND b.kind='market'",
                    "r.receipt IS NOT NULL",
                ),
            ):
                del key
                if kind == "markets" and not cold:
                    continue
                cohort_where = "true" if kind == "markets" else where
                renewed = f"{valid} AND (b.receipt IS NULL OR r.receipt>b.receipt)"
                row = conn.execute(f"""SELECT count(*), count(*) FILTER(WHERE {renewed}),
                    count(*) FILTER(WHERE NOT ({valid})), min(r.receipt),
                    count(*) FILTER(WHERE NOT ({valid}) OR r.receipt<=current_timestamp-INTERVAL '{20 if not cold else 58 * 24} hours'),
                    count(*) FILTER(WHERE NOT ({valid}) OR r.receipt<=current_timestamp-INTERVAL '{24 if not cold else 60 * 24} hours')
                    FROM oracle.{kind} s LEFT JOIN {receipts} r ON {join}
                    LEFT JOIN oracle.{receipts} b ON {seed_join} WHERE {cohort_where}""").fetchone()
                result[f"{label}_{kind}"] = dict(
                    zip(
                        ("cohort", "renewed", "missing_or_unresolved", "oldest", "due", "overdue"),
                        row,
                        strict=True,
                    )
                )
                result[f"{label}_{kind}"]["pending_unrenewed"] = row[0] - row[1]
        result["counts"] = {
            table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("events", "markets", "event_markets", "outcomes")
        }
        result["state"] = {
            key: json.loads(value) if isinstance(value, str) else value
            for key, value in conn.execute(
                "SELECT key,value FROM sync_state WHERE key IN ('generation','reconcile')"
            ).fetchall()
        }
        return result


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("seed-root", "root", "output", "executable"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--account-root", type=Path, action="append", required=True)
    p.add_argument("--invocations", type=int, default=3)
    p.add_argument("--cadence-seconds", type=float, default=3600)
    p.add_argument("--backup-every", type=int, default=0)
    p.add_argument(
        "--max-total-seconds",
        type=float,
        default=48 * 3600,
        help="Elapsed budget for admitting child commands; SQL/accounting/cleanup may exceed it",
    )
    p.add_argument("--characterize", action="store_true")
    return p


def run(args):
    if not 1 <= args.invocations <= 1000 or not 0 <= args.backup_every <= 1000:
        raise ValueError("invalid finite invocation bounds")
    if not math.isfinite(args.max_total_seconds) or not 1 <= args.max_total_seconds <= 48 * 3600:
        raise ValueError("invalid wall-clock cap")
    if not math.isfinite(args.cadence_seconds) or args.cadence_seconds < 0:
        raise ValueError("invalid cadence")
    if not args.characterize:
        raise ValueError(
            "true scheduler cadence is not implemented; select --characterize explicitly"
        )
    if args.backup_every:
        raise ValueError(
            "backup requires an explicitly accounted external verification temporary root"
        )
    seed, root, output, roots = paths(args)
    args.executable = safe_path(args.executable)
    output.mkdir(parents=True, mode=0o700)
    temporary = output / "sql-temp"
    temporary.mkdir(mode=0o700)
    started = time.monotonic()
    report = {
        "status": "partial",
        "sustained_acceptance": False,
        "mode": "characterization",
        "steps": [],
        "limits": asdict(CurrentLimits()),
        "arguments": vars(args),
        "capacity": [],
    }

    def save():
        report["elapsed_seconds"] = time.monotonic() - started
        encoded = (json.dumps(report, indent=2, default=str) + "\n").encode()
        if len(encoded) > REPORT_BYTES:
            raise ValueError("bounded report serialization exhausted; prior report retained")
        ancillary = accounting.aggregate_usage(output)["logical_owned_bytes"]
        audit = output / "source.jsonl"
        ancillary -= audit.stat().st_size if audit.exists() else 0
        if ancillary + len(encoded) > ANCILLARY_BYTES:
            raise ValueError("ancillary evidence capacity exhausted; prior report retained")
        pending = output / "report.pending"
        pending.write_bytes(encoded)
        pending.replace(output / "report.json")

    def call(name, arguments, source, timeout=1200):
        report["capacity"].append(capacity(roots, root, output))
        if time.monotonic() - started + timeout > args.max_total_seconds:
            raise TimeoutError("insufficient remaining wall-clock budget for canonical command")
        step = {
            "name": name,
            "argv": [str(args.executable), "--root", str(root), *arguments],
            "timeout_seconds": timeout,
            "log": str(output / f"{len(report['steps']):04d}-{name}.log"),
        }
        report["steps"].append(step)
        save()
        before = time.monotonic()
        try:
            installed.run_command(
                step["argv"],
                output,
                child_environment(source.url),
                Path(step["log"]),
                timeout=timeout,
                max_output_bytes=log_budget(output),
            )
            value = command_result(Path(step["log"]))
            step["result"] = value
            if getattr(source, "failure", None):
                raise ValueError(f"source failed: {source.failure}")
        finally:
            step["elapsed_seconds"] = time.monotonic() - before
            save()

    try:
        report["capacity"].append(capacity(roots, root, output))
        settings = load_current_settings(root, env=child_environment("http://127.0.0.1:9"))
        if settings.limits != CurrentLimits():
            raise ValueError("candidate limits must equal production defaults")
        report["settings"] = {"limits": asdict(settings.limits), "gamma": asdict(settings.gamma)}
        report["initial"] = snapshot(seed, root, temporary)
        with CapacitySource(seed, output / "source.jsonl") as source:
            try:
                report["source_controls"] = dict(source.controls)
                report["source_origin"] = source.url
                probe = ["probe"]
                for key, value in source.controls.items():
                    probe.extend(["--" + key.replace("_", "-"), value])
                call("probe", probe, source)
                source.enable_faults()
                call("refresh", ["refresh"], source)
                for _index in range(args.invocations):
                    call("reconcile", ["reconcile"], source)
                    report["latest"] = snapshot(seed, root, temporary)
                call("verify", ["verify"], source)
                report["characterization_complete"] = True
            finally:
                report["source"] = source.stats
        report["final"] = snapshot(seed, root, temporary)
    except TimeoutError as exc:
        report["error"] = str(exc)
    except (KeyboardInterrupt, Terminated) as exc:
        report.update(status="partial", error="interrupted; retained evidence")
        report["interrupted_exit_code"] = 128 + exc.signum if isinstance(exc, Terminated) else 130
    except Exception as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}")
    finally:
        save()
    return report.get("interrupted_exit_code", 1 if report["status"] == "failed" else 0)


def main():
    previous = {
        signum: signal.signal(signum, lambda signum, _frame: SIGNALS.receive(signum))
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }
    try:
        return run(parser().parse_args())
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
