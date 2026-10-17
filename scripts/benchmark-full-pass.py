#!/usr/bin/env python3
"""Offline full-pass characterization with frozen synthetic input.

Never contacts Gamma. Baseline mode creates a deterministic seed and saves the
runtime source used for comparison; --compare measures that runtime against the
current checkout. Smoke runs exercise the same path but never qualify a change.
"""

from __future__ import annotations

import argparse
import cProfile
import hashlib
import json
import math
import os
import pstats
import resource
import shutil
import signal
import stat
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))
from fakes.capacity_source import CapacitySource  # noqa: E402
from fakes.world import make_event, make_market  # noqa: E402
from oddsfox_catalogue.current_config import (  # noqa: E402
    CurrentSettings,
    connect_store,
    safe_path,
    validate_root,
)
from oddsfox_catalogue.current_publish import verify_current  # noqa: E402
from oddsfox_catalogue.current_rows import project_response  # noqa: E402
from oddsfox_catalogue.current_runtime import _publish  # noqa: E402
from oddsfox_catalogue.current_store import apply, initialize, set_state  # noqa: E402
from oddsfox_catalogue.signals import SIGNALS, Terminated  # noqa: E402

BASE_TIME = datetime(2026, 10, 14, tzinfo=UTC)
BUDGET_S = 600.0
SHUTDOWN_RESERVE_S = 15.0
PROFILES = {
    "smoke": (250, 1010),
    "perf": (10_000, 40_400),
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def normalize(value):
    """Remove only volatile paths, release IDs, capture IDs, and elapsed fields."""
    volatile = {
        "path",
        "release_id",
        "previous_release_id",
        "response_id",
        "raw_id",
        "capture_id",
        "elapsed_s",
        "elapsed_seconds",
        "worker_wall_s",
        "wall_s",
        "acquire_apply_s",
        "publish_verify_s",
        "publication_s",
    }
    if isinstance(value, dict):
        return {
            key: normalize(item)
            for key, item in sorted(value.items())
            if key.lower() not in volatile
        }
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def normalized_publication(manifest: dict) -> dict:
    """Compare semantic publication content, independent of Parquet encoding."""
    keys = (
        "contract",
        "projection_version",
        "generation",
        "counts",
        "coverage",
        "backlog",
        "full_pass",
        "relations",
    )
    result = normalize({key: manifest[key] for key in keys if key in manifest})
    for descriptor in result.get("relations", {}).values():
        if isinstance(descriptor, dict):
            descriptor.pop("sha256", None)
            descriptor.pop("bytes", None)
    return result


def canonical_hash(value) -> str:
    return hashlib.sha256(
        json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def comparison(baseline: list[dict], candidate: list[dict], profile: str) -> dict:
    if len(baseline) != 3 or len(candidate) != 3:
        return {"status": "insufficient_evidence", "reason": "three complete pairs are required"}
    if {row.get("pair") for row in baseline} != {1, 2, 3} or {
        row.get("pair") for row in candidate
    } != {1, 2, 3}:
        return {
            "status": "insufficient_evidence",
            "reason": "paired sample identities are incomplete",
        }
    if any(row["semantic_hash"] != baseline[0]["semantic_hash"] for row in baseline + candidate):
        return {"status": "invalid", "reason": "scheduler or publication semantics differ"}
    import statistics

    numeric = (
        "acquire_apply_s",
        "wall_s",
        "peak_rss_bytes",
        "http_attempts",
        "downloaded_bytes",
        "runtime_reserved_attempts",
        "runtime_downloaded_bytes",
    )
    require(
        all(
            type(row.get(key)) in (int, float) and math.isfinite(row[key]) and row[key] >= 0
            for row in baseline + candidate
            for key in numeric
        ),
        "sample metrics are invalid",
    )
    for row in baseline + candidate:
        for key in (
            "http_attempts",
            "downloaded_bytes",
            "runtime_reserved_attempts",
            "runtime_downloaded_bytes",
        ):
            require(type(row.get(key)) is int, f"sample counter is invalid: {key}")
    require(
        all(
            row["http_attempts"] <= row["runtime_reserved_attempts"]
            and row["downloaded_bytes"] >= row["runtime_downloaded_bytes"]
            for row in baseline + candidate
        ),
        "source and runtime HTTP accounting is inconsistent",
    )
    old = statistics.median(row["acquire_apply_s"] for row in baseline)
    new = statistics.median(row["acquire_apply_s"] for row in candidate)
    old_wall = statistics.median(row["wall_s"] for row in baseline)
    new_wall = statistics.median(row["wall_s"] for row in candidate)
    old_rss = statistics.median(row["peak_rss_bytes"] for row in baseline)
    new_rss = statistics.median(row["peak_rss_bytes"] for row in candidate)
    candidate_by_pair = {row["pair"]: row for row in candidate}
    saved = old - new
    qualifies = (
        profile == "perf"
        and saved >= 1
        and old > 0
        and saved / old >= 0.10
        and new_wall <= old_wall * 1.05
        and new_rss <= old_rss * 1.05
        and all(
            candidate_by_pair[row["pair"]]["http_attempts"] <= row["http_attempts"]
            and candidate_by_pair[row["pair"]]["downloaded_bytes"] <= row["downloaded_bytes"]
            and candidate_by_pair[row["pair"]]["runtime_reserved_attempts"]
            <= row["runtime_reserved_attempts"]
            and candidate_by_pair[row["pair"]]["runtime_downloaded_bytes"]
            <= row["runtime_downloaded_bytes"]
            for row in baseline
        )
    )
    return {
        "status": "qualified" if qualifies else "deferred",
        "baseline_median_acquire_apply_s": old,
        "candidate_median_acquire_apply_s": new,
        "saved_s": saved,
        "baseline_median_wall_s": old_wall,
        "candidate_median_wall_s": new_wall,
        "baseline_median_peak_rss_bytes": old_rss,
        "candidate_median_peak_rss_bytes": new_rss,
        "reason": "performance gate passed" if qualifies else "performance gate not met",
    }


def validate_source_accounting(
    measured: dict, expected_attempts: int, expected_bytes: int, failure: str | None = None
) -> tuple[int, int]:
    require(
        type(expected_attempts) is int
        and expected_attempts >= 0
        and type(expected_bytes) is int
        and expected_bytes >= 0,
        "runtime HTTP counters are invalid",
    )
    require(failure is None, f"synthetic source failed: {failure!r}")
    faults = measured.get("faults")
    require(
        type(faults) is int and faults == 0,
        f"synthetic source recorded invalid or injected faults: faults={faults!r}",
    )
    attempts = measured.get("http_attempts")
    downloaded = measured.get("downloaded_bytes")
    require(
        type(attempts) is int and 0 <= attempts <= expected_attempts,
        "measured fixture request attempts exceed runtime-reserved attempts: "
        f"source={attempts!r}, runtime_reserved={expected_attempts!r}",
    )
    require(
        type(downloaded) is int and downloaded >= expected_bytes,
        "runtime downloaded bytes exceed measured fixture response bodies: "
        f"source_body_bytes={downloaded!r}, runtime_consumed_bytes={expected_bytes!r}, "
        f"source_attempts={attempts!r}, runtime_attempts={expected_attempts!r}",
    )
    return attempts, downloaded


class Budget:
    def __init__(self, inherited=0.0):
        self.started = time.monotonic()
        self.inherited = inherited

    def add_inherited(self, seconds: float):
        self.inherited = validate_elapsed(seconds)

    @property
    def elapsed(self):
        return self.inherited + time.monotonic() - self.started

    @property
    def remaining(self):
        return max(0.0, BUDGET_S - self.elapsed - SHUTDOWN_RESERVE_S)

    def admit(self, estimated=0.0):
        if estimated > self.remaining:
            raise TimeoutError("insufficient_evidence: shared 600-second budget is exhausted")


def validate_elapsed(value) -> float:
    require(
        type(value) in (int, float) and math.isfinite(value) and 0 <= value <= BUDGET_S,
        "invalid baseline budget",
    )
    return float(value)


def validate_disjoint(output: Path, baseline: Path) -> None:
    require(
        output != baseline and baseline not in output.parents and output not in baseline.parents,
        "baseline and output must be disjoint",
    )


def validate_report_metrics(rows: object) -> None:
    require(
        isinstance(rows, list) and len(rows) == 3, "baseline must contain three complete samples"
    )
    required = (
        "acquire_apply_s",
        "publish_verify_s",
        "wall_s",
        "peak_rss_bytes",
        "http_attempts",
        "downloaded_bytes",
        "runtime_reserved_attempts",
        "runtime_downloaded_bytes",
    )
    for row in rows:
        require(
            isinstance(row, dict) and isinstance(row.get("semantic_hash"), str),
            "malformed baseline sample",
        )
        for key in required:
            value = row.get(key)
            require(
                type(value) in (int, float) and math.isfinite(value) and value >= 0,
                f"invalid baseline metric: {key}",
            )
        for key in (
            "http_attempts",
            "downloaded_bytes",
            "runtime_reserved_attempts",
            "runtime_downloaded_bytes",
        ):
            require(type(row[key]) is int, f"invalid baseline counter: {key}")
        require(
            row["http_attempts"] <= row["runtime_reserved_attempts"],
            "baseline source requests exceed runtime-reserved attempts",
        )
        require(
            row["downloaded_bytes"] >= row["runtime_downloaded_bytes"],
            "baseline runtime byte total exceeds source response bodies",
        )


def validate_semantic_samples(rows: list[dict]) -> str:
    require(len(rows) == 3, "baseline needs three complete semantic samples")
    hashes = [row.get("semantic_hash") for row in rows]
    require(
        isinstance(hashes[0], str) and all(value == hashes[0] for value in hashes),
        "baseline samples are semantically inconsistent",
    )
    return hashes[0]


def build_dataset(event_count: int, market_count: int):
    require(market_count >= event_count * 4, "profile needs four owned markets per event")
    events, markets = [], {}
    owned = event_count * 4
    urgent_count = max(1, event_count // 100)
    receipts = {
        "closed": "2026-10-13T23:00:00Z",
        "active": "2026-10-13T23:00:00Z",
        "urgent": "2026-10-13T01:00:00Z",
    }
    for n in range(event_count):
        eid = str(n + 1)
        closed = n < event_count * 9 // 10
        urgent = not closed and n < event_count * 9 // 10 + urgent_count
        members = []
        for offset in range(4):
            mid = str(n * 4 + offset + 1)
            market = make_market(
                mid, f"Synthetic market {mid}", closed=closed, seed=n * 4 + offset + 1
            )
            markets[mid] = market
            members.append(market)
        event = make_event(eid, f"Synthetic event {eid}", markets=members, closed=closed)
        cohort = "closed" if closed else "urgent" if urgent else "active"
        event["_benchmark_cohort"] = cohort
        event["_benchmark_receipt"] = receipts[cohort]
        events.append(event)
    for n in range(owned, market_count):
        mid = str(n + 1)
        markets[mid] = make_market(mid, f"Synthetic orphan {mid}", seed=n + 1)
    return (
        events,
        markets,
        {
            "events": event_count,
            "markets": market_count,
            "owned_markets": owned,
            "orphan_markets": market_count - owned,
            "closed_events": event_count * 9 // 10,
            "urgent_active_events": urgent_count,
            "ordinary_active_events": event_count - event_count * 9 // 10 - urgent_count,
            "membership_complete": True,
            "faults_enabled": False,
            "delay_s": 0,
        },
    )


def create_seed(root: Path, event_count: int, market_count: int) -> dict:
    events, markets, distribution = build_dataset(event_count, market_count)
    validate_root(root, initialize=True)
    settings = CurrentSettings(root)
    with connect_store(settings) as conn:
        initialize(conn)
        receipt_by_cohort = {
            "closed": "2026-10-13T23:00:00Z",
            "active": "2026-10-13T23:00:00Z",
            "urgent": "2026-10-13T01:00:00Z",
        }
        for cohort, receipt in receipt_by_cohort.items():
            selected = [event for event in events if event["_benchmark_cohort"] == cohort]
            for start in range(0, len(selected), 100):
                batch = selected[start : start + 100]
                payload = [
                    {
                        key: value
                        for key, value in event.items()
                        if not key.startswith("_benchmark_")
                    }
                    for event in batch
                ]
                apply(
                    conn,
                    project_response(
                        payload, "events", received_at=receipt, response_id=f"seed-{cohort}-{start}"
                    ),
                )
        owned_ids = {market["id"] for event in events for market in event["markets"]}
        orphan_ids = sorted(set(markets) - owned_ids, key=int)
        for start in range(0, len(orphan_ids), 100):
            payload = [markets[mid] for mid in orphan_ids[start : start + 100]]
            apply(
                conn,
                project_response(
                    payload,
                    "markets",
                    received_at="2026-10-13T23:00:00Z",
                    response_id=f"seed-orphans-{start}",
                ),
            )
        closed_count = conn.execute("SELECT count(*) FROM events WHERE closed IS TRUE").fetchone()[
            0
        ]
        urgent_count = conn.execute(
            "SELECT count(*) FROM _membership WHERE receipt < ?", [BASE_TIME - timedelta(hours=20)]
        ).fetchone()[0]
        require(
            closed_count == distribution["closed_events"]
            and urgent_count == distribution["urgent_active_events"],
            "synthetic cohort distribution does not match the stored seed",
        )
        _publish(settings, conn, deadline=time.monotonic() + 1200)
        set_state(conn, "last_publish_at", BASE_TIME.isoformat())
        conn.execute("CHECKPOINT")
        verification = verify_current(settings, connection=conn)
        require(
            verification["store_generation"] == verification["published_generation"],
            "seed publication is not current",
        )
    return distribution


def copy_runtime(target: Path) -> str:
    source = REPO / "src" / "oddsfox_catalogue"
    target.mkdir(parents=True)
    for path in sorted(source.rglob("*.py")):
        destination = target / path.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
    return canonical_hash(
        {str(path.relative_to(target)): digest(path) for path in sorted(target.rglob("*.py"))}
    )


def source_hashes(package: Path) -> dict[str, str]:
    return {str(path.relative_to(package)): digest(path) for path in sorted(package.rglob("*.py"))}


def harness_provenance() -> dict:
    files = [
        Path(__file__).resolve(),
        REPO / "tests" / "fakes" / "world.py",
        REPO / "tests" / "fakes" / "capacity_source.py",
    ]
    return {
        "git_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "source_hashes": {str(path.relative_to(REPO)): digest(path) for path in files},
    }


def validate_tree(root: Path, *, python_only=False) -> None:
    root = safe_path(root)
    require(root.is_dir() and not root.is_symlink(), "baseline tree must be a regular directory")
    for path in root.rglob("*"):
        mode = path.lstat().st_mode
        require(
            stat.S_ISDIR(mode) or stat.S_ISREG(mode), "baseline contains a symlink or special file"
        )
        if python_only and stat.S_ISREG(mode):
            require(path.suffix == ".py", "baseline runtime contains non-source files")


def copy_root(source: Path, target: Path) -> None:
    shutil.copytree(source, target, symlinks=False)


def child_environment(url: str, package_root: Path) -> dict[str, str]:
    env = base_environment()
    env.update(
        CATALOGUE_GAMMA_BASE_URL=url,
        CATALOGUE_GAMMA_ALLOW_LOOPBACK="1",
        PYTHONPATH=str(package_root.parent),
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
        FULL_PASS_EXPECTED_PACKAGE=str(package_root.resolve()),
    )
    return env


def base_environment() -> dict[str, str]:
    allowed = {"PATH", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL", "SYSTEMROOT"}
    return {key: value for key, value in os.environ.items() if key in allowed}


def command_result(log: Path) -> dict:
    data = log.read_text()
    for index in range(len(data)):
        if data[index] == "{":
            try:
                value = json.loads(data[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
    raise ValueError("benchmark worker returned no JSON result")


def worker_argv(
    seed: Path,
    root: Path,
    output: Path,
    profile_name: str,
    label: str,
    stop_after: int,
    profile: Path | None,
):
    argv = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--profile",
        profile_name,
        "--output",
        str(output),
        "--_sample",
        str(seed),
        str(root),
        label,
        str(stop_after),
    ]
    if profile:
        argv.extend(["--_profile", str(profile)])
    return argv


def prepare_argv(output: Path, profile: str) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--profile",
        profile,
        "--output",
        str(output),
        "--_prepare",
    ]


def validate_argv(output: Path, profile: str, baseline: Path, candidate_package: Path) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--profile",
        profile,
        "--output",
        str(output),
        "--_validate",
        str(baseline),
        str(candidate_package),
    ]


def supervised(
    command: list[str], output: Path, package_root: Path, budget: Budget, log: Path
) -> dict:
    budget.admit(1)
    timeout = min(1200, budget.remaining)
    if timeout <= 0:
        raise TimeoutError("insufficient_evidence: no worker budget remains")
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location(
        "benchmark_installed", REPO / "scripts" / "verify-installed-workflow.py"
    )
    installed = module_from_spec(spec)
    spec.loader.exec_module(installed)
    env = base_environment()
    env.update(
        PYTHONPATH=str(package_root.parent),
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
        FULL_PASS_EXPECTED_PACKAGE=str(package_root.resolve()),
    )
    try:
        installed.run_command(
            command, output, env, log, timeout=timeout, max_output_bytes=4 * 1024**2
        )
    except subprocess.CalledProcessError as exc:
        try:
            result = command_result(log)
        except (OSError, ValueError):
            raise
        raise ValueError(
            result.get("message", f"worker failed with status {exc.returncode}")
        ) from exc
    return command_result(log)


def supervised_prepare(command: list[str], output: Path, budget: Budget, log: Path) -> dict:
    budget.admit(1)
    timeout = budget.remaining
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location(
        "benchmark_installed_prepare", REPO / "scripts" / "verify-installed-workflow.py"
    )
    installed = module_from_spec(spec)
    spec.loader.exec_module(installed)
    env = base_environment()
    env.update(
        PYTHONPATH=os.pathsep.join((str(REPO / "src"), str(REPO / "tests"))),
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
    )
    try:
        installed.run_command(
            command, output, env, log, timeout=timeout, max_output_bytes=4 * 1024**2
        )
    except subprocess.CalledProcessError as exc:
        try:
            result = command_result(log)
        except (OSError, ValueError):
            raise
        raise ValueError(
            result.get("message", f"worker failed with status {exc.returncode}")
        ) from exc
    return command_result(log)


def validate_baseline(baseline: Path, profile: str, candidate_package: Path) -> dict:
    report_path = baseline / "report.json"
    require(
        report_path.is_file() and stat.S_ISREG(report_path.lstat().st_mode),
        "baseline report is missing or unsafe",
    )
    report = json.loads(report_path.read_text())
    elapsed = validate_elapsed(report.get("elapsed_seconds"))
    require(
        report.get("kind") == "synthetic-full-pass"
        and report.get("status") == "baseline_ready"
        and report.get("profile") == profile,
        "baseline is incomplete or profile differs",
    )
    validate_report_metrics(report.get("samples"))
    expected_distribution = build_dataset(*PROFILES[profile])[2]
    require(
        report.get("distribution") == expected_distribution,
        "baseline synthetic distribution is invalid",
    )
    require(
        all(
            row["semantic_hash"] == report["samples"][0]["semantic_hash"]
            for row in report["samples"]
        ),
        "baseline samples are semantically inconsistent",
    )
    seed = baseline / "seed"
    runtime = baseline / "runtime" / "oddsfox_catalogue"
    validate_tree(seed)
    validate_tree(runtime, python_only=True)
    source_hash_map = report.get("source_hashes")
    require(
        isinstance(source_hash_map, dict) and source_hashes(runtime) == source_hash_map,
        "baseline source snapshot is damaged",
    )
    runtime_hash = canonical_hash(source_hash_map)
    require(report.get("runtime_hash") == runtime_hash, "baseline runtime hash mismatch")
    validate_root(seed)
    seed_hash = digest(seed / "catalogue.duckdb")
    require(seed_hash == report.get("seed_sha256"), "baseline seed is damaged")
    verification = verify_current(CurrentSettings(seed))
    require(verification.get("valid") is True, "baseline seed publication is invalid")
    return {
        "status": "validated",
        "elapsed_seconds": elapsed,
        "distribution": expected_distribution,
        "source_hashes": source_hash_map,
        "runtime_hash": runtime_hash,
        "seed_sha256": seed_hash,
        "candidate_source_hashes": source_hashes(candidate_package),
        "provenance": harness_provenance(),
    }


def public_snapshot(root: Path) -> dict:
    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
        tables = {}
        for table in ("events", "markets", "event_markets", "outcomes"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            tables[table] = [dict(zip(columns, row, strict=True)) for row in rows]
        state = {
            key: json.loads(value)
            for key, value in conn.execute(
                "SELECT key,value FROM sync_state WHERE key IN ('full_pass','last_run') ORDER BY key"
            ).fetchall()
        }
        receipt_state = {
            "memberships": [
                dict(zip(("venue", "event_id", "receipt", "unresolved"), row, strict=True))
                for row in conn.execute(
                    "SELECT venue,event_id,receipt,unresolved FROM _membership ORDER BY venue,event_id"
                ).fetchall()
            ],
            "markets": [
                dict(zip(("venue", "id", "kind", "receipt"), row, strict=True))
                for row in conn.execute(
                    "SELECT venue,id,kind,receipt FROM _receipts ORDER BY venue,id,kind"
                ).fetchall()
            ],
        }
        publication = verify_current(CurrentSettings(root), connection=conn)
        full_pass = state.get("full_pass") or {}
        started = datetime.fromisoformat(full_pass["pass_started_at"])
        receipt_gaps = {
            "events": conn.execute(
                "SELECT count(*) FROM events e LEFT JOIN _membership m USING(venue,event_id) WHERE m.receipt IS NULL OR m.receipt<=? OR coalesce(m.unresolved,true)",
                [started],
            ).fetchone()[0],
            "markets": conn.execute(
                "SELECT count(*) FROM markets m LEFT JOIN _receipts r ON r.venue=m.venue AND r.id=m.market_id AND r.kind='market' WHERE r.receipt IS NULL OR r.receipt<=?",
                [started],
            ).fetchone()[0],
        }
        from oddsfox_catalogue.current_reconcile import full_pass_status

        completed = full_pass_status(conn, now=BASE_TIME + timedelta(seconds=1))
        verified = publication["store_generation"] == publication["published_generation"]
        require(
            completed["completed_at"] is not None and completed["remaining"] == 0,
            "full pass did not complete all known work",
        )
        require(
            not any(receipt_gaps.values()) and verified,
            "receipts or publication verification are incomplete",
        )
        return {
            "tables": tables,
            "state": state,
            "receipt_state": receipt_state,
            "counts": {name: len(rows) for name, rows in tables.items()},
            "receipt_gaps": receipt_gaps,
            "publication_verified": verified,
            "publication": normalized_publication(publication),
        }


def run_sample(
    seed: Path,
    output: Path,
    runtime_package: Path,
    budget: Budget,
    profile: str,
    label: str,
    warmup=False,
) -> dict:
    root = output / f"root-{label}"
    log = output / f"worker-{label}.log"
    stop_after = 1 if profile == "smoke" else 50
    command = worker_argv(
        seed,
        root,
        output,
        profile,
        label,
        stop_after,
        output / f"profile-{label}.prof" if warmup else None,
    )
    sample_started = time.monotonic()
    result = supervised(command, output, runtime_package, budget, log)
    supervised_wall_s = time.monotonic() - sample_started
    if result.get("status") == "blocked":
        raise ValueError(result.get("message", "sample worker failed"))
    require(
        Path(result.get("runtime_source", "")).resolve() == runtime_package.resolve(),
        "sample runtime provenance mismatch",
    )
    required = (
        "acquire_apply_s",
        "publish_verify_s",
        "wall_s",
        "peak_rss_bytes",
        "http_attempts",
        "downloaded_bytes",
        "runtime_reserved_attempts",
        "runtime_downloaded_bytes",
    )
    require(
        all(
            type(result.get(key)) in (int, float)
            and math.isfinite(result[key])
            and result[key] >= 0
            for key in required
        ),
        "sample metrics are invalid",
    )
    reported_worker_wall = result["wall_s"]
    source_stats = result.get("source_stats")
    require(isinstance(source_stats, dict), "sample omitted source accounting")
    validate_source_accounting(
        source_stats,
        result["runtime_reserved_attempts"],
        result["runtime_downloaded_bytes"],
        result.get("source_failure"),
    )
    result["worker_wall_s"] = reported_worker_wall
    result["wall_s"] = supervised_wall_s
    if warmup:
        profiles = [output / f"profile-{label}-part{part}.prof" for part in (0, 1)]
        result["profile_files"] = [str(path) for path in profiles]
        result["urgency_profile"] = profile_summary(profiles)
    return result


def profile_summary(paths: list[Path]) -> dict:
    selected = []
    for path in paths:
        stats = pstats.Stats(str(path))
        for (filename, _line, name), values in stats.stats.items():
            if "urgent" in name.lower() or "urgent" in filename.lower():
                selected.append(
                    {
                        "function": name,
                        "calls": values[1],
                        "total_s": values[2],
                        "cumulative_s": values[3],
                    }
                )
    selected.sort(key=lambda row: row["cumulative_s"], reverse=True)
    return {"urgent_functions": selected[:20]}


def worker(
    seed: Path,
    root: Path,
    output: Path,
    label: str,
    profile_name: str,
    stop_after: int,
    profile: Path | None,
) -> int:
    from oddsfox_catalogue import current_reconcile, current_runtime
    from oddsfox_catalogue.current_config import CurrentLimits, load_current_settings
    from oddsfox_catalogue.gamma.http import CaptureStopped
    from oddsfox_catalogue.runlock import run_lock

    started = time.monotonic()
    expected_package = Path(os.environ["FULL_PASS_EXPECTED_PACKAGE"]).resolve()
    actual_package = Path(__import__("oddsfox_catalogue").__file__).resolve().parent
    if actual_package != expected_package:
        raise ValueError("worker imported a different runtime than requested")
    copy_root(seed, root)
    trace = output / f"trace-{label}.jsonl"
    audit = output / f"source-{label}.jsonl"
    timings = {"acquire_apply_s": 0.0, "publish_verify_s": 0.0}
    http_attempts = downloaded_bytes = 0
    peak_rss_bytes = 0
    settled = []
    clock = {"now": BASE_TIME}
    original_fetch = current_reconcile.fetch_unit
    original_gamma = current_runtime.GammaClient
    original_now = current_reconcile._now
    original_datetime = current_runtime.datetime
    count = 0
    current_limit = [stop_after]

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"].astimezone(tz or UTC)

    def gamma(*args, **kwargs):
        kwargs["now"] = lambda: clock["now"] + timedelta(microseconds=1)
        return original_gamma(*args, **kwargs)

    def traced_fetch(*args, **kwargs):
        nonlocal count
        if current_limit[0] and count >= current_limit[0]:
            raise CaptureStopped()
        before = args[1].execute("SELECT value FROM sync_state WHERE key='full_pass'").fetchone()
        before_value = json.loads(before[0]) if before else {}
        result = original_fetch(*args, **kwargs)
        lane, ids = args[3], args[4]
        after = args[1].execute("SELECT value FROM sync_state WHERE key='full_pass'").fetchone()
        after_value = json.loads(after[0]) if after else {}
        record = {
            "cursor_before": before_value.get("cursor_after"),
            "cursor_after_fetch": after_value.get("cursor_after"),
            "lane": lane,
            "ids": [str(key) for key in ids],
        }
        settled.append(record)
        count += 1
        return result

    current_reconcile.fetch_unit = traced_fetch
    current_reconcile._now = lambda: clock["now"]
    current_runtime.GammaClient = gamma
    current_runtime.datetime = FrozenDateTime
    try:
        with CapacitySource(seed, audit, delay_s=0) as source:
            settings = load_current_settings(
                root, env=child_environment(source.url, expected_package)
            )
            require(
                settings.limits == CurrentLimits(), "benchmark settings changed production limits"
            )
            settings_summary = {
                "limits": asdict(settings.limits),
                "gamma": {**asdict(settings.gamma), "base_url": "http://127.0.0.1:<ephemeral>"},
            }
            controls = source.controls
            probe = {
                f"{kind[:-1]}_ids": {
                    status: [controls[f"{status}_{kind[:-1]}_id"]]
                    for status in ("open", "closed", "absent")
                }
                for kind in ("events", "markets")
            }
            setup_started = time.monotonic()
            with run_lock(settings.run_lock_path):
                probe_result = current_runtime.execute(
                    settings, "probe", probe, time.monotonic() + settings.limits.max_duration_s
                )
            setup_wall_s = time.monotonic() - setup_started
            require(
                all(
                    lane.get("supported") is True for lane in probe_result.get("lanes", {}).values()
                ),
                "synthetic provider probe did not certify both lanes",
            )
            setup_http_attempts = probe_result.get("http_attempts", 0)
            setup_downloaded_bytes = probe_result.get("downloaded_bytes", 0)
            profiler = None
            for step, limit in ((0, stop_after), (1, 0)):
                clock["now"] = BASE_TIME + timedelta(seconds=step)
                count = 0
                current_limit[0] = limit
                profiler = cProfile.Profile() if profile else None
                if profiler:
                    profiler.enable()
                with run_lock(settings.run_lock_path):
                    result = current_runtime.execute(
                        settings,
                        "reconcile",
                        {"full_pass": True},
                        time.monotonic() + settings.limits.max_duration_s,
                    )
                if profiler:
                    profiler.disable()
                timings["acquire_apply_s"] += result.get("timings", {}).get("acquire_apply_s", 0)
                timings["publish_verify_s"] += result.get("timings", {}).get("publish_verify_s", 0)
                http_attempts += result.get("http_attempts", 0)
                downloaded_bytes += result.get("downloaded_bytes", 0)
                peak_rss_bytes = max(peak_rss_bytes, result.get("peak_rss_bytes", 0))
                if step == 0:
                    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
                        state = json.loads(
                            conn.execute(
                                "SELECT value FROM sync_state WHERE key='full_pass'"
                            ).fetchone()[0]
                        )
                        require(
                            state.get("completed_at") is None and count == stop_after,
                            "deterministic interruption did not settle exactly the requested units",
                        )
                if profiler:
                    profiler.dump_stats(
                        profile.with_name(f"{profile.stem}-part{step}{profile.suffix}")
                    )
            stats = source.stats
            measured_attempts, measured_bytes = validate_source_accounting(
                stats,
                http_attempts + setup_http_attempts,
                downloaded_bytes + setup_downloaded_bytes,
                source.failure,
            )
        with trace.open("w") as stream:
            for item in settled:
                stream.write(json.dumps(item, sort_keys=True) + "\n")
        snapshot = public_snapshot(root)
        expected_counts = {
            "events": PROFILES[profile_name][0],
            "markets": PROFILES[profile_name][1],
            "event_markets": PROFILES[profile_name][0] * 4,
            "outcomes": PROFILES[profile_name][1] * 2,
        }
        require(
            snapshot["counts"] == expected_counts,
            "final entity counts differ from the synthetic oracle",
        )
        semantic = {
            "tables": snapshot["tables"],
            "receipt_state": snapshot["receipt_state"],
            "full_pass": snapshot["state"].get("full_pass"),
            "settled_requests": settled,
            "publication_verified": snapshot["publication_verified"],
            "publication": snapshot["publication"],
        }
        print(
            json.dumps(
                {
                    **timings,
                    "wall_s": time.monotonic() - started,
                    "http_attempts": measured_attempts,
                    "downloaded_bytes": measured_bytes,
                    "runtime_reserved_attempts": http_attempts + setup_http_attempts,
                    "runtime_downloaded_bytes": downloaded_bytes + setup_downloaded_bytes,
                    "setup_wall_s": setup_wall_s,
                    "setup_http_attempts": setup_http_attempts,
                    "setup_downloaded_bytes": setup_downloaded_bytes,
                    "peak_rss_bytes": max(
                        peak_rss_bytes,
                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                        * (1 if sys.platform == "darwin" else 1024),
                    ),
                    "semantic_hash": canonical_hash(semantic),
                    "cursor": snapshot["state"].get("full_pass", {}).get("cursor_after"),
                    "settled_units": len(settled),
                    "settled_requests": settled,
                    "source_stats": stats,
                    "source_failure": source.failure,
                    "counts": snapshot["counts"],
                    "publication_verified": snapshot["publication_verified"],
                    "runtime_source": str(actual_package),
                    "settings": settings_summary,
                },
                default=str,
            )
        )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
        return 3
    finally:
        current_reconcile.fetch_unit = original_fetch
        current_reconcile._now = original_now
        current_runtime.GammaClient = original_gamma
        current_runtime.datetime = original_datetime


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", choices=PROFILES, required=True)
    p.add_argument("--output", required=True, type=Path, help="fresh evidence directory")
    p.add_argument("--compare", type=Path, help="baseline output directory")
    p.add_argument(
        "--_sample", nargs=4, metavar=("SEED", "ROOT", "LABEL", "STOP"), help=argparse.SUPPRESS
    )
    p.add_argument("--_prepare", action="store_true", help=argparse.SUPPRESS)
    p.add_argument(
        "--_validate", nargs=2, metavar=("BASELINE", "CANDIDATE_PACKAGE"), help=argparse.SUPPRESS
    )
    p.add_argument("--_profile", type=Path, help=argparse.SUPPRESS)
    return p


def _save(path: Path, report: dict):
    temp = path.with_suffix(".pending")
    temp.write_text(json.dumps(report, indent=2, default=str) + "\n")
    temp.replace(path)


def run(args) -> int:
    if args._sample:
        seed, root, label, stop = args._sample
        return worker(
            Path(seed), Path(root), Path(args.output), label, args.profile, int(stop), args._profile
        )
    if args._prepare:
        try:
            events, markets = PROFILES[args.profile]
            seed = Path(args.output) / "seed"
            distribution = create_seed(seed, events, markets)
            runtime = Path(args.output) / "runtime" / "oddsfox_catalogue"
            runtime.parent.mkdir(parents=True, exist_ok=True)
            runtime_hash = copy_runtime(runtime)
            print(
                json.dumps(
                    {
                        "status": "prepared",
                        "distribution": distribution,
                        "source_hashes": source_hashes(runtime),
                        "runtime_hash": runtime_hash,
                        "seed_sha256": digest(seed / "catalogue.duckdb"),
                        "provenance": harness_provenance(),
                    }
                )
            )
            return 0
        except Exception as exc:
            print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
            return 3
    if args._validate:
        try:
            baseline, candidate = map(Path, args._validate)
            result = validate_baseline(baseline, args.profile, candidate)
            print(json.dumps(result))
            return 0
        except Exception as exc:
            print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
            return 3
    budget = Budget()
    output = safe_path(args.output)
    baseline_dir = safe_path(args.compare) if args.compare else None
    if baseline_dir:
        require(
            not baseline_dir.is_symlink() and baseline_dir.is_dir(),
            "baseline must be a regular directory",
        )
        validate_disjoint(output, baseline_dir)
    require(not output.exists(), "output must be a fresh directory")
    output.mkdir(parents=True)
    report = {
        "kind": "synthetic-full-pass",
        "profile": args.profile,
        "status": "failed",
        "started_at": datetime.now(UTC).isoformat(),
        "budget_seconds": BUDGET_S,
        "shutdown_reserve_seconds": SHUTDOWN_RESERVE_S,
        "samples": [],
        "hardware": {
            "platform": sys.platform,
            "machine": os.uname().machine if hasattr(os, "uname") else None,
            "cpu_count": os.cpu_count(),
        },
        "accounting": {
            "attempts": "CapacitySource served requests are scored; runtime-reserved attempts are also recorded because cancellation can occur before serving",
            "source_body_bytes": "CapacitySource complete response-body bytes offered are scored; this is conservative and may exceed wire-consumed bytes when a prefetched response is cancelled",
            "runtime_consumed_bytes": "GammaClient bytes consumed; scored separately so neither metric may increase",
        },
    }
    _save(output / "report.json", report)
    try:
        runtime_package = REPO / "src" / "oddsfox_catalogue"
        if baseline_dir:
            baseline_report_path = baseline_dir / "report.json"
            require(
                baseline_report_path.is_file()
                and stat.S_ISREG(baseline_report_path.lstat().st_mode),
                "baseline report is missing or unsafe",
            )
            require(
                baseline_report_path.stat().st_size <= 4 * 1024**2, "baseline report is too large"
            )
            untrusted_report = json.loads(baseline_report_path.read_text())
            budget.add_inherited(validate_elapsed(untrusted_report.get("elapsed_seconds")))
            prepared = supervised_prepare(
                validate_argv(output, args.profile, baseline_dir, runtime_package),
                output,
                budget,
                output / "validate.log",
            )
            require(
                prepared.get("status") == "validated",
                prepared.get("message", "baseline validation failed"),
            )
            seed = baseline_dir / "seed"
            baseline_runtime = baseline_dir / "runtime" / "oddsfox_catalogue"
            distribution = prepared["distribution"]
            runtime_hash = prepared["runtime_hash"]
            report["baseline_source_hashes"] = prepared["source_hashes"]
            report["candidate_source_hashes"] = prepared["candidate_source_hashes"]
            report["provenance"] = prepared.get("provenance")
        else:
            prepared = supervised_prepare(
                prepare_argv(output, args.profile), output, budget, output / "prepare.log"
            )
            require(
                prepared.get("status") == "prepared",
                prepared.get("message", "seed preparation failed"),
            )
            seed = output / "seed"
            baseline_runtime = output / "runtime" / "oddsfox_catalogue"
            distribution = prepared["distribution"]
            runtime_hash = prepared["runtime_hash"]
            report["source_hashes"] = prepared["source_hashes"]
            report["candidate_source_hashes"] = report["source_hashes"]
            report["provenance"] = prepared.get("provenance")
        report.update(
            distribution=distribution,
            runtime_hash=runtime_hash,
            seed_sha256=prepared["seed_sha256"] if baseline_dir else prepared["seed_sha256"],
        )
        _save(output / "report.json", report)

        events, markets = PROFILES[args.profile]
        report["status"] = "running"
        _save(output / "report.json", report)

        # Each run is a fresh copy and each operation is a fresh supervised process.
        if baseline_dir:
            order = [
                ("baseline", baseline_runtime),
                ("candidate", runtime_package),
                ("candidate", runtime_package),
                ("baseline", baseline_runtime),
                ("baseline", baseline_runtime),
                ("candidate", runtime_package),
            ]
            baseline_rows, candidate_rows = [], []
            # Profile only warm-up workers; scored runs have no profiler attached.
            for label, runtime in (("baseline", baseline_runtime), ("candidate", runtime_package)):
                warm_dir = output / f"warm-{label}"
                warm_dir.mkdir()
                sample = run_sample(
                    seed, warm_dir, runtime, budget, args.profile, f"warm-{label}", warmup=True
                )
                report.setdefault("warmups", {})[label] = sample
            for index, (kind, runtime) in enumerate(order, 1):
                row = run_sample(seed, output, runtime, budget, args.profile, f"{index:02d}-{kind}")
                row.update(sample=index, pair=(index + 1) // 2, kind=kind)
                report["samples"].append(row)
                (baseline_rows if kind == "baseline" else candidate_rows).append(row)
                report["comparison"] = comparison(baseline_rows, candidate_rows, args.profile)
                report["status"] = (
                    "insufficient_evidence"
                    if report["comparison"]["status"] == "insufficient_evidence"
                    else "running"
                )
                _save(output / "report.json", report)
            report["comparison"] = comparison(baseline_rows, candidate_rows, args.profile)
            report["status"] = report["comparison"]["status"]
        else:
            warm_dir = output / "warm-baseline"
            warm_dir.mkdir()
            report["warmup"] = run_sample(
                seed, warm_dir, baseline_runtime, budget, args.profile, "warm-baseline", warmup=True
            )
            for index in range(1, 4):
                report["samples"].append(
                    run_sample(
                        seed, output, baseline_runtime, budget, args.profile, f"baseline-{index}"
                    )
                )
            validate_semantic_samples(report["samples"])
            report["status"] = "baseline_ready"
        report["elapsed_seconds"] = budget.elapsed
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        report.update(
            status="insufficient_evidence", error=str(exc), elapsed_seconds=budget.elapsed
        )
    except (KeyboardInterrupt, Terminated) as exc:
        report.update(
            status="partial",
            error="interrupted; retained evidence",
            interrupted_exit_code=128 + exc.signum if isinstance(exc, Terminated) else 130,
            elapsed_seconds=budget.elapsed,
        )
    except Exception as exc:
        report.update(
            status="failed", error=f"{type(exc).__name__}: {exc}", elapsed_seconds=budget.elapsed
        )
    finally:
        report["elapsed_seconds"] = budget.elapsed
        _save(output / "report.json", report)
    return report.get(
        "interrupted_exit_code",
        0
        if report["status"] in {"baseline_ready", "qualified", "deferred", "insufficient_evidence"}
        else 1,
    )


def main() -> int:
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
