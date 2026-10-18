#!/usr/bin/env python3
"""Offline full-pass characterization with frozen synthetic input.

Never contacts Gamma. Baseline mode creates a deterministic seed and saves the
runtime source used for comparison; --compare measures that runtime against the
current checkout. Smoke runs exercise the same path but never qualify a change.
"""

from __future__ import annotations

import argparse
import contextlib
import cProfile
import hashlib
import inspect
import json
import math
import os
import pstats
import re
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
_expected_package = os.environ.get("FULL_PASS_EXPECTED_PACKAGE")
if _expected_package:
    sys.path.insert(0, str(Path(_expected_package).resolve().parent))
else:
    sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tests"))
import oddsfox_catalogue  # noqa: E402
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
from oddsfox_catalogue.platform_support import (  # noqa: E402
    is_link_or_reparse,
    operator_signals,
)
from oddsfox_catalogue.platform_support import (  # noqa: E402
    peak_rss_bytes as process_peak_rss_bytes,
)
from oddsfox_catalogue.signals import SIGNALS, Terminated  # noqa: E402

BASE_TIME = datetime(2026, 10, 14, tzinfo=UTC)
BUDGET_S = 600.0
SHUTDOWN_RESERVE_S = 15.0
MEASUREMENT_VERSION = 2
PROFILES = {
    "smoke": (250, 1010),
    "perf": (10_000, 40_400),
}
API_WORKLOADS = {
    "closed_any": {"primary": True, "kind": "market-id-lookup"},
    "discovery_dense": {"primary": False, "kind": "keyset-discovery"},
    "discovery_sparse": {"primary": True, "kind": "keyset-discovery"},
    "discovery_tail": {"primary": False, "kind": "incremental-tail"},
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
        "request_elapsed_s",
        "duration_s",
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
    versions = {measurement_version(row) for row in baseline + candidate}
    if len(versions) != 1:
        return {
            "status": "invalid",
            "reason": "measurement versions differ; create a fresh baseline with the current harness",
        }
    if len({row.get("platform") for row in baseline + candidate}) != 1:
        return {
            "status": "invalid",
            "reason": "benchmark platforms differ; create a fresh baseline on the candidate platform",
        }
    version = versions.pop()
    if any(row["semantic_hash"] != baseline[0]["semantic_hash"] for row in baseline + candidate):
        return {"status": "invalid", "reason": "scheduler or publication semantics differ"}
    if any(row.get("cleanup_deferred") for row in baseline + candidate):
        return {
            "status": "deferred",
            "reason": "publication cleanup was deferred; equal completed maintenance is required",
        }
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
    if version >= 2:
        numeric = (*numeric, "worker_peak_before_oracle_bytes")
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
    old_preoracle_rss = (
        statistics.median(row["worker_peak_before_oracle_bytes"] for row in baseline)
        if version >= 2
        else old_rss
    )
    new_preoracle_rss = (
        statistics.median(row["worker_peak_before_oracle_bytes"] for row in candidate)
        if version >= 2
        else new_rss
    )
    candidate_by_pair = {row["pair"]: row for row in candidate}
    saved = old - new
    qualifies = (
        profile == "perf"
        and saved >= 1
        and old > 0
        and saved / old >= 0.10
        and new_wall <= old_wall * 1.05
        and new_rss <= old_rss * 1.05
        and new_preoracle_rss <= old_preoracle_rss * 1.05
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
        "baseline_median_worker_peak_before_oracle_bytes": old_preoracle_rss,
        "candidate_median_worker_peak_before_oracle_bytes": new_preoracle_rss,
        "reason": "performance gate passed" if qualifies else "performance gate not met",
    }


def api_comparison(baseline: list[dict], candidate: list[dict], workload: str) -> dict:
    """Apply workload-specific API gates without weakening full-pass comparison."""
    require(workload in API_WORKLOADS, "unknown API workload")
    if len(baseline) != 3 or len(candidate) != 3:
        return {"status": "insufficient_evidence", "reason": "three complete pairs are required"}
    expected_pairs = {1, 2, 3}
    if {row.get("pair") for row in baseline} != expected_pairs or {
        row.get("pair") for row in candidate
    } != expected_pairs:
        return {
            "status": "insufficient_evidence",
            "reason": "paired sample identities are incomplete",
        }
    versions = {measurement_version(row) for row in baseline + candidate}
    if len(versions) != 1:
        return {
            "status": "invalid",
            "reason": "measurement versions differ; create a fresh baseline with the current harness",
        }
    if len({row.get("platform") for row in baseline + candidate}) != 1:
        return {
            "status": "invalid",
            "reason": "benchmark platforms differ; create a fresh baseline on the candidate platform",
        }
    require(
        len({row.get("oracle_fingerprint") for row in baseline + candidate}) == 1,
        "API workload samples have inconsistent oracle fingerprints",
    )
    normalized_work = [
        {
            key: value
            for key, value in row.get("completed_work", {}).items()
            if key not in {"settled_units", "request_or_page_count"}
        }
        for row in baseline + candidate
    ]
    require(
        len({canonical_hash(item) for item in normalized_work}) == 1,
        "API workload samples have inconsistent completed work",
    )
    pairs = {row["pair"]: row for row in baseline}
    candidates = {row["pair"]: row for row in candidate}
    for old in baseline:
        new = candidates[old["pair"]]
        require(
            old.get("workload") == workload and new.get("workload") == workload,
            "API sample workload differs from comparison",
        )
        require(
            old.get("oracle_pass") is True and new.get("oracle_pass") is True,
            "API workload oracle failed",
        )
        require(
            isinstance(old.get("oracle_fingerprint"), str)
            and bool(old["oracle_fingerprint"])
            and old["oracle_fingerprint"] == new.get("oracle_fingerprint"),
            "API workload oracle fingerprints differ",
        )
        old_work = old.get("completed_work")
        new_work = new.get("completed_work")
        require(
            isinstance(old_work, dict)
            and bool(old_work)
            and isinstance(new_work, dict)
            and old_work.get("pending") == 0
            and new_work.get("pending") == 0,
            "API workload has incomplete or unreported work",
        )
        if (
            old.get("cleanup_deferred")
            or new.get("cleanup_deferred")
            or old.get("cleanup_warnings")
            or new.get("cleanup_warnings")
        ):
            return {
                "status": "deferred",
                "reason": "API workload deferred publication maintenance",
            }
        diagnostic_keys = {"settled_units", "request_or_page_count"}
        require(
            {key: value for key, value in old_work.items() if key not in diagnostic_keys}
            == {key: value for key, value in new_work.items() if key not in diagnostic_keys},
            "API workload completed work differs",
        )
        minimum_settled = 0 if workload == "discovery_tail" else 1
        require(
            all(
                type(row.get("settled_units")) is int and row["settled_units"] >= minimum_settled
                for row in (old, new)
            ),
            "API workload settled-unit diagnostic is missing",
        )
        for row in (old, new):
            for key in (
                "runtime_sequence_wall_s",
                "wall_s",
                "peak_rss_bytes",
                "worker_peak_before_oracle_bytes",
                "http_attempts",
                "downloaded_bytes",
                "runtime_reserved_attempts",
                "runtime_downloaded_bytes",
            ):
                require(
                    type(row.get(key)) in (int, float)
                    and math.isfinite(row[key])
                    and row[key] >= 0,
                    f"API sample metric is invalid: {key}",
                )
            for key in (
                "http_attempts",
                "downloaded_bytes",
                "runtime_reserved_attempts",
                "runtime_downloaded_bytes",
            ):
                require(type(row[key]) is int, f"API sample counter is invalid: {key}")
            require(
                row["http_attempts"] <= row["runtime_reserved_attempts"]
                and row["downloaded_bytes"] >= row["runtime_downloaded_bytes"],
                "API source and runtime HTTP accounting is inconsistent",
            )
    import statistics

    old_runtime = statistics.median(row["runtime_sequence_wall_s"] for row in baseline)
    new_runtime = statistics.median(row["runtime_sequence_wall_s"] for row in candidate)
    old_wall = statistics.median(row["wall_s"] for row in baseline)
    new_wall = statistics.median(row["wall_s"] for row in candidate)
    old_rss = statistics.median(row["peak_rss_bytes"] for row in baseline)
    new_rss = statistics.median(row["peak_rss_bytes"] for row in candidate)
    old_preoracle = statistics.median(row["worker_peak_before_oracle_bytes"] for row in baseline)
    new_preoracle = statistics.median(row["worker_peak_before_oracle_bytes"] for row in candidate)
    savings = old_runtime - new_runtime
    paired_attempts_ok = all(
        candidates[pair]["http_attempts"] <= row["http_attempts"]
        and candidates[pair]["runtime_reserved_attempts"] <= row["runtime_reserved_attempts"]
        for pair, row in pairs.items()
    )
    paired_bytes_ok = all(
        candidates[pair]["downloaded_bytes"] <= row["downloaded_bytes"]
        and candidates[pair]["runtime_downloaded_bytes"] <= row["runtime_downloaded_bytes"]
        for pair, row in pairs.items()
    )
    if workload.startswith("discovery_"):
        paired_bytes_ok = all(
            candidates[pair]["downloaded_bytes"] <= row["downloaded_bytes"] * 1.05
            and candidates[pair]["runtime_downloaded_bytes"]
            <= row["runtime_downloaded_bytes"] * 1.05
            for pair, row in pairs.items()
        )
    primary = API_WORKLOADS[workload]["primary"]
    performance_ok = (
        savings >= 1 and old_runtime > 0 and savings / old_runtime >= 0.10
        if primary
        else new_runtime <= old_runtime * 1.05
    )
    qualifies = (
        performance_ok
        and new_wall <= old_wall * 1.05
        and new_rss <= old_rss * 1.05
        and new_preoracle <= old_preoracle * 1.05
        and paired_attempts_ok
        and paired_bytes_ok
    )
    return {
        "status": "qualified" if qualifies else "deferred",
        "workload": workload,
        "primary": primary,
        "baseline_median_runtime_sequence_s": old_runtime,
        "candidate_median_runtime_sequence_s": new_runtime,
        "saved_s": savings,
        "baseline_median_wall_s": old_wall,
        "candidate_median_wall_s": new_wall,
        "baseline_median_peak_rss_bytes": old_rss,
        "candidate_median_peak_rss_bytes": new_rss,
        "baseline_median_worker_peak_before_oracle_bytes": old_preoracle,
        "candidate_median_worker_peak_before_oracle_bytes": new_preoracle,
        "http_attempts_nonincreasing": paired_attempts_ok,
        "downloaded_bytes_within_gate": paired_bytes_ok,
        "reason": "API workload gate passed" if qualifies else "API workload gate not met",
    }


def measurement_version(row: dict) -> int:
    version = row.get("measurement_version", 1)
    require(
        type(version) is int and version in (1, MEASUREMENT_VERSION),
        "unsupported measurement version",
    )
    return version


def phase_attribution(
    setup_started: float,
    runtime_started: float,
    runtime_ended: float,
    oracle_started: float,
    oracle_ended: float,
) -> dict[str, float]:
    boundaries = (setup_started, runtime_started, runtime_ended, oracle_started, oracle_ended)
    require(
        all(math.isfinite(value) for value in boundaries)
        and tuple(sorted(boundaries)) == boundaries,
        "benchmark phase boundaries overlap or are invalid",
    )
    return {
        "worker_setup_wall_s": runtime_started - setup_started,
        "runtime_sequence_wall_s": runtime_ended - runtime_started,
        "oracle_wall_s": oracle_ended - oracle_started,
    }


def publication_cleanup_warnings(result: dict, step: int) -> list[dict]:
    publication = result.get("publication")
    if not isinstance(publication, dict):
        return []
    return [
        {"step": step, "kind": key, "message": str(publication[key])}
        for key in ("cleanup_warning", "retention_warning")
        if publication.get(key)
    ]


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
    def __init__(self, inherited=0.0, *, limit_s=BUDGET_S):
        require(
            type(limit_s) in (int, float) and math.isfinite(limit_s) and limit_s > 0,
            "invalid budget limit",
        )
        self.started = time.monotonic()
        self.inherited = inherited
        self.limit_s = float(limit_s)
        self.holdback_s = 0.0

    def add_inherited(self, seconds: float):
        self.inherited = validate_elapsed(seconds, limit_s=self.limit_s)

    @property
    def elapsed(self):
        return self.inherited + time.monotonic() - self.started

    @property
    def remaining(self):
        return max(
            0.0,
            self.limit_s - self.elapsed - SHUTDOWN_RESERVE_S - self.holdback_s,
        )

    def admit(self, estimated=0.0):
        if estimated > self.remaining:
            raise TimeoutError(
                f"insufficient_evidence: shared {self.limit_s:g}-second budget is exhausted"
            )


def validate_elapsed(value, *, limit_s=BUDGET_S) -> float:
    require(
        type(value) in (int, float) and math.isfinite(value) and 0 <= value <= limit_s,
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
        version = measurement_version(row)
        if version >= 2:
            value = row.get("worker_peak_before_oracle_bytes")
            require(
                type(value) in (int, float) and math.isfinite(value) and value >= 0,
                "invalid baseline metric: worker_peak_before_oracle_bytes",
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
    require(
        len({measurement_version(row) for row in rows}) == 1,
        "baseline samples mix measurement versions; create a fresh baseline",
    )


def validate_semantic_samples(rows: list[dict]) -> str:
    require(len(rows) == 3, "baseline needs three complete semantic samples")
    hashes = [row.get("semantic_hash") for row in rows]
    require(
        isinstance(hashes[0], str) and all(value == hashes[0] for value in hashes),
        "baseline samples are semantically inconsistent",
    )
    return hashes[0]


def build_dataset(
    event_count: int,
    market_count: int,
    *,
    entity_id_base: int = 0,
    entity_id_stride: int = 1,
):
    require(market_count >= event_count * 4, "profile needs four owned markets per event")
    events, markets = [], {}
    owned = event_count * 4
    urgent_count = max(1, event_count // 100) if event_count else 0
    receipts = {
        "closed": "2026-10-13T23:00:00Z",
        "active": "2026-10-13T23:00:00Z",
        "urgent": "2026-10-13T01:00:00Z",
    }
    for n in range(event_count):
        eid = str(entity_id_base + (n + 1) * entity_id_stride)
        closed = n < event_count * 9 // 10
        urgent = not closed and n < event_count * 9 // 10 + urgent_count
        members = []
        for offset in range(4):
            mid = str(entity_id_base + (event_count + n * 4 + offset + 1) * entity_id_stride)
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
        mid = str(entity_id_base + (event_count + n + 1) * entity_id_stride)
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


def create_seed(
    root: Path,
    event_count: int,
    market_count: int,
    *,
    entity_id_base: int = 0,
    entity_id_stride: int = 1,
    publish: bool = True,
) -> dict:
    events, markets, distribution = build_dataset(
        event_count,
        market_count,
        entity_id_base=entity_id_base,
        entity_id_stride=entity_id_stride,
    )
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
        if publish:
            _publish(settings, conn, deadline=time.monotonic() + 1200)
        set_state(conn, "last_publish_at", BASE_TIME.isoformat())
        conn.execute("CHECKPOINT")
        verification = verify_current(settings, connection=conn)
        require(
            verification["store_generation"] == verification["published_generation"],
            "seed publication is not current",
        )
    return distribution


def create_api_seed(root: Path, workload: str, budget: Budget | None = None) -> dict:
    """Freeze one source catalogue and the matching initial workspace for an API workload."""
    require(workload in API_WORKLOADS, "unknown API workload")
    if budget is not None:
        budget.admit(0)
    if workload == "closed_any":
        events, markets, base, stride, complete = (
            2_500,
            10_100,
            99_999_999_999_900_000_000,
            1,
            True,
        )
    else:
        events, markets = 1_000, 4_040
        base = 0
        stride = 10 if workload == "discovery_sparse" else 1
        complete = workload == "discovery_tail"
    provider, workspace = root / "provider", root / "workspace"
    distribution = create_seed(
        provider,
        events,
        markets,
        entity_id_base=base,
        entity_id_stride=stride,
    )
    if complete:
        shutil.copytree(provider, workspace)
        if workload == "discovery_tail":
            with duckdb.connect(str(workspace / "catalogue.duckdb")) as conn:
                frontiers = {}
                for lane, table, key in (
                    ("events", "events", "event_id"),
                    ("markets", "markets", "market_id"),
                ):
                    highwater = conn.execute(
                        f"SELECT coalesce(max(try_cast({key} AS HUGEINT)), 0) FROM {table}"
                    ).fetchone()[0]
                    frontiers[lane] = {
                        "frontier": int(highwater),
                        "highwater": None,
                        "phase": "tail",
                    }
                set_state(
                    conn,
                    "discovery",
                    {
                        "version": 2,
                        "next_lane": 0,
                        "lanes": frontiers,
                        "pending": None,
                    },
                )
                set_state(conn, "keyset_bootstrap_complete", True)
                conn.execute("CHECKPOINT")
        elif workload == "closed_any":
            stale_native_markets = [
                make_market(
                    str(value),
                    f"Known absent synthetic market {value}",
                    seed=markets + value,
                )
                for value in range(1, 101)
            ]
            from oddsfox_catalogue.runlock import run_lock

            workspace_settings = CurrentSettings(workspace)
            with (
                duckdb.connect(str(workspace / "catalogue.duckdb")) as conn,
                run_lock(workspace_settings.run_lock_path),
            ):
                apply(
                    conn,
                    project_response(
                        stale_native_markets,
                        "markets",
                        received_at=(BASE_TIME - timedelta(days=1)).isoformat(),
                        response_id="benchmark-known-absent-seed",
                    ),
                )
                _publish(
                    workspace_settings,
                    conn,
                    deadline=time.monotonic() + workspace_settings.limits.max_duration_s,
                )
                set_state(conn, "last_publish_at", BASE_TIME.isoformat())
                conn.execute("CHECKPOINT")
                verification = verify_current(workspace_settings, connection=conn)
                require(
                    verification["store_generation"] == verification["published_generation"],
                    "closed-any seed publication is not current",
                )
    else:
        create_seed(workspace, 0, 0)
    root.mkdir(parents=True, exist_ok=True)
    expected_root = workspace if workload == "closed_any" else provider
    with duckdb.connect(str(expected_root / "catalogue.duckdb"), read_only=True) as conn:
        expected = {}
        for table in ("events", "markets", "event_markets", "outcomes"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            expected[table] = canonical_hash(rows)
        counts = {
            table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("events", "markets", "event_markets", "outcomes")
        }
        expected_workspace_market_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT market_id FROM markets WHERE venue='polymarket' ORDER BY try_cast(market_id AS HUGEINT)"
            ).fetchall()
        ]
        expected_private = {}
        for table in ("_identity", "_identity_claims"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            expected_private[table] = canonical_hash(rows)
        expected_receipt_keys = [
            list(row)
            for row in conn.execute(
                "SELECT kind,venue,id FROM _receipts ORDER BY kind,venue,id"
            ).fetchall()
        ]
        expected_membership_keys = [
            list(row)
            for row in conn.execute(
                "SELECT venue,event_id FROM _membership ORDER BY venue,event_id"
            ).fetchall()
        ]
        expected_generation = json.loads(
            conn.execute("SELECT value FROM sync_state WHERE key='generation'").fetchone()[0]
        )
        if "_reconcile_attempts" in {row[0] for row in conn.execute("SHOW TABLES").fetchall()}:
            expected_seed_attempts = [
                [kind, entity_id, timestamp.isoformat() if timestamp is not None else None]
                for kind, entity_id, timestamp in conn.execute(
                    "SELECT kind,id,last_attempt FROM _reconcile_attempts ORDER BY kind,id"
                ).fetchall()
            ]
        else:
            expected_seed_attempts = []
    with duckdb.connect(str(provider / "catalogue.duckdb"), read_only=True) as conn:
        expected_event_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT event_id FROM events WHERE venue='polymarket' ORDER BY try_cast(event_id AS HUGEINT)"
            ).fetchall()
        ]
        expected_market_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT market_id FROM markets WHERE venue='polymarket' ORDER BY try_cast(market_id AS HUGEINT)"
            ).fetchall()
        ]
        refreshed_market_ids = []
        if workload == "closed_any":
            lifecycle_rows = conn.execute(
                """SELECT m.market_id,
                    CASE WHEN em.market_id IS NULL THEN 'orphan' ELSE coalesce(e.closed, FALSE)::VARCHAR END cohort
                FROM markets m
                LEFT JOIN event_markets em ON em.venue=m.venue AND em.market_id=m.market_id
                LEFT JOIN events e ON e.venue=em.venue AND e.event_id=em.event_id
                WHERE m.venue='polymarket'
                ORDER BY try_cast(m.market_id AS HUGEINT)"""
            ).fetchall()
            selected = {
                "true": 8900,
                "false": 900,
                "orphan": 100,
            }
            for cohort, wanted in selected.items():
                refreshed_market_ids.extend(
                    str(row[0])
                    for row in [item for item in lifecycle_rows if item[1] == cohort][:wanted]
                )
            require(len(refreshed_market_ids) == 9900, "closed-any cohort selection is incomplete")
    if budget is not None:
        budget.admit(0)
    expected_discovery_frontiers = None
    if workload.startswith("discovery_"):
        with duckdb.connect(str(provider / "catalogue.duckdb"), read_only=True) as conn:
            expected_discovery_frontiers = {}
            for lane, table, key in (
                ("events", "events", "event_id"),
                ("markets", "markets", "market_id"),
            ):
                highwater = conn.execute(
                    f"SELECT coalesce(max(try_cast({key} AS HUGEINT)), 0) FROM {table}"
                ).fetchone()[0]
                expected_discovery_frontiers[lane] = {
                    "frontier": int(highwater),
                    "highwater": None,
                    "phase": "tail",
                }
    metadata = {
        "workload": workload,
        "distribution": {
            **distribution,
            "id_stride": stride,
            "workspace_initially_complete": complete,
        },
        "expected_public_fingerprints": expected,
        "expected_private_fingerprints": expected_private,
        "expected_counts": counts,
        "expected_event_ids": expected_event_ids,
        "expected_market_ids": expected_market_ids,
        "expected_workspace_market_ids": expected_workspace_market_ids,
        "expected_refreshed_market_ids": refreshed_market_ids,
        "expected_absent_market_ids": (
            [str(value) for value in range(1, 101)] if workload == "closed_any" else []
        ),
        "expected_parent_repairs": [],
        "expected_receipt_keys": sorted(
            {tuple(key) for key in expected_receipt_keys}
            | {
                ("market", "polymarket", str(value))
                for value in range(1, 101)
                if workload == "closed_any"
            }
        ),
        "expected_membership_keys": expected_membership_keys,
        "expected_generation": expected_generation,
        "seed_attempts": expected_seed_attempts,
        "receipt_cutoff": BASE_TIME.isoformat(),
        "expected_discovery_frontiers": expected_discovery_frontiers,
        "provider_seed_sha256": digest(provider / "catalogue.duckdb"),
        "workspace_seed_sha256": digest(workspace / "catalogue.duckdb"),
    }
    (root / "api-seed.json").write_text(json.dumps(metadata, sort_keys=True) + "\n")
    return metadata


def copy_runtime(target: Path, source: Path | None = None) -> str:
    source = source or REPO / "src" / "oddsfox_catalogue"
    target.mkdir(parents=True)
    casefolded: set[str] = set()
    for path in sorted(source.rglob("*.py")):
        require(not is_link_or_reparse(path), "runtime source contains a link or reparse point")
        portable = path.relative_to(source).as_posix()
        if portable.casefold() in casefolded:
            raise ValueError("runtime source contains case-colliding paths")
        casefolded.add(portable.casefold())
        destination = target / Path(*portable.split("/"))
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
    return canonical_hash(
        {path.relative_to(target).as_posix(): digest(path) for path in sorted(target.rglob("*.py"))}
    )


def source_hashes(package: Path) -> dict[str, str]:
    hashes = {}
    for path in sorted(package.rglob("*.py")):
        require(not is_link_or_reparse(path), "runtime package contains a link or reparse point")
        name = path.relative_to(package).as_posix()
        if name.casefold() in {entry.casefold() for entry in hashes}:
            raise ValueError("runtime package contains case-colliding paths")
        hashes[name] = digest(path)
    return hashes


def harness_provenance() -> dict:
    files = [
        Path(__file__).resolve(),
        REPO / "tests" / "fakes" / "world.py",
        REPO / "tests" / "fakes" / "capacity_source.py",
        REPO / "src" / "oddsfox_catalogue" / "platform_support.py",
        REPO / "src" / "oddsfox_catalogue" / "_windows.py",
        REPO / "src" / "oddsfox_catalogue" / "_process_gate.py",
        REPO / "scripts" / "verify-installed-workflow.py",
    ]
    return {
        "git_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "source_hashes": {path.relative_to(REPO).as_posix(): digest(path) for path in files},
    }


def validate_tree(root: Path, *, python_only=False) -> None:
    root = safe_path(root)
    require(
        root.is_dir() and not is_link_or_reparse(root), "baseline tree must be a regular directory"
    )
    for path in root.rglob("*"):
        require(not is_link_or_reparse(path), "baseline contains a link or reparse point")
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
    workload: str | None = None,
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
    if workload:
        argv.extend(["--_workload", workload])
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


def prepare_api_seed_argv(output: Path, workload: str) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--profile",
        "perf",
        "--output",
        str(output),
        "--_prepare_api_seed",
        workload,
    ]


def prepare_api_seed(
    output: Path,
    workload: str,
    budget: Budget,
    runtime_package: Path | None = None,
) -> dict:
    """Build a workload seed in the existing supervised, shared-budget worker."""
    require(workload in API_WORKLOADS, "unknown API workload")
    output = safe_path(output)
    require(not output.exists(), "API seed output must be fresh")
    output.parent.mkdir(parents=True, exist_ok=True)
    result = supervised_prepare(
        prepare_api_seed_argv(output, workload),
        output.parent,
        budget,
        output.parent / f"{output.name}-prepare.log",
        package_root=runtime_package,
    )
    require(
        result.get("status") == "prepared", result.get("message", "API seed preparation failed")
    )
    require(result.get("workload") == workload, "API seed workload provenance mismatch")
    require((output / "api-seed.json").is_file(), "API seed metadata is missing")
    if runtime_package is not None:
        require(
            result.get("runtime_source_hashes") == source_hashes(runtime_package),
            "API seed preparation runtime provenance mismatch",
        )
    return result


def claim_resource_diagnostics(package: Path, output: Path, budget: Budget) -> dict:
    """Measure synthetic claim-index cost; do not infer a production admission model."""
    package = safe_path(package)
    output = safe_path(output)
    require(
        package.is_dir() and not is_link_or_reparse(package), "claim candidate package is unsafe"
    )
    result_root = output / "claim-scale"
    require(not result_root.exists(), "claim diagnostic output must be fresh")
    result_root.parent.mkdir(parents=True, exist_ok=True)
    result = supervised_prepare(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--profile",
            "perf",
            "--output",
            str(result_root),
            "--_claim_diagnostics",
        ],
        result_root.parent,
        budget,
        result_root.parent / "claim-scale-prepare.log",
        package_root=package,
    )
    require(result.get("status") == "measured", result.get("message", "claim scale failed"))
    require(
        result.get("runtime_source_hashes") == source_hashes(package),
        "claim diagnostic runtime provenance mismatch",
    )
    return {
        **result,
        "resource_gate": False,
        "reason": (
            "synthetic index costs were measured, but no self-contained production admission "
            "model covers build and sustained writes"
        ),
    }


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


def supervised_prepare(
    command: list[str],
    output: Path,
    budget: Budget,
    log: Path,
    package_root: Path | None = None,
) -> dict:
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
        PYTHONPATH=os.pathsep.join(
            (str(package_root.parent if package_root else REPO / "src"), str(REPO / "tests"))
        ),
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
    )
    if package_root:
        env["FULL_PASS_EXPECTED_PACKAGE"] = str(package_root.resolve())
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
    require(
        measurement_version(report) == measurement_version(report["samples"][0]),
        "baseline report and samples use different measurement versions; create a fresh baseline",
    )
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


def private_state_snapshot(conn) -> dict:
    raw_state = {
        key: json.loads(value)
        for key, value in conn.execute("SELECT key,value FROM sync_state ORDER BY key").fetchall()
    }
    state_keys = (
        "store_contract",
        "projection_version",
        "generation",
        "full_pass",
        "fetch_pending",
        "bulk_policy",
        "discovery",
        "reconcile",
        "last_publish_at",
    )
    state = {key: raw_state[key] for key in state_keys if key in raw_state}
    policy = state.get("bulk_policy")
    if isinstance(policy, dict) and "origin" in policy:
        if not str(policy["origin"]).startswith("http://127.0.0.1:"):
            raise ValueError("synthetic bulk policy origin is not loopback")
        policy = {**policy, "origin": "http://127.0.0.1:<ephemeral>"}
        state["bulk_policy"] = policy
    last_run = raw_state.get("last_run")
    if isinstance(last_run, dict):
        semantic_keys = (
            "operation",
            "status",
            "published",
            "pending",
            "source_complete",
            "budget_exhausted",
            "parent_event_repair_count",
        )
        state["last_run"] = {key: last_run[key] for key in semantic_keys if key in last_run}
    existing_tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    tables = {}
    for table in (
        "_receipts",
        "_membership",
        "_identity",
        "_identity_claims",
        "_parent_repairs",
        "_reconcile_attempts",
    ):
        if table not in existing_tables:
            continue
        columns = [row[0] for row in conn.execute(f'DESCRIBE "{table}"').fetchall()]
        rows = []
        for row in conn.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall():
            item = dict(zip(columns, row, strict=True))
            for generated_reference in (
                "response",
                "latest_response",
                "invalidated_response",
            ):
                if generated_reference in item and item[generated_reference] is not None:
                    item[generated_reference] = "<generated-response-reference>"
            rows.append(item)
        tables[table] = rows
    return {"state": state, "tables": tables}


def public_snapshot(root: Path) -> dict:
    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
        tables = {}
        for table in ("events", "markets", "event_markets", "outcomes"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            tables[table] = [dict(zip(columns, row, strict=True)) for row in rows]
        private_state = private_state_snapshot(conn)
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
        full_pass = private_state["state"].get("full_pass") or {}
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
            "state": private_state["state"],
            "receipt_state": receipt_state,
            "private_state": private_state["tables"],
            "counts": {name: len(rows) for name, rows in tables.items()},
            "receipt_gaps": receipt_gaps,
            "publication_verified": verified,
            "publication": normalized_publication(publication),
        }


def api_attempt_ledger_valid(metadata, observed_attempts, settled_ledger, receipt_times) -> bool:
    """Require attempt state to be exactly explained by seed state and committed units."""
    event_ids = set(metadata.get("expected_event_ids", []))
    market_ids = set(metadata.get("expected_market_ids", [])) | set(
        metadata.get("expected_workspace_market_ids", [])
    )
    cutoff = datetime.fromisoformat(metadata["receipt_cutoff"])
    upper_bound = BASE_TIME + timedelta(hours=2)

    def normalize_time(value):
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(parsed, datetime) or parsed.tzinfo is None:
            raise ValueError("attempt timestamp must be timezone-aware")
        return parsed

    def is_known(kind, entity_id):
        if kind == "events":
            return entity_id in event_ids
        if kind in {"markets", "native"}:
            return entity_id in market_ids
        return False

    def receipt_for(kind, entity_id):
        return receipt_times.get(("events" if kind == "events" else "markets", entity_id))

    def validate_row(kind, entity_id, timestamp, *, seeded):
        timestamp = normalize_time(timestamp)
        receipt = receipt_for(kind, entity_id)
        return (
            isinstance(entity_id, str)
            and re.fullmatch(r"[1-9][0-9]{0,19}", entity_id) is not None
            and is_known(kind, entity_id)
            and receipt is not None
            and timestamp <= receipt
            and timestamp <= upper_bound
            and (seeded or timestamp >= cutoff)
        )

    try:
        expected = {}
        for kind, entity_id, timestamp in metadata.get("seed_attempts", []):
            if not validate_row(kind, entity_id, timestamp, seeded=True):
                return False
            key = (kind, entity_id)
            if key in expected:
                return False
            expected[key] = normalize_time(timestamp)

        for write in settled_ledger:
            requested_kind = write["requested_kind"]
            requested_ids = write["requested_ids"]
            kind = write["kind"]
            entity_ids = write["ids"]
            timestamp = normalize_time(write["timestamp"])
            expected_requested_kind = {
                "event": "events",
                "events": "events",
                "market": "markets",
                "markets": "markets",
            }.get(requested_kind)
            native_request = write["native_request"]
            if (
                expected_requested_kind is None
                or type(native_request) is not bool
                or kind not in {"events", "markets", "native"}
                or (kind == "native") != native_request
                or (kind == "native" and expected_requested_kind != "markets")
                or (kind != "native" and kind != expected_requested_kind)
                or not entity_ids
                or len(entity_ids) != len(set(entity_ids))
                or sorted(entity_ids, key=int) != sorted(requested_ids, key=int)
                or any(
                    not validate_row(kind, entity_id, timestamp, seeded=False)
                    for entity_id in entity_ids
                )
            ):
                return False
            for entity_id in entity_ids:
                key = (kind, entity_id)
                if key in expected and timestamp - expected[key] < timedelta(hours=1):
                    return False
                expected[key] = timestamp

        observed = {}
        for kind, entity_id, timestamp in observed_attempts:
            if not validate_row(kind, entity_id, timestamp, seeded=True):
                return False
            key = (kind, entity_id)
            if key in observed:
                return False
            observed[key] = normalize_time(timestamp)
        return observed == expected
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def api_workload_oracle(
    root: Path,
    seed: Path,
    workload: str,
    settled_units: int,
    settled_attempts: list[dict] | None = None,
) -> dict:
    """Check fixed source contents, native ownership, receipt coverage and drained work."""
    require(workload in API_WORKLOADS, "unknown API workload")
    metadata = json.loads((seed / "api-seed.json").read_text())
    require(metadata.get("workload") == workload, "API seed workload mismatch")
    from oddsfox_catalogue.current_publish import verify_current

    settings = CurrentSettings(root)
    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
        public = {}
        for table in ("events", "markets", "event_markets", "outcomes"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            public[table] = rows
        counts = {table: len(rows) for table, rows in public.items()}
        private_fingerprints = {}
        for table in ("_identity", "_identity_claims"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            private_fingerprints[table] = canonical_hash(rows)
        state = {
            key: json.loads(value)
            for key, value in conn.execute(
                "SELECT key,value FROM sync_state ORDER BY key"
            ).fetchall()
        }
        event_memberships = conn.execute(
            "SELECT venue,event_id,receipt,unresolved FROM _membership ORDER BY venue,event_id"
        ).fetchall()
        market_receipt_rows = conn.execute(
            "SELECT venue,id,receipt,response FROM _receipts WHERE kind='market' ORDER BY venue,id"
        ).fetchall()
        receipt_keys = [
            list(row)
            for row in conn.execute(
                "SELECT kind,venue,id FROM _receipts ORDER BY kind,venue,id"
            ).fetchall()
        ]
        membership_keys = [
            list(row)
            for row in conn.execute(
                "SELECT venue,event_id FROM _membership ORDER BY venue,event_id"
            ).fetchall()
        ]
        parent_repairs = [
            row[0]
            for row in conn.execute(
                "SELECT event_id FROM _parent_repairs ORDER BY event_id"
            ).fetchall()
        ]
        has_attempt_table = "_reconcile_attempts" in {
            row[0] for row in conn.execute("SHOW TABLES").fetchall()
        }
        attempts = (
            conn.execute(
                "SELECT kind,id,last_attempt FROM _reconcile_attempts ORDER BY kind,id"
            ).fetchall()
            if has_attempt_table
            else []
        )
        publication = verify_current(settings, connection=conn)
    expected_counts = metadata["expected_counts"]
    expected_public = metadata["expected_public_fingerprints"]
    expected_private = metadata["expected_private_fingerprints"]
    observed_public = {name: canonical_hash(rows) for name, rows in public.items()}

    def has_unfinished(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"pending", "active", "in_progress"} and item not in (
                    None,
                    False,
                    [],
                    {},
                ):
                    return True
                if has_unfinished(item):
                    return True
        elif isinstance(value, list):
            return any(has_unfinished(item) for item in value)
        return False

    pending_keys = {
        key: value
        for key, value in state.items()
        if key in {"fetch_pending", "discovery", "keyset_pending", "keyset_discovery"}
        or key.startswith("keyset_")
    }
    no_pending = (
        state.get("fetch_pending") is None
        and state.get("keyset_pending") is None
        and not any(has_unfinished(value) for value in pending_keys.values())
    )
    # Pending units are flat durable records, not necessarily nested under a
    # `pending` property. Treat any non-null record as unfinished work.
    no_pending = (
        no_pending and state.get("fetch_pending") is None and state.get("keyset_pending") is None
    )
    keyset_state_valid = True
    keyset_state = state.get("keyset_discovery")
    if keyset_state is not None:
        try:
            if type(keyset_state.get("version")) is not int or keyset_state["version"] != 1:
                keyset_state_valid = False
            if type(keyset_state.get("next_lane")) is not int or keyset_state["next_lane"] not in (
                0,
                1,
            ):
                keyset_state_valid = False
            lanes = keyset_state["lanes"]
            required_passes = {
                "events": {"all"},
                "markets": {"open", "closed"},
            }
            if set(lanes) != set(required_passes):
                keyset_state_valid = False
            for lane, pass_names in required_passes.items():
                passes = lanes[lane]["passes"]
                if set(passes) != pass_names:
                    keyset_state_valid = False
                    break
                for pass_name in pass_names:
                    progress = passes[pass_name]
                    highwater = progress.get("highwater")
                    last_id = progress.get("last_id")
                    page_limit = progress.get("page_limit")
                    if (
                        progress.get("done") is not True
                        or progress.get("cursor") is not None
                        or type(highwater) is not int
                        or highwater < 0
                        or type(last_id) is not int
                        or not 0 <= last_id <= highwater
                        or type(page_limit) is not int
                        or not 1 <= page_limit <= 100
                    ):
                        keyset_state_valid = False
                        break
        except (KeyError, TypeError, AttributeError):
            keyset_state_valid = False
    if (
        workload.startswith("discovery_")
        and workload != "discovery_tail"
        and keyset_state is not None
    ):
        no_pending = no_pending and keyset_state_valid
    expected_event_ids = set(metadata["expected_event_ids"])
    expected_market_ids = set(metadata["expected_market_ids"])
    expected_workspace_market_ids = set(metadata.get("expected_workspace_market_ids", []))
    expected_absent_ids = set(metadata["expected_absent_market_ids"])
    expected_refreshed_ids = set(metadata["expected_refreshed_market_ids"])
    omission_marker = state.get("api_benchmark_omission_progress")
    omission_progress_valid = workload != "closed_any" or omission_marker == {
        "complete": True,
        "ids": sorted(expected_absent_ids, key=int),
    }
    cutoff = datetime.fromisoformat(metadata["receipt_cutoff"])
    observed_memberships = {
        event_id
        for venue, event_id, receipt, unresolved in event_memberships
        if venue == "polymarket" and receipt is not None and receipt > cutoff and not unresolved
    }
    all_membership_ids = {
        event_id
        for venue, event_id, _receipt, _unresolved in event_memberships
        if venue == "polymarket"
    }
    observed_market_receipts = {
        market_id
        for venue, market_id, receipt, _response in market_receipt_rows
        if venue == "polymarket" and receipt is not None and receipt > cutoff
    }
    expected_fresh_market_receipts = expected_refreshed_ids | expected_absent_ids
    if workload.startswith("discovery_") and workload != "discovery_tail":
        expected_fresh_market_receipts = expected_market_ids
    all_resolved_membership_ids = {
        event_id
        for venue, event_id, _receipt, unresolved in event_memberships
        if venue == "polymarket" and unresolved is False
    }
    event_coverage = (
        all_membership_ids == expected_event_ids
        and all_resolved_membership_ids == expected_event_ids
    )
    if workload.startswith("discovery_") and workload != "discovery_tail":
        event_coverage = event_coverage and observed_memberships == expected_event_ids
    market_coverage = (
        (
            observed_market_receipts == expected_fresh_market_receipts
            and expected_absent_ids.isdisjoint(expected_market_ids)
            and expected_absent_ids.issubset(expected_workspace_market_ids)
        )
        if expected_fresh_market_receipts
        else True
    )
    receipt_coverage = event_coverage and market_coverage
    receipt_keys_exact = sorted(receipt_keys) == sorted(metadata.get("expected_receipt_keys", []))
    membership_keys_exact = sorted(membership_keys) == sorted(
        metadata.get("expected_membership_keys", [])
    )
    receipt_coverage = receipt_coverage and receipt_keys_exact and membership_keys_exact
    absence_manifests_valid = True
    if expected_absent_ids:
        from oddsfox_catalogue.capture.writer import read_body, read_manifest

        response_by_id = {
            market_id: response
            for venue, market_id, receipt, response in market_receipt_rows
            if venue == "polymarket" and receipt is not None and receipt > cutoff
        }
        for absent_id in expected_absent_ids:
            response_id = response_by_id.get(absent_id)
            if not response_id:
                absence_manifests_valid = False
                break
            raw_dir = settings.raw_dir / response_id
            try:
                manifest = read_manifest(
                    raw_dir / "p000001.manifest.json", trusted_root=settings.root
                )
            except (OSError, ValueError, TypeError):
                manifest = None
            if (
                manifest is None
                or manifest.get("status") != 404
                or manifest.get("request") != {"endpoint": f"/markets/{absent_id}", "params": {}}
            ):
                absence_manifests_valid = False
                break
            receipt_time = next(
                receipt
                for venue, market_id, receipt, _response in market_receipt_rows
                if venue == "polymarket" and market_id == absent_id
            )
            if datetime.fromisoformat(manifest.get("received_at", "")) != receipt_time:
                absence_manifests_valid = False
                break
            try:
                body = read_body(
                    raw_dir,
                    manifest,
                    trusted_root=settings.root,
                    max_body_bytes=settings.limits.max_response_bytes,
                )
            except (OSError, ValueError, TypeError):
                body = None
            if not isinstance(manifest.get("body_sha256"), str) or not body:
                absence_manifests_valid = False
                break
    repair_state_valid = parent_repairs == metadata.get("expected_parent_repairs", [])
    receipt_times = {
        ("events", event_id): receipt
        for venue, event_id, receipt, _unresolved in event_memberships
        if venue == "polymarket"
    }
    receipt_times.update(
        {
            ("markets", market_id): receipt
            for venue, market_id, receipt, _response in market_receipt_rows
            if venue == "polymarket"
        }
    )
    attempts_valid = api_attempt_ledger_valid(
        metadata, attempts, settled_attempts or [], receipt_times
    )
    generation = state.get("generation")
    generation_valid = (
        type(generation) is int
        and type(metadata.get("expected_generation")) is int
        and generation >= metadata["expected_generation"]
        and publication.get("valid") is True
        and type(publication.get("store_generation")) is int
        and publication.get("store_generation") == generation
        and publication.get("store_generation") == publication.get("published_generation")
    )
    tail_frontiers_valid = True
    if workload.startswith("discovery_"):
        expected_frontiers = metadata.get("expected_discovery_frontiers") or {}
        discovery = state.get("discovery") or {}
        try:
            observed_frontiers = {
                lane: {
                    key: discovery["lanes"][lane][key] for key in ("frontier", "highwater", "phase")
                }
                for lane in ("events", "markets")
            }
            tail_frontiers_valid = observed_frontiers == expected_frontiers
        except (KeyError, TypeError):
            tail_frontiers_valid = False
    public_match = counts == expected_counts and observed_public == expected_public
    identity_match = private_fingerprints == expected_private
    published = generation_valid
    oracle_pass = (
        no_pending
        and receipt_coverage
        and public_match
        and identity_match
        and published
        and repair_state_valid
        and attempts_valid
        and absence_manifests_valid
        and omission_progress_valid
        and keyset_state_valid
        and tail_frontiers_valid
    )
    completed_work = {
        "events": counts["events"],
        "markets": counts["markets"],
        "event_markets": counts["event_markets"],
        "outcomes": counts["outcomes"],
        "request_or_page_count": settled_units,
        "pending": 0 if no_pending else 1,
        "receipt_coverage": receipt_coverage,
        "publication_verified": published,
    }
    return {
        "oracle_pass": oracle_pass,
        "oracle_fingerprint": canonical_hash(
            {
                "public": observed_public,
                "private_identity": private_fingerprints,
                "expected_counts": expected_counts,
                "receipt_coverage": receipt_coverage,
                "receipt_keys_exact": receipt_keys_exact,
                "membership_keys_exact": membership_keys_exact,
                "parent_repairs": parent_repairs,
                "attempts_valid": attempts_valid,
                "generation_valid": generation_valid,
                "absence_manifests_valid": absence_manifests_valid,
                "omission_progress_valid": omission_progress_valid,
                "keyset_state_valid": keyset_state_valid,
                "tail_frontiers_valid": tail_frontiers_valid,
                "publication_verified": published,
            }
        ),
        "completed_work": completed_work,
        "oracle_checks": {
            "public_contents": public_match,
            "identity_and_claims": identity_match,
            "receipt_coverage": receipt_coverage,
            "receipt_keys_exact": receipt_keys_exact,
            "membership_keys_exact": membership_keys_exact,
            "no_pending_work": no_pending,
            "parent_repairs": repair_state_valid,
            "attempts": attempts_valid,
            "generation": generation_valid,
            "absence_manifests": absence_manifests_valid,
            "omission_progress": omission_progress_valid,
            "keyset_progress": keyset_state_valid,
            "discovery_frontiers_complete": tail_frontiers_valid,
            "publication_verified": published,
        },
        "settled_units": settled_units,
    }


def run_api_workload(
    settings,
    current_runtime,
    current_sync,
    run_lock,
    source,
    root: Path,
    seed: Path,
    workload: str,
    *,
    domain_clock: dict | None = None,
    profile: Path | None = None,
    optimized_capability: str | None = None,
) -> dict:
    """Run one fixed API workload, interrupt after settled work, then resume it."""
    from oddsfox_catalogue import current_reconcile
    from oddsfox_catalogue.gamma.http import CaptureStopped

    # The worker's GammaClient factory and all frozen runtime clocks must share
    # this mutable value. In particular, repair-drain phases advance it so
    # responses are newer than the parent-repair nominations they settle.
    clock = domain_clock if domain_clock is not None else {"now": BASE_TIME}
    original_datetime = current_runtime.datetime

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"].astimezone(tz or UTC)

    current_runtime.datetime = FrozenDateTime
    original_reconcile_now = current_reconcile._now
    current_reconcile._now = lambda: clock["now"]
    runtime_started = time.monotonic()
    source_request_start = source.stats["requests"]
    acquire_apply_s = publish_verify_s = 0.0
    settled_units = 0
    operation_results = []
    cleanup_warnings = []
    settled_attempts = []
    try:
        profiler = cProfile.Profile() if profile else None
        if profiler:
            profiler.enable()
        if workload == "closed_any":
            from oddsfox_catalogue.current_config import connect_store

            seed_metadata = json.loads((seed / "api-seed.json").read_text())
            ids = seed_metadata["expected_refreshed_market_ids"]
            require(len(ids) == 9900, "closed-any seed has too few known markets")
            absent_ids = [str(value) for value in range(1, 101)]
            chunks = [ids[index : index + 100] for index in range(0, len(ids), 100)]
            original_fetch_unit = current_sync.fetch_unit
            stopped = [False]
            calls = 0

            def stop_after_fifty(*args, **kwargs):
                nonlocal calls
                if calls >= 50 and not stopped[0]:
                    stopped[0] = True
                    raise CaptureStopped()
                result = call_and_record_attempt(
                    original_fetch_unit, settled_attempts, *args, **kwargs
                )
                if result[0].get("responses", 0):
                    calls += 1
                return result

            current_sync.fetch_unit = stop_after_fifty
            try:
                for phase in range(2):
                    phase_started = time.monotonic()
                    deadline = phase_started + settings.limits.max_duration_s
                    with (
                        connect_store(settings) as phase_conn,
                        run_lock(settings.run_lock_path),
                        current_runtime.GammaClient(
                            settings.gamma,
                            max_body_bytes=settings.limits.max_response_bytes,
                            max_requests=settings.limits.max_requests,
                            max_download_bytes=settings.limits.max_download_bytes,
                            max_duration_s=settings.limits.max_duration_s,
                            deadline=deadline,
                        ) as client,
                    ):
                        current_sync.resume_pending(settings, phase_conn, client)
                        progress = current_sync.current_store.get_state(
                            phase_conn, "api_benchmark_progress"
                        ) or {"next_batch": 0}
                        next_batch = progress["next_batch"]
                        try:
                            settled_units += settle_closed_any_absences(
                                current_sync,
                                settings,
                                phase_conn,
                                client,
                                absent_ids,
                                clock,
                            )
                            for index in range(next_batch, len(chunks)):
                                clock["now"] = BASE_TIME + timedelta(seconds=index)
                                _result = current_sync.fetch_apply(
                                    settings,
                                    phase_conn,
                                    client,
                                    "markets",
                                    chunks[index],
                                    progress={"api_benchmark_progress": {"next_batch": index + 1}},
                                )
                                settled_units += 1
                        except CaptureStopped:
                            pass
                        if phase == 1:
                            clock["now"] = BASE_TIME + timedelta(hours=2)
                            repair_count = phase_conn.execute(
                                "SELECT count(*) FROM _parent_repairs"
                            ).fetchone()[0]
                            max_repair_batches = (repair_count + 99) // 100 + 2
                            for _ in range(max_repair_batches):
                                repaired = current_reconcile.repair_pending(
                                    settings, phase_conn, client, deadline=deadline
                                )
                                settled_units += repaired.get("responses", 0)
                                if not repaired.get("responses"):
                                    break
                            require(
                                phase_conn.execute(
                                    "SELECT count(*) FROM _parent_repairs"
                                ).fetchone()[0]
                                == 0,
                                "API workload left parent repairs unsettled",
                            )
                        from oddsfox_catalogue.current_runtime import _publish

                        publication_result = {}
                        if phase == 1:
                            publication_result, publication_s = _publish(
                                settings, phase_conn, deadline
                            )
                            publish_verify_s += publication_s
                        stats = client.stats.as_dict()
                        operation_results.append(
                            {
                                "publication": publication_result,
                                "status": "interrupted" if phase == 0 else "resumed",
                                "published": phase == 1,
                                "http_attempts": stats["requests"],
                                "downloaded_bytes": stats["downloaded_bytes"],
                            }
                        )
                    if phase == 0:
                        require(
                            stopped[0] and calls == 50, "API interruption did not settle 50 units"
                        )
                        settled_units = calls
                    else:
                        settled_units = calls
            finally:
                current_sync.fetch_unit = original_fetch_unit
                current_reconcile._now = original_reconcile_now
            runtime_ended = time.monotonic()
            acquire_apply_s = max(0.0, runtime_ended - runtime_started - publish_verify_s)
            cleanup_warnings.extend(
                warning
                for result in operation_results
                for warning in publication_cleanup_warnings(result, 0)
            )
            client_stats = {}
        else:
            original_fetch_unit = current_sync.fetch_unit
            original_reconcile_fetch_unit = current_reconcile.fetch_unit
            try:
                import importlib

                current_keyset = importlib.import_module("oddsfox_catalogue.current_keyset")
            except ImportError:
                current_keyset = None
            original_cold_page = getattr(current_keyset, "cold_page", None)
            stop = SettledUnitStop(50, CaptureStopped)

            def stop_after_settled(*args, **kwargs):
                stop.before_unit()
                result = call_and_record_attempt(
                    original_fetch_unit, settled_attempts, *args, **kwargs
                )
                stop.record(result[0].get("responses", 0))
                return result

            def record_reconcile_attempts(*args, **kwargs):
                return call_and_record_attempt(
                    original_reconcile_fetch_unit, settled_attempts, *args, **kwargs
                )

            def stop_after_page(*args, **kwargs):
                stop.before_unit()
                result = original_cold_page(*args, **kwargs)
                stop.record(result.get("responses", 0))
                return result

            current_sync.fetch_unit = stop_after_settled
            current_reconcile.fetch_unit = record_reconcile_attempts
            if original_cold_page is not None:
                current_keyset.cold_page = stop_after_page
            try:
                for step in range(2):
                    if step == 1:
                        stop.disable()
                    clock["now"] = BASE_TIME + timedelta(seconds=step)
                    started = time.monotonic()
                    with run_lock(settings.run_lock_path):
                        result = current_runtime.execute(
                            settings,
                            "refresh",
                            {},
                            started + settings.limits.max_duration_s,
                        )
                    operation_results.append(result)
                    timings = result.get("timings", {})
                    acquire_apply_s += timings.get("acquire_apply_s", 0)
                    publish_verify_s += timings.get("publish_verify_s", 0)
                    settled_units = stop.settled
                    cleanup_warnings.extend(publication_cleanup_warnings(result, step))
                    if step == 0 and workload != "discovery_tail":
                        require(
                            stop.triggered and stop.settled == 50,
                            "discovery interruption did not settle exactly 50 units",
                        )
                clock["now"] = BASE_TIME + timedelta(hours=2)
                from oddsfox_catalogue.current_config import connect_store

                with (
                    connect_store(settings) as conn,
                    run_lock(settings.run_lock_path),
                    current_runtime.GammaClient(
                        settings.gamma,
                        max_body_bytes=settings.limits.max_response_bytes,
                        max_requests=settings.limits.max_requests,
                        max_download_bytes=settings.limits.max_download_bytes,
                        max_duration_s=settings.limits.max_duration_s,
                        deadline=time.monotonic() + settings.limits.max_duration_s,
                    ) as client,
                ):
                    current_sync.resume_pending(settings, conn, client)
                    repair_count = conn.execute("SELECT count(*) FROM _parent_repairs").fetchone()[
                        0
                    ]
                    max_repair_batches = (repair_count + 99) // 100 + 2
                    for _ in range(max_repair_batches):
                        repaired = current_reconcile.repair_pending(
                            settings,
                            conn,
                            client,
                            deadline=time.monotonic() + settings.limits.max_duration_s,
                        )
                        if not repaired.get("responses"):
                            break
                    require(
                        conn.execute("SELECT count(*) FROM _parent_repairs").fetchone()[0] == 0,
                        "discovery workload left parent repairs unsettled",
                    )
                    publication_result, publication_s = current_runtime._publish(
                        settings, conn, time.monotonic() + settings.limits.max_duration_s
                    )
                    publish_verify_s += publication_s
                    repair_stats = client.stats.as_dict()
                    operation_results.append(
                        {
                            "publication": publication_result,
                            "http_attempts": repair_stats["requests"],
                            "downloaded_bytes": repair_stats["downloaded_bytes"],
                        }
                    )
                    cleanup_warnings.extend(
                        publication_cleanup_warnings({"publication": publication_result}, 2)
                    )
                    settled_units = stop.settled
            finally:
                current_sync.fetch_unit = original_fetch_unit
                current_reconcile.fetch_unit = original_reconcile_fetch_unit
                if original_cold_page is not None:
                    current_keyset.cold_page = original_cold_page
            client_stats = {}
            runtime_ended = time.monotonic()
        if profiler:
            profiler.disable()
            profiler.dump_stats(profile)
    finally:
        current_runtime.datetime = original_datetime
        current_reconcile._now = original_reconcile_now
        worker_peak_before_oracle = process_peak_rss_bytes()
    oracle_started = time.monotonic()
    oracle = api_workload_oracle(root, seed, workload, settled_units, settled_attempts)
    require(oracle["oracle_pass"], f"API semantic oracle failed: {oracle['oracle_checks']}")
    request_trace = [
        json.loads(line) for line in source.audit_path.read_text().splitlines() if line.strip()
    ]
    runtime_requests = [
        row for row in request_trace if row.get("request", 0) > source_request_start
    ]
    runtime_keyset = [row for row in runtime_requests if row.get("keyset") is True]
    optimized_request_shape_valid = True
    if optimized_capability == "market_closed_any":
        optimized_request_shape_valid = workload == "closed_any" and any(
            row.get("kind") == "markets"
            and row.get("closed") == "any"
            and row.get("keyset") is False
            and 0 < len(row.get("ids", [])) <= 100
            for row in runtime_requests
        )
    elif optimized_capability == "keyset_discovery":
        if workload in {"discovery_dense", "discovery_sparse"}:
            optimized_request_shape_valid = {row.get("kind") for row in runtime_keyset} == {
                "events",
                "markets",
            }
        elif workload == "discovery_tail":
            optimized_request_shape_valid = not runtime_keyset
        else:
            optimized_request_shape_valid = False
    require(
        optimized_request_shape_valid,
        "certified API capability was not exercised with its required request shape",
    )
    oracle_ended = time.monotonic()
    return {
        **oracle,
        "workload": workload,
        "optimized_request_shape_valid": optimized_request_shape_valid,
        "runtime_sequence_wall_s": runtime_ended - runtime_started,
        "acquire_apply_s": acquire_apply_s,
        "publish_verify_s": publish_verify_s,
        "oracle_wall_s": oracle_ended - oracle_started,
        "_oracle_started": oracle_started,
        "_oracle_ended": oracle_ended,
        "_runtime_ended": runtime_ended,
        "worker_peak_before_oracle_bytes": worker_peak_before_oracle,
        "settled_units": settled_units,
        "settled_attempts": settled_attempts,
        "operation_results": operation_results,
        "client_stats": client_stats,
        "cleanup_warnings": cleanup_warnings,
        "cleanup_deferred": bool(cleanup_warnings),
    }


def atomic_write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def claim_fixture_rows(start: int, end: int) -> list[tuple[str, str, str, str, str]]:
    """Generate one bounded batch of valid synthetic native identity claims."""
    rows = []
    for index in range(start, end):
        market_id = f"{10**19 + index // 4:020d}"
        asset_kind = "clob" if index % 4 < 2 else "position"
        role = "candidate" if index % 2 == 0 else "accepted"
        asset_number = int.from_bytes(
            hashlib.sha256(f"claim-fixture:{index // 4}:{asset_kind}".encode()).digest(),
            "big",
        )
        rows.append(("polymarket", market_id, role, asset_kind, str(asset_number)))
    return rows


def populate_claim_fixture(conn, count: int, *, batch_size=10_000, on_batch=None) -> dict:
    """Populate claims in independently committed, bounded transactions."""
    if type(count) is not int or count < 0:
        raise ValueError("claim fixture count must be a nonnegative integer")
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("claim fixture batch size must be a positive integer")
    completed = batches = 0
    for start in range(0, count, batch_size):
        end = min(count, start + batch_size)
        rows = claim_fixture_rows(start, end)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.executemany("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", rows)
            conn.execute("COMMIT")
        except BaseException as exc:
            with contextlib.suppress(Exception):
                conn.execute("ROLLBACK")
            if on_batch is not None:
                with contextlib.suppress(Exception):
                    on_batch(
                        {
                            "status": "population_failed",
                            "requested_rows": count,
                            "completed_rows": completed,
                            "completed_batches": batches,
                            "batch_size": batch_size,
                            "failed_batch": {"start": start, "end_exclusive": end},
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
            raise
        completed += len(rows)
        batches += 1
        if on_batch is not None:
            on_batch(
                {
                    "status": "population_in_progress",
                    "requested_rows": count,
                    "completed_rows": completed,
                    "completed_batches": batches,
                    "batch_size": batch_size,
                }
            )
    return {
        "status": "population_complete",
        "requested_rows": count,
        "completed_rows": completed,
        "completed_batches": batches,
        "batch_size": batch_size,
    }


def build_claim_fixture_indexes(conn) -> None:
    """Create both diagnostic indexes atomically; a partial pair is never left behind."""
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("CREATE INDEX idx_identity_claims_asset_id ON _identity_claims(asset_id)")
        conn.execute("CREATE INDEX idx_identity_claims_market_id ON _identity_claims(market_id)")
        conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            conn.execute("ROLLBACK")
        raise


def run_claim_scale_diagnostics(root: Path) -> dict:
    """Measure claim population, index build, and small post-index writes offline."""
    from oddsfox_catalogue.current_config import CurrentSettings, connect_store
    from oddsfox_catalogue.current_store import initialize
    from oddsfox_catalogue.runlock import run_lock

    root.mkdir(parents=True, exist_ok=False)
    scales = []
    progress_path = root / "claim-scale-progress.json"
    progress = {"status": "in_progress", "scales": []}
    atomic_write_json(progress_path, progress)

    def storage_metrics(directory, temporary_directory):
        files = [path for path in directory.rglob("*") if path.is_file()]
        temporary_files = [
            path
            for path in temporary_directory.rglob("*")
            if path.is_file() and not is_link_or_reparse(path)
        ]
        return {
            "total_bytes": sum(path.stat().st_size for path in files),
            "wal_bytes": sum(path.stat().st_size for path in files if path.suffix == ".wal"),
            "temporary_bytes": sum(path.stat().st_size for path in temporary_files),
        }

    for count in (200_000, 1_000_000):
        catalogue_root = root / f"claims-{count}"
        catalogue_root.mkdir()
        validate_root(catalogue_root, initialize=True)
        settings = CurrentSettings(catalogue_root)
        started = time.monotonic()
        with connect_store(settings) as conn, run_lock(settings.run_lock_path):
            initialize(conn)

            def save_population_progress(batch, scale=count):
                progress.update(
                    {
                        "status": batch["status"],
                        "current_scale": scale,
                        "current_population": batch,
                        "scales": scales,
                    }
                )
                atomic_write_json(progress_path, progress)

            population = populate_claim_fixture(conn, count, on_batch=save_population_progress)
            conn.execute("CHECKPOINT")
            populated_storage = storage_metrics(catalogue_root, settings.temporary_dir)
            build_started = time.monotonic()
            build_claim_fixture_indexes(conn)
            index_build_seconds = time.monotonic() - build_started
            indexed_storage = storage_metrics(catalogue_root, settings.temporary_dir)
            write_started = time.monotonic()
            write_asset = str(
                int.from_bytes(hashlib.sha256(f"post-index:{count}".encode()).digest(), "big")
            )
            conn.execute(
                "INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)",
                ("polymarket", "99999999999999999999", "candidate", "clob", write_asset),
            )
            conn.execute(
                "DELETE FROM _identity_claims WHERE asset_id=?",
                [write_asset],
            )
            conn.execute("CHECKPOINT")
            sustained_write_seconds = time.monotonic() - write_started
            after_write_storage = storage_metrics(catalogue_root, settings.temporary_dir)
            row_count = conn.execute("SELECT count(*) FROM _identity_claims").fetchone()[0]
            require(row_count == count, "claim diagnostic write fixture changed row count")
        scales.append(
            {
                "claims": count,
                "population": population,
                "population_seconds": build_started - started,
                "populated_storage": populated_storage,
                "index_build_seconds": index_build_seconds,
                "indexed_storage": indexed_storage,
                "post_index_write_seconds": sustained_write_seconds,
                "post_write_storage": after_write_storage,
            }
        )
        progress.update({"status": "in_progress", "current_scale": None, "scales": scales})
        atomic_write_json(progress_path, progress)
    peak = process_peak_rss_bytes()
    result = {
        "status": "measured",
        "scales": scales,
        "peak_rss_bytes": peak,
        "installer_supported": False,
        "resource_gate": False,
        "reason": "no self-contained production build and sustained-write admission model",
    }
    progress.update({"status": "complete", "scales": scales})
    atomic_write_json(progress_path, progress)
    return result


def semantic_snapshot(snapshot: dict, settled_requests: list[dict]) -> dict:
    return {
        "tables": snapshot["tables"],
        "state": snapshot["state"],
        "private_state": snapshot["private_state"],
        "receipt_state": snapshot["receipt_state"],
        "full_pass": snapshot["state"].get("full_pass"),
        "settled_requests": settled_requests,
        "publication_verified": snapshot["publication_verified"],
        "publication": snapshot["publication"],
    }


def settle_closed_any_absences(current_sync, settings, conn, client, absent_ids, clock) -> int:
    """Prove ordinary-list omissions before confirming each absence natively."""
    omission_marker = {"complete": True, "ids": list(absent_ids)}
    current_store = current_sync.current_store
    existing = current_store.get_state(conn, "api_benchmark_omission_progress")
    settled = 0
    if existing is None:
        result, omitted = current_sync.fetch_unit(
            settings,
            conn,
            client,
            "markets",
            absent_ids,
            progress={"api_benchmark_omission_progress": omission_marker},
        )
        require(omitted == list(absent_ids), "ordinary market lookup omissions differ from seed")
        settled += result.get("responses", 0)
    else:
        require(
            existing == omission_marker,
            "closed-any ordinary omission progress is incompatible",
        )

    progress = current_store.get_state(conn, "api_benchmark_absence_progress") or {"next_absent": 0}
    next_absent = progress.get("next_absent")
    require(
        type(next_absent) is int and 0 <= next_absent <= len(absent_ids),
        "closed-any native absence progress is invalid",
    )
    for index in range(next_absent, len(absent_ids)):
        clock["now"] = BASE_TIME + timedelta(seconds=10_000 + index)
        current_sync.fetch_apply(
            settings,
            conn,
            client,
            "markets",
            [absent_ids[index]],
            native=True,
            absence=True,
            progress={"api_benchmark_absence_progress": {"next_absent": index + 1}},
        )
        settled += 1
    return settled


class SettledUnitStop:
    """Latch a benchmark interruption until the whole supervised operation returns."""

    def __init__(self, limit: int, stop_exception):
        self.limit = limit
        self.stop_exception = stop_exception
        self.enabled = True
        self.triggered = False
        self.settled = 0

    def before_unit(self):
        if self.enabled and self.settled >= self.limit:
            self.triggered = True
            raise self.stop_exception()

    def record(self, responses):
        if type(responses) is int and responses > 0:
            self.settled += responses

    def disable(self):
        self.enabled = False


def run_sample(
    seed: Path,
    output: Path,
    runtime_package: Path,
    budget: Budget,
    profile: str,
    label: str,
    warmup=False,
    workload: str | None = None,
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
        workload,
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
    require(
        result.get("measurement_version") == MEASUREMENT_VERSION,
        "worker omitted current measurement attribution; rerun with the current harness",
    )
    result["cleanup_deferred"] = bool(result.get("cleanup_warnings"))
    if workload is not None:
        require(result.get("workload") == workload, "sample workload provenance mismatch")
        require(result.get("oracle_pass") is True, "API workload oracle failed")
    if warmup:
        profiles = (
            [output / f"profile-{label}.prof"]
            if workload is not None
            else [output / f"profile-{label}-part{part}.prof" for part in (0, 1)]
        )
        result["profile_files"] = [str(path) for path in profiles]
        result["profile_summary"] = profile_summary(profiles)
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


def frozen_gamma_factory(gamma_client, domain_clock: dict):
    """Bind synthetic HTTP receipt timestamps to the worker's frozen clock."""

    def gamma(*args, **kwargs):
        kwargs["now"] = lambda: domain_clock["now"] + timedelta(microseconds=1)
        return gamma_client(*args, **kwargs)

    return gamma


def record_settled_attempt(ledger: list[dict], args, kwargs) -> None:
    """Capture scheduler-attempt writes only after their enclosing unit commits."""
    attempt = kwargs.get("attempts")
    if attempt is None:
        return
    if len(args) < 5 or len(attempt) != 3:
        raise ValueError("malformed settled attempt unit")
    kind, ids, timestamp = attempt
    ledger.append(
        {
            "requested_kind": str(args[3]),
            "requested_ids": [str(value) for value in args[4]],
            "native_request": kwargs.get("native", False),
            "kind": str(kind),
            "ids": [str(value) for value in ids],
            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
        }
    )


def call_and_record_attempt(original_fetch, ledger: list[dict], *args, **kwargs):
    """Record only after the fetch unit returns, proving its transaction settled."""
    result = original_fetch(*args, **kwargs)
    record_settled_attempt(ledger, args, kwargs)
    return result


def worker(
    seed: Path,
    root: Path,
    output: Path,
    label: str,
    profile_name: str,
    stop_after: int,
    profile: Path | None,
    workload: str | None = None,
) -> int:
    from oddsfox_catalogue import current_reconcile, current_runtime, current_sync
    from oddsfox_catalogue.current_config import CurrentLimits, load_current_settings
    from oddsfox_catalogue.gamma.http import CaptureStopped
    from oddsfox_catalogue.runlock import run_lock

    started = time.monotonic()
    expected_package = Path(os.environ["FULL_PASS_EXPECTED_PACKAGE"]).resolve()
    actual_package = Path(__import__("oddsfox_catalogue").__file__).resolve().parent
    if actual_package != expected_package:
        raise ValueError("worker imported a different runtime than requested")
    worker_setup_started = time.monotonic()
    workspace_seed = seed / "workspace" if workload is not None else seed
    provider_seed = seed / "provider" if workload is not None else seed
    copy_root(workspace_seed, root)
    trace = output / f"trace-{label}.jsonl"
    audit = output / f"source-{label}.jsonl"
    timings = {"acquire_apply_s": 0.0, "publish_verify_s": 0.0}
    http_attempts = downloaded_bytes = 0
    peak_rss_bytes = 0
    settled = []
    cleanup_warnings = []
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

    gamma = frozen_gamma_factory(original_gamma, clock)

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

    if workload is None:
        current_reconcile.fetch_unit = traced_fetch
    current_reconcile._now = lambda: clock["now"]
    current_runtime.GammaClient = gamma
    current_runtime.datetime = FrozenDateTime
    try:
        with CapacitySource(
            provider_seed if workload is not None else seed,
            audit,
            delay_s=0,
            source_root=provider_seed if workload is not None else None,
        ) as source:
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
            probe_parameters = inspect.signature(current_sync.probe_policy).parameters
            optional_probe = getattr(current_sync, "probe_api_optimizations", None)
            optional_probe_parameters = (
                inspect.signature(optional_probe).parameters if optional_probe else {}
            )
            if workload is not None:
                if optional_probe is not None:
                    probe["api_optimizations"] = True
                    if "market_closed_any" in optional_probe_parameters:
                        probe["market_closed_any"] = workload == "closed_any"
                    if "keyset_discovery" in optional_probe_parameters:
                        probe["keyset_discovery"] = workload.startswith("discovery_")
                else:
                    if "market_closed_any" in probe_parameters:
                        probe["market_closed_any"] = workload == "closed_any"
                    if "keyset_discovery" in probe_parameters:
                        probe["keyset_discovery"] = workload.startswith("discovery_")
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
            optimized_capability = None
            if workload is not None and optional_probe is not None:
                expected_capability = (
                    "market_closed_any" if workload == "closed_any" else "keyset_discovery"
                )
                capability = probe_result.get("capabilities", {}).get(expected_capability, {})
                require(
                    capability.get("supported") is True,
                    f"synthetic provider did not certify {expected_capability}",
                )
                optimized_capability = expected_capability
            setup_http_attempts = probe_result.get("http_attempts", 0)
            setup_downloaded_bytes = probe_result.get("downloaded_bytes", 0)
            runtime_sequence_started = time.monotonic()
            if workload is not None:
                api_result = run_api_workload(
                    settings,
                    current_runtime,
                    current_sync,
                    run_lock,
                    source,
                    root,
                    seed,
                    workload,
                    domain_clock=clock,
                    profile=profile,
                    optimized_capability=optimized_capability,
                )
                runtime_sequence_ended = api_result["_runtime_ended"]
                operation_attempts = sum(
                    result.get("http_attempts", 0) for result in api_result["operation_results"]
                )
                operation_bytes = sum(
                    result.get("downloaded_bytes", 0) for result in api_result["operation_results"]
                )
                measured_attempts, measured_bytes = validate_source_accounting(
                    source.stats,
                    operation_attempts + setup_http_attempts,
                    operation_bytes + setup_downloaded_bytes,
                    source.failure,
                )
                worker_peak_before_oracle_bytes = api_result["worker_peak_before_oracle_bytes"]
                phases = phase_attribution(
                    worker_setup_started,
                    runtime_sequence_started,
                    runtime_sequence_ended,
                    api_result["_oracle_started"],
                    api_result["_oracle_ended"],
                )
                print(
                    json.dumps(
                        {
                            "measurement_version": MEASUREMENT_VERSION,
                            "platform": sys.platform,
                            **api_result,
                            "wall_s": time.monotonic() - started,
                            "http_attempts": measured_attempts,
                            "downloaded_bytes": measured_bytes,
                            "runtime_reserved_attempts": operation_attempts + setup_http_attempts,
                            "runtime_downloaded_bytes": operation_bytes + setup_downloaded_bytes,
                            "setup_wall_s": setup_wall_s,
                            **phases,
                            "worker_peak_before_oracle_bytes": worker_peak_before_oracle_bytes,
                            "worker_peak_before_oracle_includes_setup_provider_and_runtime": True,
                            "cleanup_warnings": api_result["cleanup_warnings"],
                            "cleanup_deferred": api_result["cleanup_deferred"],
                            "setup_http_attempts": setup_http_attempts,
                            "setup_downloaded_bytes": setup_downloaded_bytes,
                            "peak_rss_bytes": max(
                                worker_peak_before_oracle_bytes,
                                max(
                                    (
                                        result.get("peak_rss_bytes", 0)
                                        for result in api_result["operation_results"]
                                    ),
                                    default=0,
                                ),
                            ),
                            "source_stats": source.stats,
                            "source_failure": source.failure,
                            "runtime_source": str(actual_package),
                            "settings": settings_summary,
                        },
                        default=str,
                    )
                )
                return 0
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
                cleanup_warnings.extend(publication_cleanup_warnings(result, step))
            runtime_sequence_ended = time.monotonic()
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
        worker_peak_before_oracle_bytes = max(peak_rss_bytes, process_peak_rss_bytes())
        oracle_started = time.monotonic()
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
        semantic = semantic_snapshot(snapshot, settled)
        semantic_hash = canonical_hash(semantic)
        oracle_ended = time.monotonic()
        phases = phase_attribution(
            worker_setup_started,
            runtime_sequence_started,
            runtime_sequence_ended,
            oracle_started,
            oracle_ended,
        )
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
                    "measurement_version": MEASUREMENT_VERSION,
                    "platform": sys.platform,
                    **phases,
                    "worker_peak_before_oracle_bytes": worker_peak_before_oracle_bytes,
                    "worker_peak_before_oracle_includes_setup_provider_and_runtime": True,
                    "cleanup_warnings": cleanup_warnings,
                    "cleanup_deferred": bool(cleanup_warnings),
                    "setup_http_attempts": setup_http_attempts,
                    "setup_downloaded_bytes": setup_downloaded_bytes,
                    "peak_rss_bytes": max(peak_rss_bytes, process_peak_rss_bytes()),
                    "semantic_hash": semantic_hash,
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
    p.add_argument("--_prepare_api_seed", choices=API_WORKLOADS, help=argparse.SUPPRESS)
    p.add_argument("--_claim_diagnostics", action="store_true", help=argparse.SUPPRESS)
    p.add_argument(
        "--_validate", nargs=2, metavar=("BASELINE", "CANDIDATE_PACKAGE"), help=argparse.SUPPRESS
    )
    p.add_argument("--_profile", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--_workload", choices=API_WORKLOADS, help=argparse.SUPPRESS)
    return p


def _save(path: Path, report: dict):
    temp = path.with_suffix(".pending")
    temp.write_text(json.dumps(report, indent=2, default=str) + "\n")
    temp.replace(path)


def run(args) -> int:
    if args._sample:
        seed, root, label, stop = args._sample
        return worker(
            Path(seed),
            Path(root),
            Path(args.output),
            label,
            args.profile,
            int(stop),
            args._profile,
            args._workload,
        )
    if args._prepare:
        try:
            events, markets = PROFILES[args.profile]
            seed = Path(args.output) / "seed"
            distribution = create_seed(seed, events, markets)
            runtime = Path(args.output) / "runtime" / "oddsfox_catalogue"
            runtime.parent.mkdir(parents=True, exist_ok=True)
            runtime_hash = copy_runtime(runtime, Path(oddsfox_catalogue.__file__).resolve().parent)
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
    if args._prepare_api_seed:
        try:
            seed = Path(args.output)
            require(not seed.exists(), "API seed output must be fresh")
            metadata = create_api_seed(seed, args._prepare_api_seed)
            print(
                json.dumps(
                    {
                        "status": "prepared",
                        **metadata,
                        "runtime_source_hashes": source_hashes(
                            Path(__import__("oddsfox_catalogue").__file__).resolve().parent
                        ),
                    }
                )
            )
            return 0
        except Exception as exc:
            print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
            return 3
    if args._claim_diagnostics:
        try:
            result = run_claim_scale_diagnostics(Path(args.output))
            actual_package = Path(__import__("oddsfox_catalogue").__file__).resolve().parent
            expected_package = Path(os.environ["FULL_PASS_EXPECTED_PACKAGE"]).resolve()
            require(
                actual_package == expected_package, "claim diagnostic imported the wrong runtime"
            )
            result["runtime_source_hashes"] = source_hashes(actual_package)
            (Path(args.output) / "claim-scale-report.json").write_text(
                json.dumps(result, sort_keys=True, indent=2) + "\n"
            )
            print(json.dumps(result))
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
            not is_link_or_reparse(baseline_dir) and baseline_dir.is_dir(),
            "baseline must be a regular directory",
        )
        validate_disjoint(output, baseline_dir)
    require(not output.exists(), "output must be a fresh directory")
    output.mkdir(parents=True)
    report = {
        "kind": "synthetic-full-pass",
        "measurement_version": MEASUREMENT_VERSION,
        "platform": sys.platform,
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
        for signum in operator_signals(include_interrupt=True)
    }
    try:
        return run(parser().parse_args())
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
