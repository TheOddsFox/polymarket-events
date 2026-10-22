#!/usr/bin/env python3
"""Offline full-pass characterization with frozen synthetic input.

Never contacts Gamma. Baseline mode creates a deterministic seed and saves the
runtime source used for comparison; --compare measures that runtime against the
current checkout. Smoke runs exercise the same path but never qualify a change.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
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
import statistics
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
    is_symlink,
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
WARMUP_DIAGNOSTICS_VERSION = 1
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


def _warmup_native_profiles(root: Path, output: Path, label: str, captured: dict, settings) -> dict:
    """Replay the slowest actual bounded lookup/write on a disposable clone."""
    profile_dir = output / f"native-profile-{label}"
    profile_dir.mkdir(parents=True, exist_ok=False)
    database = profile_dir / "diagnostic.duckdb"
    temporary_dir = profile_dir / "temporary"
    temporary_dir.mkdir()
    shutil.copy2(root / "catalogue.duckdb", database)
    artifacts = {}
    try:
        if not {"shared_lookup", "receipt_write"}.issubset(captured):
            return {
                "status": "unavailable",
                "reason": "warm-up did not capture both a shared lookup and receipt write",
                "captured_categories": sorted(captured),
            }
        plans = {}
        with duckdb.connect(str(database)) as conn:
            from oddsfox_catalogue.current_config import _configure_connection

            _configure_connection(conn, settings)
            conn.execute("SET TimeZone='UTC'")
            conn.execute("SET temp_directory=?", [str(temporary_dir)])
            conn.execute(
                "SET max_temp_directory_size=?",
                [f"{settings.limits.max_temp_bytes}B"],
            )
            for category in ("shared_lookup", "receipt_write"):
                statement = captured[category]
                suffix = "lookup" if category == "shared_lookup" else "receipt-write"
                profile = profile_dir / f"{suffix}.json"
                plan = profile_dir / f"{suffix}-explain.json"
                sql, parameters = statement["sql"], statement["parameters"]
                explain = conn.execute("EXPLAIN " + sql, parameters).fetchall()
                plans[category] = [str(row[-1]) for row in explain]
                conn.execute("PRAGMA enable_profiling='json'")
                conn.execute(
                    "PRAGMA profiling_output='" + profile.as_posix().replace("'", "''") + "'"
                )
                if category == "receipt_write":
                    conn.execute("BEGIN TRANSACTION")
                statement_error = None
                try:
                    if parameters is None:
                        conn.execute(sql)
                    else:
                        conn.execute(sql, parameters)
                    conn.fetchall()
                except BaseException as exc:
                    statement_error = exc
                rollback_error = None
                if category == "receipt_write":
                    try:
                        conn.execute("ROLLBACK")
                    except BaseException as exc:
                        rollback_error = exc
                if isinstance(statement_error, (KeyboardInterrupt, Terminated)):
                    if rollback_error is not None:
                        raise statement_error from rollback_error
                    raise statement_error
                if isinstance(rollback_error, (KeyboardInterrupt, Terminated)):
                    if statement_error is not None:
                        raise rollback_error from statement_error
                    raise rollback_error
                if statement_error is not None:
                    if rollback_error is not None:
                        raise statement_error from rollback_error
                    raise statement_error
                if rollback_error is not None:
                    raise rollback_error
                conn.execute("PRAGMA disable_profiling")
                plan.write_text(json.dumps({"category": category, "plan": plans[category]}))
                artifacts[f"{category}_profile"] = str(profile)
                artifacts[f"{category}_explain"] = str(plan)
                artifacts[f"{category}_observed_wall_s"] = statement["observed_wall_s"]
                artifacts[f"{category}_parameter_bytes"] = statement["parameter_bytes"]
        for artifact in artifacts.values():
            if isinstance(artifact, str):
                require(
                    Path(artifact).is_file(), "DuckDB native profiling artifact was not written"
                )
                require(
                    Path(artifact).stat().st_size <= 1024 * 1024, "native profile exceeded 1 MiB"
                )
        return {
            "status": "captured",
            "artifacts": artifacts,
            "replayed_actual_statements": True,
            "receipt_write_rolled_back": True,
            "interpretation": "native query profiles are separate from client binding and conversion costs",
        }
    except Exception as exc:
        return {"status": "unavailable", "reason": f"{type(exc).__name__}: {exc}"}
    finally:
        active_error = sys.exc_info()[1]
        try:
            database.unlink(missing_ok=True)
        except OSError:
            if not isinstance(active_error, (KeyboardInterrupt, Terminated)):
                raise


def new_attribution_metrics() -> dict:
    return {
        "statement_count": 0,
        "fetchone_count": 0,
        "fetchall_count": 0,
        "fetchmany_count": 0,
        "parameter_scalar_count": 0,
        "parameter_list_element_count": 0,
        "sql_execute_wall_s": 0.0,
        "result_materialization_wall_s": 0.0,
        "captured_statements": {},
    }


class AttributedConnection:
    """Connection proxy used only by direct, unprofiled characterization warm-ups."""

    def __init__(self, context_manager, metrics):
        self._context_manager = context_manager
        self._connection = None
        self._metrics = metrics
        self._current_statement = None

    def _finish_statement(self):
        statement = self._current_statement
        if statement is None:
            return
        category = statement.get("category")
        if category is not None:
            previous = self._metrics["captured_statements"].get(category)
            if previous is None or statement["observed_wall_s"] > previous["observed_wall_s"]:
                self._metrics["captured_statements"][category] = statement
        self._current_statement = None

    def __enter__(self):
        self._connection = self._context_manager.__enter__()
        return self

    def __exit__(self, *args):
        finish_error = None
        close_error = None
        body_error = args[1] if len(args) > 1 and isinstance(args[1], BaseException) else None
        try:
            self._finish_statement()
        except BaseException as exc:
            finish_error = exc
        try:
            suppress = self._context_manager.__exit__(*args)
        except BaseException as exc:
            close_error = exc

        # An operator stop must win over ordinary finalization/close errors, and
        # must never be hidden by the wrapped context manager's suppression.
        operator_error = next(
            (
                error
                for error in (finish_error, body_error, close_error)
                if isinstance(error, (KeyboardInterrupt, Terminated))
            ),
            None,
        )
        if operator_error is not None:
            chained_error = next(
                (
                    error
                    for error in (finish_error, body_error, close_error)
                    if error is not None and error is not operator_error
                ),
                None,
            )
            if chained_error is not None:
                raise operator_error from chained_error
            raise operator_error

        if close_error is not None:
            if finish_error is not None:
                raise finish_error from close_error
            if body_error is not None:
                return False
            raise close_error
        if finish_error is not None:
            if body_error is not None:
                return False
            raise finish_error
        if (
            args
            and isinstance(args[0], type)
            and issubclass(args[0], (KeyboardInterrupt, Terminated))
        ):
            return False
        return suppress

    def execute(self, query, parameters=None):
        self._finish_statement()
        params = parameters if parameters is not None else ()
        self._metrics["statement_count"] += 1
        if isinstance(params, (list, tuple)):
            self._metrics["parameter_scalar_count"] += sum(
                not isinstance(value, (list, tuple)) for value in params
            )
            self._metrics["parameter_list_element_count"] += sum(
                len(value) for value in params if isinstance(value, (list, tuple))
            )
        started = time.perf_counter()
        try:
            if parameters is None:
                self._connection.execute(query)
            else:
                self._connection.execute(query, parameters)
        finally:
            execute_elapsed = time.perf_counter() - started
            self._metrics["sql_execute_wall_s"] += execute_elapsed
        normalized_sql = str(query)
        lowered_sql = normalized_sql.lower()
        category = None
        if "_receipts" in lowered_sql and lowered_sql.lstrip().startswith(
            ("insert", "update", "delete", "replace")
        ):
            category = "receipt_write"
        elif (
            lowered_sql.lstrip().startswith(("select ", "select\n", "select\t"))
            and any(
                table in lowered_sql
                for table in (
                    "events",
                    "markets",
                    "_receipts",
                    "_membership",
                    "_identity_claims",
                )
            )
            and (" where " in lowered_sql or " join " in lowered_sql or " limit " in lowered_sql)
            and (len(params) > 0 or " limit " in lowered_sql)
            and (
                sum(len(value) if isinstance(value, (list, tuple)) else 1 for value in params)
                <= 2000
            )
        ):
            category = "shared_lookup"
        if category and len(normalized_sql) <= 65536:
            try:
                captured_params = copy.deepcopy(params)
                encoded_params = json.dumps(captured_params, default=str, ensure_ascii=True)
            except Exception:
                captured_params = None
                encoded_params = "x" * (256 * 1024 + 1)
            if len(encoded_params.encode("utf-8")) <= 256 * 1024:
                self._current_statement = {
                    "category": category,
                    "sql": normalized_sql,
                    "parameters": captured_params,
                    "observed_wall_s": execute_elapsed,
                    "parameter_bytes": len(encoded_params.encode("utf-8")),
                }
        return self

    def _fetch(self, method, *args):
        self._metrics[f"{method}_count"] += 1
        started = time.perf_counter()
        try:
            return getattr(self._connection, method)(*args)
        finally:
            elapsed = time.perf_counter() - started
            self._metrics["result_materialization_wall_s"] += elapsed
            if self._current_statement is not None:
                self._current_statement["observed_wall_s"] += elapsed

    def fetchone(self):
        return self._fetch("fetchone")

    def fetchall(self):
        return self._fetch("fetchall")

    def fetchmany(self, size=1):
        return self._fetch("fetchmany", size)

    def __getattr__(self, name):
        return getattr(self._connection, name)


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
        require(not is_symlink(path), "runtime source contains a link or reparse point")
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
        require(not is_symlink(path), "runtime package contains a link or reparse point")
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
    require(root.is_dir() and not is_symlink(root), "baseline tree must be a regular directory")
    for path in root.rglob("*"):
        require(not is_symlink(path), "baseline contains a link or reparse point")
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
    attribution: bool = False,
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
    if attribution:
        argv.append("--_attribution")
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


def claim_resource_diagnostics(
    package: Path, output: Path, budget: Budget, *, extended: bool = False
) -> dict:
    """Measure synthetic claim-index cost; do not infer a production admission model."""
    package = safe_path(package)
    output = safe_path(output)
    require(package.is_dir() and not is_symlink(package), "claim candidate package is unsafe")
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
            "--_claim_diagnostics_v4" if extended else "--_claim_diagnostics",
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


def claim_fixture_rows(
    start: int, end: int, *, adversarial: bool = False
) -> list[tuple[str, str, str, str, str]]:
    """Generate one bounded batch of valid synthetic native identity claims."""
    rows = []
    special = {
        0: ("polymarket", "10000000000000000001", "candidate", "clob", "7"),
        1: ("polymarket", "10000000000000000002", "accepted", "clob", "7"),
        2: ("polymarket", "10000000000000000001", "candidate", "position", "7"),
        3: ("other-venue", "10000000000000000003", "accepted", "clob", "7"),
        4: ("polymarket", "10000000000000000001", "candidate", "clob", "7"),
    }
    for index in range(start, end):
        market_id = f"{10**19 + index // 4:020d}"
        asset_kind = "clob" if index % 4 < 2 else "position"
        role = "candidate" if index % 2 == 0 else "accepted"
        asset_number = int.from_bytes(
            hashlib.sha256(f"claim-fixture:{index // 4}:{asset_kind}".encode()).digest(),
            "big",
        )
        row = ("polymarket", market_id, role, asset_kind, str(asset_number))
        # A tiny deterministic adversarial prefix exercises the exact namespace
        # filter, duplicate preservation and multiple owners at every scale.
        rows.append(special.get(index, row) if adversarial else row)
    return rows


def _claim_multiset(rows):
    from collections import Counter

    return Counter(tuple(row) for row in rows)


def _claim_full_scan_oracle(conn, *, asset_keys=(), market_keys=()):
    """Stream the fixture once and enforce full namespace tuples independently."""
    asset_keys, market_keys = set(asset_keys), set(market_keys)
    assets, markets = [], []
    cursor = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims"
    )
    while batch := cursor.fetchmany(50_000):
        for row in batch:
            if (row[0], row[3], row[4]) in asset_keys:
                assets.append(tuple(row))
            if (row[0], row[1]) in market_keys:
                markets.append(tuple(row))
    return assets, markets


def _claim_lookup_query_plan(conn, column, ids):
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"EXPLAIN SELECT venue, market_id, role, asset_kind, asset_id "
        f"FROM _identity_claims WHERE {column} IN ({placeholders})",
        ids,
    ).fetchall()
    return [str(row[-1]) for row in rows]


def _claim_lookup_explain_analyze(conn, column, ids):
    """Capture DuckDB's executed plan for the exact bounded lookup predicate."""
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"EXPLAIN ANALYZE SELECT venue, market_id, role, asset_kind, asset_id "
        f"FROM _identity_claims WHERE {column} IN ({placeholders})",
        ids,
    ).fetchall()
    return [str(row[-1]) for row in rows]


def _replace_market_claims_diagnostic(conn, venue: str, market_id: str) -> list[tuple]:
    """Replace one complete namespaced market claim multiset and return its rows."""
    rows = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims "
        "WHERE venue=? AND market_id=?",
        [venue, market_id],
    ).fetchall()
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM _identity_claims WHERE venue=? AND market_id=?", [venue, market_id]
        )
        if rows:
            conn.executemany("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", rows)
        conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            conn.execute("ROLLBACK")
        raise
    restored = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims "
        "WHERE venue=? AND market_id=?",
        [venue, market_id],
    ).fetchall()
    require(
        _claim_multiset(restored) == _claim_multiset(rows),
        "market replacement failed to restore exact namespaced claim rows",
    )
    return rows


def _claim_lookup_diagnostics(conn, count):
    from oddsfox_catalogue.current_store import _select_claims_by_assets, _select_claims_by_markets

    fixture = claim_fixture_rows(0, min(count, 512), adversarial=True)
    asset_keys = list(dict.fromkeys((row[0], row[3], row[4]) for row in fixture))
    market_keys = list(dict.fromkeys((row[0], row[1]) for row in fixture))
    expected_assets, expected_markets = _claim_full_scan_oracle(
        conn, asset_keys=asset_keys, market_keys=market_keys
    )

    def measure(selector, keys, expected):
        samples = []
        result = []
        for _ in range(3):
            started = time.monotonic()
            result = selector(conn, keys)
            samples.append(time.monotonic() - started)
            require(
                _claim_multiset(result) == _claim_multiset(expected),
                "selective claim lookup disagrees with exact namespace oracle",
            )
        return {"samples_s": samples, "median_s": sorted(samples)[1], "rows": len(result)}

    asset_ids = list(dict.fromkeys(key[2] for key in asset_keys))
    market_ids = list(dict.fromkeys(key[1] for key in market_keys))
    return {
        "query_keys": {
            "asset_namespace_tuples": len(asset_keys),
            "market_namespace_tuples": len(market_keys),
        },
        "expected_rows": {"asset": len(expected_assets), "market": len(expected_markets)},
        "asset_query_plan": _claim_lookup_query_plan(conn, "asset_id", asset_ids),
        "market_query_plan": _claim_lookup_query_plan(conn, "market_id", market_ids),
        "asset_lookup": measure(_select_claims_by_assets, asset_keys, expected_assets),
        "market_lookup": measure(_select_claims_by_markets, market_keys, expected_markets),
    }


def _claim_storage_metrics(directory: Path, temporary_directory: Path) -> dict:
    files = [path for path in directory.rglob("*") if path.is_file() and not is_symlink(path)]
    temporary_files = [
        path for path in temporary_directory.rglob("*") if path.is_file() and not is_symlink(path)
    ]
    database_files = [path for path in files if path.suffix == ".duckdb"]
    wal_files = [path for path in files if path.suffix == ".wal"]
    return {
        "measurement": "sampled filesystem sizes; not peak storage reservation",
        "database_bytes": sum(path.stat().st_size for path in database_files),
        "wal_bytes": sum(path.stat().st_size for path in wal_files),
        "temporary_bytes": sum(path.stat().st_size for path in temporary_files),
        "catalogue_files_bytes": sum(path.stat().st_size for path in files),
        "total_bytes": sum(path.stat().st_size for path in files),
    }


def _claim_rss_metrics(peak: int) -> dict:
    return {
        "worker_lifetime_peak_rss_bytes": peak,
        "rss_measurement": "worker lifetime peak; not an isolated query or index-build peak",
        "peak_rss_bytes": peak,
    }


def _claim_write_diagnostics(conn, count: int, *, adversarial: bool) -> dict:
    """Exercise bounded replacement, conflict-owner and asset deletion writes, then restore rows."""
    if type(count) is not int or count <= 0:
        raise ValueError("claim write diagnostics require a positive fixture size")
    fixture = claim_fixture_rows(0, 1, adversarial=adversarial)[0]
    target_market = fixture[1]
    target_asset = fixture[4]
    replacement_rows = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims "
        "WHERE market_id=?",
        [target_market],
    ).fetchall()
    before_count = conn.execute("SELECT count(*) FROM _identity_claims").fetchone()[0]
    operations = {}

    started = time.monotonic()
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM _identity_claims WHERE market_id=?", [target_market])
        conn.executemany("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", replacement_rows)
        conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            conn.execute("ROLLBACK")
        raise
    operations["bounded_replacement_seconds"] = time.monotonic() - started
    replaced_rows = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims "
        "WHERE market_id=?",
        [target_market],
    ).fetchall()
    require(
        _claim_multiset(replaced_rows) == _claim_multiset(replacement_rows),
        "bounded replacement failed to restore exact market claims",
    )

    conflict_market = "99999999999999999999"
    conflict_row = (fixture[0], conflict_market, "candidate", fixture[3], target_asset)
    started = time.monotonic()
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", conflict_row)
        conn.execute("DELETE FROM _identity_claims WHERE market_id=?", [conflict_market])
        conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            conn.execute("ROLLBACK")
        raise
    operations["conflicting_owner_insert_delete_seconds"] = time.monotonic() - started
    require(
        conn.execute(
            "SELECT count(*) FROM _identity_claims WHERE market_id=?", [conflict_market]
        ).fetchone()[0]
        == 0,
        "conflicting owner cleanup left diagnostic rows behind",
    )

    asset_rows = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims WHERE asset_id=?",
        [target_asset],
    ).fetchall()
    started = time.monotonic()
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM _identity_claims WHERE asset_id=?", [target_asset])
        conn.executemany("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", asset_rows)
        conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            conn.execute("ROLLBACK")
        raise
    operations["asset_deletion_restore_seconds"] = time.monotonic() - started
    restored_asset_rows = conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims WHERE asset_id=?",
        [target_asset],
    ).fetchall()
    require(
        _claim_multiset(restored_asset_rows) == _claim_multiset(asset_rows),
        "asset deletion failed to restore exact claims",
    )
    after_count = conn.execute("SELECT count(*) FROM _identity_claims").fetchone()[0]
    require(after_count == before_count == count, "claim write diagnostics changed fixture rows")
    return operations


@contextlib.contextmanager
def _claim_diagnostic_store(settings, connector, locker, after_close):
    """Acquire writer lock before opening DuckDB; sample closed files before unlocking."""
    with locker(settings.run_lock_path):
        with connector(settings) as conn:
            yield conn
        after_close()


def populate_claim_fixture(
    conn, count: int, *, batch_size=10_000, on_batch=None, adversarial=False
) -> dict:
    """Populate claims in independently committed, bounded transactions."""
    if type(count) is not int or count < 0:
        raise ValueError("claim fixture count must be a nonnegative integer")
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("claim fixture batch size must be a positive integer")
    completed = batches = 0
    for start in range(0, count, batch_size):
        end = min(count, start + batch_size)
        rows = claim_fixture_rows(start, end, adversarial=adversarial)
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


def run_claim_scale_diagnostics(root: Path, *, extended: bool = False) -> dict:
    """Measure synthetic selective lookups, index cost and bounded writes offline."""
    from oddsfox_catalogue.current_config import CurrentSettings, connect_store
    from oddsfox_catalogue.current_store import initialize
    from oddsfox_catalogue.runlock import run_lock

    root.mkdir(parents=True, exist_ok=False)
    scales = []
    progress_path = root / "claim-scale-progress.json"
    progress = {"status": "in_progress", "scales": []}
    atomic_write_json(progress_path, progress)

    for count in (200_000, 1_000_000):
        catalogue_root = root / f"claims-{count}"
        catalogue_root.mkdir()
        validate_root(catalogue_root, initialize=True)
        settings = CurrentSettings(catalogue_root)
        started = time.monotonic()
        post_close_storage = {}

        def measure_after_close(
            storage=post_close_storage,
            root=catalogue_root,
            temporary_directory=settings.temporary_dir,
        ):
            storage.update(_claim_storage_metrics(root, temporary_directory))

        with _claim_diagnostic_store(
            settings, connect_store, run_lock, measure_after_close
        ) as conn:
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

            population_started = time.monotonic()
            population = populate_claim_fixture(
                conn,
                count,
                on_batch=save_population_progress,
                adversarial=extended,
            )
            population_seconds = time.monotonic() - population_started
            checkpoint_started = time.monotonic()
            conn.execute("CHECKPOINT")
            population_checkpoint_seconds = time.monotonic() - checkpoint_started
            populated_storage = _claim_storage_metrics(catalogue_root, settings.temporary_dir)
            unindexed_queries = _claim_lookup_diagnostics(conn, count) if extended else None
            pre_index_write_operations = {}
            pre_index_write_checkpoint_seconds = None
            pre_index_write_storage = None
            if extended:
                pre_index_write_operations = _claim_write_diagnostics(conn, count, adversarial=True)
                checkpoint_started = time.monotonic()
                conn.execute("CHECKPOINT")
                pre_index_write_checkpoint_seconds = time.monotonic() - checkpoint_started
                pre_index_write_storage = _claim_storage_metrics(
                    catalogue_root, settings.temporary_dir
                )
            build_started = time.monotonic()
            build_claim_fixture_indexes(conn)
            index_build_seconds = time.monotonic() - build_started
            indexed_storage = _claim_storage_metrics(catalogue_root, settings.temporary_dir)
            indexed_queries = _claim_lookup_diagnostics(conn, count) if extended else None
            write_started = time.monotonic()
            if extended:
                write_operations = _claim_write_diagnostics(conn, count, adversarial=True)
            else:
                write_asset = str(
                    int.from_bytes(hashlib.sha256(f"post-index:{count}".encode()).digest(), "big")
                )
                write_operations = {}
                conn.execute(
                    "INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)",
                    ("polymarket", "99999999999999999999", "candidate", "clob", write_asset),
                )
                conn.execute("DELETE FROM _identity_claims WHERE asset_id=?", [write_asset])
            checkpoint_started = time.monotonic()
            conn.execute("CHECKPOINT")
            write_checkpoint_seconds = time.monotonic() - checkpoint_started
            sustained_write_seconds = time.monotonic() - write_started
            after_write_storage = _claim_storage_metrics(catalogue_root, settings.temporary_dir)
            row_count = conn.execute("SELECT count(*) FROM _identity_claims").fetchone()[0]
            require(row_count == count, "claim diagnostic write fixture changed row count")
        scales.append(
            {
                "claims": count,
                "population": population,
                "population_seconds": population_seconds if extended else build_started - started,
                "population_checkpoint_seconds": population_checkpoint_seconds,
                "populated_storage": populated_storage,
                "index_build_seconds": index_build_seconds,
                "indexed_storage": indexed_storage,
                "post_index_write_seconds": sustained_write_seconds,
                "post_index_write_checkpoint_seconds": write_checkpoint_seconds,
                "pre_index_write_operations": pre_index_write_operations,
                "pre_index_write_checkpoint_seconds": pre_index_write_checkpoint_seconds,
                "pre_index_write_storage": pre_index_write_storage,
                "post_index_write_operations": write_operations,
                "post_write_storage": after_write_storage,
                "post_close_storage": post_close_storage,
            }
        )
        if extended:
            scales[-1]["unindexed_selective_queries"] = unindexed_queries
            scales[-1]["indexed_selective_queries"] = indexed_queries
        progress.update({"status": "in_progress", "current_scale": None, "scales": scales})
        atomic_write_json(progress_path, progress)
    peak = process_peak_rss_bytes()
    result = {
        "status": "measured",
        "scales": scales,
        **_claim_rss_metrics(peak),
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
    attribution: bool = False,
) -> dict:
    require(not attribution or warmup, "attribution is available only for unscored warm-ups")
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
        output / f"profile-{label}.prof" if warmup and not attribution else None,
        workload,
        attribution=attribution,
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
        if attribution:
            diagnostics = result.get("warmup_diagnostics")
            require(
                isinstance(diagnostics, dict)
                and diagnostics.get("version") == 1
                and diagnostics.get("attribution_mode") == "direct_unprofiled",
                "V5 warm-up omitted direct unprofiled attribution",
            )
            result["profile_files"] = []
            result["profile_summary"] = None
        else:
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
    attribution: bool = False,
) -> int:
    from oddsfox_catalogue import current_reconcile, current_runtime, current_sync
    from oddsfox_catalogue.current_config import CurrentLimits, load_current_settings
    from oddsfox_catalogue.gamma.http import CaptureStopped
    from oddsfox_catalogue.runlock import run_lock

    require(not attribution or profile is None, "V5 attribution warm-ups must not enable cProfile")

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
    original_connect_store = current_runtime.connect_store
    instrumented_store_functions = {}
    store_helper_metrics = {}
    count = 0
    current_limit = [stop_after]

    attribution_metrics = new_attribution_metrics()

    if attribution:

        def attributed_connect_store(*args, **kwargs):
            return AttributedConnection(
                original_connect_store(*args, **kwargs), attribution_metrics
            )

        current_runtime.connect_store = attributed_connect_store
        for function_name in (
            "apply",
            "_select_in",
            "_write_values",
            "_write_receipts",
            "_select_claims_by_assets",
            "_select_claims_by_markets",
        ):
            original = getattr(current_sync.current_store, function_name, None)
            if original is None:
                continue
            instrumented_store_functions[function_name] = original
            store_helper_metrics[function_name] = {"calls": 0, "wall_s": 0.0}

            def timed_helper(*args, __name=function_name, __original=original, **kwargs):
                metric = store_helper_metrics[__name]
                metric["calls"] += 1
                started_at = time.perf_counter()
                try:
                    return __original(*args, **kwargs)
                finally:
                    metric["wall_s"] += time.perf_counter() - started_at

            setattr(current_sync.current_store, function_name, timed_helper)

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
            for key in attribution_metrics:
                attribution_metrics[key] = {} if key == "captured_statements" else 0
            for metrics in store_helper_metrics.values():
                metrics.update(calls=0, wall_s=0.0)
            runtime_sequence_started = time.monotonic()
            attribution_wall_started = time.perf_counter() if attribution else None
            attribution_cpu_started = time.process_time() if attribution else None
            attribution_helper_wall_s = 0.0
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
                helper_started = time.perf_counter() if attribution else None
                with run_lock(settings.run_lock_path):
                    result = current_runtime.execute(
                        settings,
                        "reconcile",
                        {"full_pass": True},
                        time.monotonic() + settings.limits.max_duration_s,
                    )
                if attribution:
                    attribution_helper_wall_s += time.perf_counter() - helper_started
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
            attribution_runtime_wall_s = (
                time.perf_counter() - attribution_wall_started if attribution else None
            )
            attribution_process_cpu_s = (
                time.process_time() - attribution_cpu_started if attribution else None
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
        worker_peak_before_oracle_bytes = max(peak_rss_bytes, process_peak_rss_bytes())
        native_profile_started = time.monotonic() if attribution else None
        native_profile_diagnostics = (
            _warmup_native_profiles(
                root,
                output,
                label,
                attribution_metrics["captured_statements"],
                settings,
            )
            if attribution
            else None
        )
        native_profile_wall_s = time.monotonic() - native_profile_started if attribution else None
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
                    **(
                        {
                            "warmup_diagnostics": {
                                "version": 1,
                                "attribution_mode": "direct_unprofiled",
                                "production_reported_acquire_apply_s": timings["acquire_apply_s"],
                                "store_helper_timings": {
                                    name: {
                                        **metric,
                                        "inclusive": True,
                                        "overlaps_nested_sql": True,
                                    }
                                    for name, metric in store_helper_metrics.items()
                                },
                                "runtime_execute_wall_s_including_lock_and_publication": (
                                    attribution_helper_wall_s
                                ),
                                "runtime_sequence_direct_wall_s": attribution_runtime_wall_s,
                                "process_cpu_s_all_worker_threads": attribution_process_cpu_s,
                                "sql_execute_wall_s": attribution_metrics["sql_execute_wall_s"],
                                "result_materialization_wall_s": attribution_metrics[
                                    "result_materialization_wall_s"
                                ],
                                "nested_timings_overlap": True,
                                "statement_count": attribution_metrics["statement_count"],
                                "fetchone_count": attribution_metrics["fetchone_count"],
                                "fetchall_count": attribution_metrics["fetchall_count"],
                                "fetchmany_count": attribution_metrics["fetchmany_count"],
                                "parameter_scalar_count": attribution_metrics[
                                    "parameter_scalar_count"
                                ],
                                "parameter_list_element_count": attribution_metrics[
                                    "parameter_list_element_count"
                                ],
                                "counts_are_scored": False,
                                "counter_scope": "reconciliation_runtime_sequence_only",
                                "native_profiles": native_profile_diagnostics,
                                "native_profile_diagnostics_wall_s": native_profile_wall_s,
                                "native_profile_scope": (
                                    "bounded lookup and rolled-back receipt write on a disposable copy; "
                                    "not a binding-overhead estimate"
                                ),
                            }
                        }
                        if attribution
                        else {}
                    ),
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
        current_runtime.connect_store = original_connect_store
        for function_name, original in instrumented_store_functions.items():
            setattr(current_sync.current_store, function_name, original)


def scheduler_diagnostics_worker(seed: Path, root: Path) -> int:
    """Run bounded, read-only scheduler queries at two synthetic scales."""
    actual_package = Path(oddsfox_catalogue.__file__).resolve().parent
    expected_package = Path(os.environ["FULL_PASS_EXPECTED_PACKAGE"]).resolve()
    require(actual_package == expected_package, "scheduler diagnostic imported a different runtime")
    from oddsfox_catalogue.current_config import connect_store, load_current_settings
    from oddsfox_catalogue.current_reconcile import _due, _initialize, _remaining

    def measure_scale(source_root: Path, target_root: Path) -> dict:
        if target_root.exists():
            raise ValueError("scheduler diagnostic root must be fresh")
        copy_root(source_root, target_root)
        settings = load_current_settings(target_root, env=base_environment())
        # The deterministic seed has reconciler tables initialized only in the
        # disposable copy; never mutate the frozen source database.
        with connect_store(settings) as writable:
            _initialize(writable)
        captured = {}

        class CapturingConnection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, query, parameters=None):
                self.last_query = str(query)
                self.last_parameters = parameters
                if parameters is None:
                    return self.connection.execute(query)
                return self.connection.execute(query, parameters)

            def __getattr__(self, name):
                return getattr(self.connection, name)

        with connect_store(settings, read_only=True) as connection:
            event_count = connection.execute("SELECT count(*) FROM events").fetchone()[0]
            market_count = connection.execute("SELECT count(*) FROM markets").fetchone()[0]
            conn = CapturingConnection(connection)
            operations = {
                "urgent_events_due": lambda: _due(
                    conn, "events", False, BASE_TIME, settings.limits.active_recheck_s
                ),
                "residual_markets_due": lambda: _due(
                    conn,
                    "markets",
                    True,
                    BASE_TIME,
                    settings.limits.cold_recheck_s,
                    pass_started=BASE_TIME,
                ),
                "full_pass_remaining": lambda: _remaining(conn, BASE_TIME),
            }
            for name, operation in operations.items():
                elapsed, row_counts = [], []
                query, parameters = None, None
                for _ in range(3):
                    started = time.perf_counter()
                    result = operation()
                    elapsed.append(time.perf_counter() - started)
                    row_counts.append(len(result) if isinstance(result, list) else int(result))
                    query, parameters = conn.last_query, conn.last_parameters
                explain = connection.execute(
                    "EXPLAIN ANALYZE " + query, parameters or []
                ).fetchall()
                plan = [str(row[-1]) for row in explain]
                plan_text = "\n".join(plan)
                require(len(plan_text.encode("utf-8")) <= 1024 * 1024, "scheduler plan too large")
                captured[name] = {
                    "repetitions": 3,
                    "row_counts": row_counts,
                    "median_wall_s": statistics.median(elapsed),
                    "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
                    "explain_analyze": plan,
                    "measurement_scope": "read-only synthetic scheduler diagnostics",
                }
        return {
            "scale": f"{event_count} events / {market_count} markets",
            "measurements": captured,
        }

    started = time.monotonic()
    scale_results = [measure_scale(seed, root / "perf")]
    smoke_root = root / "smoke-seed"
    create_seed(smoke_root, *PROFILES["smoke"])
    scale_results.append(measure_scale(smoke_root, root / "smoke"))
    print(
        json.dumps(
            {
                "status": "measured",
                "scales": scale_results,
                "wall_s": time.monotonic() - started,
                "peak_rss_sampled_bytes": process_peak_rss_bytes(),
                "peak_rss_scope": "whole scheduler diagnostic worker lifetime",
                "source_access": "local synthetic databases only",
                "production_runtime_extrapolation": False,
                "runtime_source": str(actual_package),
            },
            default=str,
        )
    )
    return 0


def _regular_tree_hashes(root: Path) -> dict[str, str]:
    """Hash catalogue content; exclude the mutable root writer-lock file."""
    root = safe_path(root)
    require(root.is_dir() and not is_symlink(root), "catalogue source is not a regular directory")
    result = {}
    for path in sorted(root.rglob("*")):
        require(not is_symlink(path), "catalogue source contains a symlink")
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        require(stat.S_ISREG(mode), "catalogue source contains a special file")
        if path == root / ".catalogue.lock":
            continue
        result[path.relative_to(root).as_posix()] = digest(path)
    return result


def _admit_catalogue_copy(source: Path, target: Path, limits) -> dict[str, int]:
    """Bound a full-tree copy against catalogue retained and filesystem free-space limits."""
    require(
        target.parent.is_dir() and not is_symlink(target.parent),
        "catalogue copy destination parent must be a regular directory",
    )
    size_bytes = 0
    for path in source.rglob("*"):
        require(not is_symlink(path), "catalogue source contains a symlink")
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        require(stat.S_ISREG(mode), "catalogue source contains a special file")
        if path != source / ".catalogue.lock":
            size_bytes += path.stat().st_size
    require(
        size_bytes <= limits.max_retained_bytes,
        f"catalogue copy size {size_bytes} exceeds retained limit {limits.max_retained_bytes}",
    )
    free_bytes = shutil.disk_usage(target.parent).free
    require(
        free_bytes - size_bytes >= limits.free_reserve_bytes,
        "catalogue copy would violate free-space reserve "
        f"(copy={size_bytes}, free={free_bytes}, reserve={limits.free_reserve_bytes})",
    )
    return {
        "copy_bytes": size_bytes,
        "destination_free_bytes_before": free_bytes,
        "free_reserve_bytes": limits.free_reserve_bytes,
        "retained_limit_bytes": limits.max_retained_bytes,
    }


def _remaining_spill_bytes(maximum: int, existing: int) -> int:
    remaining = maximum - existing
    require(remaining > 0, "temporary storage allowance exhausted before oracle")
    return remaining


def catalogue_observation_clock(conn, *, now: datetime | None = None) -> datetime:
    """Return one frozen instant later than all persisted observation clocks."""
    expressions = [
        "SELECT receipt AS observed_at FROM _receipts",
        "SELECT receipt FROM _membership",
        "SELECT latest_receipt FROM _membership",
        "SELECT invalidated_at FROM _identity",
    ]
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    if "_reconcile_attempts" in tables:
        expressions.append("SELECT last_attempt FROM _reconcile_attempts")
    latest = conn.execute(
        "SELECT max(observed_at) FROM (" + " UNION ALL ".join(expressions) + ")"
    ).fetchone()[0]
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if latest is not None:
        latest = latest.astimezone(UTC)
    return max(value for value in (latest, current) if value is not None) + timedelta(
        microseconds=1
    )


def snapshot_catalogue(source: Path, target: Path, requests_path: Path) -> dict:
    """Take a consistent source copy; V6 requests are finalized after clone recalibration."""
    result = snapshot_catalogue_unrecalibrated(source, target, requests_path)
    with duckdb.connect(str(target / "catalogue.duckdb"), read_only=True) as conn:
        conn.execute("SET TimeZone='UTC'")
        requests = fixed_catalogue_requests(conn)
        frozen_clock = catalogue_observation_clock(conn)
    requests_path.parent.mkdir(parents=True, exist_ok=True)
    _save(requests_path, {"requests": requests, "domain_clock": frozen_clock.isoformat()})
    return {
        **result,
        "request_manifest": str(requests_path),
        "request_counts": {
            "closed_batches": len(requests["closed_event_batches"]),
            "active_batches": len(requests["active_event_batches"]),
            "orphan_market_ids": len(requests["orphan_market_ids"]),
        },
        "source_counts": requests["counts"],
        "domain_clock": frozen_clock.isoformat(),
    }


def snapshot_catalogue_unrecalibrated(source: Path, target: Path, requests_path: Path) -> dict:
    """Take a consistent, lock-protected copy of a quiescent catalogue."""
    from oddsfox_catalogue import current_sync
    from oddsfox_catalogue.current_config import CurrentLimits, load_current_settings
    from oddsfox_catalogue.runlock import run_lock

    source, target, requests_path = map(safe_path, (source, target, requests_path))
    validate_disjoint(source, target)
    validate_disjoint(source, requests_path)
    validate_disjoint(target, requests_path)
    require(not target.exists(), "catalogue snapshot destination must be fresh")
    require(not requests_path.exists(), "request manifest destination must be fresh")
    require(not is_symlink(source) and source.is_dir(), "catalogue source root is unsafe")
    validate_tree(source)
    settings = load_current_settings(source)
    with run_lock(settings.run_lock_path):
        copy_admission = _admit_catalogue_copy(source, target, settings.limits)
        before = _regular_tree_hashes(source)
        with duckdb.connect(
            str(source / "catalogue.duckdb"),
            read_only=True,
            config={
                "memory_limit": f"{CurrentLimits().duckdb_memory_bytes}B",
                "threads": str(CurrentLimits().duckdb_threads),
                "max_temp_directory_size": "0B",
            },
        ) as conn:
            conn.execute("SET TimeZone='UTC'")
            states = {
                key: json.loads(value)
                for key, value in conn.execute("SELECT key,value FROM sync_state").fetchall()
            }
            require(
                not current_sync._pending_work(conn),
                "catalogue source has pending acquisition work",
            )
            full_pass = states.get("full_pass")
            require(
                not isinstance(full_pass, dict) or full_pass.get("completed_at") is not None,
                "catalogue source has an unfinished full pass",
            )
            requests = fixed_catalogue_requests(conn)
            frozen_clock = catalogue_observation_clock(conn)
        shutil.copytree(source, target, symlinks=True)
        after = _regular_tree_hashes(source)
        require(before == after, "catalogue source changed during locked snapshot")
    return {
        "status": "snapshotted",
        "source_hashes": before,
        "snapshot_hashes": _regular_tree_hashes(target),
        "copy_admission": copy_admission,
        "request_counts": {
            "closed_batches": len(requests["closed_event_batches"]),
            "active_batches": len(requests["active_event_batches"]),
            "orphan_market_ids": len(requests["orphan_market_ids"]),
        },
        "source_counts": requests["counts"],
        "domain_clock": frozen_clock.isoformat(),
    }


def locked_catalogue_hash(source: Path) -> dict:
    """Recheck the complete source inventory while excluding concurrent writers."""
    from oddsfox_catalogue.current_config import load_current_settings
    from oddsfox_catalogue.runlock import run_lock

    source = safe_path(source)
    require(source.is_dir() and not is_symlink(source), "catalogue source root is unsafe")
    validate_tree(source)
    settings = load_current_settings(source)
    with run_lock(settings.run_lock_path):
        hashes = _regular_tree_hashes(source)
    return {"status": "hashed", "source_hashes": hashes}


def prepare_v6_snapshot(source: Path, requests_path: Path) -> dict:
    """Recalibrate the isolated copy, then freeze its final request set and clock."""
    from oddsfox_catalogue import current_runtime
    from oddsfox_catalogue.current_config import load_current_settings
    from oddsfox_catalogue.current_publish import verify_current

    source, requests_path = map(safe_path, (source, requests_path))
    validate_disjoint(source, requests_path)
    require(source.is_dir() and not is_symlink(source), "V6 snapshot root is unsafe")
    require(
        requests_path.is_file() and not is_symlink(requests_path), "request manifest is missing"
    )
    snapshot_hashes_before_recalibration = _regular_tree_hashes(source)
    settings = load_current_settings(source)
    success, result = current_runtime.supervise(
        settings,
        "publish",
        {"recalibrate": True},
        duration=settings.limits.max_duration_s,
    )
    require(success, result.get("message", "isolated catalogue recalibration failed"))
    require(
        result.get("noop") is False and result.get("valid") is True,
        "isolated catalogue recalibration did not create a fresh release",
    )
    verification = verify_current(settings)
    require(verification.get("valid") is True, "recalibrated snapshot failed verification")
    with duckdb.connect(str(source / "catalogue.duckdb"), read_only=True) as conn:
        conn.execute("SET TimeZone='UTC'")
        requests = fixed_catalogue_requests(conn)
        frozen_clock = catalogue_observation_clock(conn)
    _save(requests_path, {"requests": requests, "domain_clock": frozen_clock.isoformat()})
    after = _regular_tree_hashes(source)
    return {
        "status": "prepared",
        "publication": normalized_publication(result),
        "verification": verification,
        "snapshot_hashes_before_recalibration": snapshot_hashes_before_recalibration,
        "snapshot_hashes": after,
        "request_manifest": str(requests_path),
        "request_counts": {
            "closed_batches": len(requests["closed_event_batches"]),
            "active_batches": len(requests["active_event_batches"]),
            "orphan_market_ids": len(requests["orphan_market_ids"]),
        },
        "source_counts": requests["counts"],
        "domain_clock": frozen_clock.isoformat(),
    }


def fixed_catalogue_requests(conn) -> dict:
    """Freeze deterministic event batches across numeric ranges and fanout tails."""
    counts = {}
    for table in ("events", "markets", "event_markets", "outcomes", "_identity_claims"):
        counts[table] = conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
    batches = {}
    for label, closed, batch_count, fanout_order in (
        ("closed_event_batches", True, 32, False),
        ("active_event_batches", False, 4, True),
    ):
        if fanout_order:
            query = """WITH fanout AS (
                SELECT e.event_id, count(em.market_id) AS n
                FROM events e LEFT JOIN event_markets em USING(venue,event_id)
                WHERE e.venue='polymarket' AND e.closed IS NOT DISTINCT FROM ?
                GROUP BY e.event_id
            ), ranked AS (
                SELECT event_id,n,row_number() OVER (ORDER BY try_cast(event_id AS HUGEINT)) AS rn,
                    count(*) OVER () AS total
                FROM fanout
            ), spread AS (
                SELECT event_id,n,floor((rn-1)*?::DOUBLE/total)::INTEGER AS region,
                    row_number() OVER (
                        PARTITION BY floor((rn-1)*?::DOUBLE/total)::INTEGER
                        ORDER BY n DESC,try_cast(event_id AS HUGEINT)
                    ) AS local_rank
                FROM ranked
            ) SELECT event_id FROM spread WHERE local_rank<=100
                ORDER BY region,local_rank"""
            values = [closed, batch_count, batch_count]
        else:
            query = """WITH ranked AS (
                SELECT event_id,row_number() OVER (ORDER BY try_cast(event_id AS HUGEINT)) AS rn,
                    count(*) OVER () AS total
                FROM events WHERE venue='polymarket' AND closed IS NOT DISTINCT FROM ?
            ), spread AS (
                SELECT event_id,floor((rn-1)*?::DOUBLE/total)::INTEGER AS region,
                    row_number() OVER (
                        PARTITION BY floor((rn-1)*?::DOUBLE/total)::INTEGER
                        ORDER BY try_cast(event_id AS HUGEINT)
                    ) AS local_rank
                FROM ranked
            ) SELECT event_id FROM spread WHERE local_rank<=100 ORDER BY region,local_rank"""
            values = [closed, batch_count, batch_count]
        ids = [str(row[0]) for row in conn.execute(query, values).fetchall()]
        require(len(ids) == batch_count * 100, f"source has fewer than 100 ids per {label} region")
        batches[label] = [ids[index : index + 100] for index in range(0, len(ids), 100)]
    orphan_ids = [
        str(row[0])
        for row in conn.execute(
            "SELECT m.market_id FROM markets m ANTI JOIN event_markets em "
            "ON em.venue=m.venue AND em.market_id=m.market_id WHERE m.venue='polymarket' "
            "ORDER BY try_cast(m.market_id AS HUGEINT) LIMIT 100"
        ).fetchall()
    ]
    require(orphan_ids, "catalogue source has no orphan markets for the fixed workload")
    return {**batches, "orphan_market_ids": orphan_ids, "counts": counts}


def stream_catalogue_oracle(
    root: Path, settings=None, *, generated_responses: dict[str, str] | None = None
) -> dict:
    """Hash public/private state, normalizing only explicitly supplied fresh captures."""
    from oddsfox_catalogue.fingerprints import semantic_fingerprint

    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
        if settings is not None:
            from oddsfox_catalogue.current_config import admit

            usage = admit(settings)
            remaining_spill = _remaining_spill_bytes(
                settings.limits.max_temp_bytes, usage["temporary_bytes"]
            )
            conn.execute(f"SET memory_limit='{int(settings.limits.duckdb_memory_bytes)}B'")
            conn.execute(f"SET threads={int(settings.limits.duckdb_threads)}")
            conn.execute("SET TimeZone='UTC'")
            conn.execute("SET temp_directory=?", [str(settings.temporary_dir)])
            conn.execute(
                "SET max_temp_directory_size=?",
                [f"{remaining_spill}B"],
            )
        names = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        fingerprints = {}
        for table in sorted(names):
            if table.startswith("_") or table in {"events", "markets", "event_markets", "outcomes"}:
                fingerprints[table] = None
        generated_responses = generated_responses or {}
        require(
            isinstance(generated_responses, dict)
            and all(
                isinstance(source, str) and source and isinstance(stable, str) and stable
                for source, stable in generated_responses.items()
            ),
            "generated response reference map is malformed",
        )
        require(
            len(set(generated_responses.values())) == len(generated_responses),
            "generated response references must map one-to-one to capture units",
        )
        if generated_responses:
            require(
                "_v6_generated_response_map" not in names,
                "reserved V6 response-map table name is already present",
            )
            conn.execute(
                "CREATE TEMP TABLE _v6_generated_response_map "
                "(source_ref VARCHAR PRIMARY KEY, unit_ref VARCHAR NOT NULL)"
            )
            conn.executemany(
                "INSERT INTO _v6_generated_response_map VALUES (?, ?)",
                list(generated_responses.items()),
            )

        def normalized_query(table):
            columns = [row[0] for row in conn.execute(f'DESCRIBE "{table}"').fetchall()]
            generated_columns = {
                "_receipts": {"response"},
                "_membership": {"response", "latest_response"},
                "_identity": {"invalidated_response"},
            }.get(table, set())
            projected = []
            for column in columns:
                if column not in generated_columns:
                    projected.append(f'source."{column}"')
                    continue
                expression = f'source."{column}"'
                if generated_responses:
                    expression = f"coalesce(generated_map.unit_ref, {expression})"
                projected.append(f'{expression} AS "{column}"')
            joins = ""
            if table in {"_receipts", "_membership", "_identity"} and generated_responses:
                join_column = {
                    "_receipts": "response",
                    "_membership": "response",
                    "_identity": "invalidated_response",
                }[table]
                if table == "_membership":
                    # Both receipt columns are normalized through the same exact map.
                    # Two bounded joins preserve their independent column association.
                    joins = (
                        " LEFT JOIN _v6_generated_response_map AS generated_map "
                        'ON source."response"=generated_map.source_ref '
                        "LEFT JOIN _v6_generated_response_map AS latest_map "
                        'ON source."latest_response"=latest_map.source_ref'
                    )
                    projected = [
                        (
                            f"coalesce({'latest_map' if column == 'latest_response' else 'generated_map'}."
                            f'unit_ref, source."{column}") AS "{column}"'
                            if column in generated_columns
                            else f'source."{column}"'
                        )
                        for column in columns
                    ]
                else:
                    joins = (
                        " LEFT JOIN _v6_generated_response_map AS generated_map "
                        f'ON source."{join_column}"=generated_map.source_ref'
                    )
            return f'SELECT {", ".join(projected)} FROM "{table}" AS source{joins}'

        state = {}
        for key, value in conn.execute("SELECT key,value FROM sync_state ORDER BY key").fetchall():
            parsed = json.loads(value)
            if key == "bulk_policy" and isinstance(parsed, dict):
                origin = parsed.get("origin")
                if isinstance(origin, str) and re.fullmatch(r"http://127\.0\.0\.1:\d+", origin):
                    parsed["origin"] = "http://127.0.0.1:<ephemeral>"
            if key == "previous_publish_seconds":
                parsed = "<measured-publication-duration>"
            if key == "claim_index_resource_ledger_v1":
                ledger = conn.execute(
                    "SELECT count(*),coalesce(sum(strlen(venue)+strlen(market_id)+strlen(role)+"
                    "strlen(asset_kind)+strlen(asset_id)),0) FROM _identity_claims"
                ).fetchone()
                if (
                    not isinstance(parsed, dict)
                    or parsed.get("version") != 1
                    or parsed.get("rows") != int(ledger[0])
                    or parsed.get("identifier_bytes") != int(ledger[1])
                    or parsed.get("distinct_asset_ids")
                    != int(
                        conn.execute(
                            "SELECT count(DISTINCT asset_id) FROM _identity_claims"
                        ).fetchone()[0]
                    )
                    or set(parsed) != {"version", "rows", "identifier_bytes", "distinct_asset_ids"}
                ):
                    raise ValueError("claim-index resource ledger disagrees with stored claim rows")
                parsed = {
                    "version": 1,
                    "rows": int(ledger[0]),
                    "identifier_bytes": int(ledger[1]),
                    "distinct_asset_ids": int(
                        conn.execute(
                            "SELECT count(DISTINCT asset_id) FROM _identity_claims"
                        ).fetchone()[0]
                    ),
                    "normalized_resource_ledger": True,
                }
            if key == "last_run" and isinstance(parsed, dict):
                timings = parsed.get("timings")
                if isinstance(timings, dict):
                    parsed["timings"] = {name: "<measured-duration>" for name in timings}
                for volatile in (
                    "at",
                    "elapsed_s",
                    "peak_rss_bytes",
                    "storage",
                    "requests",
                    "http_attempts",
                    "downloaded_bytes",
                    "freshness",
                ):
                    if volatile in parsed:
                        parsed[volatile] = "<runtime-diagnostic>"
            state[key] = parsed
        if "claim_index_resource_ledger_v1" not in state:
            ledger = conn.execute(
                "SELECT count(*),coalesce(sum(strlen(venue)+strlen(market_id)+strlen(role)+"
                "strlen(asset_kind)+strlen(asset_id)),0) FROM _identity_claims"
            ).fetchone()
            state["claim_index_resource_ledger_v1"] = {
                "version": 1,
                "rows": int(ledger[0]),
                "identifier_bytes": int(ledger[1]),
                "distinct_asset_ids": int(
                    conn.execute(
                        "SELECT count(DISTINCT asset_id) FROM _identity_claims"
                    ).fetchone()[0]
                ),
                "normalized_resource_ledger": True,
            }
        for table in fingerprints:
            fingerprints[table] = semantic_fingerprint(conn, normalized_query(table))
        return {
            "tables": fingerprints,
            "state": state,
            "generated_capture_units": sorted(set(generated_responses.values())),
        }


def _v6_normalized_publication(manifest: dict) -> dict:
    """Keep publication semantics while excluding only nonsemantic file fingerprints."""
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
    result = copy.deepcopy({key: manifest[key] for key in keys if key in manifest})
    for descriptor in result.get("relations", {}).values():
        if isinstance(descriptor, dict):
            descriptor.pop("sha256", None)
            descriptor.pop("bytes", None)
    return result


def _fixed_classification_signature(conn) -> dict:
    """Compact deterministic lifecycle/coverage counts for fixed-work comparisons."""
    events = conn.execute(
        "SELECT coalesce(cast(closed AS VARCHAR), '<null>'), count(*) FROM events "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    markets = conn.execute(
        "SELECT coalesce(cast(closed AS VARCHAR), '<null>'), count(*) FROM markets "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    receipt_counts = conn.execute(
        "SELECT kind, count(*) FROM _receipts GROUP BY kind ORDER BY kind"
    ).fetchall()
    return {
        "event_lifecycle_counts": [[str(value), int(count)] for value, count in events],
        "market_lifecycle_counts": [[str(value), int(count)] for value, count in markets],
        "receipt_counts": [[str(kind), int(count)] for kind, count in receipt_counts],
    }


def _verify_fixed_work_receipts(conn, request_spec: dict, stamp: datetime) -> dict:
    """Require every frozen event and owned/orphan market unit to be durably observed."""
    event_ids = list(
        dict.fromkeys(
            str(value)
            for batch in request_spec["closed_event_batches"] + request_spec["active_event_batches"]
            for value in batch
        )
    )
    orphan_ids = list(dict.fromkeys(str(value) for value in request_spec["orphan_market_ids"]))
    event_placeholders = ",".join("?" for _ in event_ids)
    event_receipts = conn.execute(
        "SELECT id, receipt FROM _receipts WHERE kind='event' AND venue='polymarket' "
        f"AND id IN ({event_placeholders})",
        event_ids,
    ).fetchall()
    event_by_id = {str(key): value for key, value in event_receipts}
    require(
        len(event_by_id) == len(event_ids)
        and all(value >= stamp for value in event_by_id.values()),
        "fixed event units are missing committed receipts at the frozen clock",
    )
    memberships = conn.execute(
        "SELECT event_id, receipt, latest_receipt FROM _membership "
        f"WHERE venue='polymarket' AND event_id IN ({event_placeholders})",
        event_ids,
    ).fetchall()
    membership_by_id = {str(row[0]): (row[1], row[2]) for row in memberships}
    require(
        all(
            event_id in membership_by_id
            and membership_by_id[event_id][0] >= stamp
            and membership_by_id[event_id][1] >= stamp
            for event_id in event_ids
        ),
        "fixed event memberships are not fully renewed at the frozen clock",
    )
    owned_markets = [
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT em.market_id FROM event_markets em JOIN "
            "(SELECT unnest(?::VARCHAR[]) AS event_id) k USING(event_id) "
            "WHERE em.venue='polymarket'",
            [event_ids],
        ).fetchall()
    ]
    expected_market_ids = list(dict.fromkeys(owned_markets + orphan_ids))
    market_key_placeholders = ",".join("?" for _ in expected_market_ids)
    market_receipts = conn.execute(
        "SELECT id, receipt FROM _receipts WHERE kind='market' AND venue='polymarket' "
        f"AND id IN ({market_key_placeholders})",
        expected_market_ids,
    ).fetchall()
    market_by_id = {str(key): value for key, value in market_receipts}
    require(
        len(market_by_id) == len(expected_market_ids)
        and all(value >= stamp for value in market_by_id.values()),
        "fixed owned/orphan market units are missing committed receipts at the frozen clock",
    )
    return {
        "event_ids": len(event_ids),
        "event_memberships": len(memberships),
        "owned_market_ids": len(owned_markets),
        "orphan_market_ids": len(orphan_ids),
        "market_ids_with_receipts": len(market_by_id),
        "receipt_clock": stamp.isoformat(),
    }


def claim_resource_worker(source_root: Path, target_root: Path, output: Path) -> int:
    """Build and exercise the private claim-index experiment on a fresh full-size copy."""
    source_root, target_root, output = map(safe_path, (source_root, target_root, output))
    # Validate every path before creating a report or entering the failure-persistence
    # handler. A malformed output path must never be used to write into the read-only
    # source tree, even when the diagnostic fails immediately.
    validate_disjoint(source_root, target_root)
    validate_disjoint(source_root, output)
    validate_disjoint(target_root, output)
    require(not target_root.exists(), "claim resource target must be fresh")
    require(output.is_dir() and not is_symlink(output), "claim resource output must be regular")
    validate_tree(source_root)
    report_path = output / "claim-resource.json"
    require(
        not report_path.exists() and not is_symlink(report_path),
        "claim resource report destination must be fresh",
    )
    report = {
        "status": "in_progress",
        "source_root": str(source_root),
        "target_root": str(target_root),
        "resource_envelope": None,
        "qualification": False,
        "memory_limit": "2GiB",
        "threads": 2,
    }
    started = time.monotonic()
    try:
        from oddsfox_catalogue import current_claim_indexes, current_store
        from oddsfox_catalogue.current_config import (
            CurrentLimits,
            connect_store,
            load_current_settings,
        )
        from oddsfox_catalogue.runlock import run_lock

        actual_package = Path(oddsfox_catalogue.__file__).resolve().parent
        expected_package = Path(os.environ["FULL_PASS_EXPECTED_PACKAGE"]).resolve()
        require(actual_package == expected_package, "claim resource imported a different runtime")

        source_settings = load_current_settings(source_root)
        with run_lock(source_settings.run_lock_path):
            source_hashes_before = _regular_tree_hashes(source_root)
            copy_admission = _admit_catalogue_copy(source_root, target_root, source_settings.limits)
            source_stat_before = (
                source_settings.store_path.stat().st_size,
                source_settings.store_path.stat().st_mtime_ns,
            )
            copy_root(source_root, target_root)
            require(
                source_hashes_before == _regular_tree_hashes(source_root),
                "claim resource source changed during locked copy",
            )
            source_stat_after = (
                source_settings.store_path.stat().st_size,
                source_settings.store_path.stat().st_mtime_ns,
            )
        require(
            source_stat_before == source_stat_after,
            "claim resource source database changed during locked copy",
        )
        report.update(
            status="copied",
            source_hashes=source_hashes_before,
            copy_admission=copy_admission,
            source_database_identity={
                "size": source_stat_before[0],
                "mtime_ns": source_stat_before[1],
            },
            copy_wall_s=time.monotonic() - started,
        )
        _save(report_path, report)

        settings = load_current_settings(target_root)
        require(
            settings.limits == CurrentLimits(), "claim resource settings differ from production"
        )
        post_close_storage = {}

        def record_post_close_storage():
            post_close_storage.update(_claim_storage_metrics(target_root, settings.temporary_dir))

        with _claim_diagnostic_store(
            settings, connect_store, run_lock, record_post_close_storage
        ) as conn:
            usage_before = current_claim_indexes.claim_usage(conn)
            require(usage_before["rows"] > 0, "claim resource source contains no claims")
            support = getattr(current_claim_indexes, "SUPPORTED_ENVELOPE", None)
            candidate_supported_envelope = (
                None
                if support is None
                else {
                    "max_rows": support.max_rows,
                    "max_identifier_bytes": support.max_identifier_bytes,
                    "max_distinct_asset_ids": support.max_distinct_asset_ids,
                }
            )
            sampled_keys = conn.execute(
                "SELECT DISTINCT venue, asset_kind, asset_id FROM _identity_claims "
                "ORDER BY asset_id LIMIT 100"
            ).fetchall()
            sampled_asset_ids = list(dict.fromkeys(str(row[2]) for row in sampled_keys))
            require(sampled_asset_ids, "claim index query plan has no sampled asset IDs")
            plan_before = _claim_lookup_query_plan(conn, "asset_id", sampled_asset_ids)
            executed_plan_before = _claim_lookup_explain_analyze(
                conn, "asset_id", sampled_asset_ids
            )
            sampled_markets = conn.execute(
                "SELECT DISTINCT venue, market_id FROM _identity_claims "
                "ORDER BY market_id LIMIT 100"
            ).fetchall()
            sampled_market_ids = list(dict.fromkeys(str(row[1]) for row in sampled_markets))
            market_plan_before = _claim_lookup_query_plan(conn, "market_id", sampled_market_ids)
            market_executed_plan_before = _claim_lookup_explain_analyze(
                conn, "market_id", sampled_market_ids
            )
            query_samples = []
            for _ in range(3):
                query_started = time.monotonic()
                rows = current_store._select_claims_by_assets(conn, sampled_keys)
                query_samples.append(time.monotonic() - query_started)
                require(rows, "selective asset claim query returned no rows")

            storage_before = _claim_storage_metrics(target_root, settings.temporary_dir)
            report.update(
                status="measuring",
                usage_before=usage_before,
                candidate_supported_envelope=candidate_supported_envelope,
                index_plan_before=plan_before,
                index_executed_plan_before=executed_plan_before,
                market_index_plan_before=market_plan_before,
                market_index_executed_plan_before=market_executed_plan_before,
                storage_before=storage_before,
            )
            _save(report_path, report)
            build_started = time.monotonic()
            build_result = current_claim_indexes.build_index_experiment(conn)
            build_seconds = time.monotonic() - build_started
            plan_after = _claim_lookup_query_plan(conn, "asset_id", sampled_asset_ids)
            executed_plan_after = _claim_lookup_explain_analyze(conn, "asset_id", sampled_asset_ids)
            market_plan_after = _claim_lookup_query_plan(conn, "market_id", sampled_market_ids)
            market_executed_plan_after = _claim_lookup_explain_analyze(
                conn, "market_id", sampled_market_ids
            )
            index_used = any(
                marker in "\n".join(executed_plan_after).upper()
                for marker in ("ART_INDEX_SCAN", "INDEX SCAN")
            )
            report.update(
                index_build=build_result,
                index_build_seconds=build_seconds,
                index_plan_after=plan_after,
                index_executed_plan_after=executed_plan_after,
                market_index_plan_after=market_plan_after,
                market_index_executed_plan_after=market_executed_plan_after,
                index_used=index_used,
            )
            _save(report_path, report)
            query_after_samples = []
            for _ in range(3):
                query_started = time.monotonic()
                rows_after = current_store._select_claims_by_assets(conn, sampled_keys)
                query_after_samples.append(time.monotonic() - query_started)
                require(
                    _claim_multiset(rows_after) == _claim_multiset(rows),
                    "indexed claim lookup changed result contents",
                )

            selected = conn.execute(
                "SELECT venue,market_id FROM _identity_claims ORDER BY venue,market_id LIMIT 1"
            ).fetchone()
            require(selected is not None, "claim resource source has no row for bounded writes")
            market_venue, market_id = str(selected[0]), str(selected[1])
            write_rows = conn.execute(
                "SELECT venue,market_id,role,asset_kind,asset_id FROM _identity_claims "
                "WHERE venue=? AND market_id=?",
                [market_venue, market_id],
            ).fetchall()
            require(write_rows, "selected market has no complete claim multiset")
            asset_id = write_rows[0][4]
            asset_rows = conn.execute(
                "SELECT venue,market_id,role,asset_kind,asset_id FROM _identity_claims "
                "WHERE asset_id=?",
                [asset_id],
            ).fetchall()
            write_results = {}
            write_started = time.monotonic()
            replaced_rows = _replace_market_claims_diagnostic(conn, market_venue, market_id)
            write_results["replacement_seconds"] = time.monotonic() - write_started
            require(
                _claim_multiset(replaced_rows) == _claim_multiset(write_rows),
                "market replacement changed a duplicate or role claim",
            )

            conflict_suffix = 0
            conflict_market = f"v6-diagnostic-conflict-{conflict_suffix}"
            while conn.execute(
                "SELECT 1 FROM _identity_claims WHERE venue=? AND market_id=? LIMIT 1",
                [market_venue, conflict_market],
            ).fetchone():
                conflict_suffix += 1
                conflict_market = f"v6-diagnostic-conflict-{conflict_suffix}"
            conflict = (market_venue, conflict_market, "candidate", write_rows[0][3], asset_id)
            write_started = time.monotonic()
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", conflict)
                conn.execute(
                    "DELETE FROM _identity_claims WHERE venue=? AND market_id=?",
                    [market_venue, conflict_market],
                )
                conn.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    conn.execute("ROLLBACK")
                raise
            write_results["conflicting_owner_insert_delete_seconds"] = (
                time.monotonic() - write_started
            )

            write_started = time.monotonic()
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM _identity_claims WHERE asset_id=?", [asset_id])
                conn.executemany("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", asset_rows)
                conn.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    conn.execute("ROLLBACK")
                raise
            write_results["asset_delete_restore_seconds"] = time.monotonic() - write_started
            require(
                _claim_multiset(
                    conn.execute(
                        "SELECT venue,market_id,role,asset_kind,asset_id FROM _identity_claims WHERE asset_id=?",
                        [asset_id],
                    ).fetchall()
                )
                == _claim_multiset(asset_rows),
                "asset deletion workload failed to restore exact rows",
            )
            require(
                current_claim_indexes.claim_usage(conn) == usage_before,
                "claim write diagnostics changed the claim multiset totals",
            )

            checkpoint_started = time.monotonic()
            conn.execute("CHECKPOINT")
            checkpoint_seconds = time.monotonic() - checkpoint_started
            storage_before_close = _claim_storage_metrics(target_root, settings.temporary_dir)
            usage_after = current_claim_indexes.claim_usage(conn)
            report.update(
                bounded_write_seconds=write_results,
                checkpoint_seconds=checkpoint_seconds,
                storage_before_close=storage_before_close,
                usage_after=usage_after,
            )
            _save(report_path, report)

        storage_after_close = post_close_storage
        report.update(
            status="measured",
            usage_before=usage_before,
            usage_after=usage_after,
            index_build=build_result,
            index_build_seconds=build_seconds,
            index_query_plan_before=plan_before,
            index_query_plan_after=plan_after,
            index_executed_plan_before=executed_plan_before,
            index_executed_plan_after=executed_plan_after,
            market_index_query_plan_before=market_plan_before,
            market_index_query_plan_after=market_plan_after,
            market_index_executed_plan_before=market_executed_plan_before,
            market_index_executed_plan_after=market_executed_plan_after,
            index_used=index_used,
            candidate_supported_envelope=candidate_supported_envelope,
            lookup_seconds_before=query_samples,
            lookup_seconds_after=query_after_samples,
            bounded_write_seconds=write_results,
            checkpoint_seconds=checkpoint_seconds,
            storage_before=storage_before,
            storage_before_close=storage_before_close,
            storage_after_close=storage_after_close,
            peak_rss_bytes=process_peak_rss_bytes(),
            peak_rss_scope="whole resource worker lifetime; not an isolated index-build peak",
            resource_envelope=None,
            supported_envelope_established=False,
            reason="one source-scale diagnostic does not establish a safe production envelope",
            wall_s=time.monotonic() - started,
            runtime_source=str(actual_package),
        )
        _save(report_path, report)
        print(json.dumps(report, default=str))
        return 0
    except BaseException as exc:
        report.update(
            status="failed" if isinstance(exc, Exception) else "interrupted",
            message=f"{type(exc).__name__}: {exc}",
            wall_s=time.monotonic() - started,
            peak_rss_bytes=process_peak_rss_bytes(),
            peak_rss_scope="whole resource worker lifetime; not an isolated index-build peak",
            resource_envelope=None,
            supported_envelope_established=False,
        )
        with contextlib.suppress(Exception):
            _save(report_path, report)
        if not isinstance(exc, Exception):
            raise
        print(json.dumps(report, default=str))
        return 3


def fixed_catalogue_worker(
    source_root: Path,
    root: Path,
    output: Path,
    label: str,
    requests_path: Path,
    *,
    attribution: bool = False,
) -> int:
    """Apply the same frozen full-size units to an isolated source copy."""
    from oddsfox_catalogue import current_reconcile, current_runtime, current_sync
    from oddsfox_catalogue.current_config import CurrentLimits, connect_store, load_current_settings
    from oddsfox_catalogue.runlock import run_lock

    started = time.monotonic()
    source_root, root, output, requests_path = map(
        safe_path, (source_root, root, output, requests_path)
    )
    validate_disjoint(source_root, root)
    validate_disjoint(source_root, output)
    validate_disjoint(source_root, requests_path)
    validate_disjoint(root, output)
    validate_disjoint(root, requests_path)
    validate_disjoint(output, requests_path)
    require(not root.exists(), "fixed-work target must be fresh")
    require(
        output.is_dir() and not is_symlink(output), "fixed-work output must be a regular directory"
    )
    validate_tree(source_root)
    actual_package = Path(oddsfox_catalogue.__file__).resolve().parent
    expected_package = Path(os.environ["FULL_PASS_EXPECTED_PACKAGE"]).resolve()
    require(actual_package == expected_package, "fixed-work worker imported a different runtime")
    source_settings_for_copy = load_current_settings(source_root)
    with run_lock(source_settings_for_copy.run_lock_path):
        source_copy_hashes = _regular_tree_hashes(source_root)
        copy_admission = _admit_catalogue_copy(source_root, root, source_settings_for_copy.limits)
        require(
            source_copy_hashes == _regular_tree_hashes(source_root),
            "fixed-work source changed before its locked copy",
        )
        copy_root(source_root, root)
        require(
            source_copy_hashes == _regular_tree_hashes(source_root),
            "fixed-work source changed during its locked copy",
        )
    request_data = json.loads(requests_path.read_text())
    request_spec = request_data["requests"]
    clock = {"now": datetime.fromisoformat(request_data["domain_clock"])}
    original_gamma, original_now = current_runtime.GammaClient, current_reconcile._now
    original_datetime = current_runtime.datetime

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"].astimezone(tz or UTC)

    current_reconcile._now = lambda: clock["now"]
    current_runtime.datetime = FrozenDateTime
    response_clock = {"calls": 0}

    def gamma_factory(*args, **kwargs):
        def next_receipt_time():
            response_clock["calls"] += 1
            return clock["now"] + timedelta(microseconds=response_clock["calls"])

        kwargs["now"] = next_receipt_time
        return original_gamma(*args, **kwargs)

    original_sync_fetch_apply = current_sync.fetch_apply
    original_reconcile_fetch_apply = current_reconcile.fetch_apply
    native_repair_requests = []
    settled_request_order = []
    capture_units = []
    generated_responses = {}
    indexed_claim_module = None

    original_write_response = current_sync._write_response
    attribution_metrics = new_attribution_metrics()
    helper_metrics = {}
    original_connect_store = connect_store
    instrumented_helpers = {}
    require(
        not attribution or label.startswith("warm-"),
        "fixed-work SQL attribution is warm-up-only; label must start with 'warm-'",
    )

    if attribution:

        def attributed_connect_store(*args, **kwargs):
            return AttributedConnection(
                original_connect_store(*args, **kwargs), attribution_metrics
            )

        connect_store = attributed_connect_store
        for function_name in (
            "apply",
            "_select_in",
            "_write_values",
            "_write_receipts",
            "_write_columnar",
            "_select_claims_by_assets",
            "_select_claims_by_markets",
        ):
            original = getattr(current_sync.current_store, function_name, None)
            if original is None:
                continue
            instrumented_helpers[function_name] = original
            helper_metrics[function_name] = {"calls": 0, "wall_s": 0.0}

            def measured_helper(*args, _name=function_name, _original=original, **kwargs):
                helper_metrics[_name]["calls"] += 1
                helper_started = time.perf_counter()
                try:
                    return _original(*args, **kwargs)
                finally:
                    helper_metrics[_name]["wall_s"] += time.perf_counter() - helper_started

            setattr(current_sync.current_store, function_name, measured_helper)

    def traced_write_response(given_settings, raw_dir, response, request):
        result = original_write_response(given_settings, raw_dir, response, request)
        response_id = raw_dir.name
        if response_id not in generated_responses:
            unit_ref = f"capture-unit:{len(capture_units) + 1:06d}"
            generated_responses[response_id] = unit_ref
            capture_units.append(
                {
                    "unit_ref": unit_ref,
                    "request": copy.deepcopy(request),
                    "status": response.status,
                }
            )
        elif (
            capture_units[int(generated_responses[response_id].rsplit(":", 1)[1]) - 1]["request"]
            != request
        ):
            raise ValueError("one capture reference was reused for a different request")
        return result

    def traced_fetch_apply(*args, **kwargs):
        result = original_sync_fetch_apply(*args, **kwargs)
        if kwargs.get("native"):
            request = {
                "lane": str(args[3]),
                "ids": [str(key) for key in args[4]],
                "native": True,
                "response_received": bool(result.get("responses")),
            }
            native_repair_requests.append(request)
            settled_request_order.append(request)
        return result

    current_sync.fetch_apply = traced_fetch_apply
    current_reconcile.fetch_apply = traced_fetch_apply
    current_sync._write_response = traced_write_response
    audit = output / f"fixed-source-{label}.jsonl"
    try:
        with CapacitySource(source_root, audit, delay_s=0, source_root=source_root) as source:
            settings = load_current_settings(
                root, env=child_environment(source.url, expected_package)
            )
            require(
                settings.limits == CurrentLimits(),
                "fixed-workload settings changed production limits",
            )
            if (actual_package / "current_claim_indexes.py").is_file():
                from oddsfox_catalogue import current_claim_indexes

                validator = getattr(current_claim_indexes, "validate_owned_index", None)
                require(callable(validator), "indexed candidate has no owned-index validator")
                require(
                    current_claim_indexes.SUPPORTED_ENVELOPE is not None,
                    "indexed candidate has no measured supported resource envelope",
                )
                with run_lock(settings.run_lock_path), connect_store(settings) as index_conn:
                    require(
                        validator(index_conn) is True,
                        "indexed candidate requires an explicitly installed, valid claim index",
                    )
                indexed_claim_module = current_claim_indexes
            controls = source.controls
            probe_ids = {
                f"{kind[:-1]}_ids": {
                    status: [controls[f"{status}_{kind[:-1]}_id"]]
                    for status in ("open", "closed", "absent")
                }
                for kind in ("events", "markets")
            }
            operation_deadline = time.monotonic() + settings.limits.max_duration_s
            with run_lock(settings.run_lock_path):
                probe = current_runtime.execute(
                    settings,
                    "probe",
                    probe_ids,
                    operation_deadline,
                )
            require(
                all(lane.get("supported") is True for lane in probe.get("lanes", {}).values()),
                "full-size source probe did not certify both acquisition lanes",
            )
            runtime_started = time.monotonic()
            setup_wall = runtime_started - started
            operation_results = []
            acquisition_started = runtime_started
            index_ledger_refresh_s = 0.0
            attribution_wall_started = time.perf_counter() if attribution else None
            attribution_cpu_started = time.process_time() if attribution else None
            with (
                run_lock(settings.run_lock_path),
                connect_store(settings) as conn,
                gamma_factory(
                    settings.gamma,
                    max_body_bytes=settings.limits.max_response_bytes,
                    max_requests=settings.limits.max_requests,
                    max_download_bytes=settings.limits.max_download_bytes,
                    max_duration_s=settings.limits.max_duration_s,
                    deadline=operation_deadline,
                ) as client,
            ):
                if indexed_claim_module is not None:
                    ledger_started = time.monotonic()
                    indexed_claim_module.refresh_writer_ledger(
                        conn, indexed_claim_module.SUPPORTED_ENVELOPE
                    )
                    index_ledger_refresh_s = time.monotonic() - ledger_started
                groups = [
                    ("events", request_spec["closed_event_batches"]),
                    ("events", request_spec["active_event_batches"]),
                    ("markets", [request_spec["orphan_market_ids"]]),
                ]
                unit_number = 0
                from oddsfox_catalogue.current_config import publication_reserve

                acquisition_cutoff = operation_deadline - publication_reserve(settings, conn)
                for lane, lane_batches in groups:
                    for batch in lane_batches:
                        if time.monotonic() >= acquisition_cutoff:
                            raise TimeoutError("fixed workload reached its publication reserve")
                        unit_number += 1
                        result, omitted = current_sync.fetch_unit(
                            settings,
                            conn,
                            client,
                            lane,
                            batch,
                            progress={"v6_fixed_work_progress": {"unit": unit_number}},
                        )
                        operation_results.append(
                            {
                                "lane": lane,
                                "ids": batch,
                                "omitted": omitted,
                                "responses": result.get("responses", 0),
                            }
                        )
                        settled_request_order.append(
                            {
                                "lane": lane,
                                "ids": [str(key) for key in batch],
                                "omitted": [str(key) for key in omitted],
                                "native": False,
                            }
                        )
                        if omitted:
                            current_reconcile._confirm_omitted(
                                settings, conn, client, lane, omitted
                            )
                        current_reconcile.repair_identities(
                            settings,
                            conn,
                            client,
                            lane,
                            batch,
                            clock["now"],
                            acquisition_cutoff,
                            response_counter=None,
                        )
                        repair_count_now = conn.execute(
                            "SELECT count(*) FROM _parent_repairs"
                        ).fetchone()[0]
                        if repair_count_now:
                            # A later operation can add a nomination; these are part of the
                            # same bounded fixed work and are drained below before commit.
                            pass
                repair_deadline = operation_deadline
                for _ in range(1000):
                    if time.monotonic() >= acquisition_cutoff:
                        raise TimeoutError("fixed workload reached its publication reserve")
                    repaired = current_reconcile.repair_pending(
                        settings, conn, client, deadline=repair_deadline
                    )
                    if not repaired.get("responses"):
                        break
                else:
                    raise RuntimeError("fixed workload did not drain parent repair work")
                acquisition_wall = time.monotonic() - acquisition_started
                attribution_runtime_wall_s = (
                    time.perf_counter() - attribution_wall_started if attribution else None
                )
                attribution_process_cpu_s = (
                    time.process_time() - attribution_cpu_started if attribution else None
                )
                coverage_validation_started = time.monotonic()
                fixed_receipt_coverage = _verify_fixed_work_receipts(
                    conn, request_spec, clock["now"]
                )
                coverage_validation_wall_s = time.monotonic() - coverage_validation_started
                publication_started = time.monotonic()
                publication, _publish_time = _publish(
                    settings, conn, repair_deadline, recalibrate=True
                )
                publication_wall = time.monotonic() - publication_started
                require(
                    publication.get("noop") is False and publication.get("valid") is True,
                    "fixed workload did not create a fresh verified publication",
                )
                verification_started = time.monotonic()
                verification = verify_current(settings, connection=conn)
                verification_wall = time.monotonic() - verification_started
                require(
                    verification.get("valid") is True,
                    "fixed workload publication verification failed",
                )
                classification_signature = _fixed_classification_signature(conn)
                client_stats = client.stats.as_dict()
                repair_count = len(native_repair_requests)
                provider_stats = copy.deepcopy(source.stats)
                source_failure = source.failure
            runtime_wall = time.monotonic() - runtime_started
            require(source_failure is None, "frozen catalogue provider failed")
            require(
                provider_stats["http_attempts"]
                >= client_stats["requests"] + probe.get("http_attempts", 0),
                "provider observed fewer requests than the runtime clients",
            )
            require(
                provider_stats["downloaded_bytes"]
                >= client_stats["downloaded_bytes"] + probe.get("downloaded_bytes", 0),
                "provider observed fewer bytes than the runtime clients",
            )
        native_profile_started = time.monotonic() if attribution else None
        native_profile_diagnostics = (
            _warmup_native_profiles(
                root,
                output,
                label,
                attribution_metrics["captured_statements"],
                settings,
            )
            if attribution
            else None
        )
        native_profile_wall_s = time.monotonic() - native_profile_started if attribution else None
        worker_peak_before_oracle_bytes = process_peak_rss_bytes()
        oracle_started = time.monotonic()
        oracle = stream_catalogue_oracle(root, settings, generated_responses=generated_responses)
        oracle_wall = time.monotonic() - oracle_started
        publication_view = _v6_normalized_publication(publication)
        semantic_value = {
            "oracle": oracle,
            "settled_requests": settled_request_order,
            "capture_units": capture_units,
            "publication": publication_view,
        }
        semantic_hash = hashlib.sha256(
            json.dumps(
                semantic_value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                default=str,
            ).encode()
        ).hexdigest()
        print(
            json.dumps(
                {
                    "status": "measured",
                    "workload": "fixed_catalogue_application",
                    "workload_label": "fixed application characterization, not a canonical full scheduler pass",
                    "measurement_version": MEASUREMENT_VERSION,
                    "acquire_apply_s": acquisition_wall,
                    "index_ledger_refresh_s": index_ledger_refresh_s,
                    "coverage_validation_s": coverage_validation_wall_s,
                    "runtime_sequence_wall_s": runtime_wall,
                    "publication_s": publication_wall,
                    "verification_s": verification_wall,
                    "publish_verify_s": publication_wall + verification_wall,
                    "worker_setup_wall_s": setup_wall,
                    "oracle_wall_s": oracle_wall,
                    "wall_s": time.monotonic() - started,
                    "peak_rss_bytes": max(
                        worker_peak_before_oracle_bytes, process_peak_rss_bytes()
                    ),
                    "worker_peak_before_oracle_bytes": worker_peak_before_oracle_bytes,
                    "http_attempts": provider_stats["http_attempts"],
                    "downloaded_bytes": provider_stats["downloaded_bytes"],
                    "runtime_reserved_attempts": client_stats["requests"]
                    + probe.get("http_attempts", 0),
                    "runtime_downloaded_bytes": client_stats["downloaded_bytes"]
                    + probe.get("downloaded_bytes", 0),
                    "settled_requests": settled_request_order,
                    "native_repair_requests": len(native_repair_requests),
                    "native_repair_responses": sum(
                        1 for item in native_repair_requests if item.get("response_received")
                    ),
                    "fixed_receipt_coverage": True,
                    "capture_units": capture_units,
                    "settled_units": len(operation_results),
                    "completed_work": {
                        "closed_batches_requested": len(request_spec["closed_event_batches"]),
                        "active_batches_requested": len(request_spec["active_event_batches"]),
                        "event_ids_requested": sum(
                            len(batch)
                            for batch in request_spec["closed_event_batches"]
                            + request_spec["active_event_batches"]
                        ),
                        "orphan_market_ids_requested": len(request_spec["orphan_market_ids"]),
                        "omission_ids_confirmed": sum(
                            len(item["omitted"]) for item in operation_results
                        ),
                        "native_repair_requests": len(native_repair_requests),
                        "native_repair_responses": sum(
                            1 for item in native_repair_requests if item.get("response_received")
                        ),
                    },
                    "scheduler_cursor_progression": "not_applicable_fixed_work_units",
                    "cleanup_warnings": publication_cleanup_warnings(publication, 0),
                    "cleanup_deferred": bool(publication_cleanup_warnings(publication, 0)),
                    "semantic_hash": semantic_hash,
                    "publication_verified": True,
                    "publication_noop": bool(publication.get("noop")),
                    "publication": publication_view,
                    "counts": request_spec["counts"],
                    "classification_signature": classification_signature,
                    "fixed_receipt_coverage_details": fixed_receipt_coverage,
                    "deterministic_response_clock_calls": response_clock["calls"],
                    "source_stats": source.stats,
                    "parent_repairs_settled": repair_count,
                    "source_failure": source_failure,
                    "source_hashes": source_copy_hashes,
                    "copy_admission": copy_admission,
                    "settings": {"limits": asdict(settings.limits)},
                    "runtime_source": str(actual_package),
                    "attribution_diagnostics": (
                        {
                            "mode": "warmup_only_direct_unprofiled",
                            "application_wall_s": attribution_runtime_wall_s,
                            "application_wall_includes_nested_helpers": True,
                            "process_cpu_s_includes_all_worker_threads": attribution_process_cpu_s,
                            "sql_execute_wall_s": attribution_metrics["sql_execute_wall_s"],
                            "result_materialization_wall_s": attribution_metrics[
                                "result_materialization_wall_s"
                            ],
                            "statement_count": attribution_metrics["statement_count"],
                            "fetchone_count": attribution_metrics["fetchone_count"],
                            "fetchall_count": attribution_metrics["fetchall_count"],
                            "fetchmany_count": attribution_metrics["fetchmany_count"],
                            "parameter_scalar_count": attribution_metrics["parameter_scalar_count"],
                            "parameter_list_element_count": attribution_metrics[
                                "parameter_list_element_count"
                            ],
                            "helper_metrics": helper_metrics,
                            "captured_statement_categories": sorted(
                                attribution_metrics["captured_statements"]
                            ),
                            "native_profiles": native_profile_diagnostics,
                            "native_profile_wall_s_excluded_from_runtime": native_profile_wall_s,
                            "nested_timings_overlap": True,
                        }
                        if attribution
                        else None
                    ),
                },
                default=str,
            )
        )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
        return 3
    finally:
        current_runtime.GammaClient = original_gamma
        current_runtime.datetime = original_datetime
        current_reconcile._now = original_now
        current_sync.fetch_apply = original_sync_fetch_apply
        current_reconcile.fetch_apply = original_reconcile_fetch_apply
        current_sync._write_response = original_write_response
        connect_store = original_connect_store
        for function_name, original in instrumented_helpers.items():
            setattr(current_sync.current_store, function_name, original)


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
        "--_snapshot_catalogue",
        nargs=3,
        metavar=("SOURCE", "TARGET", "REQUESTS"),
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--_prepare_v6_snapshot",
        nargs=2,
        metavar=("SNAPSHOT", "REQUESTS"),
        help=argparse.SUPPRESS,
    )
    p.add_argument("--_hash_catalogue", type=Path, help=argparse.SUPPRESS)
    p.add_argument(
        "--_fixed_catalogue_sample",
        nargs=4,
        metavar=("SOURCE", "ROOT", "LABEL", "REQUESTS"),
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--_claim_resource",
        nargs=2,
        metavar=("SOURCE_ROOT", "TARGET_ROOT"),
        help=argparse.SUPPRESS,
    )
    p.add_argument("--_prepare_api_seed", choices=API_WORKLOADS, help=argparse.SUPPRESS)
    p.add_argument("--_claim_diagnostics", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--_claim_diagnostics_v4", action="store_true", help=argparse.SUPPRESS)
    p.add_argument(
        "--_validate", nargs=2, metavar=("BASELINE", "CANDIDATE_PACKAGE"), help=argparse.SUPPRESS
    )
    p.add_argument("--_profile", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--_attribution", action="store_true", help=argparse.SUPPRESS)
    p.add_argument(
        "--_scheduler_diagnostics",
        nargs=2,
        metavar=("SEED", "ROOT"),
        help=argparse.SUPPRESS,
    )
    p.add_argument("--_workload", choices=API_WORKLOADS, help=argparse.SUPPRESS)
    return p


def _save(path: Path, report: dict):
    temp = path.with_suffix(".pending")
    temp.write_text(json.dumps(report, indent=2, default=str) + "\n")
    temp.replace(path)


def run(args) -> int:
    if args._prepare_v6_snapshot:
        try:
            source, requests = map(Path, args._prepare_v6_snapshot)
            print(json.dumps(prepare_v6_snapshot(source, requests), default=str))
            return 0
        except Exception as exc:
            print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
            return 3
    if args._hash_catalogue:
        try:
            print(json.dumps(locked_catalogue_hash(args._hash_catalogue), default=str))
            return 0
        except Exception as exc:
            print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
            return 3
    if args._snapshot_catalogue:
        try:
            source, target, requests_path = map(Path, args._snapshot_catalogue)
            print(json.dumps(snapshot_catalogue(source, target, requests_path), default=str))
            return 0
        except Exception as exc:
            print(json.dumps({"status": "blocked", "message": f"{type(exc).__name__}: {exc}"}))
            return 3
    if args._fixed_catalogue_sample:
        source, root, label, requests_path = args._fixed_catalogue_sample
        return fixed_catalogue_worker(
            Path(source),
            Path(root),
            Path(args.output),
            label,
            Path(requests_path),
            attribution=args._attribution,
        )
    if args._claim_resource:
        source_root, target_root = args._claim_resource
        return claim_resource_worker(Path(source_root), Path(target_root), Path(args.output))
    if args._scheduler_diagnostics:
        seed, root = args._scheduler_diagnostics
        root = Path(root)
        root.mkdir(parents=True, exist_ok=False)
        return scheduler_diagnostics_worker(Path(seed), root)
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
            args._attribution,
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
    if args._claim_diagnostics or args._claim_diagnostics_v4:
        try:
            result = run_claim_scale_diagnostics(
                Path(args.output), extended=args._claim_diagnostics_v4
            )
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
            not is_symlink(baseline_dir) and baseline_dir.is_dir(),
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
