"""SQLite capture ledger: batches, scans, pages, and the fetched/loaded checkpoints.

The ledger is an index over ``data/raw``. It can be rebuilt from manifests
(see ``rebuild_from_raw``), so losing it never loses observations. Every
mutation that spans several rows runs inside one ``BEGIN IMMEDIATE``
transaction.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS build_validity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    status TEXT NOT NULL CHECK (status IN ('dirty', 'valid')),
    updated_at TEXT NOT NULL,
    reason TEXT,
    payload_json TEXT,
    payload_sha256 TEXT
);
CREATE TABLE IF NOT EXISTS capture_controls (
    control_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    previous_sha256 TEXT
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id          TEXT PRIMARY KEY,
    mode              TEXT NOT NULL,
    observation_date  TEXT NOT NULL,
    status            TEXT NOT NULL,
    plan_stage        INTEGER NOT NULL DEFAULT 0,
    started_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    git_sha           TEXT,
    error             TEXT,
    scope_json        TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scans (
    scan_id          TEXT PRIMARY KEY,
    batch_id         TEXT NOT NULL REFERENCES batches(batch_id),
    scan_name        TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    plan_order       INTEGER NOT NULL,
    kind             TEXT NOT NULL,
    endpoint         TEXT NOT NULL,
    record_key       TEXT NOT NULL,
    params_json      TEXT NOT NULL,
    input_ids_json   TEXT,
    status           TEXT NOT NULL,
    fetched_seq      INTEGER NOT NULL DEFAULT 0,
    fetched_cursor   TEXT,
    fetched_offset   INTEGER,
    terminal         INTEGER NOT NULL DEFAULT 0,
    last_ids_hash    TEXT,
    record_count     INTEGER NOT NULL DEFAULT 0,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    error            TEXT,
    phase            INTEGER NOT NULL DEFAULT 0,
    UNIQUE (batch_id, scan_name, attempt)
);

CREATE TABLE IF NOT EXISTS pages (
    page_id          TEXT PRIMARY KEY,
    batch_id         TEXT NOT NULL REFERENCES batches(batch_id),
    scan_id          TEXT NOT NULL REFERENCES scans(scan_id),
    seq              INTEGER NOT NULL,
    endpoint         TEXT NOT NULL,
    params_json      TEXT NOT NULL,
    input_cursor     TEXT,
    output_cursor    TEXT,
    offset_start     INTEGER,
    offset_end       INTEGER,
    record_count     INTEGER NOT NULL,
    http_status      INTEGER NOT NULL,
    retries          INTEGER NOT NULL,
    latency_s        REAL NOT NULL,
    terminal         INTEGER NOT NULL,
    body_sha256      TEXT NOT NULL,
    gz_sha256        TEXT NOT NULL,
    observed_at      TEXT NOT NULL,
    loaded_at        TEXT,
    dlt_load_id      TEXT,
    event_rows       INTEGER NOT NULL DEFAULT 0,
    market_rows      INTEGER NOT NULL DEFAULT 0,
    quarantine_rows  INTEGER NOT NULL DEFAULT 0,
    UNIQUE (scan_id, seq)
);

CREATE INDEX IF NOT EXISTS pages_by_batch ON pages(batch_id, loaded_at);

CREATE TABLE IF NOT EXISTS stage_runs (
    run_id           TEXT NOT NULL,
    batch_id         TEXT,
    stage            TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    status           TEXT NOT NULL,
    counts_json      TEXT,
    git_sha          TEXT,
    dbt_invocation_id TEXT,
    error            TEXT
);

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id         TEXT NOT NULL,
    page_id          TEXT,
    entity           TEXT NOT NULL,
    reason           TEXT NOT NULL,
    recorded_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata_requests (
    capture_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    status TEXT NOT NULL,
    received_at TEXT NOT NULL,
    manifest_path TEXT,
    error TEXT,
    PRIMARY KEY (capture_id, market_id)
);
"""


class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        # One connection shared by the capture workers. The lock serialises
        # every use; WAL lets a reader proceed while a writer commits.
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, isolation_level=None, timeout=30.0, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> Ledger:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                # Ask SQLite, not a flag: a signal can land after BEGIN or after COMMIT.
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    def _one(self, sql: str, args: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(sql, args).fetchone()
            return dict(row) if row is not None else None

    def _all(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def build_validity(self) -> dict[str, Any] | None:
        return self._one("SELECT * FROM build_validity WHERE singleton = 1")

    def set_build_validity(
        self,
        status: str,
        updated_at: str,
        *,
        reason: str | None = None,
        payload_json: str | None = None,
        payload_sha256: str | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO build_validity VALUES (1, ?, ?, ?, ?, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET status=excluded.status, "
                "updated_at=excluded.updated_at, reason=excluded.reason, "
                "payload_json=excluded.payload_json, payload_sha256=excluded.payload_sha256",
                (status, updated_at, reason, payload_json, payload_sha256),
            )

    # Batches ---------------------------------------------------------------------
    def create_batch(
        self,
        batch_id: str,
        mode: str,
        observation_date: str,
        started_at: str,
        git_sha: str | None,
        scans: list[dict[str, Any]],
        *,
        scope: dict[str, Any] | None = None,
        plan_stage: int = 0,
        control: dict[str, Any] | None = None,
    ) -> None:
        """Create a batch and its initial scan plan in one transaction."""
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO batches (batch_id, mode, observation_date, status, plan_stage, "
                "started_at, updated_at, git_sha, scope_json) VALUES (?, ?, ?, 'capturing', ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    mode,
                    observation_date,
                    plan_stage,
                    started_at,
                    started_at,
                    git_sha,
                    json.dumps(scope or {}, sort_keys=True, separators=(",", ":")),
                ),
            )
            _insert_scans(conn, scans)
            _insert_control(conn, control)

    def add_plan(
        self,
        batch_id: str,
        stage: int,
        scans: list[dict[str, Any]],
        updated_at: str,
        *,
        scope: dict[str, Any] | None = None,
        control: dict[str, Any] | None = None,
    ) -> None:
        """Append follow-up scans and advance the plan stage atomically."""
        with self.transaction() as conn:
            _insert_scans(conn, scans)
            if scope is not None:
                conn.execute(
                    "UPDATE batches SET scope_json = ? WHERE batch_id = ?",
                    (json.dumps(scope, sort_keys=True, separators=(",", ":")), batch_id),
                )
            conn.execute(
                "UPDATE batches SET plan_stage = ?, updated_at = ? WHERE batch_id = ?",
                (stage, updated_at, batch_id),
            )
            _insert_control(conn, control)

    def update_scope(
        self,
        batch_id: str,
        scope: dict[str, Any],
        updated_at: str,
        *,
        control: dict[str, Any] | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE batches SET scope_json = ?, updated_at = ? WHERE batch_id = ?",
                (json.dumps(scope, sort_keys=True, separators=(",", ":")), updated_at, batch_id),
            )

            _insert_control(conn, control)

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))

    def list_batches(self, status: str | None = None) -> list[dict[str, Any]]:
        if status is None:
            return self._all("SELECT * FROM batches ORDER BY batch_id")
        return self._all("SELECT * FROM batches WHERE status = ? ORDER BY batch_id", (status,))

    def find_resumable_batch(self, mode: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM batches WHERE mode = ? AND status = 'capturing' "
            "ORDER BY batch_id DESC LIMIT 1",
            (mode,),
        )

    def set_batch_status(
        self,
        batch_id: str,
        status: str,
        updated_at: str,
        error: str | None = None,
        *,
        control: dict[str, Any] | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE batches SET status = ?, updated_at = ?, error = ? WHERE batch_id = ?",
                (status, updated_at, error, batch_id),
            )

            _insert_control(conn, control)

    def record_batch_error(self, batch_id: str, updated_at: str, error: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE batches SET error = ?, updated_at = ? WHERE batch_id = ?",
                (error, updated_at, batch_id),
            )

    def restore_batch_state(
        self, batch_id: str, status: str, plan_stage: int, updated_at: str
    ) -> None:
        """Used only by raw-store rebuilds, where status and stage are inferred from markers."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE batches SET status = ?, plan_stage = ?, updated_at = ? WHERE batch_id = ?",
                (status, plan_stage, updated_at, batch_id),
            )

    def add_scan_attempt(
        self, scan: dict[str, Any], *, control: dict[str, Any] | None = None
    ) -> None:
        with self.transaction() as conn:
            _insert_scans(conn, [scan])
            _insert_control(conn, control)

    def set_scan_running(self, scan_id: str, started_at: str) -> None:
        """Re-open a failed scan for resume. The fetched checkpoint is left untouched."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE scans SET status = 'running', error = NULL WHERE scan_id = ?",
                (scan_id,),
            )

    def get_scan(self, scan_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM scans WHERE scan_id = ?", (scan_id,))

    def list_scans(self, batch_id: str) -> list[dict[str, Any]]:
        return self._all(
            "SELECT * FROM scans WHERE batch_id = ? ORDER BY plan_order, attempt",
            (batch_id,),
        )

    def latest_attempt(self, batch_id: str, scan_name: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM scans WHERE batch_id = ? AND scan_name = ? ORDER BY attempt DESC LIMIT 1",
            (batch_id, scan_name),
        )

    def max_plan_order(self, batch_id: str) -> int:
        row = self._one(
            "SELECT COALESCE(MAX(plan_order), 0) AS m FROM scans WHERE batch_id = ?", (batch_id,)
        )
        return int(row["m"]) if row else 0

    def set_scan_status(
        self,
        scan_id: str,
        status: str,
        finished_at: str | None,
        error: str | None = None,
        *,
        control: dict[str, Any] | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE scans SET status = ?, finished_at = ?, error = ? WHERE scan_id = ?",
                (status, finished_at, error, scan_id),
            )
            _insert_control(conn, control)

    def put_control(self, control: dict[str, Any]) -> None:
        with self.transaction() as conn:
            _insert_control(conn, control)

    def pending_controls(self, batch_id: str) -> list[dict[str, Any]]:
        return self._all(
            "SELECT * FROM capture_controls WHERE batch_id = ? ORDER BY control_id", (batch_id,)
        )

    def clear_control(self, control_id: str) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM capture_controls WHERE control_id = ?", (control_id,))

    # Pages -----------------------------------------------------------------------
    def record_page(self, page: Mapping[str, Any], scan_id: str, finished_terminal: bool) -> None:
        """Insert a durable page and advance the scan's fetched checkpoint atomically."""
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO pages (page_id, batch_id, scan_id, seq, endpoint, params_json, "
                "input_cursor, output_cursor, offset_start, offset_end, record_count, http_status, "
                "retries, latency_s, terminal, body_sha256, gz_sha256, observed_at) "
                "VALUES (:page_id, :batch_id, :scan_id, :seq, :endpoint, :params_json, "
                ":input_cursor, :output_cursor, :offset_start, :offset_end, :record_count, "
                ":http_status, :retries, :latency_s, :terminal, :body_sha256, :gz_sha256, :observed_at)",
                {**page, "scan_id": scan_id},
            )
            conn.execute(
                "UPDATE scans SET fetched_seq = ?, fetched_cursor = ?, fetched_offset = ?, "
                "record_count = record_count + ?, terminal = ?, last_ids_hash = ? WHERE scan_id = ?",
                (
                    page["seq"],
                    page["output_cursor"],
                    page["offset_end"],
                    page["record_count"],
                    1 if finished_terminal else 0,
                    page["ids_hash"],
                    scan_id,
                ),
            )

    def pages_for_scan(self, scan_id: str) -> list[dict[str, Any]]:
        return self._all("SELECT * FROM pages WHERE scan_id = ? ORDER BY seq", (scan_id,))

    def pages_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        return self._all(
            "SELECT p.*, s.scan_name, s.attempt, s.status AS scan_status, s.kind AS scan_kind, "
            "s.record_key AS record_key "
            "FROM pages p JOIN scans s ON s.scan_id = p.scan_id "
            "WHERE p.batch_id = ? ORDER BY s.plan_order, s.attempt, p.seq",
            (batch_id,),
        )

    def pending_load_pages(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        """Pages fetched but not yet loaded, ordered for deterministic loading."""
        sql = (
            "SELECT p.*, s.scan_name, s.attempt, s.status AS scan_status, s.plan_order, "
            "s.record_key AS record_key, s.kind AS scan_kind "
            "FROM pages p JOIN scans s ON s.scan_id = p.scan_id "
            "WHERE p.loaded_at IS NULL "
        )
        args: tuple[Any, ...] = ()
        if batch_id is not None:
            sql += "AND p.batch_id = ? "
            args = (batch_id,)
        sql += "ORDER BY p.batch_id, s.plan_order, s.attempt, p.seq"
        return self._all(sql, args)

    def complete_load_chunk(
        self,
        page_counts: Mapping[str, tuple[int, int, int]],
        load_id: str | None,
        loaded_at: str,
        quarantine: list[dict[str, Any]],
    ) -> None:
        """Commit a loaded chunk: page checkpoints, per-page counts, and quarantine together.

        Called only after dlt has committed the chunk. If the process dies before
        this transaction, the pages stay pending and the next load re-inserts
        the same observation IDs, which insert-only merge ignores.
        """
        with self.transaction() as conn:
            conn.executemany(
                "UPDATE pages SET loaded_at = ?, dlt_load_id = ?, event_rows = ?, market_rows = ?, "
                "quarantine_rows = ? WHERE page_id = ?",
                [
                    (loaded_at, load_id, events, markets, quarantined, page_id)
                    for page_id, (events, markets, quarantined) in page_counts.items()
                ],
            )
            conn.executemany(
                "INSERT INTO quarantine (batch_id, page_id, entity, reason, recorded_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (q["batch_id"], q["page_id"], q["entity"], q["reason"], loaded_at)
                    for q in quarantine
                ],
            )

    def batch_load_totals(self, batch_id: str) -> dict[str, Any]:
        row = self._one(
            "SELECT COUNT(*) AS page_count, COALESCE(SUM(event_rows), 0) AS event_rows, "
            "COALESCE(SUM(market_rows), 0) AS market_rows, "
            "COALESCE(SUM(quarantine_rows), 0) AS quarantine_rows, "
            "MAX(dlt_load_id) AS last_load_id FROM pages WHERE batch_id = ?",
            (batch_id,),
        )
        assert row is not None
        return row

    # Stage metrics ---------------------------------------------------------------
    def record_stage_run(
        self,
        *,
        run_id: str,
        batch_id: str | None,
        stage: str,
        started_at: str,
        finished_at: str | None,
        status: str,
        counts: Mapping[str, Any] | None = None,
        git_sha: str | None = None,
        dbt_invocation_id: str | None = None,
        error: str | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO stage_runs (run_id, batch_id, stage, started_at, finished_at, status, "
                "counts_json, git_sha, dbt_invocation_id, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    batch_id,
                    stage,
                    started_at,
                    finished_at,
                    status,
                    json.dumps(counts, sort_keys=True) if counts is not None else None,
                    git_sha,
                    dbt_invocation_id,
                    error,
                ),
            )

    def stage_runs(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is None:
            return self._all("SELECT * FROM stage_runs ORDER BY rowid")
        return self._all("SELECT * FROM stage_runs WHERE batch_id = ? ORDER BY rowid", (batch_id,))

    def quarantine_count(self, batch_id: str) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM quarantine WHERE batch_id = ?", (batch_id,))
        return int(row["n"]) if row else 0

    def record_metadata_request(self, record: Mapping[str, Any]) -> None:
        """Index targeted captures without adding them to the catalogue batch plan."""
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO metadata_requests "
                "(capture_id, market_id, status, received_at, manifest_path, error) "
                "VALUES (:capture_id, :market_id, :status, :received_at, :manifest_path, :error)",
                record,
            )


def _insert_scans(conn: sqlite3.Connection, scans: list[dict[str, Any]]) -> None:
    for scan in scans:
        conn.execute(
            "INSERT INTO scans (scan_id, batch_id, scan_name, attempt, plan_order, kind, "
            "endpoint, record_key, params_json, input_ids_json, status, started_at, phase) "
            "VALUES (:scan_id, :batch_id, :scan_name, :attempt, :plan_order, :kind, "
            ":endpoint, :record_key, :params_json, :input_ids_json, 'running', :started_at, :phase)",
            {**scan, "phase": scan.get("phase", 0)},
        )


def _insert_control(conn: sqlite3.Connection, control: dict[str, Any] | None) -> None:
    if control is not None:
        conn.execute(
            "INSERT INTO capture_controls (control_id,batch_id,payload_json,payload_sha256,previous_sha256) VALUES (:control_id,:batch_id,:payload_json,:payload_sha256,:previous_sha256)",
            control,
        )
