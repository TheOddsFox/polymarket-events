"""Operations: offline rebuild verification, replay-order independence, backups, stage metrics,
and launchd templates."""

from __future__ import annotations

import json
import plistlib
import subprocess
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, capture_and_load, copy_built
from fakes.dbt_run import run_dbt
from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import demo_world
from oddsfox_catalogue.backup import MANIFEST, create_backup, verify_backup
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import rebuild_from_raw, run_capture
from oddsfox_catalogue.pipeline import capture_stage, dbt_stage, load_stage
from oddsfox_catalogue.rebuild import (
    VERIFIED_TABLES,
    rebuild_and_verify,
    scratch_settings,
    warehouse_fingerprints,
)
from oddsfox_catalogue.warehouse import read_open_event_ids

REPO = Path(__file__).resolve().parents[2]
THRESHOLD = '{"max_open_events_drop_pct": 0.9}'


@pytest.fixture(scope="module")
def built_root(tmp_path_factory) -> Path:
    """One built warehouse for the module. Each test copies it before it changes anything."""
    root = tmp_path_factory.mktemp("ops_built")
    build_warehouse(root)
    return root


def test_rebuild_from_raw_reproduces_the_live_warehouse(built_root, tmp_path: Path) -> None:
    settings = copy_built(built_root, tmp_path)
    report = rebuild_and_verify(settings)
    assert report.matched, report.mismatches
    for table in VERIFIED_TABLES:
        assert report.tables[table]["live"] is not None, f"{table} missing from live warehouse"


def test_rebuild_detects_a_tampered_live_warehouse(built_root, tmp_path: Path) -> None:
    settings = copy_built(built_root, tmp_path)
    con = duckdb.connect(str(settings.warehouse_path))
    try:
        con.execute("UPDATE core.events_current SET title = 'tampered' WHERE event_id = '101'")
    finally:
        con.close()
    report = rebuild_and_verify(settings)
    assert not report.matched
    assert any(m.startswith("core.events_current") for m in report.mismatches)


def test_replay_order_does_not_change_the_result(built_root, tmp_path: Path) -> None:
    """Two batches loaded in reverse order must give the same tables as the normal order."""
    settings = copy_built(built_root, tmp_path)
    closed_world = demo_world()
    closed_world.events["202"]["closed"] = True
    closed_world.events["202"]["updatedAt"] = "2026-10-08T07:00:00Z"
    runtime, _ = build_runtime(
        tmp_path,
        FakeGamma(closed_world),
        now=FIXED_NOW + timedelta(days=1),
        open_event_ids=lambda: read_open_event_ids(settings.warehouse_path),
    )
    try:
        assert run_capture(runtime, "daily").status == "captured"
    finally:
        runtime.ledger.close()

    # The demo world has two open events, so closing one is a 50% drop by design.
    load_stage(settings)
    built = dbt_stage(settings, ["build", "--vars", THRESHOLD])
    assert built.returncode == 0, built.stdout[-3000:]
    normal = warehouse_fingerprints(settings.warehouse_path, VERIFIED_TABLES)

    reverse = scratch_settings(settings, tmp_path / "reverse")
    ledger = Ledger(reverse.ledger_path)
    try:
        rebuild_from_raw(reverse, ledger)
        batch_ids = [b["batch_id"] for b in ledger.list_batches()]
    finally:
        ledger.close()
    assert len(batch_ids) == 2
    for batch_id in sorted(batch_ids, reverse=True):
        load_stage(reverse, batch_id=batch_id)
    rebuilt = dbt_stage(reverse, ["build", "--vars", THRESHOLD])
    assert rebuilt.returncode == 0, rebuilt.stdout[-3000:]

    assert warehouse_fingerprints(reverse.warehouse_path, VERIFIED_TABLES) == normal


def test_backup_verifies_and_detects_tampering(tmp_path: Path) -> None:
    settings = capture_and_load(tmp_path)
    backup = create_backup(settings, now=FIXED_NOW)
    assert verify_backup(backup) == []
    assert (backup / MANIFEST).exists()
    assert (backup / "ledger.sqlite").exists()
    assert (backup / "catalogue.duckdb").exists()
    assert any((backup / "raw").rglob("*.json.gz"))

    page = next((backup / "raw").rglob("*.json.gz"))
    page.write_bytes(page.read_bytes() + b"x")
    (backup / "stray.txt").write_text("not in the manifest")
    problems = verify_backup(backup)
    assert any(p.startswith("checksum mismatch raw/") for p in problems)
    assert "unlisted file stray.txt" in problems


def test_stages_record_their_outcome_in_stage_runs(tmp_path: Path) -> None:
    settings = capture_and_load(tmp_path)
    assert run_dbt(["parse"], settings.warehouse_path, tmp_path).returncode == 0
    load_stage(settings)
    failed = dbt_stage(settings, ["no-such-dbt-command"])
    assert failed.returncode != 0

    ledger = Ledger(settings.ledger_path)
    try:
        runs = ledger.stage_runs()
    finally:
        ledger.close()
    by_stage = {r["stage"]: r for r in runs}
    assert by_stage["load"]["status"] == "succeeded"
    assert by_stage["load"]["finished_at"] is not None
    failed_rows = [r for r in runs if r["stage"].startswith("dbt:") and r["status"] == "failed"]
    assert failed_rows, "a failing dbt command must be recorded as failed"
    assert failed_rows[0]["error"] is not None


def _capture_rows(settings) -> list[dict]:
    ledger = Ledger(settings.ledger_path)
    try:
        return [r for r in ledger.stage_runs() if r["stage"].startswith("capture:")]
    finally:
        ledger.close()


def test_capture_stage_writes_exactly_one_row_on_success(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    summary = capture_stage(
        settings,
        "bootstrap",
        transport=FakeGamma(demo_world()).transport(),
        now=lambda: FIXED_NOW,
    )
    assert summary.status == "captured"

    rows = _capture_rows(settings)
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["stage"] == "capture:bootstrap"
    assert row["status"] == "succeeded"
    assert row["batch_id"] == summary.batch_id
    assert row["error"] is None
    counts = json.loads(row["counts_json"])
    assert counts["http"]["requests"] > 0
    assert counts["records"] == summary.records


def test_capture_stage_writes_one_failed_row_when_capture_raises(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    fake = FakeGamma(demo_world())
    # An ordinary error fails the first list scan at once. An HTTP 400 would be bisected instead.
    fake.raise_on("/events/keyset", RuntimeError("injected failure"))
    with pytest.raises(RuntimeError, match="injected failure"):
        capture_stage(settings, "bootstrap", transport=fake.transport(), now=lambda: FIXED_NOW)

    rows = _capture_rows(settings)
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["stage"] == "capture:bootstrap"
    assert row["status"] == "failed"
    assert row["batch_id"] is not None
    assert "RuntimeError" in row["error"]


def _close_event_202_and_load(settings) -> None:
    """Close one of the demo world's two open events and load the resulting daily batch."""
    closed_world = demo_world()
    closed_world.events["202"]["closed"] = True
    closed_world.events["202"]["updatedAt"] = "2026-10-08T07:00:00Z"
    runtime, _ = build_runtime(
        settings.root,
        FakeGamma(closed_world),
        now=FIXED_NOW + timedelta(days=1),
        open_event_ids=lambda: read_open_event_ids(settings.warehouse_path),
    )
    try:
        assert run_capture(runtime, "daily").status == "captured"
    finally:
        runtime.ledger.close()
    load_stage(settings)


def test_open_event_drop_in_warn_band_builds_and_is_recorded(built_root, tmp_path: Path) -> None:
    """A 50% drop sits between warn (40%) and error (60%): the build passes and says so."""
    # The built copy already holds the first snapshot (two open events), the baseline to compare.
    copy_built(built_root, tmp_path)

    tolerant = make_settings(
        tmp_path,
        {
            "CATALOGUE_QUALITY_OPEN_EVENTS_DROP_WARN_PCT": "40",
            "CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT": "60",
        },
    )
    _close_event_202_and_load(tolerant)
    built = dbt_stage(tolerant, ["build"])
    assert built.returncode == 0, built.stdout[-3000:]

    row = [r for r in _stage_rows(tolerant) if r["stage"] == "dbt:build"][-1]
    assert row["status"] == "succeeded"
    counts = json.loads(row["counts_json"])
    assert counts["open_events_drop_warn"] is True
    assert counts["open_events_previous"] == 2
    assert counts["open_events_latest"] == 1
    assert counts["open_events_drop_pct"] == 50.0


def test_open_event_drop_above_error_fails_the_stage(built_root, tmp_path: Path) -> None:
    """With the default 10% error cap, the same 50% drop fails the build and is recorded."""
    settings = copy_built(built_root, tmp_path)
    _close_event_202_and_load(settings)

    failed = dbt_stage(settings, ["build"])
    assert failed.returncode != 0
    assert "assert_open_events_not_dropping" in failed.stdout

    row = [r for r in _stage_rows(settings) if r["stage"] == "dbt:build"][-1]
    assert row["status"] == "failed"
    assert "open_events_drop_warn" not in json.loads(row["counts_json"])


def _stage_rows(settings) -> list[dict]:
    ledger = Ledger(settings.ledger_path)
    try:
        return list(ledger.stage_runs())
    finally:
        ledger.close()


def test_make_recipes_quote_paths_that_contain_spaces() -> None:
    dagster = subprocess.run(
        ["make", "-n", "dagster-home"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    assert 'mkdir -p "' in dagster.stdout
    assert 'cp ops/dagster.yaml "' in dagster.stdout

    backup = subprocess.run(
        ["make", "-n", "verify-backup", "BACKUP=data/backups/a b"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    assert 'backup verify "data/backups/a b"' in backup.stdout


def test_launchd_templates_are_valid_plists() -> None:
    templates = sorted((REPO / "ops" / "launchd").glob("*.plist"))
    assert {t.name for t in templates} == {
        "com.oddsfox.catalogue.daily.plist",
        "com.oddsfox.catalogue.reconcile.plist",
        "com.oddsfox.catalogue.dagster-daemon.plist",
    }
    for template in templates:
        text = template.read_text(encoding="utf-8").replace("__PROJECT_ROOT__", "/tmp/root")
        plist = plistlib.loads(text.encode("utf-8"))
        assert plist["Label"].startswith("com.oddsfox.catalogue.")
        assert "__PROJECT_ROOT__" in template.read_text(encoding="utf-8")
