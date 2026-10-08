"""End-to-end capture against the fake Gamma server (no network, no warehouse)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import iter_scan_pages
from oddsfox_catalogue.capture.runner import abandon_batch, rebuild_from_raw, run_capture
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.gamma.http import RetriesExhausted
from oddsfox_catalogue.gamma.scans import list_scans_for

TESTS_DIR = Path(__file__).resolve().parents[1]
CHILD = TESTS_DIR / "fakes" / "capture_child.py"
SIX_LIST_SCANS = [
    "events_keyset_all",
    "events_keyset_open",
    "events_keyset_closed",
    "events_archived_offset",
    "markets_keyset_open",
    "markets_keyset_closed",
]
ONE_PER_PAGE = {"CATALOGUE_GAMMA_PAGE_LIMIT": "1"}


def _world_with_references() -> object:
    """Demo world plus an event hidden from list scans and a market pointing at a missing event."""
    world = demo_world()
    hidden = make_event("909", "Hidden referenced event")
    hidden_market = make_market("9001", "Hidden market?", event_stub=event_stub(hidden))
    hidden["markets"] = [hidden_market]
    world.add_event(hidden)
    ghost = {"id": "999999", "ticker": None, "slug": None, "title": "gone"}
    world.add_direct_market(make_market("9002", "Ghost market?", event_stub=ghost))
    return world


def _page_fingerprints(ledger: Ledger, batch_id: str) -> list[tuple[str, str, str]]:
    return [
        (p["page_id"], p["body_sha256"], p["scan_status"]) for p in ledger.pages_for_batch(batch_id)
    ]


def _scans_by_name(ledger: Ledger, batch_id: str) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for scan in ledger.list_scans(batch_id):
        grouped.setdefault(scan["scan_name"], []).append(scan)
    return grouped


def test_bootstrap_captures_list_scans_and_resolves_references(tmp_path: Path) -> None:
    fake = FakeGamma(_world_with_references())
    fake.hidden_from_lists.add("909")
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        assert summary.resumed is False

        scans = runtime.ledger.list_scans(summary.batch_id)
        names = [s["scan_name"] for s in scans]
        assert names[:6] == SIX_LIST_SCANS
        assert "events_by_id_0001" in names, "references must be fetched by ID"
        assert "events_by_id_single_0001" in names, (
            "unresolved IDs must fall back to single lookups"
        )
        assert all(s["status"] == "complete" for s in scans)

        single = _scans_by_name(runtime.ledger, summary.batch_id)["events_by_id_single_0001"][0]
        assert json.loads(single["input_ids_json"]) == ["999999"]
        single_pages = runtime.ledger.pages_for_scan(single["scan_id"])
        assert [p["http_status"] for p in single_pages] == [404]
        assert single_pages[0]["record_count"] == 0

        by_id = _scans_by_name(runtime.ledger, summary.batch_id)["events_by_id_0001"][0]
        returned = []
        for _, records in iter_scan_pages(
            runtime.settings, runtime.ledger.get_batch(summary.batch_id), by_id
        ):
            returned.extend(r["id"] for r in records)
        assert returned == ["909"], "hidden event must come back through the ID filter"

        batch_marker = next(runtime.settings.raw_dir.glob("*/*/_batch.json"))
        marker = json.loads(batch_marker.read_text())
        assert marker["status"] == "captured"
    finally:
        runtime.ledger.close()


def test_archived_scan_uses_offset_and_ends_on_empty_page(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.world.events["101"]["archived"] = True
    runtime, _ = build_runtime(tmp_path, fake, env=ONE_PER_PAGE)
    try:
        summary = run_capture(runtime, "bootstrap")
        archived = _scans_by_name(runtime.ledger, summary.batch_id)["events_archived_offset"][0]
        pages = runtime.ledger.pages_for_scan(archived["scan_id"])
        assert [p["offset_start"] for p in pages] == [0, 1]
        assert [p["record_count"] for p in pages] == [1, 0]
        assert pages[-1]["terminal"] == 1
    finally:
        runtime.ledger.close()


def test_capture_is_deterministic_across_roots(tmp_path: Path) -> None:
    results = []
    for name in ("a", "b"):
        fake = FakeGamma(_world_with_references())
        fake.hidden_from_lists.add("909")
        runtime, _ = build_runtime(tmp_path / name, fake)
        try:
            summary = run_capture(runtime, "bootstrap")
            results.append(_page_fingerprints(runtime.ledger, summary.batch_id))
        finally:
            runtime.ledger.close()
    assert results[0] == results[1]
    assert results[0], "a deterministic run must still capture pages"


def test_daily_refetches_previously_open_event_that_closed(tmp_path: Path) -> None:
    world = demo_world()
    world.events["202"]["closed"] = True
    for market in world.events["202"]["markets"]:
        world.markets[market["id"]]["closed"] = True
    fake = FakeGamma(world)
    runtime, _ = build_runtime(tmp_path, fake, open_event_ids=lambda: {"101", "202"})
    try:
        summary = run_capture(runtime, "daily")
        assert summary.status == "captured"
        names = [s["scan_name"] for s in runtime.ledger.list_scans(summary.batch_id)]
        assert names == ["events_keyset_open", "events_by_id_0001"]

        follow_up = _scans_by_name(runtime.ledger, summary.batch_id)["events_by_id_0001"][0]
        assert json.loads(follow_up["input_ids_json"]) == ["202"]
        batch = runtime.ledger.get_batch(summary.batch_id)
        [(_, records)] = list(iter_scan_pages(runtime.settings, batch, follow_up))
        assert [r["id"] for r in records] == ["202"]
        assert records[0]["closed"] is True
    finally:
        runtime.ledger.close()


def test_daily_without_baseline_refuses_to_plan_refreshes(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        with pytest.raises(RuntimeError, match="open-event baseline"):
            run_capture(runtime, "daily")
    finally:
        runtime.ledger.close()


def test_expired_cursor_restarts_scan_under_new_attempt(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.expire_cursor("/events/keyset", times=1)
    runtime, _ = build_runtime(tmp_path, fake, env=ONE_PER_PAGE)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        attempts = _scans_by_name(runtime.ledger, summary.batch_id)["events_keyset_all"]
        assert [a["attempt"] for a in attempts] == [1, 2]
        assert [a["status"] for a in attempts] == ["abandoned", "complete"]
        # Abandoned pages stay recorded, but only the complete attempt covers all events.
        assert len(runtime.ledger.pages_for_scan(attempts[0]["scan_id"])) >= 1
        complete_pages = runtime.ledger.pages_for_scan(attempts[1]["scan_id"])
        assert sum(p["record_count"] for p in complete_pages) == 3
    finally:
        runtime.ledger.close()


def test_exhausted_retries_fail_the_scan_and_resume_later(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.fail_status("/events/keyset", 503, times=50)
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        with pytest.raises(RetriesExhausted):
            run_capture(runtime, "bootstrap")
        batch = runtime.ledger.list_batches()[0]
        assert batch["status"] == "capturing"
        assert "RetriesExhausted" in batch["error"]
        failed = runtime.ledger.latest_attempt(batch["batch_id"], "events_keyset_all")
        assert failed["status"] == "failed"

        fake.rules.clear()
        summary = run_capture(runtime, "bootstrap")
        assert summary.resumed is True
        assert summary.batch_id == batch["batch_id"]
        assert summary.status == "captured"
    finally:
        runtime.ledger.close()


def test_rate_limit_retry_after_is_honoured_end_to_end(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.fail_status("/events/keyset", 429, times=1, retry_after="7")
    runtime, clock = build_runtime(tmp_path, fake)
    try:
        run_capture(runtime, "bootstrap")
        assert 7.0 in clock.sleeps
    finally:
        runtime.ledger.close()


def test_resume_reuses_page_ids_after_ledger_commit_crash(tmp_path: Path) -> None:
    clean_root = tmp_path / "clean"
    clean_fake = FakeGamma(demo_world())
    clean_runtime, _ = build_runtime(clean_root, clean_fake)
    try:
        clean = run_capture(clean_runtime, "bootstrap")
        clean_pages = _page_fingerprints(clean_runtime.ledger, clean.batch_id)
    finally:
        clean_runtime.ledger.close()

    crash_root = tmp_path / "crash"
    env = {**os.environ, "CATALOGUE_FAULT": "after_ledger_commit", "PYTHONPATH": str(TESTS_DIR)}
    result = subprocess.run(
        [sys.executable, str(CHILD), str(crash_root), "bootstrap", FIXED_NOW.isoformat()],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == CRASH_EXIT_CODE, result.stderr

    runtime, _ = build_runtime(crash_root, FakeGamma(demo_world()))
    try:
        resumed = run_capture(runtime, "bootstrap")
        assert resumed.resumed is True
        assert resumed.pages_adopted == 0, "ledger already had the crashed page"
        assert resumed.status == "captured"
        assert _page_fingerprints(runtime.ledger, resumed.batch_id) == clean_pages
    finally:
        runtime.ledger.close()


def test_resume_adopts_durable_page_missing_from_ledger(tmp_path: Path) -> None:
    clean_root = tmp_path / "clean"
    clean_runtime, _ = build_runtime(clean_root, FakeGamma(demo_world()))
    try:
        clean = run_capture(clean_runtime, "bootstrap")
        clean_pages = _page_fingerprints(clean_runtime.ledger, clean.batch_id)
    finally:
        clean_runtime.ledger.close()

    crash_root = tmp_path / "crash"
    env = {**os.environ, "CATALOGUE_FAULT": "after_page_rename", "PYTHONPATH": str(TESTS_DIR)}
    result = subprocess.run(
        [sys.executable, str(CHILD), str(crash_root), "bootstrap", FIXED_NOW.isoformat()],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == CRASH_EXIT_CODE, result.stderr

    runtime, _ = build_runtime(crash_root, FakeGamma(demo_world()))
    try:
        resumed = run_capture(runtime, "bootstrap")
        assert resumed.pages_adopted >= 1, "durable file without ledger row must be adopted"
        assert resumed.status == "captured"
        assert _page_fingerprints(runtime.ledger, resumed.batch_id) == clean_pages
    finally:
        runtime.ledger.close()


def test_ledger_can_be_rebuilt_from_raw_manifests(tmp_path: Path) -> None:
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    try:
        summary = run_capture(runtime, "bootstrap")
        original = _page_fingerprints(runtime.ledger, summary.batch_id)
        runtime.ledger.close()
        (tmp_path / ".state" / "ledger.sqlite").unlink()
        for suffix in ("-wal", "-shm"):
            (tmp_path / ".state" / f"ledger.sqlite{suffix}").unlink(missing_ok=True)

        fresh = Ledger(tmp_path / ".state" / "ledger.sqlite")
        counts = rebuild_from_raw(runtime.settings, fresh)
        try:
            assert counts["batches"] == 1
            assert counts["pages"] == len(original)
            batch = fresh.get_batch(summary.batch_id)
            assert batch["status"] == "captured" and batch["plan_stage"] == 2
            assert _page_fingerprints(fresh, summary.batch_id) == original
        finally:
            fresh.close()
    finally:
        runtime.ledger.close()


def test_operator_can_abandon_a_capturing_batch(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.fail_status("/events/keyset", 503, times=50)
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        with pytest.raises(RetriesExhausted):
            run_capture(runtime, "bootstrap")
        batch_id = runtime.ledger.list_batches()[0]["batch_id"]
        abandon_batch(runtime.ledger, batch_id, FIXED_NOW, "operator")
        assert runtime.ledger.get_batch(batch_id)["status"] == "abandoned"
        with pytest.raises(ValueError):
            abandon_batch(runtime.ledger, batch_id, FIXED_NOW, "again")
    finally:
        runtime.ledger.close()


def test_daily_scan_set_is_only_the_open_list(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    daily = list_scans_for("daily", settings.gamma)
    assert [s.name for s in daily] == ["events_keyset_open"]
    bootstrap = list_scans_for("bootstrap", settings.gamma)
    assert [s.name for s in bootstrap] == SIX_LIST_SCANS
