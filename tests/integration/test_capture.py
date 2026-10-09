"""End-to-end capture against the fake Gamma server (no network, no warehouse)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import iter_scan_pages
from oddsfox_catalogue.capture.runner import (
    abandon_batch,
    page_progress_due,
    rebuild_from_raw,
    run_capture,
)
from oddsfox_catalogue.capture.writer import read_body
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.gamma.http import RetriesExhausted
from oddsfox_catalogue.gamma.scans import list_scans_for

TESTS_DIR = Path(__file__).resolve().parents[1]
CHILD = TESTS_DIR / "fakes" / "capture_child.py"
BOOTSTRAP_LIST_SCANS = [
    "events_ids_0001",
    "events_ids_tail",
    "markets_closed_ids_0001",
    "markets_closed_ids_tail",
    "markets_keyset_open",
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


def test_page_progress_is_the_first_page_and_every_hundredth() -> None:
    assert page_progress_due(1)
    assert not page_progress_due(2)
    assert not page_progress_due(99)
    assert page_progress_due(100)
    assert page_progress_due(200)


def test_bootstrap_logs_scan_start_progress_and_completion(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    try:
        with caplog.at_level(logging.INFO, logger="oddsfox_catalogue.capture.runner"):
            summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
    finally:
        runtime.ledger.close()
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "oddsfox_catalogue.capture.runner"
    ]
    assert "capture scan events_ids_0001 starting" in messages
    assert any(message.startswith("capture events_ids_0001 page 1 (") for message in messages)
    assert any(
        message.startswith("capture scan events_ids_0001 complete, ") for message in messages
    )
    assert not any(" page 2 " in message for message in messages)


def test_resume_logs_the_saved_page(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    fake = FakeGamma(demo_world())
    fake.rules.append(
        Rule(
            lambda request: (
                request.url.path == "/events/keyset" and "after_cursor" in request.url.params
            ),
            lambda request: httpx.Response(500, json={"error": "injected"}),
            remaining=6,
        )
    )
    runtime, _ = build_runtime(
        tmp_path,
        fake,
        open_event_ids=lambda: {"101", "202"},
        env={**ONE_PER_PAGE, "CATALOGUE_GAMMA_MAX_RETRIES": "5"},
    )
    try:
        with pytest.raises(RetriesExhausted):
            run_capture(runtime, "daily")
        failed = runtime.ledger.latest_attempt(
            runtime.ledger.list_batches()[0]["batch_id"], "events_keyset_open"
        )
        saved = failed["fetched_seq"]
        assert saved >= 1
        fake.rules.clear()
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="oddsfox_catalogue.capture.runner"):
            summary = run_capture(runtime, "daily")
        assert summary.resumed is True
    finally:
        runtime.ledger.close()
    messages = [record.getMessage() for record in caplog.records]
    assert f"capture scan events_keyset_open resuming after page {saved}" in messages


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
        assert names[: len(BOOTSTRAP_LIST_SCANS)] == BOOTSTRAP_LIST_SCANS
        list_requests = [params for path, params in fake.requests if path == "/events/keyset"]
        assert list_requests, "bootstrap must list events"
        assert all("closed" not in params for params in list_requests), (
            "event id ranges must not filter by closed"
        )
        assert all("include_children" not in params for params in list_requests)
        assert all("order" not in params and "ascending" not in params for params in list_requests)
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

        event_scan = _scans_by_name(runtime.ledger, summary.batch_id)["events_ids_0001"][0]
        captured_ids = []
        for _, records in iter_scan_pages(
            runtime.settings, runtime.ledger.get_batch(summary.batch_id), event_scan
        ):
            captured_ids.extend(r["id"] for r in records)
        assert "909" in captured_ids, "an id inside the high-water mark is requested by id"

        batch_marker = next(runtime.settings.raw_dir.glob("*/*/_batch.json"))
        marker = json.loads(batch_marker.read_text())
        assert marker["status"] == "captured"
    finally:
        runtime.ledger.close()


def test_id_range_records_empty_windows_and_stops_the_tail(tmp_path: Path) -> None:
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    try:
        summary = run_capture(runtime, "bootstrap")
        event_scan = _scans_by_name(runtime.ledger, summary.batch_id)["events_ids_0001"][0]
        pages = runtime.ledger.pages_for_scan(event_scan["scan_id"])
        assert pages[0]["record_count"] == 0, "ids 1-100 are a real empty window"
        assert pages[0]["terminal"] == 0
        assert pages[0]["offset_start"] == 1 and pages[0]["offset_end"] == 100
        assert any(page["record_count"] > 0 for page in pages)
        assert json.loads(event_scan["params_json"])["hi"] == 303

        tail = _scans_by_name(runtime.ledger, summary.batch_id)["events_ids_tail"][0]
        tail_pages = runtime.ledger.pages_for_scan(tail["scan_id"])
        assert [page["record_count"] for page in tail_pages] == [0, 0, 0]
        assert tail_pages[-1]["terminal"] == 1
        assert tail_pages[0]["offset_start"] == 304
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
    fake.expire_cursor("/markets/keyset", times=1)
    runtime, _ = build_runtime(tmp_path, fake, env=ONE_PER_PAGE)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        attempts = _scans_by_name(runtime.ledger, summary.batch_id)["markets_keyset_open"]
        assert [a["attempt"] for a in attempts] == [1, 2]
        assert [a["status"] for a in attempts] == ["abandoned", "complete"]
        assert len(runtime.ledger.pages_for_scan(attempts[0]["scan_id"])) >= 1
        complete_pages = runtime.ledger.pages_for_scan(attempts[1]["scan_id"])
        assert sum(p["record_count"] for p in complete_pages) == 4
    finally:
        runtime.ledger.close()


def test_exhausted_retries_fail_the_scan_and_resume_later(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.fail_status("/events/keyset", 503, times=50)
    runtime, _ = build_runtime(tmp_path, fake, open_event_ids=lambda: {"101", "202"})
    try:
        with pytest.raises(RetriesExhausted):
            run_capture(runtime, "daily")
        batch = runtime.ledger.list_batches()[0]
        assert batch["status"] == "capturing"
        assert "RetriesExhausted" in batch["error"]
        failed = runtime.ledger.latest_attempt(batch["batch_id"], "events_keyset_open")
        assert failed["status"] == "failed"

        fake.rules.clear()
        summary = run_capture(runtime, "daily")
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
    # One worker, so the fault interrupts the single in-flight page.
    serial = {"CATALOGUE_CAPTURE_WORKERS": "1"}
    clean_root = tmp_path / "clean"
    clean_fake = FakeGamma(demo_world())
    clean_runtime, _ = build_runtime(clean_root, clean_fake, env=serial)
    try:
        clean = run_capture(clean_runtime, "bootstrap")
        clean_pages = _page_fingerprints(clean_runtime.ledger, clean.batch_id)
    finally:
        clean_runtime.ledger.close()

    crash_root = tmp_path / "crash"
    env = {
        **os.environ,
        "CATALOGUE_FAULT": "after_ledger_commit",
        "CATALOGUE_CAPTURE_WORKERS": "1",
        "PYTHONPATH": str(TESTS_DIR),
    }
    result = subprocess.run(
        [sys.executable, str(CHILD), str(crash_root), "bootstrap", FIXED_NOW.isoformat()],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == CRASH_EXIT_CODE, result.stderr

    runtime, _ = build_runtime(crash_root, FakeGamma(demo_world()), env=serial)
    try:
        resumed = run_capture(runtime, "bootstrap")
        assert resumed.resumed is True
        assert resumed.pages_adopted == 0, "ledger already had the crashed page"
        assert resumed.status == "captured"
        assert _page_fingerprints(runtime.ledger, resumed.batch_id) == clean_pages
    finally:
        runtime.ledger.close()


def test_resume_adopts_durable_page_missing_from_ledger(tmp_path: Path) -> None:
    serial = {"CATALOGUE_CAPTURE_WORKERS": "1"}
    clean_root = tmp_path / "clean"
    clean_runtime, _ = build_runtime(clean_root, FakeGamma(demo_world()), env=serial)
    try:
        clean = run_capture(clean_runtime, "bootstrap")
        clean_pages = _page_fingerprints(clean_runtime.ledger, clean.batch_id)
    finally:
        clean_runtime.ledger.close()

    crash_root = tmp_path / "crash"
    env = {
        **os.environ,
        "CATALOGUE_FAULT": "after_page_rename",
        "CATALOGUE_CAPTURE_WORKERS": "1",
        "PYTHONPATH": str(TESTS_DIR),
    }
    result = subprocess.run(
        [sys.executable, str(CHILD), str(crash_root), "bootstrap", FIXED_NOW.isoformat()],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == CRASH_EXIT_CODE, result.stderr

    runtime, _ = build_runtime(crash_root, FakeGamma(demo_world()), env=serial)
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
    runtime, _ = build_runtime(tmp_path, fake, open_event_ids=lambda: {"101"})
    try:
        with pytest.raises(RetriesExhausted):
            run_capture(runtime, "daily")
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
    assert "include_children" not in daily[0].param_dict
    assert "order" not in daily[0].param_dict


def test_bootstrap_plan_uses_id_ranges(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        bootstrap = list_scans_for(
            "bootstrap",
            runtime.settings.gamma,
            capture=runtime.settings.capture,
            client=runtime.client,
        )
        reconcile = list_scans_for(
            "reconcile",
            runtime.settings.gamma,
            capture=runtime.settings.capture,
            client=runtime.client,
        )
    finally:
        runtime.ledger.close()
        runtime.client.close()
    assert [s.name for s in bootstrap] == BOOTSTRAP_LIST_SCANS
    assert [s.name for s in reconcile] == BOOTSTRAP_LIST_SCANS
    assert all(s.kind == "id_range" for s in bootstrap if s.name != "markets_keyset_open")


def test_id_range_resumes_after_a_rejected_window(tmp_path: Path) -> None:
    fake = FakeGamma(demo_world())
    fake.rules.append(
        Rule(
            lambda request: (
                request.url.path == "/events/keyset" and "101" in request.url.params.get_list("id")
            ),
            lambda request: httpx.Response(
                422, json={"type": "validation error", "error": "bad window"}
            ),
            remaining=1,
        )
    )
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        with pytest.raises(Exception, match="bad window"):
            run_capture(runtime, "bootstrap")
        batch_id = runtime.ledger.list_batches()[0]["batch_id"]
        failed = runtime.ledger.latest_attempt(batch_id, "events_ids_0001")
        assert failed["fetched_seq"] == 1
        assert failed["status"] == "failed"
        fake.rules.clear()
        summary = run_capture(runtime, "bootstrap")
        assert summary.resumed is True and summary.status == "captured"
        scan = _scans_by_name(runtime.ledger, summary.batch_id)["events_ids_0001"][0]
        captured = []
        batch = runtime.ledger.get_batch(summary.batch_id)
        for _, records in iter_scan_pages(runtime.settings, batch, scan):
            captured.extend(record["id"] for record in records)
        assert captured == ["101", "202", "303"]
    finally:
        runtime.ledger.close()


def test_max_id_override_caps_the_plan_and_skips_the_tail(tmp_path: Path) -> None:
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_MAX_ID_OVERRIDE": "100"}
    )
    try:
        summary = run_capture(runtime, "bootstrap")
        names = [scan["scan_name"] for scan in runtime.ledger.list_scans(summary.batch_id)]
        assert "events_ids_tail" not in names
        assert "markets_closed_ids_tail" not in names
        scan = _scans_by_name(runtime.ledger, summary.batch_id)["events_ids_0001"][0]
        assert json.loads(scan["params_json"])["hi"] == 100
        pages = runtime.ledger.pages_for_scan(scan["scan_id"])
        assert len(pages) == 1
        assert pages[0]["record_count"] == 0
    finally:
        runtime.ledger.close()


def test_a_single_id_that_keeps_failing_is_marked_fetch_failed(tmp_path: Path) -> None:
    from fakes.world import World

    world = World()
    kept = make_event("50", "Kept")
    kept["markets"] = [make_market("60", "Kept market", event_stub=event_stub(kept))]
    world.add_event(kept)
    world.add_event(make_event("150", "Dropped"))
    fake = FakeGamma(world)

    def hits_150(request: httpx.Request) -> bool:
        if request.url.path == "/events/150":
            return True
        return request.url.path == "/events/keyset" and "150" in request.url.params.get_list("id")

    fake.rules.append(
        Rule(
            hits_150,
            lambda request: httpx.Response(500, json={"error": "down"}),
            remaining=500,
        )
    )
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        batch = runtime.ledger.get_batch(summary.batch_id)
        captured: list[str] = []
        failed_ids: list[str] = []
        for scan in runtime.ledger.list_scans(summary.batch_id):
            if scan["kind"] != "id_range" or scan["record_key"] != "events":
                continue
            directory = (
                runtime.settings.raw_dir
                / batch["observation_date"]
                / batch["batch_id"]
                / scan["scan_id"]
            )
            for manifest, records in iter_scan_pages(runtime.settings, batch, scan):
                captured.extend(record["id"] for record in records)
                body = json.loads(read_body(directory, manifest))
                failed_ids.extend(item["id"] for item in body.get("fetch_failed", []))
        assert captured == ["50"]
        assert failed_ids == ["150"]
    finally:
        runtime.ledger.close()


def test_a_referenced_failed_event_is_not_fetched_again(tmp_path: Path) -> None:
    from fakes.world import World

    world = World()
    dropped = make_event("150", "Dropped")
    dropped["markets"] = [make_market("160", "Dropped market", event_stub=event_stub(dropped))]
    world.add_event(dropped)
    fake = FakeGamma(world)

    def hits_150(request: httpx.Request) -> bool:
        if request.url.path == "/events/150":
            return True
        return request.url.path == "/events/keyset" and "150" in request.url.params.get_list("id")

    fake.rules.append(
        Rule(hits_150, lambda request: httpx.Response(500, json={"error": "down"}), remaining=500)
    )
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        names = [scan["scan_name"] for scan in runtime.ledger.list_scans(summary.batch_id)]
        assert not any(name.startswith("events_by_id") for name in names)
        batch = runtime.ledger.get_batch(summary.batch_id)
        failed_ids: list[str] = []
        for scan in runtime.ledger.list_scans(summary.batch_id):
            if scan["kind"] != "id_range" or scan["record_key"] != "events":
                continue
            directory = (
                runtime.settings.raw_dir
                / batch["observation_date"]
                / batch["batch_id"]
                / scan["scan_id"]
            )
            for manifest, _records in iter_scan_pages(runtime.settings, batch, scan):
                body = json.loads(read_body(directory, manifest))
                failed_ids.extend(item["id"] for item in body.get("fetch_failed", []))
        assert failed_ids == ["150"]
    finally:
        runtime.ledger.close()
