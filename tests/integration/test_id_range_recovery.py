"""Id-range recovery: raw rebuilds keep planned scans, tails end under outages, pool reopens."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import BrokenThreadPool
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import iter_scan_pages
from oddsfox_catalogue.capture.runner import (
    CaptureSummary,
    _reopen_unstarted,
    _run_ready_scans,
    _scan_row,
    _state_from_row,
    _trailing_empty,
    rebuild_from_raw,
    run_capture,
)
from oddsfox_catalogue.capture.writer import read_body
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.gamma.scans import id_chunk_scan

TESTS_DIR = Path(__file__).resolve().parents[1]
CHILD = TESTS_DIR / "fakes" / "capture_child.py"
SERIAL = {"CATALOGUE_CAPTURE_WORKERS": "1"}
BOOTSTRAP_LIST_SCANS = {
    "events_ids_0001",
    "events_ids_tail",
    "markets_closed_ids_0001",
    "markets_closed_ids_tail",
    "markets_keyset_open",
}


def _page_fingerprints(ledger: Ledger, batch_id: str) -> list[tuple[str, str, str]]:
    return [
        (p["page_id"], p["body_sha256"], p["scan_status"]) for p in ledger.pages_for_batch(batch_id)
    ]


def test_rebuild_restores_planned_scans_that_never_started(tmp_path: Path) -> None:
    """A crash in stage 0 must not lose the scans that were planned but had not started."""
    clean_root = tmp_path / "clean"
    clean_runtime, _ = build_runtime(clean_root, FakeGamma(demo_world()), env=SERIAL)
    try:
        clean = run_capture(clean_runtime, "bootstrap")
        clean_pages = _page_fingerprints(clean_runtime.ledger, clean.batch_id)
    finally:
        clean_runtime.ledger.close()

    crash_root = tmp_path / "crash"
    env = {
        **os.environ,
        **SERIAL,
        "CATALOGUE_FAULT": "after_ledger_commit",
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

    runtime, _ = build_runtime(crash_root, FakeGamma(demo_world()), env=SERIAL)
    settings = runtime.settings
    runtime.ledger.close()
    for suffix in ("", "-wal", "-shm"):
        (crash_root / ".state" / f"ledger.sqlite{suffix}").unlink(missing_ok=True)

    fresh = Ledger(crash_root / ".state" / "ledger.sqlite")
    try:
        rebuild_from_raw(settings, fresh)
        batch = fresh.list_batches()[0]
        restored = {scan["scan_name"] for scan in fresh.list_scans(batch["batch_id"])}
    finally:
        fresh.close()
    assert restored == BOOTSTRAP_LIST_SCANS

    resumed_runtime, _ = build_runtime(crash_root, FakeGamma(demo_world()), env=SERIAL)
    try:
        resumed = run_capture(resumed_runtime, "bootstrap")
        assert resumed.status == "captured"
        assert _page_fingerprints(resumed_runtime.ledger, resumed.batch_id) == clean_pages
    finally:
        resumed_runtime.ledger.close()


def test_tail_above_the_mark_ends_when_every_id_fails(tmp_path: Path) -> None:
    """Every event id above the high-water mark fails. The tail still stops after 3 windows."""
    fake = FakeGamma(demo_world())

    def above_the_mark(request: httpx.Request) -> bool:
        if request.url.path == "/events/keyset":
            return any(int(i) > 303 for i in request.url.params.get_list("id"))
        last = request.url.path.rsplit("/", 1)[-1]
        return request.url.path.startswith("/events/") and last.isdigit() and int(last) > 303

    fake.rules.append(
        Rule(
            above_the_mark,
            lambda request: httpx.Response(500, json={"error": "down"}),
            remaining=10**6,
        )
    )
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        batch = runtime.ledger.get_batch(summary.batch_id)
        tail = next(
            s
            for s in runtime.ledger.list_scans(summary.batch_id)
            if s["scan_name"] == "events_ids_tail"
        )
        pages = runtime.ledger.pages_for_scan(tail["scan_id"])
        assert [page["terminal"] for page in pages] == [0, 0, 1]

        directory = runtime.settings.raw_dir / batch["observation_date"] / batch["batch_id"]
        failed: list[str] = []
        for manifest, _records in iter_scan_pages(runtime.settings, batch, tail):
            body = json.loads(read_body(directory / tail["scan_id"], manifest))
            failed.extend(item["id"] for item in body.get("fetch_failed", []))
        assert len(failed) == 300
    finally:
        runtime.ledger.close()


def test_a_batch_with_no_list_scans_is_never_captured(tmp_path: Path) -> None:
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    batch_id = "20261008T000000Z-bootstrap"
    try:
        runtime.ledger.create_batch(
            batch_id, "bootstrap", "2026-10-08", "2026-10-08T00:00:00Z", None, []
        )
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "capturing"
        assert runtime.ledger.get_batch(batch_id)["status"] == "capturing"
        assert runtime.ledger.list_scans(batch_id) == []
    finally:
        runtime.ledger.close()


def test_a_failed_scan_the_pool_never_started_is_reopened(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "ledger.sqlite")
    batch_id = "20261008T060000Z-bootstrap"
    stamp = "2026-10-08T06:00:00Z"
    try:
        rows = [
            _scan_row(
                batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
            )
            for index in (1, 2)
        ]
        ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        for row in rows:
            ledger.set_scan_status(row["scan_id"], "failed", stamp, "boom")

        listed = ledger.list_scans(batch_id)
        ran, never_ran = listed[0]["scan_id"], listed[1]["scan_id"]
        runtime = SimpleNamespace(ledger=ledger, now=lambda: FIXED_NOW)
        _reopen_unstarted(runtime, listed, started={ran})

        status = {scan["scan_id"]: scan["status"] for scan in ledger.list_scans(batch_id)}
        assert status[ran] == "failed"
        assert status[never_ran] == "running"
    finally:
        ledger.close()


def test_trailing_empty_counts_only_the_final_run() -> None:
    assert _trailing_empty([{"record_count": 2}, {"record_count": 0}, {"record_count": 0}]) == 2
    assert _trailing_empty([{"record_count": 0}, {"record_count": 0}, {"record_count": 5}]) == 0
    assert _trailing_empty([]) == 0


def _page(batch_id: str, seq: int, record_count: int) -> dict[str, Any]:
    return {
        "page_id": f"{batch_id}:{seq}",
        "batch_id": batch_id,
        "seq": seq,
        "endpoint": "/events/keyset",
        "params_json": "{}",
        "input_cursor": None,
        "output_cursor": None,
        "offset_start": None,
        "offset_end": None,
        "record_count": record_count,
        "http_status": 200,
        "retries": 0,
        "latency_s": 0.0,
        "terminal": 0,
        "body_sha256": "0" * 64,
        "gz_sha256": "0" * 64,
        "observed_at": FIXED_NOW.isoformat(),
        "ids_hash": f"ids-{seq}",
    }


def test_pool_reopens_failed_scans_it_never_started(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the pool aborts before any worker starts, failed scans are reopened, not left failed."""
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()), env=SERIAL)
    batch_id = "20261008T060000Z-bootstrap"
    stamp = "2026-10-08T06:00:00Z"
    try:
        rows = [
            _scan_row(
                batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
            )
            for index in (1, 2)
        ]
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        for row in rows:
            runtime.ledger.set_scan_status(row["scan_id"], "failed", stamp, "boom")

        def no_worker_client() -> None:
            raise RuntimeError("worker client unavailable")

        monkeypatch.setattr(runtime.client, "spawn", no_worker_client)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=True)
        with pytest.raises(BrokenThreadPool):
            _run_ready_scans(
                runtime,
                runtime.ledger.get_batch(batch_id),
                runtime.ledger.list_scans(batch_id),
                summary,
            )

        statuses = [scan["status"] for scan in runtime.ledger.list_scans(batch_id)]
        assert statuses == ["running", "running"]
    finally:
        runtime.ledger.close()


def test_resumed_state_carries_the_trailing_empty_run(tmp_path: Path) -> None:
    """Resume reads the empty run from the ledger, so a resumed tail still ends on time."""
    ledger = Ledger(tmp_path / "ledger.sqlite")
    batch_id = "20261008T070000Z-bootstrap"
    stamp = "2026-10-08T07:00:00Z"
    try:
        rows = [
            _scan_row(batch_id, id_chunk_scan(1, ["1"]), attempt=1, plan_order=1, started_at=stamp)
        ]
        ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        scan_id = rows[0]["scan_id"]
        for seq, record_count in ((1, 4), (2, 0), (3, 0)):
            ledger.record_page(_page(batch_id, seq, record_count), scan_id, finished_terminal=False)

        state = _state_from_row(SimpleNamespace(ledger=ledger), ledger.get_scan(scan_id))
    finally:
        ledger.close()

    assert state.seq == 3
    assert state.empty_run == 2


class _Crash(RuntimeError):
    """Stands in for a crash just before a scan starts, after its plan is committed."""


def _world_with_missing_event() -> object:
    """Demo world with one event hidden from the id scans and one market whose event is gone.

    The hidden event needs a stage-1 id fetch. The ghost id is missing after stage 1, so
    it needs a stage-2 single-id fetch.
    """
    world = demo_world()
    hidden = make_event("909", "Hidden referenced event")
    hidden["markets"] = [make_market("9001", "Hidden market?", event_stub=event_stub(hidden))]
    world.add_event(hidden)
    ghost = {"id": "999999", "ticker": None, "slug": None, "title": "gone"}
    world.add_direct_market(make_market("9002", "Ghost market?", event_stub=ghost))
    return world


@pytest.mark.parametrize("crash_at", ["events_by_id_0001", "events_by_id_single_0001"])
def test_rebuild_restores_each_follow_up_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash_at: str
) -> None:
    """A follow-up stage planned before the crash is restored even if none of its scans ran."""
    crash_root = tmp_path / "crash"
    runtime, _ = build_runtime(crash_root, FakeGamma(_world_with_missing_event()), env=SERIAL)
    real_run_scan = runner._run_scan

    def crash_before_scan(rt, batch, row, *args, **kwargs):
        if row["scan_name"] == crash_at:
            raise _Crash(crash_at)
        return real_run_scan(rt, batch, row, *args, **kwargs)

    monkeypatch.setattr(runner, "_run_scan", crash_before_scan)
    try:
        with pytest.raises(_Crash):
            run_capture(runtime, "bootstrap")
        batch_id = runtime.ledger.list_batches()[0]["batch_id"]
        planned = {scan["scan_name"] for scan in runtime.ledger.list_scans(batch_id)}
        settings = runtime.settings
    finally:
        runtime.ledger.close()
    assert crash_at in planned

    for suffix in ("", "-wal", "-shm"):
        (crash_root / ".state" / f"ledger.sqlite{suffix}").unlink(missing_ok=True)
    fresh = Ledger(crash_root / ".state" / "ledger.sqlite")
    try:
        rebuild_from_raw(settings, fresh)
        restored = {
            scan["scan_name"] for scan in fresh.list_scans(fresh.list_batches()[0]["batch_id"])
        }
    finally:
        fresh.close()
    assert restored == planned


def test_closed_market_windows_wait_for_the_open_crawl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A closed window that runs before the open crawl can miss a market that closes mid-batch.

    The open crawl waits up to 2 s for any closed window to start. The hold keeps every closed
    window back until the crawl ends, so that wait times out and no closed window starts during
    it. Without the hold, a closed window starts during the wait and the order check fails.
    """
    order: list[str] = []
    lock = threading.Lock()
    closed_started = threading.Event()
    real_run_scan = runner._run_scan

    def recording(rt, batch, row, *args, **kwargs):
        name = row["scan_name"]
        with lock:
            order.append(f"start:{name}")
        if name == "markets_keyset_open":
            closed_started.wait(2)
        elif name.startswith("markets_closed_ids_"):
            closed_started.set()
        try:
            return real_run_scan(rt, batch, row, *args, **kwargs)
        finally:
            with lock:
                order.append(f"end:{name}")

    monkeypatch.setattr(runner, "_run_scan", recording)
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": "4"}
    )
    try:
        summary = run_capture(runtime, "bootstrap")
    finally:
        runtime.ledger.close()

    assert summary.status == "captured"
    closed = [i for i, entry in enumerate(order) if entry.startswith("start:markets_closed_ids_")]
    assert closed, "the demo world has closed-market windows"
    assert min(closed) > order.index("end:markets_keyset_open")


def test_a_single_event_that_keeps_failing_is_quarantined(tmp_path: Path) -> None:
    """A stage-2 id that keeps failing is quarantined, so the batch still captures."""
    fake = FakeGamma(_world_with_missing_event())
    fake.rules.append(
        Rule(
            lambda request: request.url.path == "/events/999999",
            lambda _: httpx.Response(500, json={"error": "down"}),
            remaining=10**6,
        )
    )
    runtime, _ = build_runtime(tmp_path, fake, env=SERIAL)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        batch = runtime.ledger.get_batch(summary.batch_id)
        scan = next(
            s
            for s in runtime.ledger.list_scans(summary.batch_id)
            if s["scan_name"] == "events_by_id_single_0001"
        )
        directory = runtime.settings.raw_dir / batch["observation_date"] / batch["batch_id"]
        failed: list[str] = []
        for manifest, _records in iter_scan_pages(runtime.settings, batch, scan):
            body = json.loads(read_body(directory / scan["scan_id"], manifest))
            failed.extend(item["id"] for item in body.get("fetch_failed", []))
    finally:
        runtime.ledger.close()
    assert failed == ["999999"]


def test_a_non_terminated_error_during_shutdown_still_reopens_unstarted_scans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup still runs when shutdown raises something other than Terminated."""
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()), env=SERIAL)
    batch_id = "20261008T080000Z-bootstrap"
    stamp = "2026-10-08T08:00:00Z"
    try:
        rows = [
            _scan_row(
                batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
            )
            for index in (1, 2)
        ]
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        for row in rows:
            runtime.ledger.set_scan_status(row["scan_id"], "failed", stamp, "boom")

        def no_worker_client() -> None:
            raise RuntimeError("worker client unavailable")

        real_shutdown = ThreadPoolExecutor.shutdown

        def interrupted_shutdown(self, wait=True, *, cancel_futures=False):
            real_shutdown(self, wait=wait, cancel_futures=cancel_futures)
            raise KeyboardInterrupt

        monkeypatch.setattr(runtime.client, "spawn", no_worker_client)
        monkeypatch.setattr(ThreadPoolExecutor, "shutdown", interrupted_shutdown)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=True)
        with pytest.raises(KeyboardInterrupt):
            _run_ready_scans(
                runtime,
                runtime.ledger.get_batch(batch_id),
                runtime.ledger.list_scans(batch_id),
                summary,
            )

        statuses = [scan["status"] for scan in runtime.ledger.list_scans(batch_id)]
        assert statuses == ["running", "running"]
    finally:
        runtime.ledger.close()
