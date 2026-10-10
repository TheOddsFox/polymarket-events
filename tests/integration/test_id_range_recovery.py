"""Id-range recovery: raw rebuilds keep planned scans, tails end under outages, pool reopens."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import BrokenThreadPool
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import World, demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import iter_scan_pages, scan_dir_for
from oddsfox_catalogue.capture.runner import (
    CaptureSummary,
    _finalise_if_complete,
    _reopen_unstarted,
    _run_ready_scans,
    _scan_row,
    _state_from_row,
    _trailing_empty,
    rebuild_from_raw,
    run_capture,
)
from oddsfox_catalogue.capture.writer import manifest_path, read_body
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.gamma.scans import (
    ScanSpec,
    id_chunk_scan,
    id_range_scan,
    markets_keyset_open,
)
from oddsfox_catalogue.signals import SIGNALS, Terminated

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


def _scan_names(ledger: Ledger, batch_id: str) -> set[str]:
    return {scan["scan_name"] for scan in ledger.list_scans(batch_id)}


def _clean_run(
    root: Path, make_world: Callable[[], World] = demo_world
) -> tuple[set[str], list[tuple[str, str, str]]]:
    """An uninterrupted serial capture: its scan names and page fingerprints."""
    runtime, _ = build_runtime(root, FakeGamma(make_world()), env=SERIAL)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        names = _scan_names(runtime.ledger, summary.batch_id)
        return names, _page_fingerprints(runtime.ledger, summary.batch_id)
    finally:
        runtime.ledger.close()


def _lose_ledger(root: Path) -> Ledger:
    """Delete the ledger files, as a lost or corrupt ledger would, and open an empty one."""
    for suffix in ("", "-wal", "-shm"):
        (root / ".state" / f"ledger.sqlite{suffix}").unlink(missing_ok=True)
    return Ledger(root / ".state" / "ledger.sqlite")


def _crash_while_planning(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    """The first planning that adds a scan of ``kind`` writes one scan marker, then crashes."""
    real_mark = runner._mark_planned

    def mark_then_crash(rt, batch, rows):
        if rows and rows[0]["kind"] == kind:
            real_mark(rt, batch, rows[:1])
            raise SystemExit("simulated crash while planning")
        real_mark(rt, batch, rows)

    monkeypatch.setattr(runner, "_mark_planned", mark_then_crash)


def _planning_crash_then_rebuild_and_resume(
    root: Path, monkeypatch: pytest.MonkeyPatch, kind: str, make_world: Callable[[], World]
) -> None:
    """Crash while planning, lose the ledger, rebuild, and resume to the clean result."""
    clean_names, clean_pages = _clean_run(root.parent / "clean", make_world)
    runtime, _ = build_runtime(root, FakeGamma(make_world()), env=SERIAL)
    _crash_while_planning(monkeypatch, kind)
    try:
        with pytest.raises(SystemExit):
            run_capture(runtime, "bootstrap")
        batch_id = runtime.ledger.list_batches()[0]["batch_id"]
        planned = _scan_names(runtime.ledger, batch_id)  # what the ledger committed
    finally:
        runtime.ledger.close()
    monkeypatch.undo()

    fresh = _lose_ledger(root)
    try:
        rebuild_from_raw(runtime.settings, fresh)
        assert _scan_names(fresh, batch_id) == planned
    finally:
        fresh.close()

    resumed_runtime, _ = build_runtime(root, FakeGamma(make_world()), env=SERIAL)
    try:
        resumed = run_capture(resumed_runtime, "bootstrap")
        assert resumed.status == "captured"
        assert _scan_names(resumed_runtime.ledger, resumed.batch_id) == clean_names
        assert _page_fingerprints(resumed_runtime.ledger, resumed.batch_id) == clean_pages
    finally:
        resumed_runtime.ledger.close()


def test_a_crash_while_planning_stage_zero_keeps_every_planned_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The batch marker lists the whole plan before any scan marker, so a lost ledger drops none."""
    _planning_crash_then_rebuild_and_resume(tmp_path / "crash", monkeypatch, "keyset", demo_world)


def test_a_crash_while_planning_stage_one_keeps_every_planned_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """101 markets reference events that no list holds, so stage one plans two chunks.

    The crash writes only the first chunk's marker. The batch marker must keep the second.
    """
    _planning_crash_then_rebuild_and_resume(
        tmp_path / "crash", monkeypatch, "keyset_ids", lambda: _world_with_ghost_references(101)
    )


def test_a_stage_one_chunk_with_a_rejected_id_is_split_and_captured(tmp_path: Path) -> None:
    """Stage one shares the window policy. A rejected id is split off and quarantined by ID."""
    poison = "990001"
    fake = FakeGamma(_world_with_ghost_references(2))

    def rejected_in_a_window(request: httpx.Request) -> bool:
        return request.url.path == "/events/keyset" and poison in request.url.params.get_list("id")

    def down_by_id(request: httpx.Request) -> bool:
        return request.url.path == f"/events/{poison}"

    fake.rules.append(
        Rule(
            rejected_in_a_window,
            lambda request: httpx.Response(422, json={"error": "bad window"}),
            remaining=10**6,
        )
    )
    fake.rules.append(
        Rule(
            down_by_id, lambda request: httpx.Response(500, json={"error": "down"}), remaining=10**6
        )
    )
    runtime, _ = build_runtime(tmp_path, fake)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        batch = runtime.ledger.get_batch(summary.batch_id)
        scan = runtime.ledger.latest_attempt(summary.batch_id, "events_by_id_0001")
        assert scan["status"] == "complete"
        directory = runtime.settings.raw_dir / batch["observation_date"] / batch["batch_id"]
        failed: list[str] = []
        for manifest, _records in iter_scan_pages(runtime.settings, batch, scan):
            body = json.loads(read_body(directory / scan["scan_id"], manifest))
            failed.extend(item["id"] for item in body.get("fetch_failed", []))
        assert failed == [poison]
    finally:
        runtime.ledger.close()


def test_a_complete_marker_without_its_terminal_page_runs_again(tmp_path: Path) -> None:
    """A scan marked complete is trusted only with its terminal page. Without it, the scan resumes."""
    clean_names, clean_pages = _clean_run(tmp_path / "clean")
    root = tmp_path / "damaged"
    runtime, _ = build_runtime(root, FakeGamma(demo_world()), env=SERIAL)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        batch_id = summary.batch_id
        batch = runtime.ledger.get_batch(batch_id)
        scan = runtime.ledger.latest_attempt(batch_id, "events_ids_tail")
        last_seq = max(page["seq"] for page in runtime.ledger.pages_for_scan(scan["scan_id"]))
        directory = scan_dir_for(
            runtime.settings, batch["observation_date"], batch_id, scan["scan_id"]
        )
        manifest_path(directory, last_seq).unlink()  # the terminal page is gone from disk
    finally:
        runtime.ledger.close()

    fresh = _lose_ledger(root)
    try:
        rebuild_from_raw(runtime.settings, fresh)
        assert fresh.latest_attempt(batch_id, "events_ids_tail")["status"] == "running"
        assert fresh.get_batch(batch_id)["status"] == "capturing"
    finally:
        fresh.close()

    resumed_runtime, _ = build_runtime(root, FakeGamma(demo_world()), env=SERIAL)
    try:
        resumed = run_capture(resumed_runtime, "bootstrap")
        assert resumed.status == "captured"
        assert _scan_names(resumed_runtime.ledger, resumed.batch_id) == clean_names
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


def _world_with_ghost_references(count: int) -> World:
    """Demo world whose ``count`` markets each reference an event that no list holds.

    Stage one fetches those ids by ID, 100 to a chunk, so more than 100 of them plan two chunks.
    """
    world = demo_world()
    for number in range(count):
        ghost = {"id": str(990000 + number), "ticker": None, "slug": None, "title": "gone"}
        world.add_direct_market(make_market(str(9100 + number), "Ghost market?", event_stub=ghost))
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


def test_an_abandoned_attempt_with_no_successor_is_resumed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A signal between the two commits of _abandon leaves an abandoned attempt. Resume starts it."""
    fake = FakeGamma(demo_world())
    fake.expire_cursor("/markets/keyset", times=1)
    runtime, _ = build_runtime(tmp_path, fake, env={**SERIAL, "CATALOGUE_GAMMA_PAGE_LIMIT": "1"})
    add_scan_attempt = runtime.ledger.add_scan_attempt
    calls = {"n": 0}

    def crash_before_the_successor(scan: dict[str, Any]) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Crash("signal between the abandoned status and its successor")
        add_scan_attempt(scan)

    monkeypatch.setattr(runtime.ledger, "add_scan_attempt", crash_before_the_successor)
    try:
        with pytest.raises(_Crash):
            run_capture(runtime, "bootstrap")
        batch_id = runtime.ledger.list_batches()[0]["batch_id"]
        latest = runtime.ledger.latest_attempt(batch_id, "markets_keyset_open")
        assert latest is not None and latest["status"] == "abandoned"

        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        attempts = [
            s
            for s in runtime.ledger.list_scans(batch_id)
            if s["scan_name"] == "markets_keyset_open"
        ]
        assert [(a["attempt"], a["status"]) for a in attempts] == [
            (1, "abandoned"),
            (2, "complete"),
        ]
    finally:
        runtime.ledger.close()


@pytest.mark.parametrize("shape", ["legacy_order", "no_open_crawl"])
def test_a_market_batch_that_cannot_prove_coverage_is_abandoned_not_resumed(
    tmp_path: Path, shape: str
) -> None:
    """Resume only plans that put the open crawl first. Any other batch is abandoned, raw kept."""
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()), env=SERIAL)
    old_id = "20261007T000000Z-bootstrap"
    stamp = "2026-10-07T00:00:00Z"
    closed = id_range_scan(
        "markets_closed_ids_0001",
        "/markets/keyset",
        "markets",
        lo=1,
        hi=100,
        closed=True,
        tail=False,
    )
    rows = [_scan_row(old_id, closed, attempt=1, plan_order=1, started_at=stamp)]
    if shape == "legacy_order":
        crawl = markets_keyset_open(runtime.settings.gamma)
        rows.append(_scan_row(old_id, crawl, attempt=1, plan_order=2, started_at=stamp))
    try:
        runtime.ledger.create_batch(old_id, "bootstrap", "2026-10-07", stamp, None, rows)
        # This closed window finished before the open crawl, so it may have passed a market that
        # closed later and that the open crawl will never return.
        runtime.ledger.set_scan_status(rows[0]["scan_id"], "complete", stamp)

        summary = run_capture(runtime, "bootstrap")
        assert summary.resumed is False
        assert summary.status == "captured"
        assert summary.batch_id != old_id
        assert runtime.ledger.get_batch(old_id)["status"] == "abandoned"
    finally:
        runtime.ledger.close()


def test_a_market_batch_without_the_open_crawl_is_never_captured(tmp_path: Path) -> None:
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()), env=SERIAL)
    batch_id = "20261008T100000Z-bootstrap"
    stamp = "2026-10-08T10:00:00Z"
    events = id_range_scan(
        "events_ids_0001", "/events/keyset", "events", lo=1, hi=100, closed=None, tail=False
    )
    row = _scan_row(batch_id, events, attempt=1, plan_order=1, started_at=stamp)
    try:
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, [row])
        runtime.ledger.set_scan_status(row["scan_id"], "complete", stamp)
        runtime.ledger.restore_batch_state(batch_id, "capturing", 2, stamp)

        assert _finalise_if_complete(runtime, batch_id) == "capturing"
        assert runtime.ledger.get_batch(batch_id)["status"] == "capturing"
    finally:
        runtime.ledger.close()


def test_a_second_interrupt_during_the_drain_waits_for_running_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second Ctrl-C while the drain waits must not return while a worker still runs.

    Returning early would release the ledger and the run lock beside a live worker.
    """
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": "2"}
    )
    batch_id = "20261008T090000Z-bootstrap"
    stamp = "2026-10-08T09:00:00Z"
    running = threading.Event()
    release = threading.Event()
    worker_done = threading.Event()

    def slow_scan(rt, batch, row, *args, **kwargs) -> None:
        running.set()
        release.wait(5)
        worker_done.set()

    real_wait = runner.wait
    real_shutdown = ThreadPoolExecutor.shutdown
    waits = {"n": 0}
    shutdowns = {"n": 0}

    def interrupted_wait(*args, **kwargs):
        waits["n"] += 1
        if waits["n"] == 1:
            # The first Ctrl-C lands while a worker is busy. Cancelling before a worker starts
            # would drop the scan, and the test would prove nothing.
            running.wait(5)
            raise KeyboardInterrupt
        return real_wait(*args, **kwargs)

    def interrupted_shutdown(self, wait=True, *, cancel_futures=False):
        shutdowns["n"] += 1
        if shutdowns["n"] == 1:
            # The second Ctrl-C lands during the drain. The worker is released 0.3 s later.
            threading.Timer(0.3, release.set).start()
            raise KeyboardInterrupt
        return real_shutdown(self, wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(runner, "_run_scan", slow_scan)
    monkeypatch.setattr(runner, "wait", interrupted_wait)
    monkeypatch.setattr(ThreadPoolExecutor, "shutdown", interrupted_shutdown)
    rows = [
        _scan_row(
            batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
        )
        for index in (1, 2)
    ]
    try:
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=True)
        with pytest.raises(KeyboardInterrupt):
            _run_ready_scans(
                runtime,
                runtime.ledger.get_batch(batch_id),
                runtime.ledger.list_scans(batch_id),
                summary,
            )
        assert running.is_set(), "no worker started, so the drain was not exercised"
        assert worker_done.is_set(), "the drain returned while a worker was still running"
    finally:
        release.set()
        runtime.ledger.close()


def test_a_failing_worker_close_does_not_fail_the_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every worker client is closed even when one close fails, and the drain completes."""
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": "2"}
    )
    batch_id = "20261008T095000Z-bootstrap"
    stamp = "2026-10-08T09:50:00Z"
    closes: list[int] = []
    spawned = {"n": 0}

    class _BrokenClose:
        def __init__(self, number: int) -> None:
            self.number = number

        def close(self) -> None:
            closes.append(self.number)
            raise RuntimeError("socket already closed")

    def spawn() -> _BrokenClose:
        spawned["n"] += 1
        return _BrokenClose(spawned["n"])

    monkeypatch.setattr(runner, "_run_scan", lambda rt, batch, row, *args, **kwargs: None)
    monkeypatch.setattr(runtime.client, "spawn", spawn)
    rows = [
        _scan_row(
            batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
        )
        for index in (1, 2)
    ]
    try:
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=True)
        _run_ready_scans(
            runtime,
            runtime.ledger.get_batch(batch_id),
            runtime.ledger.list_scans(batch_id),
            summary,
        )
        assert spawned["n"] >= 1
        assert sorted(closes) == list(range(1, spawned["n"] + 1))
    finally:
        runtime.ledger.close()


def test_a_batch_on_a_deep_keyset_plan_is_abandoned_not_resumed(tmp_path: Path) -> None:
    """A bootstrap planned on the deep keyset is never resumed, even with the open crawl first."""
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()), env=SERIAL)
    old_id = "20261007T010000Z-bootstrap"
    stamp = "2026-10-07T01:00:00Z"
    crawl = markets_keyset_open(runtime.settings.gamma)
    deep = ScanSpec("events_keyset_all", "keyset", "/events/keyset", "events")
    rows = [
        _scan_row(old_id, crawl, attempt=1, plan_order=1, started_at=stamp),
        _scan_row(old_id, deep, attempt=1, plan_order=2, started_at=stamp),
    ]
    try:
        runtime.ledger.create_batch(old_id, "bootstrap", "2026-10-07", stamp, None, rows)
        runtime.ledger.set_scan_status(rows[0]["scan_id"], "complete", stamp)

        summary = run_capture(runtime, "bootstrap")
        assert summary.resumed is False
        assert summary.status == "captured"
        assert runtime.ledger.get_batch(old_id)["status"] == "abandoned"
    finally:
        runtime.ledger.close()


def test_a_batch_with_no_list_scans_is_abandoned_not_resumed(tmp_path: Path) -> None:
    """A batch with no scans was never planned. Resuming it would stay capturing forever."""
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()), env=SERIAL)
    old_id = "20261007T020000Z-bootstrap"
    stamp = "2026-10-07T02:00:00Z"
    try:
        runtime.ledger.create_batch(old_id, "bootstrap", "2026-10-07", stamp, None, [])

        summary = run_capture(runtime, "bootstrap")
        assert summary.resumed is False
        assert summary.status == "captured"
        assert runtime.ledger.get_batch(old_id)["status"] == "abandoned"
    finally:
        runtime.ledger.close()


def test_small_id_partitions_capture_every_event_once(tmp_path: Path) -> None:
    """Several event partitions cover the same events as one, and each event is captured once."""
    world = demo_world()  # event ids 101 to 303, so width 100 gives several partitions
    env = {**SERIAL, "CATALOGUE_CAPTURE_ID_PARTITION_SIZE": "100"}
    runtime, _ = build_runtime(tmp_path, FakeGamma(world), env=env)
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        event_scans = [
            s
            for s in runtime.ledger.list_scans(summary.batch_id)
            if s["scan_name"].startswith("events_ids_")
        ]
        partitions = [s for s in event_scans if s["scan_name"] != "events_ids_tail"]
        assert len(partitions) >= 2
        assert sum(s["record_count"] for s in event_scans) == len(world.events)
    finally:
        runtime.ledger.close()


def test_high_water_is_read_once_per_batch_even_across_a_resume(tmp_path: Path) -> None:
    """The plan stores each high-water mark, so a resumed batch does not read them again."""
    fake = FakeGamma(demo_world())
    fake.crash_on("/markets/keyset", times=1)  # after the plan is written, mid-batch
    runtime, _ = build_runtime(tmp_path, fake, env=SERIAL)
    try:
        with pytest.raises(SystemExit):
            run_capture(runtime, "bootstrap")
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        assert summary.resumed is True
        assert fake.calls_to("/events") == 1
        assert fake.calls_to("/markets") == 1
    finally:
        runtime.ledger.close()


def test_a_signal_during_the_drain_still_closes_every_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A signal in the drain re-raises, and a failing client close does not stop the others."""
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": "2"}
    )
    batch_id = "20261008T095500Z-bootstrap"
    stamp = "2026-10-08T09:55:00Z"
    closes: list[int] = []
    spawned = {"n": 0}
    shutdowns = {"n": 0}
    real_shutdown = ThreadPoolExecutor.shutdown

    class _BrokenClose:
        def __init__(self, number: int) -> None:
            self.number = number

        def close(self) -> None:
            closes.append(self.number)
            raise RuntimeError("socket already closed")

    def spawn() -> _BrokenClose:
        spawned["n"] += 1
        return _BrokenClose(spawned["n"])

    def signalled_shutdown(self, wait=True, *, cancel_futures=False):
        shutdowns["n"] += 1
        if shutdowns["n"] == 1:
            SIGNALS.receive(signal.SIGHUP)  # a signal lands while the pool drains: it is held
        return real_shutdown(self, wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(runner, "_run_scan", lambda rt, batch, row, *args, **kwargs: None)
    monkeypatch.setattr(runtime.client, "spawn", spawn)
    monkeypatch.setattr(ThreadPoolExecutor, "shutdown", signalled_shutdown)
    rows = [
        _scan_row(
            batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
        )
        for index in (1, 2)
    ]
    try:
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=True)
        with pytest.raises(Terminated) as info:
            _run_ready_scans(
                runtime,
                runtime.ledger.get_batch(batch_id),
                runtime.ledger.list_scans(batch_id),
                summary,
            )
        assert info.value.signum == signal.SIGHUP
        assert spawned["n"] >= 1
        assert sorted(closes) == list(range(1, spawned["n"] + 1))
        # The drain joined every worker before it re-raised: no capture thread outlives it.
        assert not [t for t in threading.enumerate() if t.name.startswith("capture")]
    finally:
        runtime.ledger.close()


def test_the_first_signal_is_the_one_that_is_reraised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SIGTERM in the wait loop, then SIGHUP in the drain: the stage exits as SIGTERM."""
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": "2"}
    )
    batch_id = "20261008T100500Z-bootstrap"
    stamp = "2026-10-08T10:05:00Z"
    real_wait = runner.wait
    real_shutdown = ThreadPoolExecutor.shutdown
    waits = {"n": 0}
    shutdowns = {"n": 0}

    def sigterm_wait(*args, **kwargs):
        waits["n"] += 1
        if waits["n"] == 1:
            raise Terminated(signal.SIGTERM)
        return real_wait(*args, **kwargs)

    def sighup_shutdown(self, wait=True, *, cancel_futures=False):
        shutdowns["n"] += 1
        if shutdowns["n"] == 1:
            SIGNALS.receive(signal.SIGHUP)  # held: the first signal (SIGTERM) is the one raised
        return real_shutdown(self, wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(runner, "_run_scan", lambda rt, batch, row, *args, **kwargs: None)
    monkeypatch.setattr(runner, "wait", sigterm_wait)
    monkeypatch.setattr(ThreadPoolExecutor, "shutdown", sighup_shutdown)
    rows = [
        _scan_row(
            batch_id, id_chunk_scan(index, ["1"]), attempt=1, plan_order=index, started_at=stamp
        )
        for index in (1, 2)
    ]
    try:
        runtime.ledger.create_batch(batch_id, "bootstrap", "2026-10-08", stamp, None, rows)
        summary = CaptureSummary(batch_id=batch_id, status="capturing", resumed=True)
        with pytest.raises(Terminated) as info:
            _run_ready_scans(
                runtime,
                runtime.ledger.get_batch(batch_id),
                runtime.ledger.list_scans(batch_id),
                summary,
            )
        assert info.value.signum == signal.SIGTERM
    finally:
        runtime.ledger.close()


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
        shutdowns = {"n": 0}

        def interrupted_shutdown(self, wait=True, *, cancel_futures=False):
            shutdowns["n"] += 1
            real_shutdown(self, wait=wait, cancel_futures=cancel_futures)
            if shutdowns["n"] == 1:
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
