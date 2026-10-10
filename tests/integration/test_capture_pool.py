"""Worker pool: shared rate limit, matching manifests, and a drain that keeps pages."""

from __future__ import annotations

import json
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import build_runtime
from fakes.world import World, demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import CaptureSummary, run_capture
from oddsfox_catalogue.signals import SIGNALS, Terminated


def _tiny_world() -> World:
    world = World()
    event = make_event("1", "One")
    event["markets"] = [make_market("1", "One market", event_stub=event_stub(event))]
    world.add_event(event)
    return world


def _manifests(runtime, batch_id: str) -> dict[str, list[dict]]:
    batch = runtime.ledger.get_batch(batch_id)
    assert batch is not None
    found: dict[str, list[dict]] = {}
    for scan in runtime.ledger.list_scans(batch_id):
        directory = (
            runtime.settings.raw_dir
            / batch["observation_date"]
            / batch["batch_id"]
            / scan["scan_id"]
        )
        pages = []
        for path in sorted(directory.glob("*.manifest.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload.pop("latency_s", None)
            pages.append(payload)
        found[scan["scan_name"]] = pages
    return found


CONTENT_FIELDS = (
    "seq",
    "http_status",
    "record_count",
    "terminal",
    "body_sha256",
    "ids_hash",
    "output_cursor",
)


def _content(found: dict[str, list[dict]]) -> dict[str, list[tuple]]:
    """Strip the attempt, id, and clock fields that a resumed run legitimately changes."""
    return {
        name: [tuple(page[field] for field in CONTENT_FIELDS) for page in pages]
        for name, pages in found.items()
    }


def _capture(root: Path, workers: str) -> dict[str, list[dict]]:
    runtime, _ = build_runtime(
        root, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": workers}
    )
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        return _manifests(runtime, summary.batch_id)
    finally:
        runtime.ledger.close()


@pytest.mark.parametrize("workers", ["2", "4"])
def test_parallel_manifests_match_a_serial_run(tmp_path: Path, workers: str) -> None:
    assert _capture(tmp_path / "pool", workers) == _capture(tmp_path / "serial", "1")


@pytest.mark.parametrize("workers", ["2", "4"])
def test_a_pooled_failure_then_resume_matches_a_clean_serial_run(
    tmp_path: Path, workers: str
) -> None:
    clean = _capture(tmp_path / "clean", "1")
    fake = FakeGamma(demo_world())
    fake.raise_on("/events/keyset", RuntimeError("injected failure"), times=1)  # one scan fails
    runtime, _ = build_runtime(tmp_path / "pool", fake, env={"CATALOGUE_CAPTURE_WORKERS": workers})
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            run_capture(runtime, "bootstrap")
        interrupted_id = runtime.ledger.list_batches()[0]["batch_id"]
        summary = run_capture(runtime, "bootstrap", resume=interrupted_id)
        assert summary.status == "captured"
        assert summary.resumed is True
        assert summary.batch_id == interrupted_id
        assert _content(_manifests(runtime, summary.batch_id)) == _content(clean)
    finally:
        runtime.ledger.close()


def test_shared_bucket_keeps_the_pool_inside_the_rate(tmp_path: Path) -> None:
    runtime, clock = build_runtime(
        tmp_path,
        FakeGamma(_tiny_world()),
        env={"CATALOGUE_CAPTURE_WORKERS": "4", "CATALOGUE_GAMMA_REQUESTS_PER_SECOND": "5"},
    )
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        requests = runtime.client.stats.requests
        assert requests > 1
        # Elapsed fake time is at least one gap per request after the first.
        # Workers sleep outside the bucket lock, so the fake clock can count
        # overlapping waits more than once. It must not count fewer.
        assert clock.t + 1e-9 >= (requests - 1) / 5
    finally:
        runtime.ledger.close()


def test_a_failing_scan_leaves_the_sibling_pages(tmp_path: Path) -> None:
    fake = FakeGamma(_tiny_world())
    persisted = threading.Event()

    def is_event_scan(request: httpx.Request) -> bool:
        return request.url.path == "/events/keyset"

    def fail_after_sibling(_: httpx.Request) -> httpx.Response:
        assert persisted.wait(5)
        raise RuntimeError("injected failure")  # an ordinary error fails the scan

    fake.rules.append(Rule(is_event_scan, fail_after_sibling, remaining=1))
    runtime, _ = build_runtime(tmp_path, fake, env={"CATALOGUE_CAPTURE_WORKERS": "2"})
    original = runtime.ledger.record_page

    def record_page(*args, **kwargs):
        original(*args, **kwargs)
        persisted.set()

    runtime.ledger.record_page = record_page
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            run_capture(runtime, "bootstrap")
        batch = runtime.ledger.list_batches()[0]
        assert batch["status"] == "capturing"
        scans = {scan["scan_name"]: scan for scan in runtime.ledger.list_scans(batch["batch_id"])}
        assert not any("tail" in name for name in scans)
        assert scans["events_ids_0001"]["status"] == "failed"
        sibling = scans["markets_keyset_open"]
        assert sibling["fetched_seq"] >= 1
        assert runtime.ledger.pages_for_scan(sibling["scan_id"])
    finally:
        runtime.ledger.close()


def test_a_scan_stopped_after_a_page_keeps_that_page_and_stays_running(tmp_path: Path) -> None:
    """A stop that lands mid-scan lets the page in flight finish. The scan then stays running."""
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(_two_event_world()), env={"CATALOGUE_CAPTURE_WORKERS": "1"}
    )
    stop = threading.Event()
    original = runtime.ledger.record_page

    def record_then_stop(*args, **kwargs):
        original(*args, **kwargs)
        stop.set()  # a signal arrives once the first page is durable

    try:
        batch = runner._start_batch(runtime, "selected", ["1", "2"], [])
        runner._seal_plan(runtime, batch)
        batch = runtime.ledger.get_batch(batch["batch_id"])
        selected = next(
            scan
            for scan in runtime.ledger.list_scans(batch["batch_id"])
            if scan["scan_name"] == "markets_selected"
        )
        runtime.ledger.record_page = record_then_stop
        summary = CaptureSummary(batch_id=batch["batch_id"], status="capturing", resumed=False)
        runner._run_scan(runtime, batch, selected, summary, stop)

        scan = runtime.ledger.get_scan(selected["scan_id"])
        assert scan is not None
        assert scan["status"] == "running"
        pages = runtime.ledger.pages_for_scan(selected["scan_id"])
        assert len(pages) == 1 and not pages[0]["terminal"]
    finally:
        runtime.ledger.close()


def _two_event_world() -> World:
    world = World()
    for event_id in ("1", "2"):
        event = make_event(event_id, f"Event {event_id}")
        event["markets"] = [
            make_market(event_id, f"Market {event_id}", event_stub=event_stub(event))
        ]
        world.add_event(event)
    return world


def test_a_scan_that_has_not_started_is_not_fetched(tmp_path: Path) -> None:
    # Two events, so the first events window covers two ids. An HTTP error would be bisected
    # rather than fail the scan, so this test injects an ordinary failure instead.
    fake = FakeGamma(_two_event_world())
    fake.raise_on("/events/keyset", RuntimeError("injected failure"), times=1)
    runtime, _ = build_runtime(tmp_path, fake, env={"CATALOGUE_CAPTURE_WORKERS": "2"})
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            run_capture(runtime, "bootstrap")
        batch = runtime.ledger.list_batches()[0]
        untouched = [
            scan
            for scan in runtime.ledger.list_scans(batch["batch_id"])
            if scan["status"] == "running" and scan["fetched_seq"] == 0
        ]
        assert untouched
        for scan in untouched:
            assert runtime.ledger.pages_for_scan(scan["scan_id"]) == []
    finally:
        runtime.ledger.close()


def test_two_threads_commit_on_one_ledger(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "ledger.sqlite")
    errors: list[BaseException] = []

    def work() -> None:
        try:
            for _ in range(30):
                with ledger.transaction() as conn:
                    conn.execute("SELECT 1")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    ledger.close()
    assert errors == []


def test_pool_runs_two_scans_at_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two scans must be in flight together. A serial run breaks the barrier and fails."""
    barrier = threading.Barrier(2, timeout=5)
    lock = threading.Lock()
    entered = 0
    real_run_scan = runner._run_scan

    def gated(rt, batch, row, *args, **kwargs):
        nonlocal entered
        with lock:
            entered += 1
            first_two = entered <= 2
        if first_two:
            barrier.wait()
        return real_run_scan(rt, batch, row, *args, **kwargs)

    monkeypatch.setattr(runner, "_run_scan", gated)
    runtime, _ = build_runtime(
        tmp_path, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": "4"}
    )
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        assert not barrier.broken
    finally:
        runtime.ledger.close()


def test_a_pooled_signal_then_resume_matches_a_clean_serial_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SIGTERM mid-pool keeps the pages already durable. A SIGHUP in the drain is held.

    The stage re-raises the first signal, SIGTERM. Resume then finishes with the same content.
    """
    clean = _capture(tmp_path / "clean", "1")
    root = tmp_path / "pool"
    env = {"CATALOGUE_CAPTURE_WORKERS": "2"}
    runtime, _ = build_runtime(root, FakeGamma(demo_world()), env=env)
    real_wait = runner.wait
    real_shutdown = ThreadPoolExecutor.shutdown
    waits = {"n": 0}

    def sigterm_on_second_wait(*args, **kwargs):
        waits["n"] += 1
        if waits["n"] == 2:
            raise Terminated(signal.SIGTERM)  # a stop lands while scans are still in flight
        return real_wait(*args, **kwargs)

    def sighup_during_drain(self, wait=True, *, cancel_futures=False):
        SIGNALS.receive(signal.SIGHUP)  # held: the drain goes on, and SIGTERM stays the signal
        return real_shutdown(self, wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(runner, "wait", sigterm_on_second_wait)
    monkeypatch.setattr(ThreadPoolExecutor, "shutdown", sighup_during_drain)
    try:
        with pytest.raises(Terminated) as info:
            run_capture(runtime, "bootstrap")
        assert info.value.signum == signal.SIGTERM
        (run,) = [r for r in runtime.ledger.stage_runs() if r["stage"] == "capture:bootstrap"]
        assert run["status"] == "failed"
        assert run["error"] == "Terminated: SIGTERM"
        batch = runtime.ledger.list_batches()[0]
        assert batch["status"] == "capturing"
    finally:
        runtime.ledger.close()

    monkeypatch.undo()
    resumed_runtime, _ = build_runtime(root, FakeGamma(demo_world()), env=env)
    try:
        summary = run_capture(resumed_runtime, "bootstrap", resume=batch["batch_id"])
        assert summary.status == "captured"
        assert summary.resumed is True
        assert summary.batch_id == batch["batch_id"]
        assert _content(_manifests(resumed_runtime, summary.batch_id)) == _content(clean)
    finally:
        resumed_runtime.ledger.close()
