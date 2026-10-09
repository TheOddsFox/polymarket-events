"""Worker pool: shared rate limit, matching manifests, and a drain that keeps pages."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import build_runtime
from fakes.world import World, demo_world, event_stub, make_event, make_market
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.gamma.http import MalformedResponse


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


def test_parallel_manifests_match_a_serial_run(tmp_path: Path) -> None:
    def capture(root: Path, workers: str) -> dict[str, list[dict]]:
        runtime, _ = build_runtime(
            root, FakeGamma(demo_world()), env={"CATALOGUE_CAPTURE_WORKERS": workers}
        )
        try:
            summary = run_capture(runtime, "bootstrap")
            assert summary.status == "captured"
            return _manifests(runtime, summary.batch_id)
        finally:
            runtime.ledger.close()

    assert capture(tmp_path / "serial", "1") == capture(tmp_path / "pool", "4")


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

    def is_tail(request: httpx.Request) -> bool:
        return request.url.path == "/events/keyset" and "2" in request.url.params.get_list("id")

    def fail_after_sibling(_: httpx.Request) -> httpx.Response:
        assert persisted.wait(5)
        return httpx.Response(422, json={"error": "bad window"})

    fake.rules.append(Rule(is_tail, fail_after_sibling, remaining=1))
    runtime, _ = build_runtime(tmp_path, fake, env={"CATALOGUE_CAPTURE_WORKERS": "2"})
    original = runtime.ledger.record_page

    def record_page(*args, **kwargs):
        original(*args, **kwargs)
        persisted.set()

    runtime.ledger.record_page = record_page
    try:
        with pytest.raises(MalformedResponse):
            run_capture(runtime, "bootstrap")
        batch = runtime.ledger.list_batches()[0]
        assert batch["status"] == "capturing"
        scans = {scan["scan_name"]: scan for scan in runtime.ledger.list_scans(batch["batch_id"])}
        assert scans["events_ids_tail"]["status"] == "failed"
        sibling = scans["events_ids_0001"]
        assert sibling["fetched_seq"] >= 1
        assert runtime.ledger.pages_for_scan(sibling["scan_id"])
    finally:
        runtime.ledger.close()


def test_a_scan_that_has_not_started_is_not_fetched(tmp_path: Path) -> None:
    fake = FakeGamma(_tiny_world())
    fake.fail_status("/events/keyset", 422, times=1)
    runtime, _ = build_runtime(tmp_path, fake, env={"CATALOGUE_CAPTURE_WORKERS": "2"})
    try:
        with pytest.raises(MalformedResponse):
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
