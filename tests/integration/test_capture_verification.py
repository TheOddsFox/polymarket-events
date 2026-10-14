"""Single-read capture verification preserves corruption and orphan recovery."""

from __future__ import annotations

import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import build_runtime
from fakes.world import World, make_market
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.reader import DurabilityError, iter_scan_pages


def _runtime(tmp_path, count=3, *, absent=False):
    world = World()
    for i in range(1, count + 1):
        world.add_direct_market(make_market(str(i), f"Market {i}", closed=True))
    baseline = {str(i) for i in range(1, count + 1)}
    if absent:
        baseline.add(str(count + 1))
    fake = FakeGamma(world)
    rt, _ = build_runtime(
        tmp_path, fake, open_event_ids=lambda: set(), open_market_ids=lambda: baseline
    )
    return rt, fake


def _phase(rt, batch_id, number=1):
    return [s for s in rt.ledger.list_scans(batch_id) if s["phase"] == number]


def _records(rt, batch_id):
    batch = rt.ledger.get_batch(batch_id)
    return [
        row
        for scan in _phase(rt, batch_id)
        for _, rows in iter_scan_pages(rt.settings, batch, scan)
        for row in rows
    ]


@pytest.mark.parametrize("orphan", [False, True])
def test_corrupt_body_blocks_resume_before_requests(tmp_path, monkeypatch, orphan):
    rt, fake = _runtime(tmp_path)
    record_page = rt.ledger.record_page

    def crash(row, scan_id, terminal):
        scan = rt.ledger.get_scan(scan_id)
        if scan["scan_name"].startswith("markets_refresh_"):
            if not orphan:
                record_page(row, scan_id, terminal)
            raise SystemExit("page crash")
        return record_page(row, scan_id, terminal)

    monkeypatch.setattr(rt.ledger, "record_page", crash)
    try:
        with pytest.raises(SystemExit):
            runner.run_capture(rt, "daily")
        batch_id = rt.ledger.list_batches()[0]["batch_id"]
        body = next(rt.settings.raw_dir.glob("*/*/daily_markets_closed*/*.json.gz"), None)
        if body is None:
            scan = _phase(rt, batch_id)[0]
            body = next(
                (
                    rt.settings.raw_dir
                    / rt.ledger.get_batch(batch_id)["observation_date"]
                    / batch_id
                    / scan["scan_id"]
                ).glob("*.json.gz")
            )
        body.write_bytes(body.read_bytes()[:12])
        before = len(fake.requests)
        monkeypatch.setattr(rt.ledger, "record_page", record_page)
        with pytest.raises(DurabilityError):
            runner.run_capture(rt, "daily", resume=batch_id)
        assert len(fake.requests) == before
    finally:
        rt.ledger.close()


def test_valid_orphan_page_is_adopted_without_repeating_request(tmp_path, monkeypatch):
    rt, fake = _runtime(tmp_path)
    record_page = rt.ledger.record_page

    def crash(row, scan_id, terminal):
        if rt.ledger.get_scan(scan_id)["scan_name"].startswith("markets_refresh_"):
            raise SystemExit("orphan crash")
        return record_page(row, scan_id, terminal)

    monkeypatch.setattr(rt.ledger, "record_page", crash)
    try:
        with pytest.raises(SystemExit):
            runner.run_capture(rt, "daily")
        batch_id = rt.ledger.list_batches()[0]["batch_id"]
        before = fake.calls_to("/markets/1")
        monkeypatch.setattr(rt.ledger, "record_page", record_page)
        assert runner.run_capture(rt, "daily", resume=batch_id).status == "captured"
        assert fake.calls_to("/markets/1") == before
        assert {r["id"] for r in _records(rt, batch_id)} == {"1", "2", "3"}
    finally:
        rt.ledger.close()


def test_resume_reads_each_body_once_in_integrity_verification(tmp_path, monkeypatch):
    from oddsfox_catalogue.capture import reader

    rt, _ = _runtime(tmp_path)
    try:
        summary = runner.run_capture(rt, "daily")
        read = reader.read_body
        calls = []

        def count(directory, manifest, **kwargs):
            calls.append(manifest["page_id"])
            return read(directory, manifest, **kwargs)

        def duplicate(*args, **kwargs):
            pytest.fail("resume must use the fully verified decoded read")

        monkeypatch.setattr(reader, "read_body", count)
        monkeypatch.setattr(runner, "verify_page", duplicate)
        runner._verify_resume(rt, rt.ledger.get_batch(summary.batch_id))
        expected = {p["page_id"] for p in rt.ledger.pages_for_batch(summary.batch_id)}
        assert set(calls) == expected and len(calls) == len(expected)
    finally:
        rt.ledger.close()
