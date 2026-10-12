"""Selective market-page loading cannot promote incomplete mixed captures."""

import json
from dataclasses import replace
from shutil import copytree

import duckdb
import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import World, event_stub, make_event, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.certification import BuildInvalid, assert_build_valid, read_build_validity
from oddsfox_catalogue.load import runner
from oddsfox_catalogue.pipeline import dbt_stage, load_stage
from oddsfox_catalogue.publish import PublishBlocked, current_release, publish_release
from oddsfox_catalogue.semantics import SEMANTIC_RELATIONS, published_snapshot, warehouse_snapshot


def capture(root, *, market_ids=("4", "5", "99"), event_ids=(), world=None):
    if world is None:
        world = World()
        for index in (1, 2):
            event = make_event(str(index), f"Event {index}")
            event["markets"] = [
                make_market(str(index + 3), f"Market {index}", event_stub=event_stub(event))
            ]
            world.add_event(event)
    runtime, _ = build_runtime(root, FakeGamma(world))
    try:
        result = run_capture(runtime, "selected", market_ids=market_ids, event_ids=event_ids)
        assert result.status == "captured"
        return runtime.settings, result.batch_id
    finally:
        runtime.client.close()
        runtime.ledger.close()


def pending(settings, batch_id):
    with Ledger(settings.ledger_path) as ledger:
        return ledger.pending_load_pages(batch_id)


def assert_partial(settings, batch_id, remaining):
    with Ledger(settings.ledger_path) as ledger:
        assert ledger.get_batch(batch_id)["status"] == "captured"
        assert len(ledger.pending_load_pages(batch_id)) == remaining
    assert read_build_validity(settings)["status"] == "dirty"
    with pytest.raises(BuildInvalid):
        assert_build_valid(settings)
    pointer = current_release(settings)
    with pytest.raises(PublishBlocked):
        publish_release(settings, now=FIXED_NOW)
    assert current_release(settings) == pointer
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        exists = connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='bronze' AND table_name='batch_registry'"
        ).fetchone()[0]
        if exists:
            assert (
                connection.execute(
                    "SELECT count(*) FROM bronze.batch_registry WHERE batch_id=?", [batch_id]
                ).fetchone()[0]
                == 0
            )


def test_market_filter_precedes_chunks_and_normal_completion_matches_full_contract(
    tmp_path, monkeypatch
):
    _, batch_id = capture(tmp_path / "source")
    for name in ("reference", "filtered"):
        copytree(tmp_path / "source", tmp_path / name)
    reference = make_settings(tmp_path / "reference")
    filtered = make_settings(tmp_path / "filtered", {"CATALOGUE_LOAD_MAX_PAGES_PER_RUN": "250"})
    original_pages = pending(filtered, batch_id)
    market_pages = [page for page in original_pages if page["record_key"] == "markets"]
    event_pages = [page for page in original_pages if page["record_key"] == "events"]
    assert market_pages and event_pages and any(page["http_status"] == 404 for page in market_pages)
    chunks = []
    original_chunks = runner._chunks

    def chunked(pages, size):
        chunks.append(([page["page_id"] for page in pages], size))
        return original_chunks(pages, size)

    with monkeypatch.context() as changed:
        changed.setattr(runner, "_chunks", chunked)
        selected = load_stage(
            filtered, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW
        )
        repeated = load_stage(
            filtered, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW
        )
    assert chunks == [([page["page_id"] for page in market_pages], 250), ([], 250)]
    assert selected.pages_loaded == len(market_pages) and selected.pages_remaining == len(
        event_pages
    )
    assert selected.batches_registered == repeated.batches_registered == []
    assert repeated.pages_loaded == 0 and repeated.pages_remaining == len(event_pages)
    assert [page["page_id"] for page in pending(filtered, batch_id)] == [
        page["page_id"] for page in event_pages
    ]
    with Ledger(filtered.ledger_path) as ledger:
        counts = json.loads(
            ledger._one("SELECT counts_json FROM stage_runs ORDER BY rowid DESC LIMIT 1")[
                "counts_json"
            ]
        )
        assert counts["pages_remaining"] == len(event_pages)
        for page in ledger.pages_for_batch(batch_id):
            if page["http_status"] == 404:
                assert (page["event_rows"], page["market_rows"], page["quarantine_rows"]) == (
                    0,
                    0,
                    0,
                )
    assert_partial(filtered, batch_id, len(event_pages))
    normal = replace(filtered, load=replace(filtered.load, max_pages_per_run=50))
    completed = load_stage(normal, now=lambda: FIXED_NOW)
    baseline = load_stage(reference, now=lambda: FIXED_NOW)
    assert (
        completed.pages_loaded == len(event_pages)
        and completed.pages_remaining == baseline.pages_remaining == 0
    )
    assert completed.batches_registered == baseline.batches_registered == [batch_id]
    for settings in (normal, reference):
        result = dbt_stage(settings, ["build"])
        assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
        assert_build_valid(settings)
        publish_release(settings, now=FIXED_NOW, git_sha="synthetic")
    actual, expected = warehouse_snapshot(normal), warehouse_snapshot(reference)
    assert set(actual["relations"]) == set(SEMANTIC_RELATIONS)
    assert actual == expected
    assert published_snapshot(normal) == published_snapshot(reference)
    assert len(published_snapshot(normal)) == 7
    with duckdb.connect(str(normal.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT page_count,status FROM bronze.batch_registry WHERE batch_id=?", [batch_id]
        ).fetchone() == (len(original_pages), "loaded")


@pytest.mark.parametrize("failure", ["committed-market", "adaptive"])
def test_filtered_failure_and_adaptive_retry_preserve_pending_events(
    tmp_path, monkeypatch, failure
):
    settings, batch_id = capture(tmp_path)
    settings = replace(settings, load=replace(settings.load, max_pages_per_run=250))
    pages = pending(settings, batch_id)
    markets = [page for page in pages if page["record_key"] == "markets"]
    events = [page for page in pages if page["record_key"] == "events"]
    original_chunk, original_write = runner._load_chunk, runner._run_resource
    attempted = []

    def chunk(rt, selected, summary):
        assert all(page["record_key"] == "markets" for page in selected)
        attempted.append(tuple(page["page_id"] for page in selected))
        if failure == "adaptive" and len(selected) > 1:
            raise runner.LoadReservationExceeded("split selected market pages")
        return original_chunk(rt, selected, summary)

    def write(rt, resource):
        result = original_write(rt, resource)
        if resource.name == "market_observations_values":
            raise RuntimeError("stop after market commit before acknowledgement")
        return result

    with monkeypatch.context() as changed:
        changed.setattr(runner, "_load_chunk", chunk)
        if failure == "committed-market":
            changed.setattr(runner, "_run_resource", write)
            with pytest.raises(RuntimeError, match="after market commit"):
                load_stage(settings, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW)
            assert [page["page_id"] for page in pending(settings, batch_id)] == [
                page["page_id"] for page in pages
            ]
            assert_partial(settings, batch_id, len(pages))
            changed.setattr(runner, "_run_resource", original_write)
        summary = load_stage(
            settings, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW
        )
    assert summary.pages_loaded == len(markets) and summary.pages_remaining == len(events)
    assert attempted[0] == tuple(page["page_id"] for page in markets)
    if failure == "adaptive":
        assert len(attempted) == 2 * len(markets) - 1
    assert_partial(settings, batch_id, len(events))
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*),count(DISTINCT observation_id) FROM bronze.market_observations"
        ).fetchone() == (2, 2)


def test_filtered_replay_rejects_conflicting_persisted_market_before_ack(tmp_path):
    settings, batch_id = capture(tmp_path)
    load_stage(settings, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute("UPDATE bronze.market_observations_values SET payload_hash='changed'")
    with Ledger(settings.ledger_path) as ledger, ledger.transaction() as connection:
        connection.execute(
            "UPDATE pages SET loaded_at=NULL,dlt_load_id=NULL WHERE page_id IN (SELECT p.page_id FROM pages p JOIN scans s USING(scan_id) WHERE s.record_key='markets')"
        )
    before = [page["page_id"] for page in pending(settings, batch_id)]
    with pytest.raises(runner.LoadBlocked, match="conflicting persisted observation"):
        load_stage(settings, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW)
    assert [page["page_id"] for page in pending(settings, batch_id)] == before
    assert_partial(settings, batch_id, len(before))


def test_empty_market_selection_stays_unregistered_with_event_work_pending(tmp_path):
    world = World()
    world.add_event(make_event("1", "Only an event"))
    settings, batch_id = capture(tmp_path, world=world, market_ids=(), event_ids=("1",))
    assert all(page["record_key"] == "events" for page in pending(settings, batch_id))
    for _ in range(2):
        result = load_stage(
            settings, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW
        )
        assert (
            result.pages_loaded == 0
            and result.pages_remaining == 1
            and result.batches_registered == []
        )
        assert_partial(settings, batch_id, 1)


def test_market_only_capture_can_register_when_every_actual_page_is_loaded(tmp_path):
    world = World()
    world.add_direct_market(make_market("4", "Standalone"))
    settings, batch_id = capture(tmp_path, world=world, market_ids=("4",))
    pages = pending(settings, batch_id)
    assert pages and all(page["record_key"] == "markets" for page in pages)
    result = load_stage(settings, batch_id=batch_id, record_key="markets", now=lambda: FIXED_NOW)
    assert result.pages_loaded == len(pages) and result.pages_remaining == 0
    assert result.batches_registered == [batch_id] and not pending(settings, batch_id)


@pytest.mark.parametrize(
    "batch,key", [(None, "markets"), ("unknown", "markets"), ("known", "events"), ("known", "")]
)
def test_invalid_market_scope_fails_before_warehouse_or_dirty_state(tmp_path, batch, key):
    settings, known = capture(tmp_path)
    with Ledger(settings.ledger_path) as ledger:
        before = ledger.pages_for_batch(known)
        with pytest.raises(runner.LoadBlocked):
            runner.load_pending(
                runner.LoadRuntime(settings, ledger),
                known if batch == "known" else batch,
                record_key=key,
            )
        assert ledger.pages_for_batch(known) == before
    assert not settings.warehouse_path.exists() and read_build_validity(settings) is None
