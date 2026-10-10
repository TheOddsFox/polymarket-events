"""Finite catalogue evidence stays lossless and indexed at the audited ID bounds."""

from __future__ import annotations

import copy
import json

import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import World, event_stub, make_event, make_market
from oddsfox_catalogue.capture.reader import DurabilityError, scan_dir_for
from oddsfox_catalogue.capture.runner import _adopt_durable_pages, run_capture
from oddsfox_catalogue.capture.writer import manifest_path
from oddsfox_catalogue.config import CaptureSettings, GammaSettings
from oddsfox_catalogue.gamma.paginators import _window_params
from oddsfox_catalogue.gamma.scans import ID_STEP, sealed_plan
from oddsfox_catalogue.ids import make_page_id, make_scan_id, parse_batch_id
from oddsfox_catalogue.inventory import (
    COVERAGE_SCHEMA_REVISION,
    JSON_LIMIT,
    coverage_unit,
    expand_coverage_id_range,
)
from oddsfox_catalogue.load.runner import LoadRuntime, _rows_for_chunk
from oddsfox_catalogue.publish import _validate_coverage

BATCH = "20261010T142904Z-bootstrap"


def manifest(ids=None):
    return {
        "batch_id": BATCH,
        "page_id": make_page_id(make_scan_id(BATCH, "events_ids_0001", 1), 1),
        "endpoint": "/events/keyset",
        "params": {"limit": 100, "id": [1] if ids is None else ids},
        "observed_at": "2026-10-10T14:29:04.000Z",
        "record_count": 0,
        "http_status": 200,
    }


def json_size(value):
    return len(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


@pytest.fixture
def captured_keyset(tmp_path):
    world = World()
    event = make_event("10", "Source cursor regression")
    event["markets"] = [
        make_market(str(index), f"Market {index}", event_stub=event_stub(event))
        for index in range(1, 5)
    ]
    world.add_event(event)
    runtime, _ = build_runtime(
        tmp_path,
        FakeGamma(world),
        env={"CATALOGUE_GAMMA_PAGE_LIMIT": "1", "CATALOGUE_CAPTURE_WORKERS": "1"},
    )
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        batch = runtime.ledger.get_batch(summary.batch_id)
        scan = next(
            row
            for row in runtime.ledger.list_scans(summary.batch_id)
            if row["scan_name"] == "markets_keyset_open"
        )
        pages = runtime.ledger.pages_for_scan(scan["scan_id"])
        assert len(pages) == 4
        yield runtime, batch, scan, pages
    finally:
        runtime.client.close()
        runtime.ledger.close()


def no_full_scan(*args, **kwargs):
    pytest.fail("predecessor lookup enumerated the whole scan")


def orphan_after_first(runtime, scan, pages):
    first = pages[0]
    with runtime.ledger.transaction() as connection:
        connection.execute("DELETE FROM pages WHERE scan_id=? AND seq>1", (scan["scan_id"],))
        connection.execute(
            "UPDATE scans SET fetched_seq=1,fetched_cursor=?,fetched_offset=?,"
            "terminal=0,record_count=?,status='running' WHERE scan_id=?",
            (first["output_cursor"], first["offset_end"], first["record_count"], scan["scan_id"]),
        )
    return runtime.ledger.get_scan(scan["scan_id"])


def test_exact_page_lookup_uses_unique_index_and_does_not_cross_scans(
    captured_keyset,
    monkeypatch,
):
    runtime, batch, scan, pages = captured_keyset
    ledger = runtime.ledger
    other = next(row for row in ledger.list_scans(batch["batch_id"]) if row != scan)
    other_page = ledger.page_for_scan(other["scan_id"], 1)
    query_plan = ledger._conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM pages WHERE scan_id=? AND seq=?",
        (scan["scan_id"], 2),
    ).fetchall()
    assert any("SEARCH" in row[3] and "INDEX" in row[3] for row in query_plan)
    monkeypatch.setattr(ledger, "pages_for_scan", no_full_scan)
    monkeypatch.setattr(ledger, "_all", no_full_scan)
    for seq in (1, 2, 4):
        assert ledger.page_for_scan(scan["scan_id"], seq) == pages[seq - 1]
    assert ledger.page_for_scan(scan["scan_id"], 0) is None
    assert ledger.page_for_scan(scan["scan_id"], 5) is None
    assert ledger.page_for_scan("unknown-scan", 1) is None
    assert ledger.page_for_scan(other["scan_id"], 1) == other_page
    assert other_page["page_id"] != pages[0]["page_id"]


def test_loader_reads_predecessor_without_enumerating_entire_scan(captured_keyset, monkeypatch):
    runtime, _, _, pages = captured_keyset
    monkeypatch.setattr(runtime.ledger, "pages_for_scan", no_full_scan)
    rows, counts = _rows_for_chunk(
        LoadRuntime(runtime.settings, runtime.ledger, now=lambda: FIXED_NOW),
        runtime.ledger.pages_for_batch(pages[0]["batch_id"])[1:3],
    )
    assert len(counts) == 2
    assert rows.markets


def test_orphan_adoption_reads_only_each_exact_predecessor(captured_keyset, monkeypatch):
    runtime, batch, scan, pages = captured_keyset
    scan = orphan_after_first(runtime, scan, pages)
    monkeypatch.setattr(runtime.ledger, "pages_for_scan", no_full_scan)
    directory = scan_dir_for(
        runtime.settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
    )
    assert _adopt_durable_pages(runtime, batch, scan, directory) == 3
    assert runtime.ledger.page_for_scan(scan["scan_id"], 4) == pages[-1]


@pytest.mark.parametrize("consumer", ["loader", "orphan"])
def test_indexed_predecessor_preserves_source_cursor_validation(
    captured_keyset,
    monkeypatch,
    consumer,
):
    runtime, batch, scan, pages = captured_keyset
    directory = scan_dir_for(
        runtime.settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
    )
    path = manifest_path(directory, 2)
    raw = json.loads(path.read_bytes())
    raw["input_cursor"] = "different-source-cursor"
    path.write_text(json.dumps(raw))
    monkeypatch.setattr(runtime.ledger, "pages_for_scan", no_full_scan)
    with pytest.raises(DurabilityError, match="identity"):
        if consumer == "loader":
            page = runtime.ledger.pages_for_batch(batch["batch_id"])
            chosen = next(row for row in page if row["page_id"] == pages[1]["page_id"])
            _rows_for_chunk(LoadRuntime(runtime.settings, runtime.ledger), [chosen])
        else:
            scan = orphan_after_first(runtime, scan, pages)
            _adopt_durable_pages(runtime, batch, scan, directory)


@pytest.mark.parametrize(
    "ids",
    [
        list(range(1, 101)),
        list(range(5503201, 5503270)),
        [99999999999999999999],
    ],
)
def test_compact_range_is_lossless_and_does_not_mutate_raw_params(ids):
    raw = manifest(ids)
    original = copy.deepcopy(raw)
    unit = coverage_unit(raw, "id_range")
    assert raw == original
    assert "id" not in unit["params"]
    assert unit["params"] == {"limit": 100}
    assert [int(value) for value in expand_coverage_id_range(unit["id_range"])] == ids
    assert unit["records"] == 0 and unit["status"] == "success_empty"


@pytest.mark.parametrize("kind", ["keyset_ids", "keyset", "single_ids", "offset"])
def test_nonrange_units_preserve_noncontiguous_request_lists(kind):
    raw = manifest([1, 4, 17])
    original = copy.deepcopy(raw)
    unit = coverage_unit(raw, kind)
    assert unit["params"] == original["params"]
    assert "id_range" not in unit
    assert raw == original


@pytest.mark.parametrize(
    "ids",
    [
        [],
        list(range(1, 102)),
        [2, 1],
        [1, 3],
        ["01"],
        [1.0],
        [True],
        ["١"],
        [100000000000000000000],
        [1, 1],
    ],
)
def test_range_producer_rejects_ambiguous_or_unbounded_request_lists(ids):
    with pytest.raises(ValueError):
        coverage_unit(manifest(ids), "id_range")


@pytest.mark.parametrize(
    "interval",
    [
        {"start": "1", "end": "101"},
        {"start": "2", "end": "1"},
        {"start": "01", "end": "1"},
        {"start": 1.0, "end": "1"},
        {"start": True, "end": "1"},
        {"start": "١", "end": "١"},
        {"start": "1", "end": "100000000000000000000"},
        {"start": "1", "end": "1", "step": 1},
        {"start": "0", "end": "0"},
    ],
)
def test_range_consumer_rejects_invalid_bounds(interval):
    with pytest.raises(ValueError):
        expand_coverage_id_range(interval)


def coverage_fixture():
    unit = coverage_unit(manifest([1, 2]), "id_range")
    scan_id, page_suffix = unit["page_id"].rsplit(".p", 1)
    prefix = f"2026-10-10/{BATCH}/{scan_id}/p{page_suffix}"
    inventory = sorted(
        [
            {"path": prefix + suffix, "bytes": 1, "sha256": "a" * 64}
            for suffix in (".json.gz", ".manifest.json")
        ],
        key=lambda row: row["path"],
    )
    coverage = {
        "coverage_schema_revision": COVERAGE_SCHEMA_REVISION,
        "batches": [
            {"batch_id": BATCH, "mode": "bootstrap", "scope": {"revision": 2, "sealed": True}}
        ],
        "units": [unit],
        "declared_scans_complete": True,
        "source_catalogue_complete": False,
        "discovery_limits": ["Explicit source limitation"],
    }
    return coverage, inventory


def test_release_verifier_accepts_lossless_compact_coverage():
    coverage, inventory = coverage_fixture()
    _validate_coverage(coverage, inventory, [BATCH])


@pytest.mark.parametrize("revision", [None, 0, 2, True, 1.0, "1"])
def test_release_verifier_requires_explicit_supported_integer_coverage_revision(revision):
    coverage, inventory = coverage_fixture()
    if revision is None:
        coverage.pop("coverage_schema_revision")
    else:
        coverage["coverage_schema_revision"] = revision
    with pytest.raises(ValueError, match="revision"):
        _validate_coverage(coverage, inventory, [BATCH])


def test_release_verifier_rejects_dual_id_representations():
    coverage, inventory = coverage_fixture()
    coverage["units"][0]["params"]["id"] = [1, 2]
    with pytest.raises(ValueError, match="conflicting"):
        _validate_coverage(coverage, inventory, [BATCH])


def test_bootstrap_and_reconcile_coverage_and_file_inventory_fit_audited_bounds(record_property):
    """Synthetic sealed snapshots, all markets closed; no claim about future catalogue growth."""
    assert JSON_LIMIT == 128 * 1024**2
    marks = {"events": 1164836, "markets": 5503269}
    settings = CaptureSettings()
    gamma = GammaSettings()
    coverage_bytes, expanded_bytes, file_bytes, units = 0, 0, 2, 0
    batches = []

    def descriptor_size(path):
        # Bounded 16 MiB markers and at most twice the raw page bound for gzip overhead.
        return (
            json_size({"path": path, "bytes": settings.max_response_bytes * 2, "sha256": "a" * 64})
            + 1
        )

    for batch_id, mode in (
        (BATCH, "bootstrap"),
        ("20261011T010000Z-reconcile", "reconcile"),
    ):
        stamp, _ = parse_batch_id(batch_id)
        date = stamp.date().isoformat()
        batches.append(
            {
                "batch_id": batch_id,
                "mode": mode,
                "scope": {
                    "revision": 2,
                    "kind": "catalogue",
                    "market_ids": [],
                    "event_ids": [],
                    "parent_event_ids": [],
                    "baseline": {"events": [], "markets": []},
                    "sealed": True,
                    "high_water": marks,
                },
            }
        )
        file_bytes += descriptor_size(f"{date}/{batch_id}/_batch.json")
        batch_units = 0
        for spec in sealed_plan(mode, gamma, settings, marks):
            scan_id = make_scan_id(batch_id, spec.name, 1)
            directory = f"{date}/{batch_id}/{scan_id}"
            file_bytes += descriptor_size(directory + "/_scan.json")
            params = spec.param_dict
            requests = (
                (
                    (seq, first, min(first + ID_STEP - 1, params["hi"]))
                    for seq, first in enumerate(range(params["lo"], params["hi"] + 1, ID_STEP), 1)
                )
                if spec.kind == "id_range"
                else ((1, None, None),)
            )
            for seq, first, last in requests:
                request_params = (
                    _window_params(
                        list(range(first, last + 1)), spec.record_key, params.get("closed")
                    )
                    if first is not None
                    else {key: value for key, value in params.items() if not key.startswith("_")}
                )
                raw = {
                    "batch_id": batch_id,
                    "page_id": make_page_id(scan_id, seq),
                    "endpoint": spec.endpoint,
                    "params": request_params,
                    "observed_at": stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    "record_count": 0,
                    "http_status": 200,
                }
                coverage_bytes += json_size(coverage_unit(raw, spec.kind)) + 1
                expanded_bytes += json_size(coverage_unit(raw, "keyset_ids")) + 1
                for suffix in (".manifest.json", ".json.gz"):
                    file_bytes += descriptor_size(f"{directory}/p{seq:06d}{suffix}")
                units += 1
                batch_units += int(spec.kind == "id_range")
        assert batch_units == 66682
    envelope = {
        "coverage_schema_revision": COVERAGE_SCHEMA_REVISION,
        "batches": batches,
        "units": [],
        "declared_scans_complete": True,
        "source_catalogue_complete": False,
        "discovery_limits": [
            "Inactive events absent from lists and without captured market references are not exhaustively discovered."
        ],
    }
    coverage_bytes += json_size(envelope)
    expanded_bytes += json_size(envelope)
    record_property("compact_coverage_bytes", coverage_bytes)
    record_property("expanded_coverage_bytes", expanded_bytes)
    record_property("capture_inventory_bytes_upper_bound", file_bytes)
    record_property("units", units)
    assert units == 133366
    assert expanded_bytes > JSON_LIMIT, (
        "this fixture must reproduce the original expanded-list size failure"
    )
    assert coverage_bytes < JSON_LIMIT // 2
    assert file_bytes < JSON_LIMIT // 2
