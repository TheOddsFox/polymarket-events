"""Finite catalogue evidence stays lossless and indexed at the audited ID bounds."""

from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import duckdb
import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import World, event_stub, make_event, make_market
from oddsfox_catalogue.capture.reader import DurabilityError, scan_dir_for
from oddsfox_catalogue.capture.runner import _adopt_durable_pages, run_capture
from oddsfox_catalogue.capture.writer import manifest_path
from oddsfox_catalogue.config import CaptureSettings, GammaSettings, Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.gamma.paginators import _window_params
from oddsfox_catalogue.gamma.scans import ID_STEP, sealed_plan
from oddsfox_catalogue.ids import canonical_json, make_page_id, make_scan_id, parse_batch_id
from oddsfox_catalogue.inventory import (
    COVERAGE_SCHEMA_REVISION,
    JSON_LIMIT,
    coverage_unit,
    expand_coverage_id_range,
)
from oddsfox_catalogue.load.rows import PageContext, RowSet, rows_for_page
from oddsfox_catalogue.load.runner import (
    LOAD_EXPANSION_FACTOR,
    LOAD_FIXED_RESERVE,
    LoadBlocked,
    LoadRuntime,
    LoadSummary,
    _check_existing_observations,
    _encoded_row_bytes,
    _load_chunk,
    _preflight_load_bytes,
    _rows_for_chunk,
)
from oddsfox_catalogue.load.source import event_columns, market_columns
from oddsfox_catalogue.publish import _validate_coverage

BATCH = "20261010T142904Z-bootstrap"


def observation_row(index, *, market=False):
    row = {
        "observation_id": f"observation-{index}",
        "venue": "polymarket",
        "entity_id": str(index),
        "batch_id": BATCH,
        "page_id": f"{BATCH}.source.p{index:06d}",
        "observed_at": FIXED_NOW,
        "source_updated_at": None,
        "endpoint": "/markets/keyset" if market else "/events/keyset",
        "payload_hash": f"payload-{index}",
        "normalized": {
            "amount": Decimal("0.12345678901234567890123456789"),
            "nullable": None,
            "labels": ["Yes", "No"],
            "minimum": Decimal("0.0100"),
            "active": None,
        },
    }
    if market:
        row.update(source_kind="market_direct", json_pointer=f"/markets/{index}")
    return row


def stored_observation(row):
    stored = copy.deepcopy(row)
    stored["normalized"] = canonical_json(row["normalized"])
    return stored


class ObservationCursor:
    def __init__(self, rows, *, prohibit_fetchall=False, fail_after=None):
        self.rows = rows
        self.offset = 0
        self.prohibit_fetchall = prohibit_fetchall
        self.fail_after = fail_after
        self.fetch_sizes = []

    def fetchall(self):
        assert not self.prohibit_fetchall, "observation results must be read in bounded batches"
        rows = self.rows[self.offset :]
        self.offset = len(self.rows)
        return rows

    def fetchone(self):
        if self.offset == len(self.rows):
            return None
        row = self.rows[self.offset]
        self.offset += 1
        return row

    def fetchmany(self, size):
        assert 0 < size <= 512, "observation result batches must stay bounded"
        if self.fail_after is not None and self.offset >= self.fail_after:
            raise RuntimeError("observation cursor interrupted")
        self.fetch_sizes.append(size)
        rows = self.rows[self.offset : self.offset + size]
        self.offset += len(rows)
        return rows


class ObservationConnection:
    def __init__(self, tables, *, prohibit_fetchall=False, fail_after=None):
        self.tables = tables
        self.prohibit_fetchall = prohibit_fetchall
        self.fail_after = fail_after
        self.queries = []
        self.schema_binds = []
        self.wide_queries = []
        self.query_order = []
        self.cursors = []
        self.closed = False

    def execute(self, query, params=None):
        if "information_schema.tables" in query:
            assert params is None
            return ObservationCursor([(table,) for table in self.tables])
        table = query.split("FROM bronze.", 1)[1].split()[0]
        columns = query.removeprefix("SELECT ").split(" FROM ", 1)[0].split(", ")
        if query.endswith("WHERE FALSE"):
            assert params is None
            self.schema_binds.append((table, columns))
            self.query_order.append((table, "schema"))
            return ObservationCursor([])
        assert "WHERE observation_id IN (SELECT unnest(?))" in query
        assert len(params) == 1 and isinstance(params[0], list)
        ids = params[0]
        assert all(value not in query for value in ids), "IDs must remain bound parameters"
        selected = set(ids)
        if columns == ["1"]:
            assert query.endswith("LIMIT 1")
            self.queries.append((table, ids))
            self.query_order.append((table, "probe"))
            matching = any(row["observation_id"] in selected for row in self.tables[table])
            return ObservationCursor([(1,)] if matching else [])
        self.wide_queries.append((table, ids))
        self.query_order.append((table, "wide"))
        cursor = ObservationCursor(
            [
                tuple(row[column] for column in columns)
                for row in self.tables[table]
                if row["observation_id"] in selected
            ],
            prohibit_fetchall=self.prohibit_fetchall,
            fail_after=self.fail_after,
        )
        self.cursors.append(cursor)
        return cursor


def observation_guard(monkeypatch, tmp_path, tables, **cursor_options):
    path = tmp_path / "warehouse.duckdb"
    path.touch()
    runtime = SimpleNamespace(settings=SimpleNamespace(warehouse_path=path), connection=None)
    observed = ObservationConnection(tables, **cursor_options)

    @contextmanager
    def connect(settings, *, connection):
        assert settings is runtime.settings
        assert connection is runtime.connection is None
        try:
            yield observed
        finally:
            observed.closed = True

    monkeypatch.setattr("oddsfox_catalogue.load.runner.bounded_connection", connect)
    return runtime, observed


def observation_rowset(incoming, *, market):
    return RowSet(markets=incoming) if market else RowSet(events=incoming)


@pytest.mark.parametrize("market", [False, True])
@pytest.mark.parametrize("prohibit_fetchall", [False, True])
@pytest.mark.parametrize("count", [512, 513])
def test_existing_observations_query_each_relation_once_and_read_bounded_results(
    monkeypatch, tmp_path, market, prohibit_fetchall, count
):
    incoming = [observation_row(index, market=market) for index in range(1, count + 1)]
    stored = [stored_observation(row) for row in incoming]
    if count > 512:
        stored.append(copy.deepcopy(stored[0]))
    table = "market_observations" if market else "event_observations"
    runtime, connection = observation_guard(
        monkeypatch, tmp_path, {table: stored}, prohibit_fetchall=prohibit_fetchall
    )
    _check_existing_observations(runtime, observation_rowset(incoming, market=market))
    assert connection.queries == [(table, [row["observation_id"] for row in incoming])]
    assert connection.wide_queries == connection.queries
    assert connection.cursors[0].offset == len(stored)
    assert connection.cursors[0].fetch_sizes
    assert connection.closed


@pytest.mark.parametrize("market", [False, True])
@pytest.mark.parametrize("conflict", ["last_observation", "persisted_duplicate"])
def test_existing_observation_conflicts_after_first_result_batch_block_loading(
    monkeypatch, tmp_path, market, conflict
):
    incoming = [observation_row(index, market=market) for index in range(1, 610)]
    stored = [stored_observation(row) for row in incoming]
    if conflict == "persisted_duplicate":
        stored.append(copy.deepcopy(stored[0]))
    stored[-1]["payload_hash"] = "conflicting-retained-evidence"
    table = "market_observations" if market else "event_observations"
    runtime, connection = observation_guard(monkeypatch, tmp_path, {table: stored})
    with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
        _check_existing_observations(runtime, observation_rowset(incoming, market=market))
    assert connection.closed


@pytest.mark.parametrize("market", [False, True])
def test_existing_observation_comparison_preserves_exact_json_nulls_and_timestamp_instants(
    monkeypatch, tmp_path, market
):
    row = observation_row(1, market=market)
    row["source_updated_at"] = FIXED_NOW
    original = copy.deepcopy(row)
    stored = stored_observation(row)
    stored["observed_at"] = FIXED_NOW.astimezone(timezone(timedelta(hours=2)))
    stored["source_updated_at"] = FIXED_NOW.astimezone(timezone(timedelta(hours=-5)))
    stored["normalized"] = (
        '{ "labels": ["Yes", "No"], "nullable": null, '
        '"amount": 0.12345678901234567890123456789, "minimum": 0.01, "active": null }'
    )
    table = "market_observations" if market else "event_observations"
    runtime, connection = observation_guard(monkeypatch, tmp_path, {table: [stored]})
    _check_existing_observations(runtime, observation_rowset([row], market=market))
    assert row == original
    assert connection.closed


@pytest.mark.parametrize(
    "field,value",
    [
        ("venue", "other-venue"),
        ("entity_id", "2"),
        ("batch_id", "20261011T010000Z-daily"),
        ("page_id", "different-page"),
        ("observed_at", FIXED_NOW + timedelta(microseconds=1)),
        ("source_updated_at", FIXED_NOW),
        ("endpoint", "/events/keyset"),
        ("payload_hash", "different-payload-hash"),
        ("normalized", '{"amount":0.12345678901234567890123456788}'),
        ("normalized", '{"amount":0.01,"active":false}'),
        ("source_kind", "event_embedded"),
        ("json_pointer", "/events/0/markets/0"),
    ],
)
def test_existing_observation_rejects_changes_to_projected_evidence(
    monkeypatch, tmp_path, field, value
):
    row = observation_row(1, market=True)
    stored = stored_observation(row)
    stored[field] = value
    runtime, connection = observation_guard(
        monkeypatch, tmp_path, {"market_observations": [stored]}
    )
    with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
        _check_existing_observations(runtime, RowSet(markets=[row]))
    assert connection.closed


@pytest.mark.parametrize("invalid_json", ['{"amount":NaN}', '{"amount":Infinity}', "{"])
def test_existing_observation_rejects_untrustworthy_persisted_json(
    monkeypatch, tmp_path, invalid_json
):
    row = observation_row(1)
    stored = stored_observation(row)
    stored["normalized"] = invalid_json
    runtime, connection = observation_guard(monkeypatch, tmp_path, {"event_observations": [stored]})
    with pytest.raises((LoadBlocked, ValueError)):
        _check_existing_observations(runtime, RowSet(events=[row]))
    assert connection.closed


def test_missing_warehouse_does_not_open_a_connection(monkeypatch, tmp_path):
    runtime = SimpleNamespace(
        settings=SimpleNamespace(warehouse_path=tmp_path / "missing.duckdb"), connection=None
    )
    monkeypatch.setattr(
        "oddsfox_catalogue.load.runner.bounded_connection",
        lambda *_, **__: pytest.fail("a missing warehouse must not be opened"),
    )
    _check_existing_observations(runtime, RowSet(events=[observation_row(1)]))


@pytest.mark.parametrize("case", ["missing_table", "empty_incoming", "absent_ids"])
def test_existing_observation_guard_allows_missing_tables_empty_input_and_new_ids(
    monkeypatch, tmp_path, case
):
    tables = {"event_observations": [stored_observation(observation_row(2))]}
    incoming = RowSet(events=[observation_row(1)])
    if case == "missing_table":
        tables = {"market_observations": []}
    elif case == "empty_incoming":
        incoming = RowSet()
    runtime, connection = observation_guard(monkeypatch, tmp_path, tables)
    _check_existing_observations(runtime, incoming)
    assert len(connection.queries) == int(case == "absent_ids")
    assert connection.closed


def test_existing_observation_result_failure_after_first_batch_is_not_silently_accepted(
    monkeypatch, tmp_path
):
    incoming = [observation_row(index) for index in range(1, 610)]
    runtime, connection = observation_guard(
        monkeypatch,
        tmp_path,
        {"event_observations": [stored_observation(row) for row in incoming]},
        fail_after=512,
    )
    with pytest.raises(RuntimeError, match="observation cursor interrupted"):
        _check_existing_observations(runtime, RowSet(events=incoming))
    assert connection.closed


@pytest.mark.parametrize("market", [False, True])
def test_new_observation_ids_bind_full_schema_before_probe_without_reading_wide_rows(
    monkeypatch, tmp_path, market
):
    incoming = observation_row(1, market=market)
    incoming["observation_id"] = "new-observation') OR TRUE --"
    stored = stored_observation(observation_row(2, market=market))
    stored["normalized"] = "unread malformed normalization"
    table = "market_observations" if market else "event_observations"
    runtime, connection = observation_guard(monkeypatch, tmp_path, {table: [stored]})
    _check_existing_observations(runtime, observation_rowset([incoming], market=market))
    assert connection.schema_binds == [(table, list(incoming))]
    assert connection.queries == [(table, [incoming["observation_id"]])]
    assert connection.query_order == [(table, "schema"), (table, "probe")]
    assert connection.wide_queries == [] and connection.cursors == []
    assert connection.closed


@pytest.mark.parametrize(
    "market,missing",
    [(False, "payload_hash"), (False, "normalized"), (True, "source_kind"), (True, "json_pointer")],
)
def test_empty_existing_relation_still_rejects_missing_required_columns(tmp_path, market, missing):
    settings = Settings(tmp_path)
    path = settings.warehouse_path
    path.parent.mkdir(parents=True)
    table = "market_observations" if market else "event_observations"
    columns = market_columns() if market else event_columns()
    types = {"text": "VARCHAR", "json": "JSON", "timestamp": "TIMESTAMPTZ"}
    declaration = ", ".join(
        f"{name} {types[hint['data_type']]}" for name, hint in columns.items() if name != missing
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA bronze")
        connection.execute(f"CREATE TABLE bronze.{table} ({declaration})")
    runtime = SimpleNamespace(settings=settings, connection=None)
    with pytest.raises(duckdb.BinderException, match=missing):
        _check_existing_observations(
            runtime, observation_rowset([observation_row(1, market=market)], market=market)
        )


@pytest.mark.parametrize("market", [False, True])
def test_mixed_new_and_existing_ids_still_detect_late_conflicting_persisted_duplicate(
    monkeypatch, tmp_path, market
):
    existing = [observation_row(index, market=market) for index in range(1, 513)]
    incoming = [*existing, observation_row(1000, market=market)]
    stored = [stored_observation(row) for row in existing]
    unrelated = stored_observation(observation_row(2000, market=market))
    unrelated["normalized"] = "unread unrelated invalid JSON"
    conflicting = copy.deepcopy(stored[0])
    conflicting["payload_hash"] = "late conflicting duplicate"
    table = "market_observations" if market else "event_observations"
    runtime, connection = observation_guard(
        monkeypatch, tmp_path, {table: [*stored, unrelated, conflicting]}, prohibit_fetchall=True
    )
    with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
        _check_existing_observations(runtime, observation_rowset(incoming, market=market))
    expected = [(table, [row["observation_id"] for row in incoming])]
    assert connection.queries == expected and connection.wide_queries == expected
    assert connection.query_order == [(table, "schema"), (table, "probe"), (table, "wide")]
    assert connection.cursors[0].offset == 513
    assert connection.cursors[0].fetch_sizes == [512, 512]
    assert connection.closed


@pytest.mark.parametrize("market", [False, True])
def test_unmodeled_source_change_still_conflicts_with_compact_observation(
    monkeypatch, tmp_path, market
):
    key = "markets" if market else "events"
    record = make_market("1", "Compact evidence") if market else make_event("1", "Compact evidence")
    ctx = PageContext("same-page", BATCH, f"/{key}/keyset", FIXED_NOW, key, "a" * 64)
    first = rows_for_page(ctx, [record])
    record["unmodeled_source_field"] = {
        "exact": Decimal("0.12345678901234567890123456789"),
        "nullable": None,
    }
    changed = rows_for_page(ctx, [record])
    before, after = getattr(first, key)[0], getattr(changed, key)[0]
    assert "payload" not in before and "payload" not in after
    assert before["normalized"] == after["normalized"]
    assert before["observation_id"] == after["observation_id"]
    assert before["payload_hash"] != after["payload_hash"]
    table = "market_observations" if market else "event_observations"
    runtime, connection = observation_guard(
        monkeypatch, tmp_path, {table: [stored_observation(before)]}
    )
    with pytest.raises(LoadBlocked, match="conflicting persisted observation"):
        _check_existing_observations(runtime, changed)
    assert connection.closed


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
    rows, counts, _ = _rows_for_chunk(
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
    with pytest.raises(DurabilityError, match="identity|ledger evidence"):
        if consumer == "loader":
            page = runtime.ledger.pages_for_batch(batch["batch_id"])
            chosen = next(row for row in page if row["page_id"] == pages[1]["page_id"])
            _rows_for_chunk(LoadRuntime(runtime.settings, runtime.ledger), [chosen])
        else:
            scan = orphan_after_first(runtime, scan, pages)
            _adopt_durable_pages(runtime, batch, scan, directory)


def catalogue_scope(mark=10):
    return {
        "revision": 2,
        "kind": "catalogue",
        "sealed": True,
        "high_water": {"events": mark, "markets": mark},
        "baseline": {"events": [], "markets": []},
        "parent_event_ids": [],
    }


def scope_chunk(monkeypatch, tmp_path, batch_order):
    """Stub only page I/O; retain real scope decoding, normalization and row accounting."""
    batches = {
        key: {
            "batch_id": key,
            "observation_date": "2026-10-10",
            "scope_json": json.dumps(catalogue_scope(10 if key == "A" else 1)),
        }
        for key in set(batch_order)
    }
    pages, manifests, records, checks, decodes = [], {}, {}, [], []
    for index, key in enumerate(batch_order, 1):
        page = {
            "page_id": f"{key}.page-{index}",
            "batch_id": key,
            "scan_id": f"{key}.scan-{index}",
            "seq": 1,
            "record_key": "events",
            "endpoint": "/events/keyset",
            "observed_at": FIXED_NOW.isoformat(),
        }
        pages.append(page)
        manifests[page["page_id"]] = {
            "page_id": page["page_id"],
            "body_bytes": 100,
            "body_sha256": str(index) * 64,
            "http_status": 200,
        }
        records[page["page_id"]] = [
            make_event(
                str(index),
                f"Scope row {index}",
                markets=[make_market(str(index + 3), "Nested market"), {"id": "bad"}],
            ),
            None,
        ]
    ledger = SimpleNamespace(
        get_batch=lambda key: batches[key],
        get_scan=lambda key: {"scan_id": key},
        page_for_scan=lambda key, seq: None,
    )
    runtime = LoadRuntime(Settings(tmp_path), ledger, now=lambda: FIXED_NOW)

    def read_manifest(settings, directory, page):
        if page is None:
            return None
        checks.append(("manifest", page["page_id"]))
        return manifests[page["page_id"]]

    def validate(batch, scan, manifest, **kwargs):
        checks.append(("identity", manifest["page_id"]))

    def read_records(settings, directory, manifest, record_key, **kwargs):
        checks.append(("records", manifest["page_id"]))
        return records[manifest["page_id"]]

    original_loads = json.loads

    def loads(value, *args, **kwargs):
        if value in [batch["scope_json"] for batch in batches.values()]:
            decodes.append(value)
        return original_loads(value, *args, **kwargs)

    monkeypatch.setattr("oddsfox_catalogue.load.runner.read_indexed_manifest", read_manifest)
    monkeypatch.setattr("oddsfox_catalogue.load.runner.validate_page_identity", validate)
    monkeypatch.setattr("oddsfox_catalogue.load.runner.read_page_records", read_records)
    monkeypatch.setattr(
        "oddsfox_catalogue.load.runner._preflight_load_bytes",
        lambda rt, size: checks.append(("bytes", size)),
    )
    monkeypatch.setattr("oddsfox_catalogue.load.runner.json.loads", loads)
    return SimpleNamespace(
        runtime=runtime,
        pages=pages,
        batches=batches,
        manifests=manifests,
        records=records,
        checks=checks,
        decodes=decodes,
    )


@pytest.mark.parametrize(
    "batch_order,missing,expected_decodes",
    [("AA", None, 1), ("ABA", None, 3), ("ABA", 1, 1)],
)
def test_scope_cache_preserves_complete_rows_counts_and_every_page_preflight(
    monkeypatch, tmp_path, batch_order, missing, expected_decodes
):
    fixture = scope_chunk(monkeypatch, tmp_path, batch_order)
    if missing is not None:
        fixture.manifests[fixture.pages[missing]["page_id"]]["http_status"] = 404
        fixture.batches["B"]["scope_json"] = "invalid JSON skipped only for 404"
    expected_rows, expected_counts = RowSet(), {}
    for page in fixture.pages:
        rows, counts, _ = _rows_for_chunk(fixture.runtime, [page])
        expected_rows.extend(rows)
        expected_counts.update(counts)
    fixture.decodes.clear()
    fixture.checks.clear()
    original_batches = copy.deepcopy(fixture.batches)
    rows, counts, _ = _rows_for_chunk(fixture.runtime, fixture.pages)
    assert rows == expected_rows and counts == expected_counts
    assert rows.events and rows.markets and rows.quarantine
    assert len(fixture.decodes) == expected_decodes
    assert fixture.batches == original_batches
    expected_checks = []
    raw_bytes = 0
    for page in fixture.pages:
        raw_bytes += 100
        expected_checks.extend(
            [
                ("manifest", page["page_id"]),
                ("bytes", raw_bytes),
                ("identity", page["page_id"]),
                ("records", page["page_id"]),
            ]
        )
        if fixture.manifests[page["page_id"]]["http_status"] != 404:
            check = fixture.checks[len(expected_checks)]
            assert check[0] == "bytes" and check[1] > 0
            expected_checks.append(check)
    assert fixture.checks == expected_checks


def test_decoded_scope_is_not_reused_between_chunks(monkeypatch, tmp_path):
    fixture = scope_chunk(monkeypatch, tmp_path, "A")
    first = _rows_for_chunk(fixture.runtime, fixture.pages)
    assert len(fixture.decodes) == 1
    assert _rows_for_chunk(fixture.runtime, fixture.pages) == first
    assert len(fixture.decodes) == 2
    fixture.batches["A"]["scope_json"] = json.dumps(catalogue_scope(0))
    changed = _rows_for_chunk(fixture.runtime, fixture.pages)
    assert len(fixture.decodes) == 3
    assert changed != first and not changed[0].events


def streamed_relation_totals(rows):
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=lambda value: (
            value.isoformat() if hasattr(value, "isoformat") else json.loads(canonical_json(value))
        ),
    )
    return tuple(
        sum(sum(len(part.encode()) for part in encoder.iterencode(row)) + 1 for row in relation)
        for relation in (rows.events, rows.markets, rows.quarantine)
    )


@pytest.mark.parametrize("case", ["mixed", "mixed404", "empty", "404"])
def test_local_relation_totals_are_exact_and_fresh_for_mixed_empty_and_404_pages(
    monkeypatch, tmp_path, case
):
    fixture = scope_chunk(monkeypatch, tmp_path, "AA")
    for index, page in enumerate(fixture.pages):
        event = fixture.records[page["page_id"]][0]
        event["title"] = "£雪🦊"
        event["volume"] = Decimal("0.12345678901234567890123456789")
        event["markets"][1]["nullable"] = None
        if case == "empty":
            fixture.records[page["page_id"]] = []
        if case == "404" or (case == "mixed404" and index == 1):
            fixture.manifests[page["page_id"]]["http_status"] = 404
    original = copy.deepcopy((fixture.records, fixture.batches, fixture.manifests))
    rows, counts, sizes = _rows_for_chunk(fixture.runtime, fixture.pages)
    assert sizes == streamed_relation_totals(rows)
    assert type(sizes) is tuple and all(type(size) is int for size in sizes)
    if case in {"empty", "404"}:
        assert sizes == (0, 0, 0) and rows == RowSet()
        assert set(counts.values()) == {(0, 0, 0)}
    else:
        assert all(size > 0 for size in sizes)
    assert (fixture.records, fixture.batches, fixture.manifests) == original
    assert _rows_for_chunk(fixture.runtime, fixture.pages) == (rows, counts, sizes)
    assert ("bytes", 100) in fixture.checks and ("bytes", 200) in fixture.checks


def chunk_admission(monkeypatch, fixture, *, exhaust=None):
    """Use real byte admission and resources, replacing only storage measurements and writes."""
    operations, writes, acknowledgments = [], [], []
    storage = {"retained": 0, "temporary": 0}

    def reset(rt):
        operations.append("reset")
        rt.storage_snapshot = (storage["retained"], storage["temporary"])

    def preflight(rt, size):
        operations.append(("preflight", size))
        _preflight_load_bytes(rt, size)

    def existing(rt, rows):
        operations.append("existing")
        if exhaust is not None and exhaust[0] == 0:
            exhaust[1](storage)

    def write(rt, resource):
        operations.append(("write", resource.name))
        writes.append((resource.name, list(resource)))
        if exhaust is not None and len(writes) == exhaust[0]:
            exhaust[1](storage)
        return SimpleNamespace(loads_ids=[resource.name])

    monkeypatch.setattr("oddsfox_catalogue.load.runner._reset_load_budget", reset)
    monkeypatch.setattr("oddsfox_catalogue.load.runner._preflight_load_bytes", preflight)
    monkeypatch.setattr("oddsfox_catalogue.load.runner._check_existing_observations", existing)
    monkeypatch.setattr("oddsfox_catalogue.load.runner._run_resource", write)
    fixture.runtime.ledger.complete_load_chunk = lambda **kwargs: acknowledgments.append(kwargs)
    return operations, writes, acknowledgments


def test_load_chunk_encodes_each_row_once_and_keeps_all_admission_calls(monkeypatch, tmp_path):
    fixture = scope_chunk(monkeypatch, tmp_path, "AA")
    expected_rows, expected_counts, sizes = _rows_for_chunk(fixture.runtime, fixture.pages)
    encoded = []

    def encode(row):
        encoded.append(id(row))
        return _encoded_row_bytes(row)

    original = copy.deepcopy(fixture.records)
    monkeypatch.setattr("oddsfox_catalogue.load.runner._encoded_row_bytes", encode)
    operations, writes, acknowledgments = chunk_admission(monkeypatch, fixture)
    summary = LoadSummary()
    _load_chunk(fixture.runtime, fixture.pages, summary)
    relations = (expected_rows.events, expected_rows.markets, expected_rows.quarantine)
    assert [data for _, data in writes] == list(relations)
    assert len(encoded) == len(set(encoded)) == sum(map(len, relations))
    page_totals = [
        sum(
            streamed_relation_totals(
                RowSet(
                    **{
                        name: [
                            row
                            for row in getattr(expected_rows, name)
                            if row["page_id"] == page["page_id"]
                        ]
                        for name in ("events", "markets", "quarantine")
                    }
                )
            )
        )
        for page in fixture.pages
    ]
    assert operations == [
        "reset",
        ("preflight", 100),
        ("preflight", page_totals[0]),
        ("preflight", 200),
        ("preflight", sum(page_totals)),
        "existing",
        ("preflight", sum(sizes)),
        "reset",
        ("preflight", sizes[0]),
        ("write", "event_observations"),
        "reset",
        "reset",
        ("preflight", sizes[1]),
        ("write", "market_observations"),
        "reset",
        "reset",
        ("preflight", sizes[2]),
        ("write", "quarantined_records"),
        "reset",
    ]
    assert acknowledgments[0]["page_counts"] == expected_counts
    assert acknowledgments[0]["quarantine"] == expected_rows.quarantine
    assert (summary.pages_loaded, summary.event_rows, summary.market_rows, summary.quarantined) == (
        2,
        len(relations[0]),
        len(relations[1]),
        len(relations[2]),
    )
    assert fixture.records == original


@pytest.mark.parametrize("allowance", ["retained", "temporary"])
@pytest.mark.parametrize("remaining", [0, -1])
def test_exact_total_admission_boundary_precedes_writes(
    monkeypatch, tmp_path, allowance, remaining
):
    fixture = scope_chunk(monkeypatch, tmp_path, "AA")
    _, _, sizes = _rows_for_chunk(fixture.runtime, fixture.pages)
    limit = LOAD_FIXED_RESERVE + sum(sizes) * LOAD_EXPANSION_FACTOR + remaining
    field = "max_retained_bytes" if allowance == "retained" else "max_temp_bytes"
    fixture.runtime.settings = replace(
        fixture.runtime.settings,
        capture=replace(fixture.runtime.settings.capture, **{field: limit}),
    )
    _, writes, acknowledgments = chunk_admission(monkeypatch, fixture)
    summary = LoadSummary()
    if remaining == 0:
        _load_chunk(fixture.runtime, fixture.pages, summary)
        assert len(writes) == 3 and len(acknowledgments) == 1
    else:
        with pytest.raises(RequestBudgetExceeded, match="allowance"):
            _load_chunk(fixture.runtime, fixture.pages, summary)
        assert not writes and not acknowledgments and summary.pages_loaded == 0


@pytest.mark.parametrize("allowance", ["retained", "temporary"])
@pytest.mark.parametrize("resource_index", [0, 1, 2])
def test_resource_exhaustion_refreshes_admission_and_leaves_chunk_unacknowledged(
    monkeypatch, tmp_path, allowance, resource_index
):
    fixture = scope_chunk(monkeypatch, tmp_path, "AA")
    _, _, sizes = _rows_for_chunk(fixture.runtime, fixture.pages)
    field = "max_retained_bytes" if allowance == "retained" else "max_temp_bytes"

    def exhaust(storage):
        storage[allowance] = (
            getattr(fixture.runtime.settings.capture, field)
            - LOAD_FIXED_RESERVE
            - sizes[resource_index] * LOAD_EXPANSION_FACTOR
            + 1
        )

    operations, writes, acknowledgments = chunk_admission(
        monkeypatch, fixture, exhaust=(resource_index, exhaust)
    )
    summary = LoadSummary()
    with pytest.raises(RequestBudgetExceeded, match="allowance"):
        _load_chunk(fixture.runtime, fixture.pages, summary)
    assert len(writes) == resource_index and not acknowledgments
    assert operations[-1] == ("preflight", sizes[resource_index])
    assert (
        summary.pages_loaded
        == summary.event_rows
        == summary.market_rows
        == summary.quarantined
        == 0
    )


@pytest.mark.parametrize("scope", [{"revision": 1, "sealed": True}, {"revision": 2}])
def test_new_non404_batch_validates_scope_on_its_first_use(monkeypatch, tmp_path, scope):
    fixture = scope_chunk(monkeypatch, tmp_path, "AB")
    fixture.batches["B"]["scope_json"] = json.dumps(scope)
    with pytest.raises(LoadBlocked, match="supported sealed capture scope"):
        _rows_for_chunk(fixture.runtime, fixture.pages)
    assert len(fixture.decodes) == 2
    assert ("records", fixture.pages[1]["page_id"]) in fixture.checks


@pytest.mark.parametrize("observed_at", [0, "not a timestamp", ""])
def test_timestamp_validation_precedes_even_first_scope_decode(monkeypatch, tmp_path, observed_at):
    fixture = scope_chunk(monkeypatch, tmp_path, "A")
    fixture.pages[0]["observed_at"] = observed_at
    fixture.batches["A"]["scope_json"] = "invalid JSON"
    with pytest.raises(LoadBlocked, match="unparseable observed_at"):
        _rows_for_chunk(fixture.runtime, fixture.pages)
    assert not fixture.decodes
    assert ("records", fixture.pages[0]["page_id"]) in fixture.checks


@pytest.mark.parametrize("failure", ["manifest", "identity", "records", "bytes"])
def test_cached_scope_cannot_bypass_later_page_failure(monkeypatch, tmp_path, failure):
    fixture = scope_chunk(monkeypatch, tmp_path, "AA")
    if failure == "manifest":
        fixture.manifests[fixture.pages[1]["page_id"]] = None
        error, match = LoadBlocked, "not durable"
    else:
        name = {
            "identity": "validate_page_identity",
            "records": "read_page_records",
            "bytes": "_preflight_load_bytes",
        }[failure]
        from oddsfox_catalogue.load import runner

        original = getattr(runner, name)

        def fail_on_second(*args, **kwargs):
            second_page = (
                args[1] == 200
                if failure == "bytes"
                else args[2]["page_id"] == fixture.pages[1]["page_id"]
            )
            if second_page:
                raise DurabilityError("second-page guard failed")
            return original(*args, **kwargs)

        monkeypatch.setattr(runner, name, fail_on_second)
        error, match = DurabilityError, "second-page guard failed"
    with pytest.raises(error, match=match):
        _rows_for_chunk(fixture.runtime, fixture.pages)
    assert len(fixture.decodes) == 1


def scoped_rows(entity, value, scope):
    ctx = PageContext("scope-page", BATCH, f"/{entity}/keyset", FIXED_NOW, entity)
    record = make_market("1", "HWM market") if entity == "markets" else make_event("1", "HWM event")
    record["id"] = value
    return rows_for_page(ctx, [record], scope=scope)


class MembershipTrap(str):
    def __eq__(self, other):
        pytest.fail("below-HWM selection scanned a baseline or parent list")

    __hash__ = str.__hash__


@pytest.mark.parametrize("entity", ["events", "markets"])
@pytest.mark.parametrize("value", ["1", "99999999999999999999"])
def test_bounded_integer_hwm_skips_membership_and_preserves_complete_rows(entity, value):
    scope = catalogue_scope(int(value))
    expected = scoped_rows(entity, value, scope)
    assert getattr(expected, entity)
    scope["baseline"][entity] = [MembershipTrap("unrelated")]
    scope["parent_event_ids"] = [MembershipTrap("unrelated")] if entity == "events" else None
    assert scoped_rows(entity, value, scope) == expected


@pytest.mark.parametrize(
    "entity,membership", [("events", "baseline"), ("events", "parent"), ("markets", "baseline")]
)
def test_above_hwm_baseline_and_parent_exceptions_are_retained(entity, membership):
    scope = catalogue_scope(1)
    assert scoped_rows(entity, "2", scope) == RowSet()
    if membership == "baseline":
        scope["baseline"][entity] = ["2"]
    else:
        scope["parent_event_ids"] = ["2"]
    original = copy.deepcopy(scope)
    assert scoped_rows(entity, "2", scope) == scoped_rows(entity, "2", None)
    assert scope == original


@pytest.mark.parametrize(
    "entity,field,error",
    [
        ("events", "baseline", TypeError),
        ("markets", "baseline", TypeError),
        ("events", "parent", TypeError),
        ("events", "missing_parent", KeyError),
    ],
)
def test_hwm_shortcut_preserves_malformed_container_errors(entity, field, error):
    scope = catalogue_scope(2)
    if field == "baseline":
        scope["baseline"][entity] = None
    elif field == "parent":
        scope["parent_event_ids"] = None
    else:
        scope.pop("parent_event_ids")
    with pytest.raises(error):
        scoped_rows(entity, "1", scope)


def test_hwm_shortcut_requires_exact_list_containers():
    checks = []

    class ObservedList(list):
        def __contains__(self, value):
            checks.append(value)
            return super().__contains__(value)

    scope = catalogue_scope(2)
    scope["baseline"]["events"] = ObservedList()
    assert scoped_rows("events", "1", scope) == scoped_rows("events", "1", None)
    assert checks == ["1"]
    checks.clear()
    scope["baseline"]["events"] = []
    scope["parent_event_ids"] = ObservedList()
    assert scoped_rows("events", "1", scope) == scoped_rows("events", "1", None)
    assert checks == ["1"]


@pytest.mark.parametrize("mark,accepted", [(2.0, True), (True, True), (False, False), (0, False)])
def test_nonstandard_numeric_hwm_keeps_original_comparison_semantics(mark, accepted):
    scope = catalogue_scope(mark)
    expected = scoped_rows("events", "1", None) if accepted else RowSet()
    assert scoped_rows("events", "1", scope) == expected


def test_string_hwm_retains_baseline_exception_and_comparison_error():
    scope = catalogue_scope("2")
    with pytest.raises(TypeError):
        scoped_rows("events", "1", scope)
    scope["baseline"]["events"] = ["1"]
    assert scoped_rows("events", "1", scope) == scoped_rows("events", "1", None)


@pytest.mark.parametrize("value,mark", [("1", None), ("١", 2), ("bad", 2)])
def test_existing_hwm_guards_still_precede_membership(value, mark):
    scope = catalogue_scope(mark)
    scope["baseline"]["events"] = None
    scope["parent_event_ids"] = None
    assert scoped_rows("events", value, scope) == scoped_rows("events", value, None)


def test_oversized_decimal_baseline_id_quarantines_before_integer_conversion():
    value = "9" * 5000
    scope = catalogue_scope(10)
    scope["baseline"]["events"] = [value]
    rows = scoped_rows("events", value, scope)
    assert rows == scoped_rows("events", value, None)
    assert not rows.events and rows.quarantine[0]["reason"] == "record has an invalid id"


@pytest.mark.parametrize(
    "entity,membership",
    [("events", "event_ids"), ("events", "parent_event_ids"), ("markets", "market_ids")],
)
def test_selected_scope_uses_explicit_ids_even_above_hwm(entity, membership):
    scope = catalogue_scope(0)
    scope.update(kind="selected", event_ids=[], market_ids=[])
    scope["baseline"][entity] = ["1"]
    assert scoped_rows(entity, "1", scope) == RowSet()
    scope[membership] = ["2"]
    assert scoped_rows(entity, "2", scope) == scoped_rows(entity, "2", None)
    assert scoped_rows(entity, "1", scope) == RowSet()


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
