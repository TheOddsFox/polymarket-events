"""Chunked bronze loading with loaded checkpoints and batch registration.

Flow per run:

1. Select pages that are fetched but not loaded, in ``(batch, plan order, attempt, seq)``.
2. For each chunk of ``load.max_pages_per_run`` pages: verify raw checksums,
   build envelope rows, and run one dlt pipeline. After dlt commits, one ledger
   transaction records the loaded checkpoints, per-page counts, and quarantine.
3. For each captured batch with every page loaded, insert a ``batch_registry``
   row and mark the batch ``loaded``. dbt reads only registered batches.

Failure at any point leaves unloaded pages pending. Rerunning produces the
same observation IDs, and insert-only merge makes the rerun a no-op for rows
that already committed.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

import dlt
from dlt.sources import DltResource

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import read_page_records, scan_dir_for, validate_page_identity
from oddsfox_catalogue.capture.writer import manifest_path, read_manifest
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import fault_point
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.ids import canonical_json, iso_utc, utc_now
from oddsfox_catalogue.limits import (
    enforce_storage_limits,
    remaining_temp_bytes,
    retained_bytes,
    temporary_bytes,
)
from oddsfox_catalogue.load.rows import (
    PageContext,
    RowSet,
    assert_unique,
    parse_timestamp,
    rows_for_page,
)
from oddsfox_catalogue.load.runtime import confined_runtime
from oddsfox_catalogue.load.source import (
    event_resource,
    interrupted,
    make_destination,
    make_pipeline,
    market_resource,
    quarantine_resource,
    registry_resource,
)
from oddsfox_catalogue.semantics import bounded_connection
from oddsfox_catalogue.warehouse_version import ensure_warehouse_contract


def _invalid_number(value: str) -> None:
    raise LoadBlocked("raw page contains a non-finite number")


class LoadBlocked(RuntimeError):
    """Loading refused: a page is not durable or a batch is inconsistent."""


@dataclass
class LoadRuntime:
    settings: Settings
    ledger: Ledger
    now: Callable[[], datetime] = utc_now
    pipeline: dlt.Pipeline | None = None
    batch_cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    storage_snapshot: tuple[int, int] | None = None


@dataclass
class LoadSummary:
    pages_loaded: int = 0
    event_rows: int = 0
    market_rows: int = 0
    quarantined: int = 0
    load_ids: list[str] = field(default_factory=list)
    batches_registered: list[str] = field(default_factory=list)


def _pipeline(rt: LoadRuntime) -> dlt.Pipeline:
    if rt.pipeline is None:
        rt.pipeline = make_pipeline(
            rt.settings.warehouse_path,
            rt.settings.dlt_pipelines_dir,
            rt.settings.load,
            temp_directory=rt.settings.temporary_dir,
            max_temp_bytes=remaining_temp_bytes(rt.settings),
        )
    return rt.pipeline


def _run_resource(rt: LoadRuntime, resource: DltResource):
    # Pipeline state is retained; each public run gets a fresh destination config.
    pipeline = _pipeline(rt)
    destination = make_destination(
        rt.settings.warehouse_path,
        rt.settings.load,
        temp_directory=rt.settings.temporary_dir,
        max_temp_bytes=remaining_temp_bytes(rt.settings),
    )
    return pipeline.run(resource, destination=destination)


def _batch(rt: LoadRuntime, batch_id: str) -> dict[str, Any]:
    if batch_id not in rt.batch_cache:
        batch = rt.ledger.get_batch(batch_id)
        if batch is None:
            raise LoadBlocked(f"page references unknown batch {batch_id}")
        rt.batch_cache[batch_id] = batch
    return rt.batch_cache[batch_id]


LOAD_FIXED_RESERVE = 8 * 1024**2
LOAD_EXPANSION_FACTOR = 8


def _encoded_row_bytes(row: dict[str, Any]) -> int:
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=lambda value: (
            value.isoformat() if isinstance(value, datetime) else json.loads(canonical_json(value))
        ),
    )
    return sum(len(part.encode()) for part in encoder.iterencode(row)) + 1


def _reset_load_budget(rt: LoadRuntime) -> None:
    retained = retained_bytes(rt.settings)
    temporary = temporary_bytes(rt.settings)
    if (
        retained > rt.settings.capture.max_retained_bytes
        or temporary > rt.settings.capture.max_temp_bytes
    ):
        raise RequestBudgetExceeded("load storage allowance exhausted")
    rt.storage_snapshot = (retained, temporary)


def _preflight_load_bytes(rt: LoadRuntime, size: int) -> None:
    """Reserve room for dlt staging, warehouse/WAL growth and write copies."""
    if rt.storage_snapshot is None:
        _reset_load_budget(rt)
    assert rt.storage_snapshot is not None
    retained, temporary = rt.storage_snapshot
    allowance = LOAD_FIXED_RESERVE + size * LOAD_EXPANSION_FACTOR
    if temporary + allowance > rt.settings.capture.max_temp_bytes:
        raise RequestBudgetExceeded("load staging estimate exceeds temporary storage allowance")
    if retained + allowance > rt.settings.capture.max_retained_bytes:
        raise RequestBudgetExceeded("load estimate exceeds retained storage allowance")


def _rows_for_chunk(
    rt: LoadRuntime,
    pages: list[dict[str, Any]],
) -> tuple[RowSet, dict[str, tuple[int, int, int]]]:
    """Read and verify each page, then build its rows. Raises on any non-durable page."""
    rows = RowSet()
    counts: dict[str, tuple[int, int, int]] = {}
    raw_bytes = 0
    serialized_bytes = 0
    for page in pages:
        batch = _batch(rt, page["batch_id"])
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], page["scan_id"]
        )
        manifest = read_manifest(
            manifest_path(directory, page["seq"]), trusted_root=rt.settings.raw_dir
        )
        if manifest is None:
            raise LoadBlocked(f"page {page['page_id']} is not durable; refusing to load it")

        raw_bytes += manifest["body_bytes"]
        _preflight_load_bytes(rt, raw_bytes)
        scan = rt.ledger.get_scan(page["scan_id"])
        if scan is None:
            raise LoadBlocked("raw page has no committed scan")
        prior = rt.ledger.pages_for_scan(page["scan_id"])
        previous = next((value for value in prior if value["seq"] == page["seq"] - 1), None)
        validate_page_identity(batch, scan, manifest, seq=page["seq"], previous=previous)
        durable_fields = (
            "page_id",
            "batch_id",
            "scan_id",
            "seq",
            "endpoint",
            "input_cursor",
            "output_cursor",
            "offset_start",
            "offset_end",
            "record_count",
            "http_status",
            "retries",
            "latency_s",
            "body_sha256",
            "gz_sha256",
            "observed_at",
        )
        if (
            any(manifest.get(key) != page[key] for key in durable_fields)
            or bool(manifest["terminal"]) != bool(page["terminal"])
            or canonical_json(manifest["params"]) != page["params_json"]
        ):
            raise LoadBlocked("raw page manifest differs from the committed ledger evidence")
        records = read_page_records(
            rt.settings, directory, manifest, page["record_key"], scan=scan, previous=previous
        )
        if manifest["http_status"] == 404:
            counts[page["page_id"]] = (0, 0, 0)
            continue
        fetch_failed = None
        observed_at = parse_timestamp(page["observed_at"])
        if observed_at is None:
            raise LoadBlocked(f"page {page['page_id']} has an unparseable observed_at")
        ctx = PageContext(
            page_id=page["page_id"],
            batch_id=page["batch_id"],
            endpoint=page["endpoint"],
            observed_at=observed_at,
            record_key=page["record_key"],
            body_sha256=manifest["body_sha256"],
        )
        scope = json.loads(batch.get("scope_json") or "{}")
        if scope.get("revision") != 2 or not scope.get("sealed"):
            raise LoadBlocked("loading requires a supported sealed capture scope")
        page_rows = rows_for_page(ctx, records, fetch_failed, scope=scope)
        counts[page["page_id"]] = (
            len(page_rows.events),
            len(page_rows.markets),
            len(page_rows.quarantine),
        )
        serialized_bytes += sum(
            _encoded_row_bytes(row)
            for relation in (page_rows.events, page_rows.markets, page_rows.quarantine)
            for row in relation
        )
        _preflight_load_bytes(rt, serialized_bytes)
        rows.extend(page_rows)

    assert_unique(rows.events)
    assert_unique(rows.markets)
    assert_unique(rows.quarantine, "quarantine_id")
    return rows, counts


def _check_existing_observations(rt: LoadRuntime, rows: RowSet) -> None:
    """Insert-only merge must not hide conflicting evidence behind an existing key."""
    if not rt.settings.warehouse_path.exists():
        return
    fields = (
        "observation_id",
        "venue",
        "entity_id",
        "batch_id",
        "page_id",
        "observed_at",
        "source_updated_at",
        "endpoint",
        "payload_hash",
        "payload",
        "normalized",
    )
    with bounded_connection(rt.settings) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
            ).fetchall()
        }
        for table, incoming in (
            ("event_observations", rows.events),
            ("market_observations", rows.markets),
        ):
            if table not in tables:
                continue
            columns = fields + (
                ("source_kind", "json_pointer") if table == "market_observations" else ()
            )
            by_id = {row["observation_id"]: row for row in incoming}
            for offset in range(0, len(incoming), 512):
                ids = [row["observation_id"] for row in incoming[offset : offset + 512]]
                existing = connection.execute(
                    f"SELECT {', '.join(columns)} FROM bronze.{table} "
                    "WHERE observation_id IN (SELECT unnest(?))",
                    [ids],
                ).fetchall()
                for values in existing:
                    row = by_id[values[0]]
                    prior = dict(zip(columns, values, strict=True))
                    for field in columns:
                        current = row[field]
                        previous = prior[field]
                        if field in {"payload", "normalized"}:
                            previous = canonical_json(
                                json.loads(
                                    previous, parse_float=Decimal, parse_constant=_invalid_number
                                )
                            )
                            current = canonical_json(current)
                        if previous != current:
                            raise LoadBlocked(
                                "conflicting persisted observation; refusing insert-only merge"
                            )


def _mid_load_fault() -> None:
    fault_point("mid_dlt_load")


def _chunk_resources(rows: RowSet) -> Iterator[DltResource]:
    """Build each resource lazily so it is created immediately before its run."""
    # Typed empty resources are required for selected/empty captures and quarantine exports.
    yield event_resource(
        interrupted(rows.events, _mid_load_fault), materialize_only=not rows.events
    )
    yield market_resource(rows.markets, materialize_only=not rows.markets)
    yield quarantine_resource(rows.quarantine, materialize_only=not rows.quarantine)


def _load_chunk(
    rt: LoadRuntime,
    pages: list[dict[str, Any]],
    summary: LoadSummary,
) -> None:
    _reset_load_budget(rt)
    rows, counts = _rows_for_chunk(rt, pages)
    _check_existing_observations(rt, rows)
    _preflight_load_bytes(
        rt,
        sum(
            _encoded_row_bytes(row)
            for relation in (rows.events, rows.markets, rows.quarantine)
            for row in relation
        ),
    )
    load_id: str | None = None

    # One pipeline.run per table, each resource created just before its run. Creating
    # several resources before running them corrupts dlt's shared schema (json_pointer
    # hints leak into event_observations). Each run is idempotent under insert-only merge,
    # so a crash between runs is safe.
    for resource, relation in zip(
        _chunk_resources(rows), (rows.events, rows.markets, rows.quarantine), strict=True
    ):
        _reset_load_budget(rt)
        _preflight_load_bytes(rt, sum(_encoded_row_bytes(row) for row in relation))
        info = _run_resource(rt, resource)
        _reset_load_budget(rt)
        if info.loads_ids:
            load_id = str(info.loads_ids[-1])
            summary.load_ids.append(load_id)

    rt.ledger.complete_load_chunk(
        page_counts=counts,
        load_id=load_id,
        loaded_at=iso_utc(rt.now()),
        quarantine=rows.quarantine,
    )
    summary.pages_loaded += len(pages)
    summary.event_rows += len(rows.events)
    summary.market_rows += len(rows.markets)
    summary.quarantined += len(rows.quarantine)


def _chunks(items: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def quarantine_over_limit(
    event_rows: int, market_rows: int, quarantine_rows: int, max_ratio: float
) -> bool:
    """True when quarantined records are a larger share than ``max_ratio`` of the batch.

    The share is quarantined over every record the batch produced (loaded or quarantined).
    A share exactly at the limit passes. A batch with no records passes.
    """
    observed = event_rows + market_rows + quarantine_rows
    if observed <= 0:
        return False
    return quarantine_rows / observed > max_ratio


def _register_complete_batches(rt: LoadRuntime, summary: LoadSummary, batch_id: str | None) -> None:
    blocked: list[str] = []
    for batch in rt.ledger.list_batches("captured"):
        if batch_id is not None and batch["batch_id"] != batch_id:
            continue
        if rt.ledger.pending_load_pages(batch["batch_id"]):
            continue
        totals = rt.ledger.batch_load_totals(batch["batch_id"])
        if quarantine_over_limit(
            int(totals["event_rows"]),
            int(totals["market_rows"]),
            int(totals["quarantine_rows"]),
            rt.settings.quality.quarantine_max_ratio,
        ):
            # Left unregistered, so dbt never reads it. Other batches still register.
            blocked.append(batch["batch_id"])
            continue
        row = {
            "batch_id": batch["batch_id"],
            "mode": batch["mode"],
            "observation_date": batch["observation_date"],
            "status": "loaded",
            "page_count": totals["page_count"],
            "event_observation_count": totals["event_rows"],
            "market_observation_count": totals["market_rows"],
            "quarantine_count": totals["quarantine_rows"],
            "loaded_at": iso_utc(rt.now()),
            "load_id": totals["last_load_id"],
        }
        fault_point("before_registry")
        _reset_load_budget(rt)
        _preflight_load_bytes(rt, _encoded_row_bytes(row))
        info = _run_resource(rt, registry_resource([row]))
        _reset_load_budget(rt)
        if info.loads_ids:
            summary.load_ids.append(str(info.loads_ids[-1]))
        rt.ledger.set_batch_status(batch["batch_id"], "loaded", row["loaded_at"])
        summary.batches_registered.append(batch["batch_id"])
    if blocked:
        raise LoadBlocked(
            f"quarantine share above [quality] quarantine_max_ratio "
            f"({rt.settings.quality.quarantine_max_ratio}) for batch(es) {', '.join(blocked)}; "
            "left unregistered. Inspect bronze.quarantined_records and correct the capture "
            "before replaying; quality limits are not relaxed automatically."
        )


def load_pending(rt: LoadRuntime, batch_id: str | None = None) -> LoadSummary:
    with confined_runtime(rt.settings.dlt_pipelines_dir):
        return _load_pending(rt, batch_id)


def _load_pending(rt: LoadRuntime, batch_id: str | None = None) -> LoadSummary:
    """Load every fetched-but-unloaded page (optionally for one batch), then register batches.

    Raises ``LoadBlocked`` after registering every batch that passes the quarantine gate, if
    any batch fails it.
    """
    eligible_batches = rt.ledger.list_batches()
    for candidate in eligible_batches:
        if (batch_id is None or batch_id == candidate["batch_id"]) and rt.ledger.pending_controls(
            candidate["batch_id"]
        ):
            raise LoadBlocked(
                "capture has an interrupted control commit; explicitly resume its batch first"
            )
    _reset_load_budget(rt)
    _preflight_load_bytes(rt, 0)
    from oddsfox_catalogue.certification import mark_dirty

    mark_dirty(rt.settings, "warehouse load")
    ensure_warehouse_contract(rt.settings, create=True)
    enforce_storage_limits(rt.settings)
    summary = LoadSummary()
    pending = rt.ledger.pending_load_pages(batch_id)
    for chunk in _chunks(pending, rt.settings.load.max_pages_per_run):
        _load_chunk(rt, chunk, summary)
    _register_complete_batches(rt, summary, batch_id)
    return summary
