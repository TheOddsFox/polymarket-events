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
from typing import Any

import dlt
from dlt.sources import DltResource

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import scan_dir_for
from oddsfox_catalogue.capture.writer import manifest_path, read_body, read_manifest, verify_page
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import fault_point
from oddsfox_catalogue.gamma.paginators import unpack
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.load.rows import (
    PageContext,
    RowSet,
    assert_unique,
    parse_timestamp,
    rows_for_page,
)
from oddsfox_catalogue.load.source import (
    event_resource,
    interrupted,
    make_pipeline,
    market_resource,
    quarantine_resource,
    registry_resource,
)


class LoadBlocked(RuntimeError):
    """Loading refused: a page is not durable or a batch is inconsistent."""


@dataclass
class LoadRuntime:
    settings: Settings
    ledger: Ledger
    now: Callable[[], datetime] = utc_now
    pipeline: dlt.Pipeline | None = None
    batch_cache: dict[str, dict[str, Any]] = field(default_factory=dict)


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
        )
    return rt.pipeline


def _batch(rt: LoadRuntime, batch_id: str) -> dict[str, Any]:
    if batch_id not in rt.batch_cache:
        batch = rt.ledger.get_batch(batch_id)
        if batch is None:
            raise LoadBlocked(f"page references unknown batch {batch_id}")
        rt.batch_cache[batch_id] = batch
    return rt.batch_cache[batch_id]


def _rows_for_chunk(
    rt: LoadRuntime,
    pages: list[dict[str, Any]],
) -> tuple[RowSet, dict[str, tuple[int, int, int]]]:
    """Read and verify each page, then build its rows. Raises on any non-durable page."""
    rows = RowSet()
    counts: dict[str, tuple[int, int, int]] = {}
    for page in pages:
        batch = _batch(rt, page["batch_id"])
        directory = scan_dir_for(
            rt.settings, batch["observation_date"], batch["batch_id"], page["scan_id"]
        )
        manifest = read_manifest(manifest_path(directory, page["seq"]))
        if manifest is None or not verify_page(directory, manifest):
            raise LoadBlocked(f"page {page['page_id']} is not durable; refusing to load it")

        if manifest["http_status"] != 200:
            counts[page["page_id"]] = (0, 0, 0)
            continue

        body = json.loads(read_body(directory, manifest))
        records, _ = unpack(body, page["record_key"])
        observed_at = parse_timestamp(page["observed_at"])
        if observed_at is None:
            raise LoadBlocked(f"page {page['page_id']} has an unparseable observed_at")
        ctx = PageContext(
            page_id=page["page_id"],
            batch_id=page["batch_id"],
            endpoint=page["endpoint"],
            observed_at=observed_at,
            record_key=page["record_key"],
        )
        page_rows = rows_for_page(ctx, records)
        counts[page["page_id"]] = (
            len(page_rows.events),
            len(page_rows.markets),
            len(page_rows.quarantine),
        )
        rows.extend(page_rows)

    assert_unique(rows.events)
    assert_unique(rows.markets)
    assert_unique(rows.quarantine, "quarantine_id")
    return rows, counts


def _mid_load_fault() -> None:
    fault_point("mid_dlt_load")


def _chunk_resources(rows: RowSet) -> Iterator[DltResource]:
    """Build each resource lazily so it is created immediately before its run."""
    if rows.events:
        yield event_resource(interrupted(rows.events, _mid_load_fault))
    if rows.markets:
        yield market_resource(rows.markets)
    if rows.quarantine:
        yield quarantine_resource(rows.quarantine)


def _load_chunk(
    rt: LoadRuntime,
    pages: list[dict[str, Any]],
    summary: LoadSummary,
) -> None:
    rows, counts = _rows_for_chunk(rt, pages)
    load_id: str | None = None

    # One pipeline.run per table, each resource created just before its run. Creating
    # several resources before running them corrupts dlt's shared schema (json_pointer
    # hints leak into event_observations). Each run is idempotent under insert-only merge,
    # so a crash between runs is safe.
    for resource in _chunk_resources(rows):
        info = _pipeline(rt).run(resource)
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
        info = _pipeline(rt).run(registry_resource([row]))
        if info.loads_ids:
            summary.load_ids.append(str(info.loads_ids[-1]))
        rt.ledger.set_batch_status(batch["batch_id"], "loaded", row["loaded_at"])
        summary.batches_registered.append(batch["batch_id"])
    if blocked:
        raise LoadBlocked(
            f"quarantine share above [quality] quarantine_max_ratio "
            f"({rt.settings.quality.quarantine_max_ratio}) for batch(es) {', '.join(blocked)}; "
            "left unregistered. Inspect bronze.quarantined_records, then raise the limit "
            "and run make replay to register them."
        )


def load_pending(rt: LoadRuntime, batch_id: str | None = None) -> LoadSummary:
    """Load every fetched-but-unloaded page (optionally for one batch), then register batches.

    Raises ``LoadBlocked`` after registering every batch that passes the quarantine gate, if
    any batch fails it.
    """
    summary = LoadSummary()
    pending = rt.ledger.pending_load_pages(batch_id)
    for chunk in _chunks(pending, rt.settings.load.max_pages_per_run):
        _load_chunk(rt, chunk, summary)
    _register_complete_batches(rt, summary, batch_id)
    return summary
