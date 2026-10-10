"""Bounded warehouse queries and the complete semantic rebuild boundary."""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import projection_queries
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.limits import enforce_storage_limits, remaining_temp_bytes

SEMANTIC_RELATIONS = (
    "bronze.event_observations",
    "bronze.market_observations",
    "bronze.quarantined_records",
    "bronze.batch_registry",
    "staging.stg_gamma__batches",
    "staging.stg_gamma__quarantined_records",
    "staging.stg_gamma__event_observations",
    "staging.stg_gamma__market_observations",
    "staging.stg_gamma__event_tags",
    "staging.stg_gamma__event_series",
    "staging.stg_gamma__market_event_refs",
    "staging.stg_gamma__market_tags",
    "core.int_market_selected",
    "core.int_market_outcomes",
    "core.markets_current",
    "core.events_current",
    "core.market_event_bridge",
    "core.event_tags_current",
    "core.event_series_current",
    "core.outcomes_current",
    "core.market_tags_current",
    "core.quarantine_market_outcomes",
    "history.int_market_semantic",
    "history.int_event_semantic",
    "history.event_history",
    "history.market_history",
    "history.event_metrics",
    "history.market_metrics",
    "marts.mart_event_catalogue",
)
OPERATIONAL_RELATIONS = ("marts.catalogue_snapshots",)
PROCESSING_COLUMNS = {"loaded_at", "load_id", "batch_loaded_at", "built_through"}


def canonical_json_chunks(value, *, max_bytes=128 * 1024**2):
    total = 0
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), allow_nan=False)
    for part in encoder.iterencode(value):
        chunk = part.encode("utf-8")
        total += len(chunk)
        if total + 1 > max_bytes:
            raise ValueError("canonical JSON exceeds its size limit")
        yield chunk
    yield b"\n"


def json_descriptor(value):
    digest, size = hashlib.sha256(), 0
    for chunk in canonical_json_chunks(value):
        digest.update(chunk)
        size += len(chunk)
    return {"bytes": size, "sha256": digest.hexdigest()}


@contextmanager
def bounded_connection(settings: Settings, path: Path | None = None, *, read_only=True):
    """Use the operator's memory, spill and retained limits for every semantic query."""
    enforce_storage_limits(settings)
    settings.temporary_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(
        str(path or settings.warehouse_path),
        read_only=read_only,
        config={
            "memory_limit": settings.load.duckdb_memory_limit,
            "threads": settings.load.duckdb_threads,
            "temp_directory": str(settings.temporary_dir),
            "max_temp_directory_size": f"{remaining_temp_bytes(settings)}B",
        },
    )
    try:
        connection.execute("SET TimeZone = 'UTC'")
        yield connection
    finally:
        connection.close()
        enforce_storage_limits(settings)


def relation_schema(connection, relation: str):
    schema, name = relation.split(".")
    rows = connection.execute(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, name],
    ).fetchall()
    if not rows:
        raise ValueError(f"required warehouse relation is missing: {relation}")
    return [
        {"name": name, "type": kind, "nullable": nullable == "YES"} for name, kind, nullable in rows
    ]


def semantic_query(connection, relation: str) -> str:
    columns = [
        column["name"]
        for column in relation_schema(connection, relation)
        if column["name"] not in PROCESSING_COLUMNS and not column["name"].startswith("_dlt")
    ]
    names = ", ".join('"' + name.replace('"', '""') + '"' for name in columns)
    return f"SELECT {names} FROM {relation}"


def warehouse_snapshot(settings: Settings):
    with bounded_connection(settings) as connection:
        return {
            "schemas": {
                relation: relation_schema(connection, relation)
                for relation in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)
            },
            "relations": {
                relation: semantic_fingerprint(connection, semantic_query(connection, relation))
                for relation in SEMANTIC_RELATIONS
            },
        }


def published_snapshot(settings: Settings):
    with bounded_connection(settings) as connection:
        return {
            name: semantic_fingerprint(connection, query)
            for name, query in projection_queries().items()
        }
