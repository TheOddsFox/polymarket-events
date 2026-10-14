"""Bounded warehouse queries and the complete semantic rebuild boundary."""

import hashlib
import json
import os
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import projection_queries
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.limits import enforce_storage_limits, measure_storage, remaining_temp_bytes
from oddsfox_catalogue.load.storage import ensure_observation_views
from oddsfox_catalogue.timing import phase_timing

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
    "core.int_market_selection_state",
    "core.int_event_selection_state",
    "core.int_market_asset_nominations",
    "core.int_market_asset_conflicts",
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
CHECKPOINT_MODELS = (
    "core.int_market_selection_state",
    "core.int_event_selection_state",
    "history.event_history",
    "history.event_metrics",
    "history.market_metrics",
)


def validate_checkpoints(connection):
    """Complete certification requires current, model-private execution progress."""
    expected_schema = [
        {"name": name, "type": "VARCHAR", "nullable": False}
        for name in ("record_kind", "batch_id", "processing_version")
    ]
    for model in CHECKPOINT_MODELS:
        schema, name = model.split(".")
        checkpoint = f"{schema}.__checkpoint_{name}"
        if relation_schema(connection, checkpoint) != expected_schema:
            raise ValueError("catalogue checkpoint schema is incompatible; run --full-refresh")
        kind = connection.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            [schema, "__checkpoint_" + name],
        ).fetchone()
        if kind != ("BASE TABLE",):
            raise ValueError("catalogue checkpoint is not a private state table")
        rows = connection.execute(
            f"SELECT record_kind,batch_id,processing_version FROM {checkpoint}"
        ).fetchall()
        metadata = [(kind, batch, version) for kind, batch, version in rows if kind == "metadata"]
        batches = [batch for kind, batch, _ in rows if kind == "batch"]
        if (
            metadata != [("metadata", "", "1")]
            or any(
                kind not in {"metadata", "batch"}
                or version != "1"
                or (kind == "batch" and not batch)
                for kind, batch, version in rows
            )
            or len(rows) != len(set(rows))
        ):
            raise ValueError("catalogue checkpoint records are invalid; run --full-refresh")
        registered = connection.execute(
            "SELECT batch_id FROM bronze.batch_registry WHERE status='loaded'"
        ).fetchall()
        if set(batches) != {row[0] for row in registered}:
            raise ValueError(
                "catalogue checkpoint does not cover the complete registered batch set"
            )


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
def bounded_connection(
    settings: Settings,
    path: Path | None = None,
    *,
    read_only=True,
    connection: duckdb.DuckDBPyConnection | None = None,
):
    """Use the operator's memory, spill and retained limits for every semantic query."""
    usage = measure_storage(settings)
    enforce_storage_limits(settings, usage=usage)
    if not settings.temporary_dir.is_dir():
        settings.temporary_dir.mkdir(parents=True, exist_ok=True)
        usage = measure_storage(settings)
        enforce_storage_limits(settings, usage=usage)
    config = {
        "memory_limit": settings.load.duckdb_memory_limit,
        "threads": settings.load.duckdb_threads,
        "temp_directory": str(settings.temporary_dir),
        "max_temp_directory_size": f"{remaining_temp_bytes(settings, usage=usage)}B",
    }
    borrowed = connection is not None
    connection = (
        connection.cursor()
        if borrowed
        else duckdb.connect(
            str(path or settings.warehouse_path), read_only=read_only, config=config
        )
    )
    try:
        if borrowed:
            current_temp_directory = connection.execute(
                "SELECT current_setting('temp_directory')"
            ).fetchone()[0]
            if not current_temp_directory or os.path.abspath(
                current_temp_directory
            ) != os.path.abspath(settings.temporary_dir):
                raise ValueError("borrowed DuckDB connection has an incompatible spill directory")
            del config["temp_directory"]
            for key, value in config.items():
                connection.execute(f"SET {key} = ?", [value])
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


VIEW_ALIASES = {
    "history.int_event_semantic": "staging.stg_gamma__event_observations",
    "history.int_market_semantic": "staging.stg_gamma__market_observations",
    "history.market_history": "history.int_market_semantic",
}


def _verified_alias(connection, relation, schemas, proven):
    if relation in proven:
        return proven[relation]
    source = VIEW_ALIASES.get(relation)
    if source is None:
        return None
    proven[relation] = None
    if schemas[relation] != schemas[source]:
        return None
    if source in VIEW_ALIASES and _verified_alias(connection, source, schemas, proven) is None:
        return None
    schema, name = relation.split(".")
    actual = connection.execute(
        "SELECT sql FROM duckdb_views() WHERE NOT temporary AND schema_name=? AND view_name=?",
        [schema, name],
    ).fetchone()
    if actual is None:
        return None

    def quote(name):
        return '"' + name.replace('"', '""') + '"'

    database = connection.execute("SELECT current_database()").fetchone()[0]
    source_parts = source.split(".")
    for parts in (source_parts, [database, *source_parts]):
        qualified = ".".join(quote(part) for part in parts)
        connection.execute(
            f"CREATE OR REPLACE TEMP VIEW _catalogue_expected_alias AS SELECT * FROM {qualified}"
        )
        try:
            expected = connection.execute(
                "SELECT sql FROM duckdb_views() WHERE temporary AND view_name='_catalogue_expected_alias'"
            ).fetchone()[0]
            if actual[0].split(" AS ", 1)[1] == expected.split(" AS ", 1)[1]:
                proven[relation] = source
                return source
        finally:
            connection.execute("DROP VIEW _catalogue_expected_alias")
    return None


@phase_timing("certification.warehouse")
def warehouse_snapshot(settings: Settings):
    with bounded_connection(settings) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            ensure_observation_views(connection, settings.load.dataset_name)
            validate_checkpoints(connection)
            schemas = {
                relation: relation_schema(connection, relation)
                for relation in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)
            }
            relations, proven = {}, {}
            for relation in SEMANTIC_RELATIONS:
                source = _verified_alias(connection, relation, schemas, proven)
                relations[relation] = (
                    deepcopy(relations[source])
                    if source is not None
                    else semantic_fingerprint(connection, semantic_query(connection, relation))
                )
            return {"schemas": schemas, "relations": relations}
        finally:
            connection.execute("ROLLBACK")


@phase_timing("certification.public")
def published_snapshot(settings: Settings):
    with bounded_connection(settings) as connection:
        return {
            name: semantic_fingerprint(connection, query)
            for name, query in projection_queries(ordered=False).items()
        }
