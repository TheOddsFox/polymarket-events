"""dlt resources for the bronze layer.

Every resource shares one contract:

* ``merge`` with ``insert-only``: a row whose key already exists is never
  rewritten, so replaying a chunk is a no-op and history is never overwritten.
* ``schema_contract`` freezes columns and data types. Unknown fields belong
  inside the ``payload`` JSON column, so only a deliberate code change can add
  a column. A changed envelope therefore fails the load instead of drifting.
* ``payload`` is declared as ``json``, which stops dlt from exploding nested
  Gamma objects into child tables.
* Resources must be created immediately before their ``pipeline.run``. Building
  several resources first and running them later corrupts dlt's shared schema.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import dlt
from dlt.extract import materialize_schema_item
from dlt.sources import DltResource

from oddsfox_catalogue.config import LoadSettings
from oddsfox_catalogue.load.runtime import confined_runtime

PIPELINE_NAME = "polymarket_catalogue"
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
INSERT_ONLY = {"disposition": "merge", "strategy": "insert-only"}

EVENT_TABLE = "event_observations"
MARKET_TABLE = "market_observations"
QUARANTINE_TABLE = "quarantined_records"
REGISTRY_TABLE = "batch_registry"

_TEXT_REQUIRED = {"data_type": "text", "nullable": False}
_TIMESTAMP_REQUIRED = {"data_type": "timestamp", "timezone": True, "nullable": False}
_TIMESTAMP_OPTIONAL = {"data_type": "timestamp", "timezone": True, "nullable": True}
_JSON_REQUIRED = {"data_type": "json", "nullable": False}

ENVELOPE_COLUMNS: dict[str, dict[str, Any]] = {
    "observation_id": _TEXT_REQUIRED,
    "venue": _TEXT_REQUIRED,
    "entity_id": _TEXT_REQUIRED,
    "batch_id": _TEXT_REQUIRED,
    "page_id": _TEXT_REQUIRED,
    "observed_at": _TIMESTAMP_REQUIRED,
    "source_updated_at": _TIMESTAMP_OPTIONAL,
    "endpoint": _TEXT_REQUIRED,
    "payload_hash": _TEXT_REQUIRED,
    "payload": _JSON_REQUIRED,
    "normalized": _JSON_REQUIRED,
}

MARKET_EXTRA_COLUMNS: dict[str, dict[str, Any]] = {
    "source_kind": _TEXT_REQUIRED,
    "json_pointer": _TEXT_REQUIRED,
}

QUARANTINE_COLUMNS: dict[str, dict[str, Any]] = {
    "quarantine_id": _TEXT_REQUIRED,
    "batch_id": _TEXT_REQUIRED,
    "page_id": _TEXT_REQUIRED,
    "entity": _TEXT_REQUIRED,
    "json_pointer": _TEXT_REQUIRED,
    "reason": _TEXT_REQUIRED,
    "observed_at": _TIMESTAMP_REQUIRED,
    "payload": _JSON_REQUIRED,
}

REGISTRY_COLUMNS: dict[str, dict[str, Any]] = {
    "batch_id": _TEXT_REQUIRED,
    "mode": _TEXT_REQUIRED,
    "observation_date": _TEXT_REQUIRED,
    "status": _TEXT_REQUIRED,
    "page_count": {"data_type": "bigint", "nullable": False},
    "event_observation_count": {"data_type": "bigint", "nullable": False},
    "market_observation_count": {"data_type": "bigint", "nullable": False},
    "quarantine_count": {"data_type": "bigint", "nullable": False},
    "loaded_at": _TIMESTAMP_REQUIRED,
    "load_id": {"data_type": "text", "nullable": True},
}


def _copy_columns(spec: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Fresh hint dicts for every resource.

    dlt writes ``primary_key`` into column hint dicts in place. The hint dicts above are
    shared constants, so without copying, a key set on one column leaks onto others and
    the MERGE joins on the wrong column.
    """
    return {name: dict(hint) for name, hint in spec.items()}


def event_columns() -> dict[str, dict[str, Any]]:
    return _copy_columns(ENVELOPE_COLUMNS)


def market_columns() -> dict[str, dict[str, Any]]:
    return _copy_columns({**ENVELOPE_COLUMNS, **MARKET_EXTRA_COLUMNS})


def _resource(
    data: Iterable[dict[str, Any]],
    *,
    name: str,
    columns: dict[str, dict[str, Any]],
    primary_key: str,
    contract: dict[str, str] | None = None,
    materialize_only: bool = False,
) -> DltResource:
    return dlt.resource(
        [materialize_schema_item()] if materialize_only else data,
        name=name,
        columns=columns,
        primary_key=None if materialize_only else primary_key,
        write_disposition="append" if materialize_only else INSERT_ONLY,
        schema_contract=contract or SCHEMA_CONTRACT,
    )


def event_resource(rows: Iterable[dict[str, Any]], **kwargs: Any) -> DltResource:
    return _resource(
        rows,
        name=EVENT_TABLE,
        columns=event_columns(),
        primary_key="observation_id",
        **kwargs,
    )


def market_resource(rows: Iterable[dict[str, Any]], **kwargs: Any) -> DltResource:
    return _resource(
        rows,
        name=MARKET_TABLE,
        columns=market_columns(),
        primary_key="observation_id",
        **kwargs,
    )


def quarantine_resource(rows: Iterable[dict[str, Any]], **kwargs: Any) -> DltResource:
    return _resource(
        rows,
        name=QUARANTINE_TABLE,
        columns=_copy_columns(QUARANTINE_COLUMNS),
        primary_key="quarantine_id",
        **kwargs,
    )


def registry_resource(rows: Iterable[dict[str, Any]], **kwargs: Any) -> DltResource:
    return _resource(
        rows,
        name=REGISTRY_TABLE,
        columns=_copy_columns(REGISTRY_COLUMNS),
        primary_key="batch_id",
        **kwargs,
    )


def make_pipeline(
    warehouse: Path,
    pipelines_dir: Path,
    load: LoadSettings,
    *,
    temp_directory: Path | None = None,
    max_temp_bytes: int | None = None,
) -> dlt.Pipeline:
    """The one dlt pipeline that owns bronze. State lives under ``pipelines_dir``."""
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    with confined_runtime(pipelines_dir):
        return dlt.pipeline(
            pipeline_name=PIPELINE_NAME,
            destination=make_destination(
                warehouse, load, temp_directory=temp_directory, max_temp_bytes=max_temp_bytes
            ),
            dataset_name=load.dataset_name,
            pipelines_dir=str(pipelines_dir),
            enable_runtime_trace=False,
        )


def make_destination(
    warehouse: Path,
    load: LoadSettings,
    *,
    temp_directory: Path | None = None,
    max_temp_bytes: int | None = None,
):
    """Fresh connection settings for the current remaining operator allowance."""
    temporary_config = {}
    if temp_directory is not None:
        temp_directory.mkdir(parents=True, exist_ok=True)
        temporary_config["temp_directory"] = str(temp_directory)
    if max_temp_bytes is not None:
        temporary_config["max_temp_directory_size"] = f"{max_temp_bytes}B"
    return dlt.destinations.duckdb(
        credentials={
            "database": str(warehouse),
            "global_config": {
                **temporary_config,
                "memory_limit": load.duckdb_memory_limit,
                "threads": load.duckdb_threads,
            },
        }
    )


def interrupted(rows: list[dict[str, Any]], fault: Any) -> Iterator[dict[str, Any]]:
    """Yield rows, calling ``fault`` once halfway, to simulate a crash mid-load."""
    midpoint = len(rows) // 2
    for index, row in enumerate(rows):
        if index == midpoint:
            fault()
        yield row
