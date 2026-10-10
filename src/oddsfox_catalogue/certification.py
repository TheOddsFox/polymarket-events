"""A durable, fail-closed receipt for one complete warehouse build."""

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import duckdb

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.writer import read_regular_bytes
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import CONTRACT, NORMALIZATION_REVISION
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.inventory import capture_inventory
from oddsfox_catalogue.semantics import (
    OPERATIONAL_RELATIONS,
    SEMANTIC_RELATIONS,
    bounded_connection,
    json_descriptor,
    published_snapshot,
    warehouse_snapshot,
)
from oddsfox_catalogue.warehouse_version import ensure_warehouse_contract

RECEIPT_REVISION = 1
ARTIFACT_LIMIT = 64 * 1024**2


class BuildInvalid(ValueError):
    """The current warehouse lacks a matching, complete certification."""


def read_build_validity(settings: Settings):
    """Inspect status without creating or modifying an operator's ledger."""
    if not settings.ledger_path.exists():
        return None
    if any(path.is_symlink() for path in (settings.ledger_path, *settings.ledger_path.parents)):
        raise BuildInvalid("unsafe build ledger path")
    try:
        connection = sqlite3.connect(settings.ledger_path.as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute("SELECT * FROM build_validity WHERE singleton=1").fetchone()
            return dict(row) if row is not None else None
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise BuildInvalid("build-validity ledger is missing or corrupt") from exc


def mark_dirty(settings: Settings, reason: str):
    """Commit invalidation before a warehouse mutation can begin."""
    with Ledger(settings.ledger_path) as ledger:
        ledger.set_build_validity("dirty", iso_utc(utc_now()), reason=reason)


def prepare_dbt_execution(settings: Settings, target: Path, *, args=()):
    """Derive required nodes from a fresh parse before starting the mutating build."""
    if target.exists() or target.is_symlink():
        raise BuildInvalid("dbt invocation requires a fresh artifact directory")
    try:
        relative = (
            Path(os.path.abspath(target))
            .relative_to(Path(os.path.abspath(settings.state_dir)))
            .as_posix()
        )
    except ValueError as exc:
        raise BuildInvalid("dbt artifacts must remain under the operator state root") from exc
    if any(path.is_symlink() for path in target.parents):
        raise BuildInvalid("dbt artifact path contains a symlink")
    from oddsfox_catalogue.dbt_runner import dbt_vars

    effective_vars, _ = dbt_vars(args)
    mark_dirty(settings, "dbt required-node preflight")
    revision = _model_revision(settings)
    required = _parse_required_nodes(settings, target, effective_vars)
    if _model_revision(settings) != revision:
        raise BuildInvalid("dbt project changed during required-node preflight")
    prepared_at = iso_utc(utc_now())
    payload = json.dumps(
        {
            "target": relative,
            "model_revision": revision,
            "required_nodes": required,
            "effective_vars": effective_vars,
        },
        sort_keys=True,
    )
    if len(payload.encode()) > ARTIFACT_LIMIT:
        raise BuildInvalid("dbt required-node inventory exceeds its size limit")
    with Ledger(settings.ledger_path) as ledger:
        ledger.set_build_validity(
            "dirty",
            prepared_at,
            reason="dbt execution prepared",
            payload_json=payload,
            payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        )


def _parse_required_nodes(settings: Settings, target: Path, effective_vars):
    from oddsfox_catalogue.dbt_runner import run_dbt_at
    from oddsfox_catalogue.limits import enforce_storage_limits, remaining_temp_bytes

    preflight = target.with_name(target.name + "-preflight")
    if preflight.exists() or preflight.is_symlink():
        raise BuildInvalid("required-node preflight requires a fresh directory")
    result = run_dbt_at(
        [
            "parse",
            "--no-partial-parse",
            "--vars",
            json.dumps(effective_vars, sort_keys=True, allow_nan=False),
        ],
        project_dir=settings.dbt_project_dir,
        profiles_dir=settings.dbt_profiles_dir,
        warehouse=settings.warehouse_path,
        work_dir=preflight,
        memory_limit=settings.load.duckdb_memory_limit,
        max_temp_bytes=remaining_temp_bytes(settings),
        threads=settings.load.duckdb_threads,
        temporary_dir=settings.temporary_dir,
    )
    enforce_storage_limits(settings)
    if result.returncode != 0:
        raise BuildInvalid("dbt required-node preflight parse failed; build was not started")
    return _node_inventory(_read_artifact(settings, preflight / "target", "manifest.json"))


def _node_inventory(manifest):
    metadata, nodes, disabled = (
        manifest.get("metadata"),
        manifest.get("nodes"),
        manifest.get("disabled", {}),
    )
    if (
        not isinstance(metadata, dict)
        or metadata.get("dbt_schema_version") != "https://schemas.getdbt.com/dbt/manifest/v12.json"
    ):
        raise BuildInvalid("unsupported dbt manifest schema")
    if not isinstance(nodes, dict) or not isinstance(disabled, dict):
        raise BuildInvalid("malformed dbt execution-artifact nodes")
    for entries in disabled.values():
        if not isinstance(entries, list) or any(not isinstance(node, dict) for node in entries):
            raise BuildInvalid("malformed disabled dbt nodes")
        if any(not isinstance(node.get("resource_type"), str) for node in entries):
            raise BuildInvalid("malformed disabled dbt nodes")
        if any(node.get("resource_type") in {"model", "test"} for node in entries):
            raise BuildInvalid("disabled catalogue models or tests cannot certify a full build")
    required = {}
    for key, node in nodes.items():
        if (
            not isinstance(node, dict)
            or not isinstance(node.get("config", {}), dict)
            or not isinstance(node.get("resource_type"), str)
            or not isinstance(node.get("name"), str)
        ):
            raise BuildInvalid("malformed dbt execution-artifact nodes")
        if node["resource_type"] not in {"model", "test"}:
            continue
        config = node.get("config", {})
        if type(config.get("enabled", True)) is not bool:
            raise BuildInvalid("dbt node enabled flag must be boolean")
        if not config.get("enabled", True):
            raise BuildInvalid("disabled catalogue models or tests cannot certify a full build")
        required[key] = {
            "resource_type": node["resource_type"],
            "name": node["name"],
            "checksum": node.get("checksum"),
            "config": config,
        }
    expected_models = {
        relation.split(".")[1]
        for relation in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)
        if not relation.startswith("bronze.")
    }
    actual_models = {node["name"] for node in required.values() if node["resource_type"] == "model"}
    if actual_models != expected_models or not any(
        node["resource_type"] == "test" for node in required.values()
    ):
        raise BuildInvalid("dbt manifest does not declare the complete catalogue models and tests")
    return required


def _prepared_execution(settings: Settings, target: Path):
    row = read_build_validity(settings)
    if row is None or row["status"] != "dirty":
        raise BuildInvalid("a build must invalidate its prior receipt before execution")
    payload = row["payload_json"]
    if (
        not isinstance(payload, str)
        or len(payload.encode()) > ARTIFACT_LIMIT
        or hashlib.sha256(payload.encode()).hexdigest() != row["payload_sha256"]
    ):
        raise BuildInvalid("dbt execution was not prepared for certification")
    prepared = json.loads(payload)
    if (
        not isinstance(prepared, dict)
        or not isinstance(prepared.get("required_nodes"), dict)
        or not isinstance(prepared.get("effective_vars"), dict)
    ):
        raise BuildInvalid("dbt required-node inventory is missing or malformed")
    if Path(os.path.abspath(target)).relative_to(
        Path(os.path.abspath(settings.state_dir))
    ).as_posix() != prepared.get("target"):
        raise BuildInvalid("dbt artifacts are stale or belong to a different prepared execution")
    return row, prepared


def _read_artifact(settings: Settings, target: Path, name: str):
    try:
        value = json.loads(
            read_regular_bytes(
                target / name, max_bytes=ARTIFACT_LIMIT, trusted_root=settings.state_dir
            )
        )
    except (OSError, ValueError) as exc:
        raise BuildInvalid(f"dbt {name} is missing, unsafe or malformed") from exc
    if not isinstance(value, dict):
        raise BuildInvalid(f"dbt {name} is not an object")
    return value


def _model_revision(settings: Settings):
    files = {}
    total = 0
    for root in (settings.dbt_project_dir,):
        for path in sorted(root.rglob("*")):
            relative = path.relative_to(root)
            if relative.parts[0] in {"target", "logs", "dbt_packages"} or not path.is_file():
                continue
            if path.suffix not in {".sql", ".yml", ".yaml"}:
                continue
            body = read_regular_bytes(path, max_bytes=ARTIFACT_LIMIT, trusted_root=root)
            total += len(body)
            if total > ARTIFACT_LIMIT:
                raise BuildInvalid("dbt project exceeds the source-revision size limit")
            files[relative.as_posix()] = hashlib.sha256(body).hexdigest()
    if "dbt_project.yml" not in files:
        raise BuildInvalid("dbt project source is incomplete")
    return json_descriptor(files)["sha256"]


def capture_binding(settings: Settings):
    """Require every declared capture to be complete, loaded and registered exactly."""
    with bounded_connection(settings) as connection:
        registered = connection.execute(
            "SELECT batch_id, page_count, status FROM bronze.batch_registry ORDER BY batch_id"
        ).fetchall()
    if not registered or len({row[0] for row in registered}) != len(registered):
        raise BuildInvalid("capture registry is empty or contains duplicate batches")
    if any(row[2] != "loaded" for row in registered):
        raise BuildInvalid("capture registry contains incomplete work")
    with Ledger(settings.ledger_path) as ledger:
        batches = ledger.list_batches()
        if {row[0] for row in registered} != {batch["batch_id"] for batch in batches}:
            raise BuildInvalid("unregistered capture work blocks certification")
        if any(batch["status"] != "loaded" for batch in batches):
            raise BuildInvalid("incomplete capture work blocks certification")
        if ledger.pending_load_pages():
            raise BuildInvalid("unloaded capture pages block certification")
    try:
        inventory = capture_inventory(
            settings, [row[0] for row in registered], {row[0]: row[1] for row in registered}
        )
    except (KeyError, ValueError, OSError) as exc:
        raise BuildInvalid("capture evidence cannot be verified") from exc
    return {
        "batch_ids": [row[0] for row in registered],
        "inventory": json_descriptor(inventory["files"]),
        "coverage": json_descriptor(inventory["coverage"]),
    }


def _binding(settings: Settings, effective_vars):
    ensure_warehouse_contract(settings)
    return {
        "contract": CONTRACT,
        "normalization_revision": NORMALIZATION_REVISION,
        "model_revision": _model_revision(settings),
        "quality": asdict(settings.quality),
        "effective_vars": effective_vars,
        "capture": capture_binding(settings),
        "warehouse": warehouse_snapshot(settings),
        "published": published_snapshot(settings),
    }


def _validate_artifacts(settings: Settings, target: Path):
    manifest = _read_artifact(settings, target, "manifest.json")
    results = _read_artifact(settings, target, "run_results.json")
    metadata = manifest.get("metadata", {})
    result_metadata = results.get("metadata", {})
    args = results.get("args", {})
    if not all(isinstance(value, dict) for value in (metadata, result_metadata, args)):
        raise BuildInvalid("malformed dbt execution-artifact metadata or arguments")
    if (
        metadata.get("dbt_schema_version") != "https://schemas.getdbt.com/dbt/manifest/v12.json"
        or result_metadata.get("dbt_schema_version")
        != "https://schemas.getdbt.com/dbt/run-results/v6.json"
    ):
        raise BuildInvalid("unsupported dbt execution-artifact schema")
    invocation = metadata.get("invocation_id")
    if (
        not isinstance(invocation, str)
        or not invocation
        or result_metadata.get("invocation_id") != invocation
    ):
        raise BuildInvalid("dbt execution artifacts belong to different invocations")
    if args.get("which") != "build" or args.get("defer") or args.get("empty"):
        raise BuildInvalid("only a complete non-deferred dbt build can be certified")
    rows = results.get("results")
    if not isinstance(rows, list):
        raise BuildInvalid("malformed dbt execution artifacts")
    required = _node_inventory(manifest)
    _, prepared = _prepared_execution(settings, target)
    if required != prepared["required_nodes"]:
        raise BuildInvalid("dbt build changed or omitted its required model/test inventory")
    seen = set()
    for row in rows:
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("unique_id"), str)
            or row.get("unique_id") not in required
            or row["unique_id"] in seen
        ):
            raise BuildInvalid("dbt build has unexpected or duplicate execution results")
        seen.add(row["unique_id"])
        expected = "success" if required[row["unique_id"]]["resource_type"] == "model" else "pass"
        if row.get("status") != expected:
            raise BuildInvalid("dbt build contains failed, warned or skipped work")
    if seen != set(required):
        raise BuildInvalid("dbt build omitted required models or tests")
    effective_vars = args.get("vars", {})
    if not isinstance(effective_vars, dict):
        raise BuildInvalid("dbt effective vars are malformed")
    if effective_vars != prepared["effective_vars"]:
        raise BuildInvalid("dbt effective vars differ from its required-node preflight")
    return invocation, effective_vars


def certify_build(settings: Settings, target_path: Path):
    """Certify successful artifacts only after their actual warehouse passes all bindings."""
    try:
        row, prepared = _prepared_execution(settings, target_path)
        if _model_revision(settings) != prepared["model_revision"]:
            raise BuildInvalid("dbt project changed during execution")
        metadata = _read_artifact(settings, target_path, "manifest.json").get("metadata")
        if not isinstance(metadata, dict) or not isinstance(
            metadata.get("invocation_started_at"), str
        ):
            raise BuildInvalid("dbt execution start metadata is malformed")
        started = datetime.fromisoformat(metadata["invocation_started_at"].replace("Z", "+00:00"))
        prepared_at = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
        if started.tzinfo is None or started < prepared_at:
            raise BuildInvalid("dbt execution artifacts predate the prepared build")
    except (ValueError, KeyError, TypeError) as exc:
        if isinstance(exc, BuildInvalid):
            raise
        raise BuildInvalid("dbt execution preparation is invalid") from exc
    invocation, effective_vars = _validate_artifacts(settings, target_path)
    # Import lazily: publication itself uses certification and verifies the previous release.
    from oddsfox_catalogue.pipeline import open_event_drop_warning

    warning = open_event_drop_warning(settings)
    receipt = {
        "revision": RECEIPT_REVISION,
        "invocation_id": invocation,
        "certified_at": iso_utc(utc_now()),
        "binding": _binding(settings, effective_vars),
        "warning": warning,
    }
    payload = json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False)
    with Ledger(settings.ledger_path) as ledger:
        ledger.set_build_validity(
            "valid",
            receipt["certified_at"],
            payload_json=payload,
            payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        )
    return receipt


def assert_build_valid(settings: Settings):
    """Detect partial loads/builds, changed quality, raw tampering and external dbt writes."""
    row = read_build_validity(settings)
    if row is None or row["status"] != "valid":
        raise BuildInvalid("warehouse is dirty or uncertified; run a complete dbt build")
    try:
        payload = row["payload_json"]
        if not isinstance(payload, str) or len(payload.encode()) > ARTIFACT_LIMIT:
            raise BuildInvalid("invalid build receipt size")
        if hashlib.sha256(payload.encode()).hexdigest() != row["payload_sha256"]:
            raise BuildInvalid("build receipt checksum mismatch")
        receipt = json.loads(payload)
        if (
            not isinstance(receipt, dict)
            or type(receipt.get("revision")) is not int
            or receipt.get("revision") != RECEIPT_REVISION
        ):
            raise BuildInvalid("unsupported build receipt revision")
        binding = receipt["binding"]
        if not isinstance(binding, dict) or not isinstance(binding.get("effective_vars"), dict):
            raise BuildInvalid("build receipt binding is malformed")
        if _binding(settings, binding["effective_vars"]) != binding:
            raise BuildInvalid(
                "warehouse or its certified inputs have changed; rebuild before publication"
            )
    except (KeyError, TypeError, ValueError, duckdb.Error) as exc:
        if isinstance(exc, BuildInvalid):
            raise
        raise BuildInvalid("build receipt or warehouse is invalid") from exc
    return receipt
