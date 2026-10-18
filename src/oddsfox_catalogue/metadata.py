"""Targeted, immutable metadata.v1 handoffs: fresh Gamma reads plus catalogue identity state."""

from __future__ import annotations

import json
import re
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from oddsfox_catalogue.capture.writer import atomic_write_bytes, write_marker, write_page
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, safe_path
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.gamma.http import GammaClient, GammaError
from oddsfox_catalogue.ids import (
    canonical_json,
    ensure_utc,
    iso_utc,
    observation_id,
    sha256_bytes,
)
from oddsfox_catalogue.normalization import (
    NormalizationError as MetadataError,
)
from oddsfox_catalogue.normalization import (
    Observation,
    _read_json,
    _timestamp,
    memberships,
    parse_timestamp,
    project,
    validate_market_ids,
)
from oddsfox_catalogue.platform_support import (
    is_link_or_reparse,
    pinned_path,
    remove_tree,
    replace_directory,
    require_durability,
    sync_directory,
)
from oddsfox_catalogue.runlock import run_lock

CONTRACT = "oddsfox.polymarket.metadata.v1"
RELATIONS = ("markets", "outcomes", "memberships", "identity_history", "coverage")
MAX_MARKETS = 100
MAX_BODY_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_BYTES = 128 * 1024 * 1024
ID_RE = re.compile(r"[1-9][0-9]{0,19}\Z")
CONDITION_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")
CONFLICT_ERROR = "native identity has conflicting owners"


def _existing_ancestor(path: Path) -> Path:
    while not path.exists():
        if path.parent == path:
            raise MetadataError("metadata path has no existing filesystem ancestor")
        path = path.parent
    return path


def _json_bytes(value: Any) -> bytes:
    def exact(item: Any) -> Any:
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, dict):
            return {key: exact(val) for key, val in item.items()}
        if isinstance(item, list):
            return [exact(val) for val in item]
        return item

    return (canonical_json(exact(value), max_bytes=MAX_OUTPUT_BYTES) + "\n").encode()


def _records(payload: Any, key: str) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "id" in payload:
            return [payload]
        if isinstance(payload.get(key), list):
            return payload[key]
    raise MetadataError("raw page has an unsupported envelope")


def _observations(
    manifest: dict[str, Any], payload: Any, record_key: str, requested: set[str]
) -> list[Observation]:
    records = _records(payload, record_key)
    found = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        nested = [(record, f"/markets/{index}", None)]
        if record_key == "events":
            markets = record.get("markets")
            if not isinstance(markets, list):
                continue
            event_id = str(record["id"]) if record.get("id") is not None else None
            nested = [(m, f"/events/{index}/markets/{n}", event_id) for n, m in enumerate(markets)]
        for market, pointer, event_id in nested:
            if not isinstance(market, dict) or str(market.get("id")) not in requested:
                continue
            page_id = manifest["page_id"]
            found.append(
                Observation(
                    market,
                    {
                        "observation_id": observation_id(page_id, pointer),
                        "page_id": page_id,
                        "capture_id": manifest.get("capture_id", manifest.get("batch_id")),
                        "received_at": manifest["observed_at"],
                        "source_kind": "market_direct"
                        if record_key == "markets"
                        else "event_embedded",
                        "json_pointer": pointer,
                        "payload_sha256": manifest["body_sha256"],
                    },
                    event_id,
                )
            )
    return found


def _identity_history(conn: Any, market_ids: list[str]) -> list[dict[str, Any]]:
    """Identity rows from the catalogue's own identity tables, never from fetched observations."""
    rows = []
    for market_id in market_ids:
        identity = conn.execute(
            "SELECT candidate,accepted,invalidated_at,invalidated_response FROM _identity "
            "WHERE venue='polymarket' AND market_id=?",
            [market_id],
        ).fetchone()
        receipt = conn.execute(
            "SELECT receipt,response FROM _receipts "
            "WHERE kind='market' AND venue='polymarket' AND id=?",
            [market_id],
        ).fetchone()
        market = conn.execute(
            "SELECT condition_id,source_market_version,usable,identity_error FROM markets "
            "WHERE venue='polymarket' AND market_id=?",
            [market_id],
        ).fetchone()
        if identity is None or receipt is None or market is None:
            continue
        candidate_json, accepted_json, invalidated_at, invalidated_response = identity
        candidate = json.loads(candidate_json) if candidate_json else None
        accepted = json.loads(accepted_json) if accepted_json else None
        identities = candidate or accepted or []
        conflict = any(
            {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT market_id FROM _identity_claims "
                    "WHERE venue='polymarket' AND asset_kind=? AND asset_id=?",
                    [outcome.get("asset_kind"), outcome.get("asset_id")],
                ).fetchall()
            }
            - {market_id}
            for outcome in identities
        )
        events = [
            row[0]
            for row in conn.execute(
                "SELECT event_id FROM event_markets WHERE venue='polymarket' AND market_id=? "
                "ORDER BY event_id",
                [market_id],
            ).fetchall()
        ]
        condition_id, version, usable, error = market
        received_at, response_id = receipt
        received = _timestamp(ensure_utc(received_at).isoformat())
        state = {
            "candidate": candidate,
            "accepted": accepted,
            "invalidated_at": invalidated_at.isoformat() if invalidated_at else None,
            "invalidated_response": invalidated_response,
        }
        rows.append(
            {
                "market_id": market_id,
                "condition_id": condition_id,
                "source_market_version": version,
                "raw_clob_token_ids": [o.get("clob_token_id") for o in identities],
                "raw_position_ids": [o.get("position_id") for o in identities],
                "raw_outcomes": [o.get("outcome_label") for o in identities],
                "enclosing_event_id": events[0] if len(events) == 1 else None,
                "usable": bool(usable) and not conflict,
                "identity_error": CONFLICT_ERROR if conflict else error,
                "identities": identities,
                "observation_id": observation_id(response_id, "/identity"),
                "page_id": response_id,
                "capture_id": response_id,
                "received_at": received,
                "source_kind": "catalogue_store",
                "json_pointer": "/identity",
                "payload_sha256": sha256_bytes(
                    canonical_json(state, max_bytes=MAX_OUTPUT_BYTES).encode()
                ),
            }
        )
    return rows


def _relations(
    market_ids: list[str],
    observations: list[Observation],
    coverage: list[dict[str, Any]],
    identity: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    relations: dict[str, list[dict[str, Any]]] = {name: [] for name in RELATIONS}
    allowed = {row["market_id"] for row in coverage if row["status"] == "found"}
    by_id = {market_id: [] for market_id in market_ids}
    for obs in observations:
        by_id[str(obs.market["id"])].append(obs)
    for market_id in market_ids:
        candidates = by_id[market_id]
        if not candidates or market_id not in allowed:
            continue
        selected = max(candidates, key=lambda obs: obs.rank)
        market, outcomes = project(selected)
        relations["markets"].append(market)
        relations["outcomes"].extend(outcomes)
        relations["memberships"].extend(memberships(selected, candidates))
    # A native identity may never nominate conflicting markets/conditions in one bundle.
    owners: dict[tuple[str, str], set[tuple[str, str | None]]] = {}
    for row in relations["outcomes"]:
        owners.setdefault((row["asset_kind"], row["asset_id"]), set()).add(
            (row["market_id"], row["condition_id"])
        )
    ambiguous = {key for key, values in owners.items() if len(values) != 1}
    conflicted_markets = {
        row["market_id"]
        for row in relations["outcomes"]
        if (row["asset_kind"], row["asset_id"]) in ambiguous
    }
    relations["outcomes"] = [
        row for row in relations["outcomes"] if row["market_id"] not in conflicted_markets
    ]
    for market in relations["markets"]:
        if market["market_id"] in conflicted_markets:
            market["usable"], market["identity_error"] = (False, CONFLICT_ERROR)
    relations["identity_history"] = [
        {
            **row,
            **(
                {"usable": False, "identity_error": CONFLICT_ERROR}
                if row["market_id"] in conflicted_markets
                else {}
            ),
        }
        for row in identity
    ]
    relations["coverage"] = coverage
    relations["identity_history"].sort(
        key=lambda row: (
            int(row["market_id"]),
            parse_timestamp(row["received_at"]),
            row["observation_id"],
        )
    )
    return relations


def _write_bundle(
    output: Path, relations: dict[str, Any], received_at: str, *, max_output_bytes=MAX_OUTPUT_BYTES
) -> dict[str, Any]:
    if (
        isinstance(max_output_bytes, bool)
        or not isinstance(max_output_bytes, int)
        or not 0 < max_output_bytes <= MAX_OUTPUT_BYTES
    ):
        raise MetadataError("metadata output limit must be positive")
    if is_link_or_reparse(output) or output.exists():
        raise MetadataError("output must be a new immutable directory")
    try:
        output = safe_path(output)
    except CurrentError as exc:
        raise MetadataError("output path is unsafe") from exc
    anchor = _existing_ancestor(output.parent)
    with pinned_path(anchor):
        output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".metadata-", dir=output.parent))
    try:
        files, total = {}, 0
        for name in RELATIONS:
            body = _json_bytes(relations[name])
            total += len(body)
            if total > max_output_bytes:
                raise MetadataError("metadata output exceeds byte limit")
            filename = f"{name}.json"
            atomic_write_bytes(stage / filename, body)
            files[filename] = {"sha256": sha256_bytes(body), "size": len(body)}
        release_id = sha256_bytes(_json_bytes(files))
        manifest = {
            "contract": CONTRACT,
            "source_release_id": release_id,
            "received_at": received_at,
            "coverage_scope": "targeted",
            "files": files,
        }
        manifest_bytes = _json_bytes(manifest)
        if total + len(manifest_bytes) > max_output_bytes:
            raise MetadataError("metadata output exceeds byte limit")
        atomic_write_bytes(stage / "manifest.json", manifest_bytes)
        # No global catalogue pointer is read or changed.
        if is_link_or_reparse(output) or output.exists():
            raise MetadataError("output must be a new immutable directory")
        replace_directory(stage, output)
        sync_directory(output.parent)
        return manifest
    finally:
        if stage.exists():
            remove_tree(stage)
            sync_directory(output.parent)


def _validate_endpoint(gamma: GammaSettings) -> None:
    try:
        url = urlsplit(gamma.base_url)
        port = url.port
    except ValueError as exc:
        raise MetadataError("invalid targeted Gamma endpoint") from exc
    production = (
        url.scheme == "https" and url.hostname == "gamma-api.polymarket.com" and port in {None, 443}
    )
    local_test = (
        gamma.allow_loopback
        and url.scheme == "http"
        and url.hostname in {"127.0.0.1", "localhost", "::1"}
    )
    if (
        not (production or local_test)
        or url.username
        or url.password
        or url.query
        or url.fragment
        or url.path not in {"", "/"}
    ):
        raise MetadataError(
            "targeted metadata uses the fixed Gamma HTTPS host (or local test server)"
        )


def _catalogue_identity(catalogue: CurrentSettings, market_ids: list[str]) -> list[dict[str, Any]]:
    """Read identity under the catalogue writer lock; the catalogue store is never written here."""
    if not catalogue.store_path.exists():
        raise MetadataError("catalogue identity store is not initialized")
    with run_lock(catalogue.run_lock_path), connect_store(catalogue, read_only=True) as conn:
        return _identity_history(conn, market_ids)


def refresh_metadata(
    gamma: GammaSettings,
    catalogue: CurrentSettings | None,
    producer_root: Path,
    market_ids: list[str],
    output: Path,
    *,
    client: GammaClient | None = None,
    max_output_bytes=MAX_OUTPUT_BYTES,
    max_body_bytes=MAX_BODY_BYTES,
    max_requests: int = 500,
    max_download_bytes: int = 128 * 1024**2,
) -> dict[str, Any]:
    """Fetch only named markets and export this capture's evidence, never stale fallbacks."""
    market_ids = validate_market_ids(market_ids)
    if is_link_or_reparse(output) or output.exists():
        raise MetadataError("output must be a new immutable directory")
    try:
        output = safe_path(output)
    except CurrentError as exc:
        raise MetadataError("output path is unsafe") from exc
    try:
        producer_root = safe_path(Path(producer_root))
    except CurrentError as exc:
        raise MetadataError("producer path is unsafe") from exc
    if not 0 < max_body_bytes <= MAX_BODY_BYTES or not 0 < max_output_bytes <= MAX_OUTPUT_BYTES:
        raise MetadataError("metadata byte limits must be positive and within contract bounds")
    if any(
        isinstance(limit, bool) or not isinstance(limit, int) or limit < 0
        for limit in (max_requests, max_download_bytes)
    ):
        raise MetadataError("metadata request/download limits must be nonnegative integers")
    _validate_endpoint(gamma)
    require_durability(_existing_ancestor(producer_root))
    require_durability(_existing_ancestor(output.parent))
    owned_client = client is None
    client = client or GammaClient(
        gamma,
        max_body_bytes=max_body_bytes,
        max_requests=max_requests,
        max_download_bytes=max_download_bytes,
        trust_env=False,
    )
    initial_attempts = client.stats.requests
    initial_bytes = client.stats.downloaded_bytes
    capture_id = uuid.uuid4().hex
    directory = producer_root / "metadata" / "raw" / capture_id
    observations: list[Observation] = []
    coverage: list[dict[str, Any]] = []
    try:
        for market_id in market_ids:
            # Preserve receipt ordering against raw evidence with microsecond timestamps.
            received = _timestamp(ensure_utc(client.now()).isoformat())
            record = {
                "capture_id": capture_id,
                "market_id": market_id,
                "received_at": received,
                "status": "failed",
                "manifest_path": None,
                "error": None,
            }
            try:
                response = client.get(f"/markets/{market_id}", max_retries=4, backoff_cap_s=30)
                received = _timestamp(ensure_utc(response.received_at).isoformat())
                fields = {
                    "page_id": f"metadata.{capture_id}.{market_id}",
                    "capture_id": capture_id,
                    "market_id": market_id,
                    "endpoint": response.endpoint,
                    "record_key": "markets",
                    "observed_at": received,
                    "http_status": response.status,
                    "retries": response.retries,
                    "latency_s": response.latency_s,
                }
                page = write_page(directory / market_id, 1, response.body, fields)
                record["manifest_path"] = str(page.manifest_path.relative_to(producer_root))
                record["received_at"] = received
                if response.status == 404:
                    record["status"] = "absent"
                elif response.status == 200:
                    payload = _read_json(response.body)
                    records = _records(payload, "markets")
                    if (
                        len(records) != 1
                        or not isinstance(records[0], dict)
                        or str(records[0].get("id")) != market_id
                    ):
                        raise MetadataError("single market response has mismatched identity")
                    manifest = {**fields, "body_sha256": page.body_sha256}
                    observations.extend(_observations(manifest, payload, "markets", {market_id}))
                    record["status"] = "found"
                else:
                    raise MetadataError("unexpected single market HTTP status")
            except (GammaError, ValueError, UnicodeError) as exc:
                record["error"] = type(exc).__name__
            write_marker(directory / market_id / "_request.json", record)
            coverage.append(
                {
                    "market_id": market_id,
                    "requested": True,
                    "status": record["status"],
                    "error": record["error"],
                    "capture_id": capture_id,
                    "received_at": record["received_at"],
                }
            )
        identity = _catalogue_identity(catalogue, market_ids) if catalogue is not None else []
        manifest = _write_bundle(
            output,
            _relations(market_ids, observations, coverage, identity),
            iso_utc(client.now()),
            max_output_bytes=max_output_bytes,
        )
        return {
            **manifest,
            "http_attempts": client.stats.requests - initial_attempts,
            "downloaded_bytes": client.stats.downloaded_bytes - initial_bytes,
        }
    finally:
        if owned_client:
            client.close()
