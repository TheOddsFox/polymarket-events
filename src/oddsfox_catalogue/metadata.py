"""Targeted, immutable metadata handoffs, independent of the six catalogue exports."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.writer import (
    atomic_write_bytes,
    read_body,
    read_regular_bytes,
    write_marker,
    write_page,
)
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import GammaClient, GammaError
from oddsfox_catalogue.ids import canonical_json, iso_utc, observation_id, sha256_bytes, utc_now
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

CONTRACT = "oddsfox.polymarket.metadata.v1"
RELATIONS = ("markets", "outcomes", "memberships", "identity_history", "coverage")
MAX_MARKETS = 100
MAX_BODY_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_BYTES = 128 * 1024 * 1024
ID_RE = re.compile(r"[1-9][0-9]{0,19}\Z")
CONDITION_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")


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


def _verified_payload(
    path: Path, *, trusted_root: Path | None = None
) -> tuple[dict[str, Any], Any]:
    """Read only regular local files with a literal sibling payload name."""
    if path.is_symlink() or not path.is_file():
        raise MetadataError("raw manifest is not a regular file")
    try:
        manifest = _read_json(
            read_regular_bytes(path, max_bytes=1024**2, trusted_root=trusted_root)
        )
    except (OSError, ValueError) as exc:
        raise MetadataError("invalid raw manifest file") from exc
    if not isinstance(manifest, dict) or manifest.get("manifest_version") != 1:
        raise MetadataError("unsupported raw manifest")
    if (
        not isinstance(manifest.get("page_id"), str)
        or not isinstance(manifest.get("observed_at"), str)
        or _timestamp(manifest["observed_at"]) is None
        or manifest.get("http_status") not in {200, 404}
        or not isinstance(manifest.get("body_bytes"), int)
    ):
        raise MetadataError("invalid raw manifest fields")
    try:
        body = read_body(
            path.parent,
            manifest,
            trusted_root=trusted_root or path.parent,
            max_body_bytes=MAX_BODY_BYTES,
        )
    except (OSError, ValueError, EOFError) as exc:
        raise MetadataError(str(exc)) from exc
    return manifest, _read_json(body) if manifest["http_status"] == 200 else None


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


def _relations(
    market_ids: list[str], observations: list[Observation], coverage: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    relations: dict[str, list[dict[str, Any]]] = {name: [] for name in RELATIONS}
    allowed = {row["market_id"] for row in coverage if row["status"] == "found"}
    by_id = {market_id: [] for market_id in market_ids}
    for obs in observations:
        by_id[str(obs.market["id"])].append(obs)
        market, outcomes = project(obs)
        relations["identity_history"].append(
            {
                "market_id": market["market_id"],
                "condition_id": market["condition_id"],
                "source_market_version": market["source_market_version"],
                "raw_clob_token_ids": obs.market.get("clobTokenIds"),
                "raw_position_ids": obs.market.get("positionIds"),
                "raw_outcomes": obs.market.get("outcomes"),
                "enclosing_event_id": obs.enclosing_event_id,
                "usable": market["usable"],
                "identity_error": market["identity_error"],
                "identities": outcomes,
                **obs.provenance,
            }
        )
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
            market["usable"], market["identity_error"] = (
                False,
                "native identity has conflicting owners",
            )
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
    if output.exists() or output.is_symlink():
        raise MetadataError("output must be a new immutable directory")
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
        os.rename(stage, output)
        fd = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return manifest
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def export_metadata(
    settings: Settings, market_ids: list[str], output: Path, *, max_output_bytes=MAX_OUTPUT_BYTES
) -> dict[str, Any]:
    """Offline export from immutable raw evidence; absence is never inferred from silence."""
    market_ids = validate_market_ids(market_ids)
    requested = set(market_ids)
    observations: list[Observation] = []
    coverage_evidence: dict[str, tuple[int, Any, str, str, str | None]] = {}
    request_markers: dict[Path, dict[str, Any]] = {}
    for path in sorted((settings.data_dir / "metadata" / "raw").glob("**/_request.json")):
        if path.is_symlink() or not path.is_file():
            raise MetadataError("request marker is not a regular file")
        try:
            record = _read_json(
                read_regular_bytes(
                    path, max_bytes=1024**2, trusted_root=settings.data_dir / "metadata" / "raw"
                )
            )
        except (OSError, ValueError) as exc:
            raise MetadataError("invalid request marker file") from exc
        if not isinstance(record, dict) or record.get("status") not in {
            "found",
            "absent",
            "failed",
        }:
            raise MetadataError("invalid metadata request marker")
        request_markers[path.parent] = record
        market_id = record.get("market_id")
        if market_id in requested:
            evidence = (
                1,
                parse_timestamp(record["received_at"]),
                record["capture_id"],
                record["status"],
                record.get("error"),
            )
            current = coverage_evidence.get(market_id)
            if current is None or evidence[:3] > current[:3]:
                coverage_evidence[market_id] = evidence
    for directory in (settings.raw_dir, settings.data_dir / "metadata" / "raw"):
        for path in sorted(directory.glob("**/p*.manifest.json")):
            if directory != settings.raw_dir:
                record = request_markers.get(path.parent)
                if record is None or record["status"] == "failed":
                    continue
            manifest, payload = _verified_payload(path, trusted_root=directory)
            record_key = manifest.get("record_key")
            if record_key is None:
                record_key = (
                    "markets" if manifest.get("endpoint", "").startswith("/markets") else "events"
                )
            if payload is not None:
                page_observations = _observations(manifest, payload, record_key, requested)
                observations.extend(page_observations)
                for obs in page_observations:
                    market_id = str(obs.market["id"])
                    evidence = (*obs.rank, "found", None)
                    current = coverage_evidence.get(market_id)
                    if current is None or evidence[:3] > current[:3]:
                        coverage_evidence[market_id] = evidence
    found = {str(obs.market["id"]) for obs in observations}
    coverage = []
    for market_id in market_ids:
        evidence = coverage_evidence.get(market_id)
        status = evidence[3] if evidence else "found" if market_id in found else "failed"
        error = evidence[4] if evidence else "no_observation" if status == "failed" else None
        coverage.append(
            {"market_id": market_id, "requested": True, "status": status, "error": error}
        )
    received = max(
        (obs.provenance["received_at"] for obs in observations), default=iso_utc(utc_now())
    )
    return _write_bundle(
        output,
        _relations(market_ids, observations, coverage),
        received,
        max_output_bytes=max_output_bytes,
    )


def _validate_endpoint(settings: Settings) -> None:
    try:
        url = urlsplit(settings.gamma.base_url)
        port = url.port
    except ValueError as exc:
        raise MetadataError("invalid targeted Gamma endpoint") from exc
    production = (
        url.scheme == "https" and url.hostname == "gamma-api.polymarket.com" and port in {None, 443}
    )
    local_test = (
        settings.gamma.allow_loopback
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


def refresh_metadata(
    settings: Settings,
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
    if output.exists() or output.is_symlink():
        raise MetadataError("output must be a new immutable directory")
    if not 0 < max_body_bytes <= MAX_BODY_BYTES or not 0 < max_output_bytes <= MAX_OUTPUT_BYTES:
        raise MetadataError("metadata byte limits must be positive and within contract bounds")
    if any(
        isinstance(limit, bool) or not isinstance(limit, int) or limit < 0
        for limit in (max_requests, max_download_bytes)
    ):
        raise MetadataError("metadata request/download limits must be nonnegative integers")
    _validate_endpoint(settings)
    owned_client = client is None
    client = client or GammaClient(
        settings.gamma,
        max_body_bytes=max_body_bytes,
        max_requests=max_requests,
        max_download_bytes=max_download_bytes,
        trust_env=False,
    )
    initial_attempts = client.stats.requests
    initial_bytes = client.stats.downloaded_bytes
    capture_id = uuid.uuid4().hex
    directory = settings.data_dir / "metadata" / "raw" / capture_id
    observations: list[Observation] = []
    coverage: list[dict[str, Any]] = []
    try:
        with Ledger(settings.ledger_path) as ledger:
            for market_id in market_ids:
                received = iso_utc(client.now())
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
                    received = iso_utc(response.received_at)
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
                    record["manifest_path"] = str(page.manifest_path.relative_to(settings.data_dir))
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
                        observations.extend(
                            _observations(manifest, payload, "markets", {market_id})
                        )
                        record["status"] = "found"
                    else:
                        raise MetadataError("unexpected single market HTTP status")
                except (GammaError, ValueError, UnicodeError) as exc:
                    record["error"] = type(exc).__name__
                write_marker(directory / market_id / "_request.json", record)
                ledger.record_metadata_request(record)
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
        manifest = _write_bundle(
            output,
            _relations(market_ids, observations, coverage),
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
