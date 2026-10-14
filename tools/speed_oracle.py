"""Content comparison only; release provenance and raw recovery are separate gates."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from oddsfox_catalogue.capture.writer import read_regular_bytes
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import projection_schemas
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.ids import canonical_json, parse_batch_id
from oddsfox_catalogue.publish import verify_release
from oddsfox_catalogue.semantics import bounded_connection, json_descriptor

EXCLUDED_COLUMNS = {
    "events": {"last_observed_at"},
    "markets": {"observation_id", "first_observed_at", "last_observed_at"},
    "outcomes": {"observation_id"},
    "market_event_bridge": {"membership_observation_id", "membership_received_at"},
    "quarantine": {"observation_id", "page_id", "received_at"},
    "event_tags": set(),
    "event_series": set(),
}
SCOPE_FIELDS = (
    "kind",
    "high_water",
    "baseline",
    "market_ids",
    "event_ids",
    "parent_event_ids",
    "sealed",
    "revision",
)


def content_snapshot(settings: Settings, batch_id: str, release_path: Path) -> dict:
    """Read verified public content and new-batch evidence without certifying recovery."""
    releases = settings.published_dir / "releases"
    release_path = Path(release_path)
    if not release_path.is_absolute():
        release_path = releases / release_path
    manifest = verify_release(settings, release_path)
    if batch_id not in manifest["batch_ids"]:
        raise ValueError("requested batch is absent from the verified release")
    observed_at, _ = parse_batch_id(batch_id)
    marker = settings.raw_dir / observed_at.date().isoformat() / batch_id / "_batch.json"
    batch = json.loads(
        read_regular_bytes(marker, max_bytes=32 * 1024**2, trusted_root=settings.raw_dir)
    )
    if (
        not isinstance(batch, dict)
        or batch.get("batch_id") != batch_id
        or not isinstance(batch.get("scope"), dict)
    ):
        raise ValueError("capture marker does not identify the requested batch scope")
    scope = {key: batch["scope"][key] for key in SCOPE_FIELDS if key in batch["scope"]}
    schemas = projection_schemas()
    content = {}
    with bounded_connection(settings, read_only=True) as connection:
        for name, schema in schemas.items():
            columns = ", ".join(
                '"' + column["name"] + '"'
                for column in schema
                if column["name"] not in EXCLUDED_COLUMNS[name]
            )
            content[name] = semantic_fingerprint(
                connection,
                f"SELECT {columns} FROM read_parquet(?)",
                [str(release_path / f"{name}.parquet")],
            )
        evidence = {
            "events": semantic_fingerprint(
                connection,
                "SELECT entity_id, payload_hash, 'event_direct'::VARCHAR AS source_kind, "
                "NULL::VARCHAR AS enclosing_event_id FROM bronze.event_observations WHERE batch_id = ?",
                [batch_id],
            ),
            "markets": semantic_fingerprint(
                connection,
                "SELECT entity_id, payload_hash, source_kind, "
                "json_extract_string(normalized, '$.enclosing_event_id') AS enclosing_event_id "
                "FROM bronze.market_observations WHERE batch_id = ?",
                [batch_id],
            ),
            "quarantine": _quarantine_fingerprint(connection, batch_id),
        }
    return {
        "public_schemas": schemas,
        "public_content": content,
        "source_evidence": evidence,
        "capture_scope": json_descriptor(scope),
    }


def _quarantine_fingerprint(connection, batch_id):
    # Stream canonical payloads through bounded SQL storage; Python UDFs require NumPy.
    connection.execute(
        "CREATE TEMP TABLE _speed_quarantine (entity VARCHAR, reason VARCHAR, payload VARCHAR)"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT entity, reason, cast(payload as varchar) FROM bronze.quarantined_records WHERE batch_id = ?",
            [batch_id],
        )
        while rows := cursor.fetchmany(512):
            connection.executemany(
                "INSERT INTO _speed_quarantine VALUES (?, ?, ?)",
                [
                    (entity, reason, canonical_json(json.loads(payload, parse_float=Decimal)))
                    for entity, reason, payload in rows
                ],
            )
        return semantic_fingerprint(connection, "SELECT * FROM _speed_quarantine")
    finally:
        cursor.close()
        connection.execute("DROP TABLE _speed_quarantine")


def compare_snapshots(baseline: dict, candidate: dict) -> dict:
    """Equality here deliberately makes no claim about the complete semantic gate."""
    differing = sorted(
        key
        for key in baseline.keys() | candidate.keys()
        if key not in baseline or key not in candidate or baseline[key] != candidate[key]
    )
    return {
        "status": "semantic_mismatch" if differing else "content_equal",
        "differing_keys": differing,
    }
