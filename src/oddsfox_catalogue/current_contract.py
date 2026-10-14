"""The deliberately small current catalogue contract; metadata.v1 is independent."""

from dataclasses import dataclass, field
from typing import Any

CONTRACT = "oddsfox.polymarket.catalogue.v3"
STORE_CONTRACT = "oddsfox.polymarket.catalogue_store.v2"
BACKUP_CONTRACT = "oddsfox.polymarket.catalogue_backup.v3"
PROJECTION_VERSION = 1

SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "events": (
        ("venue", "VARCHAR"),
        ("event_id", "VARCHAR"),
        ("title", "VARCHAR"),
        ("slug", "VARCHAR"),
        ("active", "BOOLEAN"),
        ("closed", "BOOLEAN"),
        ("archived", "BOOLEAN"),
    ),
    "markets": (
        ("venue", "VARCHAR"),
        ("market_id", "VARCHAR"),
        ("question", "VARCHAR"),
        ("slug", "VARCHAR"),
        ("condition_id", "VARCHAR"),
        ("source_market_version", "VARCHAR"),
        ("protocol", "VARCHAR"),
        ("active", "BOOLEAN"),
        ("closed", "BOOLEAN"),
        ("archived", "BOOLEAN"),
        ("usable", "BOOLEAN"),
        ("identity_error", "VARCHAR"),
    ),
    "event_markets": (
        ("venue", "VARCHAR"),
        ("event_id", "VARCHAR"),
        ("market_id", "VARCHAR"),
    ),
    "outcomes": (
        ("venue", "VARCHAR"),
        ("market_id", "VARCHAR"),
        ("condition_id", "VARCHAR"),
        ("outcome_index", "INTEGER"),
        ("outcome_label", "VARCHAR"),
        ("asset_kind", "VARCHAR"),
        ("asset_id", "VARCHAR"),
        ("clob_token_id", "VARCHAR"),
        ("position_id", "VARCHAR"),
    ),
}
KEYS = {
    "events": ("venue", "event_id"),
    "markets": ("venue", "market_id"),
    "event_markets": ("venue", "event_id", "market_id"),
    "outcomes": ("venue", "market_id", "outcome_index"),
}


class CurrentError(ValueError):
    """A rejected response or unsafe current-store operation."""


@dataclass
class ResponseRows:
    """One verified response, applied together with its progress update."""

    received_at: str
    response_id: str
    events: list[dict[str, Any]] = field(default_factory=list)
    markets: list[dict[str, Any]] = field(default_factory=list)
    outcomes: list[dict[str, Any]] = field(default_factory=list)
    # None means unknown/incomplete; [] is an authoritative empty membership.
    memberships: dict[str, list[str] | None] = field(default_factory=dict)
    native_market_ids: set[str] = field(default_factory=set)
    parent_event_ids: set[str] = field(default_factory=set)
    raw_path: str | None = None


def columns(relation: str) -> tuple[str, ...]:
    return tuple(name for name, _ in SCHEMAS[relation])


def projection(relation: str) -> str:
    return ", ".join(f'"{name}"' for name in columns(relation))
