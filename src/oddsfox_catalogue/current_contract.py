"""The deliberately small current catalogue contract; metadata.v1 is independent."""

from dataclasses import dataclass, field
from typing import Any

CONTRACT = "oddsfox.polymarket.catalogue.v3"
STORE_CONTRACT = "oddsfox.polymarket.catalogue_store.v4"
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


def native_integrity_queries(events: str, markets: str, outcomes: str) -> list[str]:
    """Shared domain checks for stored rows and independently read Parquets."""
    maximum = str(2**256 - 1)

    def uint256(value):
        return (
            f"({value} IS NOT NULL AND regexp_full_match({value}, '[1-9][0-9]{{0,77}}') "
            f"AND (length({value})<78 OR {value}<='{maximum}'))"
        )

    return [
        f"SELECT 1 FROM {events} WHERE venue IS NULL OR venue='' OR event_id IS NULL "
        "OR NOT regexp_full_match(event_id, '[1-9][0-9]{0,19}')",
        f"SELECT 1 FROM {markets} WHERE venue IS NULL OR venue='' OR market_id IS NULL "
        "OR NOT regexp_full_match(market_id, '[1-9][0-9]{0,19}') OR usable IS NULL "
        "OR (condition_id IS NOT NULL AND NOT regexp_full_match(condition_id, '0x[0-9a-f]{64}')) "
        "OR (usable AND (identity_error IS NOT NULL OR NOT coalesce("
        "(lower(source_market_version)='v1' AND protocol='ctf' AND condition_id IS NOT NULL) "
        "OR (lower(source_market_version)='v2' AND protocol='polymarket_v2'), false))) "
        "OR (NOT usable AND (identity_error IS NULL OR identity_error=''))",
        f"SELECT 1 FROM {outcomes} o JOIN {markets} m USING(venue,market_id) "
        "WHERE o.condition_id IS DISTINCT FROM m.condition_id OR o.outcome_index IS NULL "
        "OR o.outcome_index<1 OR o.outcome_label IS NULL OR o.outcome_label='' "
        f"OR NOT {uint256('o.asset_id')} "
        f"OR (o.clob_token_id IS NOT NULL AND NOT {uint256('o.clob_token_id')}) "
        f"OR (o.position_id IS NOT NULL AND NOT {uint256('o.position_id')}) "
        "OR NOT coalesce((m.protocol='ctf' AND o.asset_kind='ctf_token' "
        "AND o.asset_id=o.clob_token_id) OR (m.protocol='polymarket_v2' "
        "AND o.asset_kind='poly_v2_position' AND o.asset_id=o.position_id), false)",
        f"SELECT 1 FROM {outcomes} GROUP BY venue,market_id HAVING min(outcome_index)<>1 "
        "OR max(outcome_index)<>count(*) OR count(DISTINCT asset_id)<>count(*) "
        "OR count(clob_token_id) NOT IN (0,count(*)) "
        "OR count(DISTINCT clob_token_id)<>count(clob_token_id) "
        "OR count(position_id) NOT IN (0,count(*)) "
        "OR count(DISTINCT position_id)<>count(position_id)",
        f"SELECT 1 FROM {markets} m WHERE m.usable AND NOT EXISTS "
        f"(SELECT 1 FROM {outcomes} o WHERE o.venue=m.venue AND o.market_id=m.market_id)",
    ]
