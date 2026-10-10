"""Pure Gamma normalization shared by handoffs and warehouse ingestion."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from oddsfox_catalogue.contract import NORMALIZATION_REVISION as NORMALIZATION_REVISION
from oddsfox_catalogue.ids import (
    MAX_CANONICAL_BYTES,
    canonical_json,
    sha256_text,
    validate_exact_size,
)

MAX_MARKETS = 100
ID_RE = re.compile(r"[1-9][0-9]{0,19}\Z")
CONDITION_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")
MAX_DECIMAL_CHARACTERS = 4096


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def decimal_string(value: Decimal) -> str:
    if not value.is_finite():
        raise NormalizationError("non-finite decimal")
    if value.is_zero():
        return "0"
    _, digits, exponent = value.as_tuple()
    # A tiny exponent payload must not expand into an unbounded allocation.
    if max(len(digits) + max(exponent, 0), 2 - exponent) > MAX_DECIMAL_CHARACTERS:
        raise NormalizationError("decimal expansion exceeds the normalization limit")
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def exact_json(value: Any, *, max_bytes: int = MAX_CANONICAL_BYTES) -> Any:
    validate_exact_size(value, max_bytes=max_bytes)
    return _exact_json(value)


def _exact_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise NormalizationError("non-finite decimal")
        return decimal_string(value)
    if isinstance(value, dict):
        return {key: _exact_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_exact_json(item) for item in value]
    return value


class NormalizationError(ValueError):
    """The requested handoff cannot be produced safely."""


@dataclass(frozen=True)
class Observation:
    market: dict[str, Any]
    provenance: dict[str, Any]
    enclosing_event_id: str | None = None

    @property
    def rank(self) -> tuple[int, datetime, str]:
        received = parse_timestamp(self.provenance["received_at"])
        if received is None:
            raise NormalizationError("invalid observation receipt timestamp")
        return (
            int(self.provenance["source_kind"] == "market_direct"),
            received,
            self.provenance["observation_id"],
        )


def validate_market_ids(values: list[str]) -> list[str]:
    if not values or len(values) > MAX_MARKETS:
        raise NormalizationError(f"select between 1 and {MAX_MARKETS} explicit market IDs")
    if any(not isinstance(value, str) or not ID_RE.fullmatch(value) for value in values):
        raise NormalizationError("market IDs must be positive canonical decimal integers")
    return sorted(set(values), key=int)


def _read_json(data: bytes) -> Any:
    try:
        return json.loads(data, parse_float=Decimal, parse_constant=lambda _: _invalid_json())
    except (ValueError, UnicodeError) as exc:
        raise NormalizationError("invalid JSON evidence") from exc


def _invalid_json() -> None:
    raise NormalizationError("non-finite JSON number")


def _array(value: Any, name: str) -> list[Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = _read_json(value.encode())
        except (ValueError, UnicodeError) as exc:
            raise NormalizationError(f"{name} is not a JSON array") from exc
    if not isinstance(value, list):
        raise NormalizationError(f"{name} is not an array")
    return value


def _asset_ids(value: Any, name: str, count: int) -> list[str] | None:
    items = _array(value, name)
    if items is None:
        return None
    if len(items) != count:
        raise NormalizationError(f"{name} does not align with outcomes")
    result: list[str] = []
    for item in items:
        if isinstance(item, bool) or not isinstance(item, str | int):
            raise NormalizationError(f"{name} contains an invalid native ID")
        text = str(item)
        if not text.isascii() or not text.isdigit() or str(int(text)) != text:
            raise NormalizationError(f"{name} contains a noncanonical native ID")
        if not 0 < int(text) < 2**256:
            raise NormalizationError(f"{name} contains an out-of-range native ID")
        result.append(text)
    if len(set(result)) != len(result):
        raise NormalizationError(f"{name} contains duplicate native IDs")
    return result


def _decimal(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    return decimal_string(parsed) if parsed.is_finite() and parsed > 0 else None


def _boolean(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _timestamp(value: Any) -> str | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    return parsed.isoformat(
        timespec="microseconds" if parsed.microsecond % 1000 else "milliseconds"
    ).replace("+00:00", "Z")


def project(observation: Observation) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Validate identities without prices. An unusable row never nominates an asset."""
    raw, provenance = observation.market, observation.provenance
    market_id = str(raw["id"])
    source_version = raw.get("version") if isinstance(raw.get("version"), str) else None
    version = source_version.lower() if source_version is not None else None
    condition = raw.get("conditionId")
    condition = (
        condition.lower()
        if isinstance(condition, str) and CONDITION_RE.fullmatch(condition)
        else None
    )
    market = {
        "venue": "polymarket",
        "market_id": market_id,
        "condition_id": condition,
        "question": raw.get("question") if isinstance(raw.get("question"), str) else None,
        "slug": raw.get("slug") if isinstance(raw.get("slug"), str) else None,
        "description": raw.get("description") if isinstance(raw.get("description"), str) else None,
        "source_market_version": source_version,
        "protocol": None,
        "active": _boolean(raw.get("active")),
        "closed": _boolean(raw.get("closed")),
        "archived": _boolean(raw.get("archived")),
        "resolved": _boolean(raw.get("resolved")),
        "enable_order_book": _boolean(raw.get("enableOrderBook")),
        "accepting_orders": _boolean(raw.get("acceptingOrders")),
        "neg_risk": _boolean(raw.get("negRisk")),
        "tick_size": _decimal(raw.get("orderPriceMinTickSize")),
        "minimum_order_size": _decimal(raw.get("orderMinSize")),
        "created_at": _timestamp(raw.get("createdAt")),
        "start_at": _timestamp(raw.get("startDate")),
        "end_at": _timestamp(raw.get("endDate")),
        "close_at": _timestamp(raw.get("closedTime")),
        "resolved_at": _timestamp(raw.get("resolvedAt")),
        "source_updated_at": _timestamp(raw.get("updatedAt")),
        "usable": False,
        "identity_error": None,
        **provenance,
    }
    try:
        labels = _array(raw.get("outcomes"), "outcomes")
        if not labels or any(not isinstance(item, str) or not item for item in labels):
            raise NormalizationError("outcomes must contain nonempty labels")
        tokens = _asset_ids(raw.get("clobTokenIds"), "clobTokenIds", len(labels))
        positions = _asset_ids(raw.get("positionIds"), "positionIds", len(labels))
        if version == "v2":
            if not positions:
                raise NormalizationError("v2 market lacks positionIds")
            kind, asset_ids, protocol = "poly_v2_position", positions, "polymarket_v2"
        elif version == "v1":
            if not tokens:
                raise NormalizationError("CTF market lacks clobTokenIds")
            if condition is None:
                raise NormalizationError("CTF market lacks a valid conditionId")
            kind, asset_ids, protocol = "ctf_token", tokens, "ctf"
        else:
            raise NormalizationError(
                "missing source market version"
                if version is None
                else "unsupported source market version"
            )
        market["usable"], market["protocol"] = True, protocol
        outcomes = [
            {
                "venue": "polymarket",
                "market_id": market_id,
                "condition_id": condition,
                "outcome_index": index + 1,
                "outcome_label": label,
                "asset_kind": kind,
                "asset_id": asset_ids[index],
                "clob_token_id": tokens[index] if tokens else None,
                "position_id": positions[index] if positions else None,
                "chain_index_set": None,
                "usable": True,
                "identity_error": None,
                **provenance,
            }
            for index, label in enumerate(labels)
        ]
        return market, outcomes
    except NormalizationError as exc:
        market["identity_error"] = str(exc)
        return market, []


def memberships(selected: Observation, observations: list[Observation]) -> list[dict[str, Any]]:
    refs = selected.market.get("events")
    if isinstance(refs, list):
        if any(
            not isinstance(ref, dict) or not ID_RE.fullmatch(str(ref.get("id", ""))) for ref in refs
        ):
            raise NormalizationError("events contains an invalid relationship ID")
        evidence = {
            str(ref["id"]): selected.provenance
            for ref in refs
            if isinstance(ref, dict) and ID_RE.fullmatch(str(ref.get("id", "")))
        }
    elif "events" in selected.market:
        evidence = {}
    else:
        evidence = {
            obs.enclosing_event_id: obs.provenance
            for obs in sorted(observations, key=lambda obs: obs.rank)
            if obs.enclosing_event_id and ID_RE.fullmatch(obs.enclosing_event_id)
        }
    return [
        {
            "venue": "polymarket",
            "market_id": str(selected.market["id"]),
            "event_id": event_id,
            **provenance,
        }
        for event_id, provenance in sorted(evidence.items())
    ]


def _measurement(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise NormalizationError("financial measurement is not a decimal")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise NormalizationError("financial measurement is not a decimal") from exc
    if not number.is_finite() or number < 0:
        raise NormalizationError("financial measurement is not a finite nonnegative decimal")
    return decimal_string(number)


def _relationships(raw: dict[str, Any], key: str, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    values = raw.get(key)
    if values is None:
        return []
    if not isinstance(values, list):
        raise NormalizationError(f"{key} is not an array")
    by_id: dict[str, dict[str, Any]] = {}
    for value in values:
        if not isinstance(value, dict) or not ID_RE.fullmatch(str(value.get("id", ""))):
            raise NormalizationError(f"{key} contains an invalid relationship ID")
        row = {
            "id": str(value["id"]),
            **{
                field: value.get(field) if isinstance(value.get(field), str) else None
                for field in fields
            },
        }
        if row["id"] in by_id and by_id[row["id"]] != row:
            raise NormalizationError(f"{key} contains conflicting relationship records")
        by_id[row["id"]] = row
    return [by_id[key] for key in sorted(by_id, key=int)]


def normalize_event(raw: dict[str, Any]) -> dict[str, Any]:
    result = {
        name: raw.get(name) if isinstance(raw.get(name), str) else None
        for name in ("title", "slug", "ticker", "description")
    }
    result.update(
        {
            name: _boolean(raw.get(source))
            for name, source in (
                ("active", "active"),
                ("closed", "closed"),
                ("archived", "archived"),
                ("neg_risk", "negRisk"),
            )
        }
    )
    result.update(
        {
            name: _timestamp(raw.get(source))
            for name, source in (
                ("created_at", "createdAt"),
                ("start_date", "startDate"),
                ("end_date", "endDate"),
                ("source_updated_at", "updatedAt"),
            )
        }
    )
    result.update(
        {
            name: _measurement(raw.get(source))
            for name, source in (
                ("volume", "volume"),
                ("liquidity", "liquidity"),
                ("open_interest", "openInterest"),
            )
        }
    )
    result.update(
        tags_present=isinstance(raw.get("tags"), list),
        series_present=isinstance(raw.get("series"), list),
        tags=_relationships(raw, "tags", ("label", "slug")),
        series=_relationships(raw, "series", ("title", "slug")),
    )
    result["semantic_hash"] = sha256_text(
        canonical_json(
            {
                key: value
                for key, value in result.items()
                if key not in {"volume", "liquidity", "open_interest", "source_updated_at"}
            }
        )
    )
    return result


def normalize_market(
    raw: dict[str, Any], provenance: dict[str, Any], enclosing_event_id: str | None = None
) -> dict[str, Any]:
    observed = Observation(raw, provenance, enclosing_event_id)
    market, outcomes = project(observed)
    # Keep metadata fields byte-for-byte equivalent; extra warehouse fields are separate.
    market.update(
        outcomes=outcomes,
        raw_outcomes=exact_json(raw.get("outcomes")),
        raw_clob_token_ids=exact_json(raw.get("clobTokenIds")),
        raw_position_ids=exact_json(raw.get("positionIds")),
        volume=_measurement(raw.get("volume")),
        liquidity=_measurement(raw.get("liquidity")),
        tags_present=isinstance(raw.get("tags"), list),
        tags=_relationships(raw, "tags", ("label", "slug")),
        membership_mode="explicit"
        if isinstance(raw.get("events"), list)
        else "invalid"
        if "events" in raw
        else "missing",
        memberships=memberships(observed, [observed]),
        enclosing_event_id=enclosing_event_id,
    )
    excluded = set(provenance) | {"volume", "liquidity", "source_updated_at", "semantic_hash"}
    semantic = {key: value for key, value in market.items() if key not in excluded}
    semantic["outcomes"] = [
        {key: value for key, value in row.items() if key not in set(provenance)} for row in outcomes
    ]
    semantic["memberships"] = [
        {key: value for key, value in row.items() if key not in set(provenance)}
        for row in market["memberships"]
    ]
    market["semantic_hash"] = sha256_text(canonical_json(semantic))
    return market
