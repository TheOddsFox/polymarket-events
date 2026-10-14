"""Project one Gamma response into the bounded current catalogue contract."""

from datetime import datetime, timedelta
from typing import Any

from oddsfox_catalogue.current_contract import SCHEMAS as SCHEMAS
from oddsfox_catalogue.current_contract import CurrentError, ResponseRows, columns
from oddsfox_catalogue.ids import observation_id
from oddsfox_catalogue.normalization import ID_RE, Observation, project


def entity_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, str | int):
        raise CurrentError("entity ID must be a positive canonical decimal")
    value = str(value)
    if not ID_RE.fullmatch(value):
        raise CurrentError("entity ID must be a positive canonical decimal")
    return value


def _fields(raw: dict, strings: tuple[str, ...]) -> None:
    for name, expected in [(name, str) for name in strings] + [
        (name, bool) for name in ("active", "closed", "archived")
    ]:
        if raw.get(name) is not None and not isinstance(raw[name], expected):
            raise CurrentError(f"{name} has an invalid retained field type")


def _incomplete(raw: dict, count: int) -> bool:
    for key, value in raw.items():
        name = key.replace("_", "").lower()
        if name in {
            "truncated",
            "hasmore",
            "hasnext",
            "hasnextpage",
            "marketstruncated",
            "marketshasmore",
        } and value not in (False, None):
            return True
        if name in {"nextcursor", "next", "marketsnextcursor", "marketscursor"} and value:
            return True
        if name in {
            "total",
            "totalcount",
            "marketcount",
            "marketscount",
            "totalmarkets",
            "marketstotal",
        } and (isinstance(value, bool) or not isinstance(value, int) or value != count):
            return True
        if name in {"pagination", "pageinfo", "marketspagination"} and value:
            return True
    return False


def project_response(
    payload: Any,
    record_key: str,
    *,
    received_at: str,
    response_id: str,
    native_market_ids=(),
) -> ResponseRows:
    """Reject conflicting entity snapshots; only complete event arrays own links."""
    try:
        receipt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise CurrentError("invalid UTC receipt timestamp") from exc
    if receipt.utcoffset() != timedelta(0):
        raise CurrentError("invalid UTC receipt timestamp")
    if not isinstance(response_id, str) or not response_id:
        raise CurrentError("response_id must be nonempty")
    if record_key not in {"events", "markets"}:
        raise CurrentError("unknown record key")
    if isinstance(payload, list):
        records, prefix = payload, ""
    elif isinstance(payload, dict) and "id" in payload:
        records, prefix = [payload], None
    elif isinstance(payload, dict) and isinstance(payload.get(record_key), list):
        if ({"events", "markets"} - {record_key}) & payload.keys():
            raise CurrentError("mismatched response envelope")
        records, prefix = payload[record_key], f"/{record_key}"
    else:
        raise CurrentError("unrecognised response envelope")
    result = ResponseRows(received_at=received_at, response_id=response_id)
    result.native_market_ids.update(entity_id(value) for value in native_market_ids)
    entities: dict[str, dict[str, dict]] = {"events": {}, "markets": {}}
    identities: dict[str, list[dict]] = {}

    def market(raw: dict, pointer: str, enclosing: str | None = None) -> str:
        mid = entity_id(raw.get("id"))
        _fields(raw, ("question", "slug", "version", "conditionId"))
        # Do not pass financial or unrelated metadata through normalization.
        retained = {
            name: raw.get(name)
            for name in (
                "id",
                "question",
                "slug",
                "version",
                "conditionId",
                "active",
                "closed",
                "archived",
                "outcomes",
                "clobTokenIds",
                "positionIds",
            )
        }
        retained["id"] = mid
        projected, outcomes = project(
            Observation(
                retained,
                {
                    "received_at": received_at,
                    "observation_id": observation_id(response_id, pointer),
                    "source_kind": "market_direct" if enclosing is None else "event_nested",
                },
                enclosing,
            )
        )
        row = {name: projected[name] for name in columns("markets")}
        outcomes = [{name: outcome[name] for name in columns("outcomes")} for outcome in outcomes]
        if mid in entities["markets"] and (
            entities["markets"][mid] != row or identities[mid] != outcomes
        ):
            raise CurrentError(f"conflicting market {mid}")
        entities["markets"][mid], identities[mid] = row, outcomes
        parents = raw.get("events")
        if isinstance(parents, list):
            for parent in parents:
                try:
                    result.parent_event_ids.add(entity_id(parent.get("id")))
                except (CurrentError, AttributeError):
                    continue
        return mid

    for index, raw in enumerate(records):
        if not isinstance(raw, dict):
            raise CurrentError("response contains a nonrecord item")
        pointer = "/" if prefix is None else f"{prefix}/{index}"
        if record_key == "markets":
            market(raw, pointer)
            continue
        eid = entity_id(raw.get("id"))
        _fields(raw, ("title", "slug"))
        row = {
            "venue": "polymarket",
            "event_id": eid,
            **{name: raw.get(name) for name in ("title", "slug", "active", "closed", "archived")},
        }
        children = raw.get("markets")
        membership = [] if isinstance(children, list) else None
        if isinstance(children, list):
            for child_index, child in enumerate(children):
                try:
                    if not isinstance(child, dict):
                        raise CurrentError("invalid child record")
                    entity_id(child.get("id"))
                except CurrentError:
                    membership = None
                    continue
                mid = market(child, f"{pointer.rstrip('/')}/markets/{child_index}", eid)
                if membership is not None:
                    membership.append(mid)
            if _incomplete(raw, len(children)):
                membership = None
        if membership is not None:
            membership = sorted(set(membership), key=int)
        if eid in entities["events"] and (
            entities["events"][eid] != row or result.memberships[eid] != membership
        ):
            raise CurrentError(f"conflicting event {eid}")
        entities["events"][eid] = row
        result.memberships[eid] = membership
    result.events.extend(entities["events"].values())
    result.markets.extend(entities["markets"].values())
    result.outcomes.extend(outcome for rows in identities.values() for outcome in rows)
    result.parent_event_ids.difference_update(
        eid for eid, members in result.memberships.items() if members is not None
    )
    return result
