"""Deterministic builders for Gamma-shaped events, markets, series, and tags.

The shapes follow the documented Gamma OpenAPI schema (field names, nesting,
and the JSON-encoded string fields ``outcomes`` / ``outcomePrices`` /
``clobTokenIds``). Values are synthetic. Live captures are recorded with
``tools/record_gamma_fixtures.py`` and should be preferred when available.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

DEFAULT_UPDATED_AT = "2026-10-01T12:00:00.000Z"
DEFAULT_CREATED_AT = "2026-09-01T09:00:00.000Z"


def _hex_condition(seed: int) -> str:
    return "0x" + f"{seed:064x}"


def _token_ids(seed: int, count: int) -> tuple[list[str], list[str]]:
    base = 651150819117105875331414918119047680898421632356043490229292782433651916800
    position = [str(base + seed * 16 + i) for i in range(count)]
    clob = [str(10**70 + seed * 16 + i) for i in range(count)]
    return position, clob


def make_series(series_id: str, title: str, *, closed: bool = False) -> dict[str, Any]:
    return {
        "id": series_id,
        "ticker": title.lower().replace(" ", "-"),
        "slug": title.lower().replace(" ", "-"),
        "title": title,
        "subtitle": None,
        "seriesType": "single",
        "recurrence": "weekly",
        "active": not closed,
        "closed": closed,
        "archived": False,
        "createdAt": DEFAULT_CREATED_AT,
        "updatedAt": DEFAULT_UPDATED_AT,
    }


def make_tag(tag_id: str, label: str) -> dict[str, Any]:
    return {
        "id": tag_id,
        "label": label,
        "slug": label.lower().replace(" ", "-"),
        "forceShow": False,
        "publishedAt": DEFAULT_CREATED_AT,
        "createdAt": DEFAULT_CREATED_AT,
        "updatedAt": DEFAULT_UPDATED_AT,
    }


def make_market(
    market_id: str,
    question: str,
    *,
    event_stub: dict[str, Any] | None = None,
    closed: bool = False,
    archived: bool = False,
    version: str = "v2",
    outcomes: tuple[str, ...] = ("Yes", "No"),
    prices: tuple[float, ...] = (0.4, 0.6),
    updated_at: str = DEFAULT_UPDATED_AT,
    tags: list[dict[str, Any]] | None = None,
    include_tags_key: bool = False,
    seed: int | None = None,
) -> dict[str, Any]:
    """Build a Market object. ``include_tags_key=False`` omits the ``tags`` key entirely."""
    seed = int(market_id) if seed is None else seed
    position_ids, clob_ids = _token_ids(seed, len(outcomes))
    market: dict[str, Any] = {
        "id": str(market_id),
        "question": question,
        "conditionId": _hex_condition(seed),
        "slug": "".join(c if c.isalnum() else "-" for c in question.lower()).strip("-")[:80],
        "description": f"Resolves per the rules of: {question}",
        "outcomes": json.dumps(list(outcomes)),
        "outcomePrices": json.dumps([f"{p:.3f}" for p in prices]),
        "clobTokenIds": None if version == "v2" else json.dumps(clob_ids),
        "positionIds": position_ids if version == "v2" else None,
        "version": version,
        "marketType": "normal",
        "formatType": "decimal",
        "active": not closed,
        "closed": closed,
        "archived": archived,
        "new": False,
        "featured": False,
        "restricted": False,
        "enableOrderBook": True,
        "acceptingOrders": not closed,
        "negRisk": False,
        "createdAt": DEFAULT_CREATED_AT,
        "updatedAt": updated_at,
        "startDate": "2026-09-02T00:00:00Z",
        "endDate": "2026-12-31T23:59:59Z",
        "category": "Politics",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "volume": "1250.5",
        "volumeNum": 1250.5,
        "liquidity": "310.25",
        "liquidityNum": 310.25,
        "volume24hr": 12.0,
        "lastTradePrice": prices[0],
        "bestBid": max(prices[0] - 0.01, 0.0),
        "bestAsk": min(prices[0] + 0.01, 1.0),
        "events": [event_stub] if event_stub is not None else [],
    }
    if closed:
        market["closedTime"] = updated_at
        market["umaResolutionStatus"] = "resolved"
    if include_tags_key:
        market["tags"] = list(tags or [])
    return market


def make_event(
    event_id: str,
    title: str,
    *,
    markets: list[dict[str, Any]] | None = None,
    series: list[dict[str, Any]] | None = None,
    tags: list[dict[str, Any]] | None = None,
    closed: bool = False,
    archived: bool = False,
    updated_at: str = DEFAULT_UPDATED_AT,
    ticker: str | None = None,
    neg_risk: bool = False,
) -> dict[str, Any]:
    slug = "".join(c if c.isalnum() else "-" for c in title.lower()).strip("-")[:80]
    event: dict[str, Any] = {
        "id": str(event_id),
        "ticker": ticker if ticker is not None else slug,
        "slug": slug,
        "title": title,
        "subtitle": None,
        "description": f"Event: {title}",
        "resolutionSource": "https://example.test/source",
        "startDate": "2026-09-02T00:00:00Z",
        "creationDate": DEFAULT_CREATED_AT,
        "endDate": "2026-12-31T23:59:59Z",
        "image": None,
        "icon": None,
        "active": not closed,
        "closed": closed,
        "archived": archived,
        "new": False,
        "featured": False,
        "restricted": False,
        "liquidity": 800.0,
        "volume": 4200.0,
        "openInterest": 0.0,
        "category": "Politics",
        "subcategory": None,
        "createdAt": DEFAULT_CREATED_AT,
        "updatedAt": updated_at,
        "commentsEnabled": True,
        "competitive": 0.5,
        "volume24hr": 40.0,
        "negRisk": neg_risk,
        "negRiskMarketID": None,
        "commentCount": 3,
        "markets": list(markets or []),
        "series": list(series or []),
        "categories": [],
        "collections": [],
        "tags": list(tags or []),
        "eventCreators": [],
        "chats": [],
        "templates": [],
    }
    if closed:
        event["closedTime"] = updated_at
    return event


def event_stub(event: dict[str, Any]) -> dict[str, Any]:
    """Reference-sized copy of an event, as nested under a market's ``events`` array."""
    return {
        "id": event["id"],
        "ticker": event.get("ticker"),
        "slug": event.get("slug"),
        "title": event.get("title"),
    }


@dataclass
class World:
    """A mutable snapshot of the Gamma universe used by fakes and fixtures."""

    events: dict[str, dict[str, Any]] = field(default_factory=dict)
    markets: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Markets that the events-nested view omits (direct-only markets).
    direct_only_market_ids: set[str] = field(default_factory=set)

    def add_event(self, event: dict[str, Any]) -> dict[str, Any]:
        stored = copy.deepcopy(event)
        self.events[stored["id"]] = stored
        for market in stored.get("markets", []):
            self.markets[market["id"]] = copy.deepcopy(market)
        return stored

    def add_direct_market(self, market: dict[str, Any]) -> dict[str, Any]:
        stored = copy.deepcopy(market)
        self.markets[stored["id"]] = stored
        self.direct_only_market_ids.add(stored["id"])
        return stored

    def snapshot(self) -> World:
        return copy.deepcopy(self)


def demo_world() -> World:
    """A small, realistic universe covering binary, multi-outcome, v1 and v2 markets."""
    world = World()
    politics = make_series("1001", "Presidential Elections")
    crypto_tag = make_tag("21", "Crypto")
    politics_tag = make_tag("2", "Politics")

    e1 = make_event(
        "101",
        "Who wins the 2028 election?",
        series=[politics],
        tags=[politics_tag],
        neg_risk=True,
    )
    m1 = make_market(
        "5001",
        "Will candidate A win?",
        event_stub=event_stub(e1),
        version="v2",
        tags=[politics_tag],
        include_tags_key=True,
    )
    m2 = make_market(
        "5002",
        "Will candidate B win?",
        event_stub=event_stub(e1),
        version="v1",
        prices=(0.3, 0.7),
        tags=[],
        include_tags_key=True,
    )
    e1["markets"] = [m1, m2]

    e2 = make_event("202", "Bitcoin price on Dec 31?", tags=[crypto_tag])
    m3 = make_market(
        "6001",
        "Will BTC exceed 200k?",
        event_stub=event_stub(e2),
        outcomes=("Yes", "No", "Maybe"),
        prices=(0.2, 0.7, 0.1),
        version="v2",
    )
    e2["markets"] = [m3]

    e3 = make_event("303", "Closed weather bet", closed=True, updated_at="2026-09-20T00:00:00Z")
    m4 = make_market(
        "7001",
        "Will it rain on Sept 20?",
        event_stub=event_stub(e3),
        closed=True,
        prices=(1.0, 0.0),
        updated_at="2026-09-20T00:00:00Z",
    )
    e3["markets"] = [m4]

    for event in (e1, e2, e3):
        world.add_event(event)
    world.add_direct_market(
        make_market(
            "8001",
            "Standalone market without parent in the page",
            event_stub=event_stub(e2),
            version="v2",
        )
    )
    return world
