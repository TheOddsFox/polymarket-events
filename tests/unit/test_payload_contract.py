"""Shape assumptions about Gamma payloads, checked against fixtures.

Synthetic fixtures are generated from ``tests/fakes/world.py``. When live
captures exist under ``tests/fixtures/gamma/live/``, the same checks run on
them too, so a real-world shape change fails here before it reaches dbt.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gamma"
SYNTHETIC = sorted((FIXTURES / "synthetic").glob("*.json"))
LIVE = sorted((FIXTURES / "live").glob("*.json"))
LIVE = [p for p in LIVE if p.name != "manifest.json"]

EVENT_REQUIRED = {"id", "title", "slug", "closed", "archived", "updatedAt", "markets"}
MARKET_REQUIRED = {
    "id",
    "conditionId",
    "outcomes",
    "outcomePrices",
    "clobTokenIds",
    "positionIds",
    "version",
    "closed",
    "updatedAt",
    "events",
}


def _events_of(body: object) -> list[dict]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and "events" in body:
        return body["events"]
    if isinstance(body, dict) and "id" in body:
        return [body]
    return []


STUB_KEYS = {"id", "ticker", "slug", "title"}


def _all_markets(body: object) -> list[dict]:
    """Markets from a page: direct ``markets`` lists plus markets embedded in events."""
    out: list[dict] = []
    if isinstance(body, dict) and "markets" in body:
        out.extend(body["markets"])
    for event in _events_of(body):
        out.extend(event.get("markets", []))
    return out


def _load(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


ALL_FIXTURES = SYNTHETIC + LIVE


@pytest.mark.parametrize("path", ALL_FIXTURES, ids=lambda p: p.parent.name + "/" + p.name)
def test_event_objects_have_required_fields(path: Path) -> None:
    for event in _events_of(_load(path)):
        if set(event) <= STUB_KEYS:
            continue  # identity-only reference, not a replacement event record
        missing = EVENT_REQUIRED - set(event)
        assert not missing, f"{path.name}: event {event.get('id')} missing {sorted(missing)}"
        assert isinstance(event["id"], str), "event ids are strings in the schema"


@pytest.mark.parametrize("path", ALL_FIXTURES, ids=lambda p: p.parent.name + "/" + p.name)
def test_market_objects_have_required_fields_and_encoded_arrays(path: Path) -> None:
    body = _load(path)
    for market in _all_markets(body):
        missing = MARKET_REQUIRED - set(market)
        assert not missing, f"{path.name}: market {market.get('id')} missing {sorted(missing)}"

        # outcomes and outcomePrices are JSON-encoded strings, index-aligned.
        labels = json.loads(market["outcomes"])
        prices = json.loads(market["outcomePrices"])
        assert isinstance(labels, list) and isinstance(prices, list)
        assert len(labels) == len(prices), f"market {market['id']} outcome/price length mismatch"

        assert market["version"] in {"v1", "v2"}
        if market["version"] == "v2":
            assert isinstance(market["positionIds"], list)
            assert len(market["positionIds"]) == len(labels)
        else:
            assert market["clobTokenIds"] is not None
            assert len(json.loads(market["clobTokenIds"])) == len(labels)

        # Nested event references are identity-only stubs.
        for stub in market["events"]:
            assert isinstance(stub["id"], str)
            assert "markets" not in stub, "market.events entries must be stubs, not full events"


def test_synthetic_fixtures_exist() -> None:
    assert SYNTHETIC, "run tools/write_synthetic_fixtures.py to generate fixtures"


def test_keyset_cursor_is_only_present_when_more_pages_may_exist() -> None:
    open_p1 = _load(FIXTURES / "synthetic" / "events_keyset_open_p1.json")
    open_p2 = _load(FIXTURES / "synthetic" / "events_keyset_open_p2.json")
    assert "next_cursor" in open_p1
    assert "next_cursor" not in open_p2


def test_archived_endpoint_is_a_bare_list() -> None:
    body = _load(FIXTURES / "synthetic" / "events_archived_offset.json")
    assert isinstance(body, list)
    assert all(event["archived"] for event in body)


def test_stub_reference_fixture_is_identity_only() -> None:
    stub = _load(FIXTURES / "synthetic" / "event_stub_reference.json")
    assert set(stub) <= {"id", "ticker", "slug", "title"}


def test_outcome_identifier_fields_follow_version() -> None:
    """v2 carries positionIds (clobTokenIds null); v1 carries clobTokenIds. Version decides."""
    markets = _all_markets(_load(FIXTURES / "synthetic" / "markets_keyset_open.json"))
    assert {m["version"] for m in markets} == {"v1", "v2"}, "fixture should cover both versions"
    for market in markets:
        if market["version"] == "v2":
            assert market["clobTokenIds"] is None
        else:
            assert market["positionIds"] is None
