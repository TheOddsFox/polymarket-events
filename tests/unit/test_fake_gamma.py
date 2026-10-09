"""The fake Gamma server matches the live limits the id-range plan depends on."""

from __future__ import annotations

import httpx

from fakes.fake_gamma import OFFSET_CEILING, FakeGamma
from fakes.world import demo_world


def _client(fake: FakeGamma) -> httpx.Client:
    return httpx.Client(transport=fake.transport(), base_url="https://gamma.fake.test")


def test_id_lists_longer_than_100_are_rejected() -> None:
    with _client(FakeGamma(demo_world())) as client:
        response = client.get("/events/keyset", params={"limit": 250, "id": list(range(1, 252))})
    assert response.status_code == 422


def test_events_offset_ceiling_is_422() -> None:
    with _client(FakeGamma(demo_world())) as client:
        response = client.get("/events", params={"limit": 100, "offset": OFFSET_CEILING})
    assert response.status_code == 422
    assert "offset too large" in response.text


def test_deep_keyset_cursor_is_500_and_an_id_list_is_not() -> None:
    fake = FakeGamma(demo_world())
    fake.deep_cursor_after = 0
    with _client(fake) as client:
        first = client.get("/events/keyset", params={"limit": 1})
        cursor = first.json()["next_cursor"]
        deep = client.get("/events/keyset", params={"limit": 1, "after_cursor": cursor})
        by_id = client.get("/events/keyset", params={"limit": 1, "id": 101})
    assert deep.status_code == 500
    assert by_id.status_code == 200
    assert by_id.json()["events"][0]["id"] == "101"


def test_markets_keyset_defaults_to_open() -> None:
    with _client(FakeGamma(demo_world())) as client:
        response = client.get("/markets/keyset", params={"limit": 100})
    ids = {market["id"] for market in response.json()["markets"]}
    assert "7001" not in ids
    assert "5001" in ids
