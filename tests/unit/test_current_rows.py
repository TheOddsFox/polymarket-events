import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from oddsfox_catalogue.current_contract import CurrentError, columns
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.normalization import NormalizationError, _read_json


def rows(payload, key="events", **kwargs):
    return project_response(
        payload, key, received_at="2026-10-14T00:00:00Z", response_id="r", **kwargs
    )


def market(version="v1"):
    return {
        "id": "2",
        "question": "Will it happen?",
        "version": version,
        "conditionId": "0x" + "a" * 64,
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["123456789012345678901234567890","8"]',
        "positionIds": '["987654321098765432109876543210","9"]',
    }


def test_minimal_projection_ignores_financial_and_taxonomy():
    raw = market()
    raw.update(volume=object(), orderMinSize=Decimal("1e99999999"), tags=False)
    result = rows([{"id": 1, "title": "Event", "markets": [raw]}])
    assert result.memberships == {"1": ["2"]}
    assert set(result.events[0]) == set(columns("events"))
    assert set(result.markets[0]) == set(columns("markets"))
    assert set(result.outcomes[0]) == set(columns("outcomes"))
    assert result.outcomes[0]["asset_id"] == "123456789012345678901234567890"


def test_v2_native_positions_and_parent_nomination_only():
    raw = market("v2")
    raw["events"] = [{"id": "7", "markets": [market()]}]
    result = rows(raw, "markets", native_market_ids=["2"])
    assert result.events == [] and result.memberships == {}
    assert result.parent_event_ids == {"7"}
    assert result.native_market_ids == {"2"}
    assert result.outcomes[0]["asset_id"] == "987654321098765432109876543210"


@pytest.mark.parametrize("children", [None, False, {}, "[]"])
def test_incomplete_membership(children):
    assert rows([{"id": "1", "markets": children}]).memberships == {"1": None}


def test_empty_absent_and_bad_child_are_distinct():
    assert rows([{"id": "1", "markets": []}]).memberships == {"1": []}
    assert rows([{"id": "1"}]).memberships == {"1": None}
    result = rows([{"id": "1", "markets": [{"id": True}, market()]}])
    assert result.memberships == {"1": None}
    assert len(result.markets) == 1


@pytest.mark.parametrize(
    "indicator", [{"hasMore": True}, {"totalMarkets": 2}, {"pagination": {"next": 2}}]
)
def test_explicit_incomplete_indicators(indicator):
    assert rows([{"id": "1", "markets": [market()], **indicator}]).memberships == {"1": None}


def test_equivalent_duplicates_deduplicate_and_conflicts_reject():
    raw = market()
    result = rows([{"id": "1", "markets": [raw, deepcopy(raw)]}])
    assert len(result.markets) == 1 and len(result.outcomes) == 2
    conflict = {**raw, "question": "Different"}
    with pytest.raises(CurrentError, match="conflicting market"):
        rows([raw, conflict], "markets")
    with pytest.raises(CurrentError, match="conflicting event"):
        rows([{"id": "1", "title": "A"}, {"id": "1", "title": None}])


@pytest.mark.parametrize("bad", [True, 1.0, "01", "0", "١", "1" * 21, None])
def test_invalid_entity_ids(bad):
    with pytest.raises(CurrentError):
        rows([{"id": bad}])


@pytest.mark.parametrize("payload", [[None], {"markets": []}, {"events": [], "markets": []}])
def test_bad_envelopes(payload):
    with pytest.raises(CurrentError):
        rows(payload)


def test_null_and_unusable_identity_preserved():
    raw = market()
    raw.update(question=None, positionIds=[False])
    result = rows(raw, "markets")
    assert result.markets[0]["question"] is None
    assert not result.markets[0]["usable"] and result.outcomes == []
    assert result.markets[0]["market_id"] == "2"


def test_wrong_retained_type_and_non_utc_receipt_rejected():
    with pytest.raises(CurrentError):
        rows([{"id": "1", "active": "true"}])
    with pytest.raises(CurrentError):
        project_response([], "events", received_at="2026-10-14T00:00:00", response_id="r")


def test_sanitized_fixture_preserves_native_ids():
    payload = json.loads(
        (Path(__file__).parents[1] / "fixtures/gamma/synthetic/event_by_id_101.json").read_text()
    )
    result = rows(payload)
    assert result.memberships == {"101": ["5001", "5002"]}
    assert all(row["usable"] for row in result.markets)
    assert result.outcomes[0]["asset_id"] == payload["markets"][0]["positionIds"][0]
    assert result.outcomes[2]["asset_id"] == json.loads(payload["markets"][1]["clobTokenIds"])[0]


@pytest.mark.parametrize("field", ["outcomes", "clobTokenIds"])
def test_deeply_nested_market_json_is_quarantined_without_losing_siblings(field):
    bad_id, good_id, event_id = "9007199254740993", "9007199254740994", "9007199254740995"
    malformed = market()
    malformed["id"] = bad_id
    malformed[field] = "[" * 10_000 + '"Yes"' + "]" * 10_000
    valid = market()
    valid["id"] = good_id

    result = rows(
        [{"id": event_id, "markets": [malformed, valid]}],
        native_market_ids=[bad_id, good_id],
    )

    markets = {row["market_id"]: row for row in result.markets}
    assert result.memberships == {event_id: [bad_id, good_id]}
    assert result.native_market_ids == {bad_id, good_id}
    assert not markets[bad_id]["usable"]
    assert markets[bad_id]["identity_error"] == f"{field} is not a JSON array"
    assert markets[good_id]["usable"]
    assert [row["asset_id"] for row in result.outcomes] == [
        "123456789012345678901234567890",
        "8",
    ]


def test_deeply_nested_top_level_json_uses_normalization_error():
    malformed = ("[" * 10_000 + '"Yes"' + "]" * 10_000).encode()
    with pytest.raises(NormalizationError, match="invalid JSON evidence"):
        _read_json(malformed)
