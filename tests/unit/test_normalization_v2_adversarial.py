"""Source identity and observation rules shared by metadata and the catalogue."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.metadata import export_metadata
from oddsfox_catalogue.normalization import (
    NormalizationError,
    Observation,
    normalize_event,
    normalize_market,
    project,
)


def market(market_id="101", **changes):
    return {
        "id": market_id,
        "version": "v1",
        "conditionId": "0x" + "a" * 64,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["11", "12"],
        **changes,
    }


def observation(raw, *, name="observation", kind="market_direct", received="2026-10-10T00:00:00Z"):
    return Observation(
        raw,
        {"observation_id": name, "source_kind": kind, "received_at": received},
    )


def capture(settings, payload, *, name, received="2026-10-10T00:00:00Z", key="markets"):
    write_page(
        settings.raw_dir / name,
        1,
        json.dumps(payload).encode(),
        {
            "page_id": name,
            "batch_id": name,
            "observed_at": received,
            "record_key": key,
            "endpoint": "/" + key,
            "http_status": 200,
        },
    )


def relation(directory, name):
    return json.loads((directory / f"{name}.json").read_text())


@pytest.mark.parametrize(
    ("version", "kind", "selected"),
    [("v1", "ctf_token", ["11", "12"]), ("v2", "poly_v2_position", ["21", "22"])],
)
def test_explicit_version_selects_native_identity_when_both_arrays_are_populated(
    version, kind, selected
):
    projected, outcomes = project(observation(market(version=version, positionIds=["21", "22"])))
    assert projected["usable"] is True
    assert [row["asset_kind"] for row in outcomes] == [kind, kind]
    assert [row["asset_id"] for row in outcomes] == selected
    assert [row["clob_token_id"] for row in outcomes] == ["11", "12"]
    assert [row["position_id"] for row in outcomes] == ["21", "22"]
    assert [row["outcome_index"] for row in outcomes] == [1, 2]
    assert all(row["chain_index_set"] is None for row in outcomes)


@pytest.mark.parametrize("version", [None, "", "v3", "unknown"])
def test_unknown_or_missing_version_cannot_infer_ctf_identity_from_token_array(version):
    raw = market(version=version)
    if version is None:
        raw.pop("version")
    projected, outcomes = project(observation(raw))
    assert projected["usable"] is False
    assert projected["identity_error"]
    assert outcomes == []


@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize("ids", [None, [], ["11"], ["11", "11"], [True, "12"], ["01", "12"]])
def test_selected_native_ids_must_be_complete_unique_canonical_values(version, ids):
    raw = market(version=version, positionIds=["21", "22"])
    raw["clobTokenIds" if version == "v1" else "positionIds"] = ids
    projected, outcomes = project(observation(raw))
    assert projected["usable"] is False and projected["identity_error"]
    assert outcomes == []


@pytest.mark.parametrize("prices", [None, "not JSON", [], ["0.1"], {"Yes": "0.4"}])
def test_optional_invalid_price_arrays_do_not_change_identity_or_labels(prices):
    projected, outcomes = project(observation(market(outcomePrices=prices)))
    assert projected["usable"] is True
    assert [(row["outcome_index"], row["outcome_label"], row["asset_id"]) for row in outcomes] == [
        (1, "Yes", "11"),
        (2, "No", "12"),
    ]
    assert all("outcome_price" not in row for row in outcomes)


def test_market_financial_values_preserve_more_than_float_precision():
    precise = Decimal("123456789.123456789123456789123456789")
    tick = Decimal("0.000000000000000000000000000001")
    raw = market(
        volume=precise,
        liquidity=Decimal("987654321.123456789123456789123456789"),
        orderPriceMinTickSize=tick,
        orderMinSize=Decimal("0.00000000000000000003"),
    )
    normalized = normalize_market(raw, observation(raw).provenance)
    for key, expected in (
        ("volume", precise),
        ("liquidity", raw["liquidity"]),
        ("tick_size", tick),
        ("minimum_order_size", raw["orderMinSize"]),
    ):
        assert isinstance(normalized[key], str)
        assert Decimal(normalized[key]) == expected


def test_event_nullable_flags_and_financial_measurements_are_not_coerced():
    normalized = normalize_event(
        {
            "id": "1",
            "active": None,
            "closed": False,
            "archived": "false",
            "volume": Decimal("123456789.123456789123456789123456789"),
            "liquidity": None,
            "openInterest": Decimal("0"),
        }
    )
    assert normalized["active"] is None
    assert normalized["closed"] is False
    assert normalized["archived"] is None
    assert normalized["liquidity"] is None
    assert normalized["open_interest"] == "0"
    assert normalized["volume"] == "123456789.123456789123456789123456789"


@pytest.mark.parametrize("number", [True, "not a number", Decimal("NaN"), Decimal("Infinity"), -1])
def test_invalid_financial_measurements_are_explicit_errors(number):
    with pytest.raises(NormalizationError):
        normalize_event({"id": "1", "volume": number})
    raw = market(volume=number)
    with pytest.raises(NormalizationError):
        normalize_market(raw, observation(raw).provenance)


@pytest.mark.parametrize("relationship", [None, {}, {"id": "../1"}, {"id": True}, {"id": "01"}])
def test_invalid_relationship_keys_cannot_enter_aggregations(relationship):
    with pytest.raises(NormalizationError):
        normalize_event({"id": "1", "tags": [relationship]})


def test_conflicting_relationship_records_cannot_pick_an_arbitrary_label():
    with pytest.raises(NormalizationError):
        normalize_event(
            {"id": "1", "tags": [{"id": "2", "label": "First"}, {"id": "2", "label": "Second"}]}
        )


def test_newer_explicit_nulls_replace_entire_older_observation(tmp_path):
    settings = Settings(tmp_path)
    capture(
        settings,
        [market(active=True, closed=False, enableOrderBook=True, question="Old label")],
        name="older",
        received="2026-10-01T00:00:00Z",
    )
    capture(
        settings,
        [market(active=None, closed=None, enableOrderBook=None, question=None)],
        name="newer",
        received="2026-10-02T00:00:00Z",
    )
    output = tmp_path / "bundle"
    export_metadata(settings, ["101"], output)
    current = relation(output, "markets")[0]
    assert current["active"] is None
    assert current["closed"] is None
    assert current["enable_order_book"] is None
    assert current["question"] is None


def test_older_direct_observation_is_authoritative_over_newer_embedded_record(tmp_path):
    settings = Settings(tmp_path)
    capture(
        settings,
        [market(question="Authoritative", active=None)],
        name="direct",
        received="2026-10-01T00:00:00Z",
    )
    capture(
        settings,
        [{"id": "1", "markets": [market(question="Embedded", active=True)]}],
        name="embedded",
        received="2026-10-09T00:00:00Z",
        key="events",
    )
    output = tmp_path / "bundle"
    export_metadata(settings, ["101"], output)
    current = relation(output, "markets")[0]
    assert current["question"] == "Authoritative"
    assert current["active"] is None
    assert current["source_kind"] == "market_direct"


def test_receipt_ties_choose_observation_id_independent_of_file_order(tmp_path):
    settings = Settings(tmp_path)
    for name, label in [("z-page", "First"), ("a-page", "Second")]:
        capture(settings, [market(question=label)], name=name)
    output = tmp_path / "bundle"
    export_metadata(settings, ["101"], output)
    history = relation(output, "identity_history")
    winning_id = max(row["observation_id"] for row in history)
    assert relation(output, "markets")[0]["observation_id"] == winning_id


def test_multi_outcome_position_and_ctf_equal_ids_use_different_asset_keys(tmp_path):
    settings = Settings(tmp_path)
    capture(
        settings,
        [
            market(),
            market(
                "102",
                version="v2",
                positionIds=["11", "12", "13"],
                clobTokenIds=None,
                outcomes=["Red", "Green", "Blue"],
            ),
        ],
        name="two-kinds",
    )
    output = tmp_path / "bundle"
    export_metadata(settings, ["101", "102"], output)
    outcomes = relation(output, "outcomes")
    assert all(row["usable"] for row in relation(output, "markets"))
    keys = {(row["venue"], row["asset_kind"], row["asset_id"]) for row in outcomes}
    assert len(keys) == len(outcomes) == 5
    assert [row["outcome_index"] for row in outcomes if row["market_id"] == "102"] == [1, 2, 3]


def test_changed_labels_and_native_ids_remain_observations_without_validity_intervals(tmp_path):
    settings = Settings(tmp_path)
    capture(settings, [market()], name="old", received="2026-10-01T00:00:00Z")
    capture(
        settings,
        [market(outcomes=["Up", "Down"], clobTokenIds=["31", "32"])],
        name="new",
        received="2026-10-02T00:00:00Z",
    )
    output = tmp_path / "bundle"
    export_metadata(settings, ["101"], output)
    history = relation(output, "identity_history")
    assert len(history) == 2
    assert [row["asset_id"] for row in history[0]["identities"]] == ["11", "12"]
    assert [row["asset_id"] for row in history[1]["identities"]] == ["31", "32"]
    assert [(row["outcome_label"], row["asset_id"]) for row in relation(output, "outcomes")] == [
        ("Up", "31"),
        ("Down", "32"),
    ]
    assert all("valid_from" not in row and "valid_to" not in row for row in history)


def test_conflicting_ownership_quarantines_every_owner_without_nominating_assets(tmp_path):
    settings = Settings(tmp_path)
    capture(settings, [market(), market("102")], name="conflict")
    output = tmp_path / "bundle"
    export_metadata(settings, ["101", "102"], output)
    owners = relation(output, "markets")
    assert len(owners) == 2
    assert all(row["usable"] is False and row["identity_error"] for row in owners)
    assert relation(output, "outcomes") == []


def test_explicit_empty_membership_never_refills_enclosing_relationship(tmp_path):
    settings = Settings(tmp_path)
    capture(settings, [market(events=[])], name="direct")
    capture(
        settings,
        [{"id": "1", "markets": [market()]}],
        name="embedded",
        key="events",
    )
    output = tmp_path / "bundle"
    export_metadata(settings, ["101"], output)
    assert relation(output, "memberships") == []


def test_duplicate_membership_refs_do_not_multiply_relations(tmp_path):
    settings = Settings(tmp_path)
    capture(settings, [market(events=[{"id": "1"}, {"id": "1"}])], name="direct")
    output = tmp_path / "bundle"
    export_metadata(settings, ["101"], output)
    memberships = relation(output, "memberships")
    assert len(memberships) == 1
    assert memberships[0]["event_id"] == "1"
