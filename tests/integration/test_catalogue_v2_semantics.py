"""The replacement warehouse must preserve source semantics through replay."""

from __future__ import annotations

import copy
import json
import shutil
from datetime import timedelta
from decimal import Decimal

import duckdb
import httpx
import pytest

from fakes.dbt_run import run_dbt
from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime, make_settings
from fakes.world import World, make_event, make_market, make_series, make_tag
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.metadata import export_metadata

EXACT_VOLUME = "123456789.123456789123456789123456789"
EXACT_TICK = "0.000000000000000000000000000001"
pytestmark = pytest.mark.timeout(240)


class NumericGamma(FakeGamma):
    """Return exact JSON numbers rather than rounding through Python floats."""

    def handle(self, request):
        response = super().handle(request)
        body = response.content
        for number in (EXACT_VOLUME, EXACT_TICK):
            body = body.replace(json.dumps(number).encode(), number.encode())
        headers = {key: value for key, value in response.headers.items() if key != "content-length"}
        return httpx.Response(response.status_code, content=body, headers=headers)


def _market(market_id, *, version="v1", tokens=None, positions=None, **changes):
    raw = make_market(market_id, f"Market {market_id}?", version=version)
    raw.update(
        clobTokenIds=tokens,
        positionIds=positions,
        volume=EXACT_VOLUME,
        orderPriceMinTickSize=EXACT_TICK,
        **changes,
    )
    return raw


def _world() -> World:
    world = World()
    tag = make_tag("1", "Synthetic")
    series = make_series("1", "Synthetic series")
    first = [
        _market("101", tokens=["11", "12"], positions=["911", "912"]),
        _market("102", version="v2", tokens=["911", "912"], positions=["11", "12"]),
        _market("103", tokens=["31", "32"], outcomePrices="broken optional prices"),
        _market("104", tokens=["41", "42"], active=None, closed=None, archived=None),
        _market(
            "105",
            version="v2",
            positions=["51", "52", "53"],
            outcomes=json.dumps(["Red", "Green", "Blue"]),
        ),
        _market("106", tokens=["61", "62"], question="Embedded label", active=True),
    ]
    first[3].pop("version")
    for raw in first:
        raw["events"] = [{"id": "1"}, {"id": "1"}]
    world.add_event(
        make_event("1", "Primary event", markets=first, tags=[tag, tag], series=[series, series])
    )
    world.events["1"]["volume"] = EXACT_VOLUME
    direct = copy.deepcopy(world.markets["106"])
    direct.update(question="Direct label", active=None)
    world.markets["106"] = direct

    missing = _market("107", tokens=["71", "72"])
    missing.pop("events")
    inferred = _market("111", tokens=["171", "172"])
    inferred.pop("events")
    world.add_event(make_event("2", "Inferred membership", markets=[missing, inferred]))
    empty = _market("108", tokens=["81", "82"], events=[])
    world.add_event(make_event("3", "Explicit empty membership", markets=[empty]))
    world.events["3"].update(active=None, closed=None, archived=None)
    conflicting = [_market(mid, tokens=["91", "92"]) for mid in ("109", "110")]
    for raw in conflicting:
        raw["events"] = [{"id": "4"}]
    world.add_event(make_event("4", "Conflicting ownership", markets=conflicting))
    return world


def _replace_market(world, market_id, **changes):
    world.markets[market_id].update(changes)
    for event in world.events.values():
        for raw in event.get("markets", []):
            if raw["id"] == market_id:
                raw.update(changes)


def _capture(root, world, day):
    runtime, _ = build_runtime(root, NumericGamma(world), now=FIXED_NOW + timedelta(days=day))
    try:
        summary = run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        return summary.batch_id
    finally:
        runtime.ledger.close()


def _load_and_build(settings, root, batch_id):
    with Ledger(settings.ledger_path) as ledger:
        summary = load_pending(
            LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW), batch_id=batch_id
        )
    assert summary.batches_registered == [batch_id]
    result = run_dbt(["build"], settings.warehouse_path, root)
    assert result.returncode == 0, result.stdout[-5000:] + result.stderr[-5000:]


def _query(settings, sql):
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        return con.execute(sql).fetchall()


@pytest.fixture(scope="module")
def built_pair(tmp_path_factory):
    """Replay exactly the same retained captures in opposite load orders."""
    captured = tmp_path_factory.mktemp("v2-captures")
    world = _world()
    older = _capture(captured, world, 0)
    _replace_market(
        world,
        "101",
        outcomes=json.dumps(["Up", "Down"]),
        clobTokenIds=["21", "22"],
        positionIds=["921", "922"],
        question=None,
        active=None,
        closed=None,
        archived=None,
        enableOrderBook=None,
        orderMinSize=None,
    )
    _replace_market(world, "107", events=[])
    world.events["1"]["title"] = None
    newer = _capture(captured, world, 1)
    built = []
    for name, batches in (("forward", (older, newer)), ("reverse", (newer, older))):
        root = tmp_path_factory.mktemp("v2-" + name)
        shutil.copytree(captured, root, dirs_exist_ok=True)
        settings = make_settings(root)
        for batch_id in batches:
            _load_and_build(settings, root, batch_id)
        built.append(settings)
    return tuple(built)


def test_version_selection_and_native_fields_survive_real_load_and_dbt(built_pair):
    settings, _ = built_pair
    assert _query(
        settings,
        "SELECT market_id, asset_kind, asset_id, clob_token_id, position_id, outcome_index "
        "FROM core.outcomes_current WHERE market_id IN ('101','102') "
        "ORDER BY market_id, outcome_index",
    ) == [
        ("101", "ctf_token", "21", "21", "921", 1),
        ("101", "ctf_token", "22", "22", "922", 2),
        ("102", "poly_v2_position", "11", "911", "11", 1),
        ("102", "poly_v2_position", "12", "912", "12", 2),
    ]
    assert _query(
        settings,
        "SELECT outcome_index, outcome_label, asset_id, chain_index_set "
        "FROM core.outcomes_current WHERE market_id = '105' ORDER BY outcome_index",
    ) == [(1, "Red", "51", None), (2, "Green", "52", None), (3, "Blue", "53", None)]


def test_optional_prices_do_not_quarantine_valid_identity_or_enter_outcome_schema(built_pair):
    settings, _ = built_pair
    assert _query(settings, "SELECT usable FROM core.markets_current WHERE market_id = '103'") == [
        (True,)
    ]
    assert _query(
        settings, "SELECT COUNT(*) FROM core.outcomes_current WHERE market_id = '103'"
    ) == [(2,)]
    columns = {row[0] for row in _query(settings, "DESCRIBE core.outcomes_current")}
    assert "outcome_price" not in columns


def test_missing_version_and_ownership_conflicts_remain_accounted_without_assets(built_pair):
    settings, _ = built_pair
    assert _query(
        settings,
        "SELECT market_id, usable, identity_error IS NOT NULL FROM core.markets_current "
        "WHERE market_id IN ('104','109','110') ORDER BY market_id",
    ) == [("104", False, True), ("109", False, True), ("110", False, True)]
    assert _query(
        settings,
        "SELECT COUNT(*) FROM core.outcomes_current WHERE market_id IN ('104','109','110')",
    ) == [(0,)]
    assert _query(
        settings,
        "SELECT DISTINCT market_id FROM core.quarantine_market_outcomes "
        "WHERE market_id IN ('104','109','110') ORDER BY market_id",
    ) == [("104",), ("109",), ("110",)]


def test_nullable_current_fields_do_not_reuse_old_values(built_pair):
    settings, _ = built_pair
    assert _query(
        settings,
        "SELECT question, active, closed, archived, enable_order_book, minimum_order_size "
        "FROM core.markets_current WHERE market_id = '101'",
    ) == [(None, None, None, None, None, None)]
    assert _query(settings, "SELECT title FROM core.events_current WHERE event_id = '1'") == [
        (None,)
    ]
    assert _query(
        settings,
        "SELECT active, closed, archived, is_open FROM marts.mart_event_catalogue "
        "WHERE event_id = '3'",
    ) == [(None, None, None, None)]


def test_direct_source_priority_and_decimal_precision_match_metadata_handoff(built_pair, tmp_path):
    settings, _ = built_pair
    assert _query(
        settings,
        "SELECT question, active, source_kind FROM core.markets_current WHERE market_id = '106'",
    ) == [("Direct label", None, "market_direct")]
    financial = _query(
        settings, "SELECT volume, tick_size FROM core.markets_current WHERE market_id = '106'"
    )[0]
    assert all(isinstance(value, str) for value in financial)
    assert tuple(Decimal(value) for value in financial) == (
        Decimal(EXACT_VOLUME),
        Decimal(EXACT_TICK),
    )
    output = tmp_path / "metadata"
    export_metadata(settings, ["106"], output)
    handoff = json.loads((output / "markets.json").read_text())[0]
    assert handoff["question"] == "Direct label" and handoff["active"] is None
    assert Decimal(handoff["tick_size"]) == Decimal(financial[1])


def test_observation_history_preserves_identity_changes_without_scd_intervals(built_pair):
    settings, _ = built_pair
    columns = {row[0] for row in _query(settings, "DESCRIBE history.market_history")}
    assert {"valid_from", "valid_to", "is_current"}.isdisjoint(columns)
    assert {"observation_id", "observed_at", "page_id", "source_kind", "payload_hash"} <= columns
    history_count = _query(
        settings, "SELECT COUNT(*) FROM history.market_history WHERE market_id = '101'"
    )
    bronze_count = _query(
        settings, "SELECT COUNT(*) FROM bronze.market_observations WHERE entity_id = '101'"
    )
    assert history_count == bronze_count
    assert _query(
        settings,
        "SELECT DISTINCT question, active FROM history.market_history "
        "WHERE market_id = '101' ORDER BY question NULLS LAST",
    ) == [("Market 101?", True), (None, None)]
    identities = _query(
        settings,
        "SELECT DISTINCT raw_clob_token_ids, raw_position_ids, "
        "json_extract_string(outcomes_json, '$[0].outcome_label') "
        "FROM history.market_history WHERE market_id = '101'",
    )
    assert {
        (tuple(json.loads(tokens)), tuple(json.loads(positions)), label)
        for tokens, positions, label in identities
    } == {
        (("11", "12"), ("911", "912"), "Yes"),
        (("21", "22"), ("921", "922"), "Up"),
    }


def test_relationship_keys_are_deduplicated_before_event_aggregation(built_pair):
    settings, _ = built_pair
    assert (
        _query(
            settings,
            "SELECT event_id, market_id, COUNT(*) FROM core.market_event_bridge "
            "GROUP BY event_id, market_id HAVING COUNT(*) > 1",
        )
        == []
    )
    assert _query(
        settings, "SELECT COUNT(*) FROM core.market_event_bridge WHERE market_id IN ('107','108')"
    ) == [(0,)]
    assert _query(
        settings,
        "SELECT event_id, inferred, membership_source_kind "
        "FROM core.market_event_bridge WHERE market_id = '111'",
    ) == [("2", True, "event_embedded")]
    assert _query(
        settings, "SELECT COUNT(*) FROM core.event_tags_current WHERE event_id = '1'"
    ) == [(1,)]
    assert _query(
        settings, "SELECT COUNT(*) FROM core.event_series_current WHERE event_id = '1'"
    ) == [(1,)]
    assert _query(
        settings, "SELECT market_count FROM marts.mart_event_catalogue WHERE event_id = '1'"
    ) == [(6,)]


SEMANTIC_TABLES = (
    "core.events_current",
    "core.markets_current",
    "core.outcomes_current",
    "core.market_event_bridge",
    "core.event_tags_current",
    "core.event_series_current",
    "core.market_tags_current",
    "core.quarantine_market_outcomes",
    "history.event_history",
    "history.market_history",
    "history.event_metrics",
    "history.market_metrics",
    "marts.mart_event_catalogue",
)


def _semantic_state(settings: Settings):
    state = {}
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        for table in SEMANTIC_TABLES:
            fields = [
                row[0]
                for row in con.execute(f"DESCRIBE {table}").fetchall()
                if row[0] not in {"built_through", "batch_loaded_at"}
            ]
            projection = ", ".join('"' + field + '"' for field in fields)
            rows = con.execute(f"SELECT {projection} FROM {table}").fetchall()
            state[table] = (fields, sorted(rows, key=repr))
    return state


def test_loading_same_evidence_out_of_order_is_semantically_equivalent(built_pair):
    forward, reverse = built_pair
    assert _semantic_state(forward) == _semantic_state(reverse)
