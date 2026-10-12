"""Live market views still require a complete certified build before publication."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import World, event_stub, make_event, make_market
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.certification import BuildInvalid, assert_build_valid, read_build_validity
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.metadata import export_metadata
from oddsfox_catalogue.pipeline import dbt_stage, publish_stage
from oddsfox_catalogue.publish import CURRENT_POINTER, PublishBlocked, verify_release
from oddsfox_catalogue.semantics import bounded_connection, published_snapshot, warehouse_snapshot


def _world():
    world = World()
    event = make_event("11", "Market stage event")
    market = make_market("201", "Embedded original", event_stub=event_stub(event), version="v1")
    market.update(clobTokenIds='["101","102"]', positionIds=["101", "102"])
    event["markets"] = [market]
    world.add_event(event)
    world.markets["201"]["question"] = "Direct original"
    return world


def _capture_and_load(root, world, now):
    embedded = {
        market["id"]: market for event in world.events.values() for market in event["markets"]
    }
    runtime, _ = build_runtime(root, FakeGamma(world, nested_overrides=embedded), now=now)
    try:
        capture = run_capture(runtime, "selected", market_ids=["201"], event_ids=["11"])
        assert capture.status == "captured"
        settings = runtime.settings
    finally:
        runtime.client.close()
        runtime.ledger.close()
    with Ledger(settings.ledger_path) as ledger:
        loaded = load_pending(LoadRuntime(settings=settings, ledger=ledger, now=lambda: now))
        assert loaded.pages_loaded > 0
    return settings


def _build(settings, *args):
    result = dbt_stage(settings, ["build", *args])
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]


def _rows(settings, sql, parameters=None):
    with bounded_connection(settings) as connection:
        return connection.execute(sql, parameters or []).fetchall()


def _observations(settings, relation):
    return _rows(
        settings,
        f"SELECT observation_id, count(*) FROM {relation} GROUP BY observation_id "
        "ORDER BY observation_id",
    )


def test_market_views_preserve_observations_and_require_full_build_certification(tmp_path):
    world = _world()
    settings = _capture_and_load(tmp_path, world, FIXED_NOW)
    _build(settings)
    assert_build_valid(settings)
    stage = "staging.stg_gamma__market_observations"
    assert _rows(
        settings,
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_schema='staging' AND table_name='stg_gamma__market_observations'",
    ) == [("VIEW",)]
    assert _rows(
        settings,
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='markets_current'",
    ) == [("VIEW",)]
    original = _observations(settings, stage)
    assert len(original) == 2 and all(count == 1 for _, count in original)
    assert _rows(settings, "SELECT question, source_kind FROM core.markets_current") == [
        ("Direct original", "market_direct")
    ]
    assert _rows(settings, "SELECT event_id FROM core.market_event_bridge") == [("11",)]
    first = publish_stage(settings, now=lambda: FIXED_NOW)
    pointer = (settings.published_dir / CURRENT_POINTER).read_bytes()

    world.markets["201"].update(
        question="Direct revised",
        outcomes='["Agree","Disagree"]',
        version="v2",
        active=None,
        closed=None,
        enableOrderBook=None,
        acceptingOrders=None,
        events=[],
    )
    world.events["11"]["markets"][0]["question"] = "Embedded revised must not win"
    settings = _capture_and_load(tmp_path, world, FIXED_NOW + timedelta(days=1))
    expected = _observations(settings, "bronze.market_observations")
    assert len(expected) == 4 and all(count == 1 for _, count in expected)
    assert _observations(settings, stage) == expected
    assert _observations(settings, "history.market_history") == expected
    assert read_build_validity(settings)["status"] == "dirty"
    assert _rows(settings, "SELECT question FROM core.markets_current") == [("Direct revised",)]
    with pytest.raises(PublishBlocked, match="certification"):
        publish_stage(settings, now=lambda: FIXED_NOW + timedelta(days=1))
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == pointer
    verify_release(settings, first.path)

    _build(settings, "--select", "markets_current market_history")
    assert _observations(settings, stage) == expected
    assert _observations(settings, "history.market_history") == expected
    assert _rows(settings, "SELECT question FROM core.markets_current") == [("Direct revised",)]
    with pytest.raises(BuildInvalid, match="dirty"):
        assert_build_valid(settings)
    with pytest.raises(PublishBlocked, match="certification"):
        publish_stage(settings, now=lambda: FIXED_NOW + timedelta(days=1))
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == pointer

    _build(settings)
    assert_build_valid(settings)
    assert _observations(settings, stage) == expected
    assert _observations(settings, "history.market_history") == expected
    assert _rows(
        settings,
        "SELECT question, source_kind, active, closed, enable_order_book, accepting_orders, "
        "source_market_version, protocol, observation_count, usable FROM core.markets_current",
    ) == [
        ("Direct revised", "market_direct", None, None, None, None, "v2", "polymarket_v2", 4, True)
    ]
    assert _rows(settings, "SELECT event_id FROM core.market_event_bridge") == [], (
        "explicit empty membership must override embedded event evidence"
    )
    assert _rows(
        settings,
        "SELECT source_kind, question, active, source_market_version, "
        "json_extract_string(outcomes_json, '$[0].asset_kind') "
        "FROM history.market_history ORDER BY observed_at, source_kind",
    ) == [
        ("event_embedded", "Embedded original", True, "v1", "ctf_token"),
        ("market_direct", "Direct original", True, "v1", "ctf_token"),
        ("event_embedded", "Embedded revised must not win", True, "v1", "ctf_token"),
        ("market_direct", "Direct revised", None, "v2", "poly_v2_position"),
    ]

    second = publish_stage(settings, now=lambda: FIXED_NOW + timedelta(days=1))
    verify_release(settings, second.path)
    public = published_snapshot(settings)
    for name, fingerprint in public.items():
        assert second.tables[name]["rows"] == fingerprint["rows"]
        assert second.tables[name]["semantic_sha256"] == fingerprint["semantic_sha256"]
    metadata = tmp_path / "metadata-after-rebuild"
    export_metadata(settings, ["201"], metadata)
    market = json.loads((metadata / "markets.json").read_text())[0]
    assert (
        market["question"],
        market["active"],
        market["closed"],
        market["enable_order_book"],
    ) == (
        "Direct revised",
        None,
        None,
        None,
    )
    assert json.loads((metadata / "memberships.json").read_text()) == []
    fields = (
        "outcome_index outcome_label asset_kind asset_id clob_token_id position_id chain_index_set"
    )
    metadata_outcomes = json.loads((metadata / "outcomes.json").read_text())
    assert (
        [tuple(row[field] for field in fields.split()) for row in metadata_outcomes]
        == _rows(
            settings,
            "SELECT " + ", ".join(fields.split()) + " FROM read_parquet(?) ORDER BY outcome_index",
            [str(second.path / "outcomes.parquet")],
        )
        == [
            (1, "Agree", "poly_v2_position", "101", "101", "101", None),
            (2, "Disagree", "poly_v2_position", "102", "102", "102", None),
        ]
    )
    semantic = warehouse_snapshot(settings)
    _build(settings)
    assert_build_valid(settings)
    assert warehouse_snapshot(settings) == semantic
    assert published_snapshot(settings) == public
    assert _observations(settings, stage) == expected
    assert _observations(settings, "history.market_history") == expected
