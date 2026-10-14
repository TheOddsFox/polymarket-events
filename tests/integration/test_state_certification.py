"""Checkpoint claims cannot certify corrupted material state or a partially built load."""

import json
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, copy_built
from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import demo_world
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.certification import BuildInvalid, assert_build_valid, read_build_validity
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.pipeline import dbt_stage, publish_stage
from oddsfox_catalogue.publish import CURRENT_POINTER, PublishBlocked, verify_release
from oddsfox_catalogue.semantics import (
    CHECKPOINT_MODELS,
    bounded_connection,
    validate_checkpoints,
    warehouse_snapshot,
)


def _sql(settings: Settings, query: str, parameters=None) -> list[tuple]:
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        return connection.execute(query, parameters or []).fetchall()


def _checkpoints(settings: Settings) -> dict:
    return {
        model: _sql(settings, f"select * from {model.replace('.', '.__checkpoint_')} order by all")
        for model in CHECKPOINT_MODELS
    }


def _assert_ok(result) -> None:
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]


def _capture_and_load(settings: Settings, *, day: int, world, event_ids=()) -> str:
    now = FIXED_NOW + timedelta(days=day)
    runtime, _ = build_runtime(settings.root, FakeGamma(world), now=now)
    try:
        captured = run_capture(runtime, "selected", market_ids=["5001"], event_ids=event_ids)
        assert captured.status == "captured"
    finally:
        runtime.client.close()
        runtime.ledger.close()
    with Ledger(settings.ledger_path) as ledger:
        loaded = load_pending(LoadRuntime(settings, ledger, now=lambda: now))
    assert loaded.batches_registered == [captured.batch_id]
    return captured.batch_id


def _assert_publication_blocked(settings: Settings, release: Path, pointer: bytes) -> None:
    with pytest.raises((BuildInvalid, ValueError, duckdb.Error)):
        assert_build_valid(settings)
    with pytest.raises((PublishBlocked, ValueError, duckdb.Error)):
        publish_stage(settings)
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == pointer
    verify_release(settings, release)


@pytest.fixture(scope="module")
def certified(tmp_path_factory):
    root = tmp_path_factory.mktemp("state-certification")
    settings = build_warehouse(root)
    # Two real observations of event 101 allow a same-entity stale winner corruption.
    _capture_and_load(settings, day=1, world=demo_world(), event_ids=["101"])
    _assert_ok(dbt_stage(settings, ["build"]))
    release = publish_stage(settings)
    return root, release.path.relative_to(root)


@pytest.fixture
def state(certified, tmp_path):
    root, relative_release = certified
    settings = copy_built(root, tmp_path)
    return (
        settings,
        tmp_path / relative_release,
        (settings.published_dir / CURRENT_POINTER).read_bytes(),
    )


@pytest.mark.parametrize(
    ("model", "damage"),
    [
        ("core.int_market_selection_state", "count"),
        ("core.int_market_selection_state", "winner"),
        ("core.int_event_selection_state", "count"),
        ("core.int_event_selection_state", "winner"),
        ("history.event_history", "value"),
        ("history.event_metrics", "value"),
        ("history.market_metrics", "value"),
    ],
)
def test_complete_checkpoint_cannot_hide_corrupted_state_and_selected_refresh_repairs_it(
    state, model: str, damage: str
) -> None:
    settings, release, pointer = state
    baseline = warehouse_snapshot(settings)
    checkpoints = _checkpoints(settings)
    registry = _sql(settings, "select * from bronze.batch_registry order by batch_id")
    if damage == "count":
        _sql(settings, f"update {model} set observation_count=observation_count+1")
    elif damage == "winner":
        entity = "market" if "market" in model else "event"
        candidates = _sql(
            settings,
            f"select s.venue, s.{entity}_id, o.observation_id from {model} s "
            f"join staging.stg_gamma__{entity}_observations o "
            f"on s.venue=o.venue and s.{entity}_id=o.{entity}_id "
            "where o.observation_id<>s.observation_id order by o.observation_id limit 1",
        )
        assert candidates, "a real losing observation is required to test stale selection"
        venue, entity_id, observation_id = candidates[0]
        _sql(
            settings,
            f"update {model} set observation_id=? where venue=? and {entity}_id=?",
            [observation_id, venue, entity_id],
        )
    else:
        column = "title" if model.endswith("event_history") else "volume"
        _sql(settings, f"update {model} set {column}='corrupted persisted value'")
    corrupted = _sql(settings, f"select * from {model} order by all")
    with bounded_connection(settings) as connection:
        validate_checkpoints(connection)
    result = dbt_stage(settings, ["build"])
    assert result.returncode != 0, result.stdout
    results = json.loads((result.target_path / "run_results.json").read_text())["results"]
    required_test = (
        "assert_selection_state_matches_source"
        if "selection_state" in model
        else "assert_incremental_observations_match_source"
    )
    assert any(row["status"] == "fail" and required_test in row["unique_id"] for row in results), (
        result.stdout[-6000:]
    )
    assert _sql(settings, f"select * from {model} order by all") == corrupted
    assert _checkpoints(settings) == checkpoints
    assert _sql(settings, "select * from bronze.batch_registry order by batch_id") == registry
    assert read_build_validity(settings)["status"] == "dirty"
    _assert_publication_blocked(settings, release, pointer)

    _assert_ok(dbt_stage(settings, ["run", "--select", model.split(".")[1], "--full-refresh"]))
    assert read_build_validity(settings)["status"] == "dirty"
    assert _checkpoints(settings) == checkpoints
    _assert_ok(dbt_stage(settings, ["build"]))
    assert warehouse_snapshot(settings) == baseline
    assert assert_build_valid(settings)["binding"]["warehouse"] == baseline
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == pointer
    verify_release(settings, release)


@pytest.mark.parametrize(
    "damage", ["version", "missing-batch", "extra-batch", "schema", "duplicate", "view"]
)
def test_private_checkpoint_corruption_overrides_a_valid_ledger_claim(state, damage: str) -> None:
    settings, release, pointer = state
    checkpoint = "core.__checkpoint_int_market_selection_state"
    if damage == "version":
        _sql(settings, f"update {checkpoint} set processing_version='unknown-version'")
    elif damage == "missing-batch":
        _sql(settings, f"delete from {checkpoint} where record_kind='batch'")
        assert _sql(settings, f"select * from {checkpoint}") == [("metadata", "", "1")]
    elif damage == "extra-batch":
        _sql(settings, f"insert into {checkpoint} values ('batch', 'unregistered-batch', '1')")
    elif damage == "schema":
        _sql(settings, f"alter table {checkpoint} rename column batch_id to claimed_batch")
    else:
        with duckdb.connect(str(settings.warehouse_path)) as connection:
            connection.execute(f"create temp table saved_checkpoint as select * from {checkpoint}")
            connection.execute(f"drop table {checkpoint}")
            if damage == "duplicate":
                connection.execute(
                    f"create table {checkpoint} (record_kind varchar not null, "
                    "batch_id varchar not null, processing_version varchar not null)"
                )
                connection.execute(f"insert into {checkpoint} select * from saved_checkpoint")
                connection.execute(
                    f"insert into {checkpoint} select * from saved_checkpoint where record_kind='batch'"
                )
            else:
                connection.execute(
                    "create table core.retained_checkpoint_claim as select * from saved_checkpoint"
                )
                connection.execute(
                    f"create view {checkpoint} as select * from core.retained_checkpoint_claim"
                )
    assert read_build_validity(settings)["status"] == "valid"
    _assert_publication_blocked(settings, release, pointer)


def test_dirty_load_exposes_history_but_keeps_prior_market_winner_until_complete_build(
    state,
) -> None:
    settings, release, pointer = state
    before = _sql(
        settings,
        "select observation_id, question from core.markets_current where market_id='5001'",
    )
    assert len(before) == 1
    world = demo_world()
    question = "New direct observation after certified build?"
    world.markets["5001"]["question"] = question
    batch_id = _capture_and_load(settings, day=2, world=world)
    assert read_build_validity(settings)["status"] == "dirty"
    assert (
        _sql(
            settings,
            "select observation_id, question from core.markets_current where market_id='5001'",
        )
        == before
    )
    assert _sql(
        settings,
        "select source_kind from bronze.market_observations "
        "where batch_id=? and entity_id='5001' order by source_kind",
        [batch_id],
    ) == [("event_embedded",), ("market_direct",)]
    for relation in (
        "staging.stg_gamma__market_observations",
        "history.int_market_semantic",
        "history.market_history",
    ):
        assert _sql(
            settings,
            f"select question from {relation} where batch_id=? and market_id='5001' "
            "and source_kind='market_direct'",
            [batch_id],
        ) == [(question,)]
    _assert_publication_blocked(settings, release, pointer)

    _assert_ok(dbt_stage(settings, ["build", "--select", "events_current"]))
    assert read_build_validity(settings)["status"] == "dirty"
    _assert_publication_blocked(settings, release, pointer)
    _assert_ok(dbt_stage(settings, ["build"]))
    current = _sql(
        settings,
        "select observation_id, question from core.markets_current where market_id='5001'",
    )
    assert current[0][0] != before[0][0]
    assert current[0][1] == question
    assert read_build_validity(settings)["status"] == "valid"
    assert_build_valid(settings)
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == pointer
    verify_release(settings, release)
