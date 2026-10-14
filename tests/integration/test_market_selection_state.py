"""Market winner state must match the independent full-history ranking."""

from __future__ import annotations

import shutil
from dataclasses import replace

import duckdb
import pytest

from fakes.built_warehouse import capture_and_load
from fakes.dbt_run import run_dbt
from fakes.world import World, make_event, make_market

pytestmark = pytest.mark.timeout(240)

OLD_SELECTION = """
with ranked as (
    select observation_id,
        row_number() over(partition by venue, market_id
            order by (source_kind = 'market_direct') desc, observed_at desc,
                observation_id desc) as recency_rank,
        count(*) over(partition by venue, market_id) as observation_count,
        min(observed_at) over(partition by venue, market_id) as first_observed_at
    from staging.stg_gamma__market_observations
)
select s.*, r.observation_count, r.first_observed_at
from staging.stg_gamma__market_observations s
join ranked r using(observation_id)
where r.recency_rank = 1
"""


def _run(settings, root, *arguments):
    result = run_dbt(list(arguments), settings.warehouse_path, root)
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]


def _assert_selection(settings):
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        expected = con.execute(OLD_SELECTION + " order by venue, market_id").fetchall()
        assert (
            con.execute(
                "select * from core.int_market_selected order by venue, market_id"
            ).fetchall()
            == expected
        )
        assert [row[:2] for row in con.execute("describe core.int_market_selected").fetchall()] == [
            row[:2] for row in con.execute("describe " + OLD_SELECTION).fetchall()
        ]
        expected_state = con.execute(
            "select venue, market_id, observation_id, observation_count, first_observed_at "
            "from (" + OLD_SELECTION + ") order by venue, market_id"
        ).fetchall()
        state = con.execute(
            "select * from core.int_market_selection_state order by venue, market_id"
        ).fetchall()
        assert state == expected_state
        assert [
            row[0] for row in con.execute("describe core.int_market_selection_state").fetchall()
        ] == ["venue", "market_id", "observation_id", "observation_count", "first_observed_at"]
        checkpoint = con.execute(
            "select * from core.__checkpoint_int_market_selection_state order by record_kind, batch_id"
        ).fetchall()
        assert ("metadata", "", "1") in checkpoint
        assert {row[1] for row in checkpoint if row[0] == "batch"} == {
            row[0]
            for row in con.execute(
                "select batch_id from bronze.batch_registry where status = 'loaded'"
            ).fetchall()
        }
        return state, checkpoint


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("market-selection")
    world = World()
    world.add_event(
        make_event(
            "1",
            "Selection",
            markets=[
                make_market("101", "First?"),
                make_market("102", "Second?"),
                make_market("103", "Untouched?"),
            ],
        )
    )
    settings = capture_and_load(root, world)
    with duckdb.connect(str(settings.warehouse_path)) as con:
        con.execute(
            "update bronze.market_observations_values set source_kind = 'event_embedded', observed_at = '2026-10-01'"
        )
    _run(settings, root, "build", "--full-refresh")
    _assert_selection(settings)
    return settings, root


def test_incremental_late_direct_ties_nulls_counts_and_refresh_match_full_history(built, tmp_path):
    settings, _ = built
    # Copy the real loaded fixture so the initial baseline remains reusable.
    warehouse = tmp_path / "warehouse.duckdb"
    shutil.copyfile(settings.warehouse_path, warehouse)
    settings = replace(settings, paths=replace(settings.paths, warehouse_file=str(warehouse)))
    with duckdb.connect(str(warehouse)) as con:
        con.execute(
            "create temp table originals as select * from bronze.market_observations_values qualify row_number() over(partition by entity_id order by observation_id) = 1"
        )
        con.execute(
            "insert into bronze.batch_registry select * replace ('late-batch' as batch_id) from bronze.batch_registry limit 1"
        )
        con.execute(
            "insert into bronze.market_observations_values select * replace ('late-direct' as observation_id, 'late-batch' as batch_id, 'market_direct' as source_kind, cast('2020-01-01' as timestamptz) as observed_at) from originals where entity_id = '101'"
        )
        for observation_id, question in (("tie-a", '"Old label"'), ("tie-z", "null")):
            con.execute(
                "insert into bronze.market_observations_values select * replace (? as observation_id, 'late-batch' as batch_id, 'market_direct' as source_kind, cast('2030-01-01' as timestamptz) as observed_at, ? as n_question) from originals where entity_id = '102'",
                [observation_id, question],
            )
    _run(settings, tmp_path, "run")
    incremental = _assert_selection(settings)
    baseline_state = _assert_selection(built[0])[0]
    with duckdb.connect(str(warehouse), read_only=True) as con:
        assert con.execute(
            "select market_id, observation_id, question from core.int_market_selected where market_id in ('101', '102') order by market_id"
        ).fetchall() == [("101", "late-direct", "First?"), ("102", "tie-z", None)]
        for market, count, first in con.execute(
            "select market_id, observation_count, first_observed_at from core.int_market_selection_state"
        ).fetchall():
            baseline = next(row for row in baseline_state if row[1] == market)
            assert count == baseline[3] + {"101": 1, "102": 2, "103": 0}[market]
            if market == "103":
                assert next(row for row in incremental[0] if row[1] == market) == baseline
            assert first.year == (2020 if market == "101" else 2026)
    _run(settings, tmp_path, "run")
    assert _assert_selection(settings) == incremental
    _run(settings, tmp_path, "run", "--full-refresh")
    assert _assert_selection(settings) == incremental


def test_empty_initial_state_is_typed_and_preserves_checkpoint_metadata(built, tmp_path):
    settings, _ = built
    warehouse = tmp_path / "empty.duckdb"
    shutil.copyfile(settings.warehouse_path, warehouse)
    settings = replace(settings, paths=replace(settings.paths, warehouse_file=str(warehouse)))
    with duckdb.connect(str(warehouse)) as con:
        con.execute("delete from bronze.batch_registry")
    _run(settings, tmp_path, "run", "--full-refresh")
    assert _assert_selection(settings) == ([], [("metadata", "", "1")])
