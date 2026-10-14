"""Incremental event winners remain equivalent to the original full-history rank."""

from datetime import timedelta

import duckdb

from fakes.built_warehouse import capture_and_load
from fakes.dbt_run import run_dbt
from fakes.harness import FIXED_NOW

ORIGINAL_CURRENT = """
select * exclude(recency_rank, observed_at, batch_loaded_at),
    observed_at as last_observed_at, batch_loaded_at as built_through
from (
    select *, row_number() over(partition by venue, event_id
        order by observed_at desc, observation_id desc) as recency_rank,
        count(*) over(partition by venue, event_id) as observation_count,
        min(observed_at) over(partition by venue, event_id) as first_observed_at
    from staging.stg_gamma__event_observations
)
where recency_rank = 1
"""
ORIGINAL_STATE = """
select venue, event_id, observation_id, observation_count, first_observed_at
from (
    select venue, event_id, observation_id,
        row_number() over(partition by venue, event_id
            order by observed_at desc, observation_id desc) as recency_rank,
        count(*) over(partition by venue, event_id) as observation_count,
        min(observed_at) over(partition by venue, event_id) as first_observed_at
    from staging.stg_gamma__event_observations
)
where recency_rank = 1
"""


def build(settings, root, *, full_refresh=False):
    args = ["run", "--select", "+events_current"]
    if full_refresh:
        args.append("--full-refresh")
    result = run_dbt(args, settings.warehouse_path, root)
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]


def assert_equivalent(settings):
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        for relation, expected in (
            ("core.events_current", ORIGINAL_CURRENT),
            ("core.int_event_selection_state", ORIGINAL_STATE),
        ):
            actual = connection.execute(f"SELECT * FROM {relation} ORDER BY venue, event_id")
            schema = [(column[0], column[1]) for column in actual.description]
            rows = actual.fetchall()
            reference = connection.execute(expected + " ORDER BY venue, event_id")
            assert schema == [(column[0], column[1]) for column in reference.description]
            assert rows == reference.fetchall()
        return connection.execute(
            "SELECT * FROM core.int_event_selection_state ORDER BY venue, event_id"
        ).fetchall()


def add_registered_batch(connection, batch_id, loaded_at):
    connection.execute(
        "INSERT INTO bronze.batch_registry SELECT * REPLACE (? AS batch_id, ? AS loaded_at) "
        "FROM bronze.batch_registry LIMIT 1",
        [batch_id, loaded_at],
    )


def add_event_observation(connection, batch_id, observation_id, observed_at, *, title='"older"'):
    connection.execute(
        "INSERT INTO bronze.event_observations_values SELECT * REPLACE "
        "(? AS batch_id, ? AS observation_id, ? AS observed_at, ? AS n_title, "
        "'null' AS n_description) FROM bronze.event_observations_values "
        "WHERE entity_id = '101' LIMIT 1",
        [batch_id, observation_id, observed_at, title],
    )


def test_event_selection_matches_full_rank_for_replays_late_batches_ties_and_empty_batches(
    tmp_path,
):
    settings = capture_and_load(tmp_path)
    build(settings, tmp_path)
    initial = assert_equivalent(settings)
    assert initial
    assert all(len(row) == 5 for row in initial)

    build(settings, tmp_path)
    assert assert_equivalent(settings) == initial

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_registered_batch(connection, "late-event-batch", FIXED_NOW + timedelta(days=2))
        add_event_observation(
            connection, "late-event-batch", "late-older", FIXED_NOW - timedelta(days=1)
        )
    build(settings, tmp_path)
    late = assert_equivalent(settings)
    before = next(row for row in initial if row[1] == "101")
    after = next(row for row in late if row[1] == "101")
    assert after[2] == before[2]
    assert after[3] == before[3] + 1
    assert after[4] == FIXED_NOW - timedelta(days=1)

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_registered_batch(connection, "tie-event-batch", FIXED_NOW + timedelta(days=3))
        add_event_observation(
            connection, "tie-event-batch", "zz-tie-loser", FIXED_NOW + timedelta(days=1)
        )
        add_event_observation(
            connection,
            "tie-event-batch",
            "zz-tie-winner",
            FIXED_NOW + timedelta(days=1),
            title="null",
        )
    build(settings, tmp_path)
    assert_equivalent(settings)
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT observation_id, title, description FROM core.events_current WHERE event_id='101'"
        ).fetchone() == ("zz-tie-winner", None, None)

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_registered_batch(connection, "empty-event-batch", FIXED_NOW + timedelta(days=4))
    build(settings, tmp_path)
    final = assert_equivalent(settings)
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM core.__checkpoint_int_event_selection_state "
            "WHERE record_kind='batch' AND batch_id='empty-event-batch'"
        ).fetchone() == (1,)
    build(settings, tmp_path, full_refresh=True)
    assert assert_equivalent(settings) == final
