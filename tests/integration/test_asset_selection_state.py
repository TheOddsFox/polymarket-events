"""Compact asset ownership preserves the original global conflict behavior."""

import json
from datetime import timedelta

import duckdb

from fakes.built_warehouse import capture_and_load
from fakes.dbt_run import run_dbt
from fakes.harness import FIXED_NOW

ORIGINAL = """
with nominated as (
    select venue, market_id, unnest(json_transform(outcomes_json,
        '[{"asset_kind":"VARCHAR","asset_id":"VARCHAR","condition_id":"VARCHAR"}]')) as identity
    from core.int_market_selected where usable
), conflicts as (
    select venue, identity.asset_kind as asset_kind, identity.asset_id as asset_id
    from nominated group by venue, identity.asset_kind, identity.asset_id
    having count(distinct market_id) > 1
), conflicted_markets as (
    select distinct n.venue, n.market_id from nominated n join conflicts c
        on n.venue = c.venue and n.identity.asset_kind = c.asset_kind and n.identity.asset_id = c.asset_id
)
select m.venue, m.market_id, m.observation_id, m.outcomes_json,
    m.usable and c.market_id is null as usable,
    case when c.market_id is not null then 'native identity has conflicting owners' else m.identity_error end as identity_error
from core.int_market_selected m
left join conflicted_markets c using(venue, market_id)
"""


def build(settings, root, *, full_refresh=False):
    args = ["run", "--select", "+int_market_outcomes"]
    if full_refresh:
        args.append("--full-refresh")
    result = run_dbt(args, settings.warehouse_path, root)
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute("CREATE TEMP VIEW original_outcomes AS " + ORIGINAL)

        def schema(name):
            return connection.execute(
                "SELECT column_name,data_type,is_nullable FROM information_schema.columns "
                "WHERE table_name = ? ORDER BY ordinal_position",
                [name],
            ).fetchall()

        assert schema("int_market_outcomes") == schema("original_outcomes")
        actual = connection.execute(
            "SELECT * FROM core.int_market_outcomes ORDER BY venue,market_id"
        ).fetchall()
        expected = connection.execute(ORIGINAL + " ORDER BY venue,market_id").fetchall()
        assert actual == expected
        assert connection.execute(
            "SELECT count(*) FROM core.int_market_asset_conflicts WHERE owner_count <= 1"
        ).fetchone() == (0,)
        return actual


def add_observation(connection, sequence, market_id, outcomes, *, usable=True, venue="polymarket"):
    batch = "asset-batch-" + str(sequence)
    received = FIXED_NOW + timedelta(days=sequence)
    connection.execute(
        "INSERT INTO bronze.batch_registry SELECT * REPLACE (? AS batch_id, ? AS loaded_at) "
        "FROM bronze.batch_registry LIMIT 1",
        [batch, received],
    )
    connection.execute(
        "INSERT INTO bronze.market_observations_values SELECT * REPLACE "
        "(? AS batch_id, ? AS observation_id, ? AS entity_id, ? AS venue, ? AS observed_at, "
        "'market_direct' AS source_kind, ? AS n_outcomes, ? AS n_usable, 'null' AS n_identity_error) "
        "FROM bronze.market_observations_values LIMIT 1",
        [
            batch,
            f"asset-observation-{sequence}",
            market_id,
            venue,
            received,
            json.dumps(outcomes),
            json.dumps(usable),
        ],
    )


def test_asset_conflicts_appear_and_resolve_without_losing_unchanged_owner_nominations(tmp_path):
    settings = capture_and_load(tmp_path)
    build(settings, tmp_path)
    asset = {
        "asset_kind": "clob_token",
        "asset_id": "shared-test-asset",
        "condition_id": "condition",
    }
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_observation(connection, 1, "90001", [asset, asset])
        add_observation(connection, 2, "90002", [asset])
    conflicted = build(settings, tmp_path)
    assert [(row[1], row[4], row[5]) for row in conflicted if row[1] in {"90001", "90002"}] == [
        ("90001", False, "native identity has conflicting owners"),
        ("90002", False, "native identity has conflicting owners"),
    ]
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT owner_count FROM core.int_market_asset_conflicts WHERE asset_id='shared-test-asset'"
        ).fetchall() == [(2,)]
        assert connection.execute(
            "SELECT count(*) FROM core.int_market_asset_nominations WHERE asset_id='shared-test-asset'"
        ).fetchone() == (3,)

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_observation(connection, 3, "90002", [])
    resolved = build(settings, tmp_path)
    assert next(row for row in resolved if row[1] == "90001")[4] is True
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM core.int_market_asset_nominations WHERE market_id='90002'"
        ).fetchone() == (0,)

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_observation(connection, 4, "90002", [asset], usable=False)
    unusable = build(settings, tmp_path)
    assert next(row for row in unusable if row[1] == "90001")[4] is True
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM core.int_market_asset_nominations WHERE market_id='90002'"
        ).fetchone() == (0,)

    with duckdb.connect(str(settings.warehouse_path)) as connection:
        add_observation(connection, 5, "90002", [{**asset, "asset_kind": "position"}])
        add_observation(connection, 6, "90003", [asset], venue="other-venue")
    final = build(settings, tmp_path)
    assert all(row[4] is True for row in final if row[1] in {"90001", "90002", "90003"})
    assert build(settings, tmp_path) == final
    assert build(settings, tmp_path, full_refresh=True) == final
