"""Membership selection is independent of outcome ownership validation."""

import json
from collections import Counter
from datetime import UTC, datetime, timedelta

import duckdb
import pytest
import yaml
from jinja2 import Environment, StrictUndefined

from oddsfox_catalogue.resources import dbt_project_dir

START = datetime(2026, 10, 1, tzinfo=UTC)
VENUE = "polymarket"
MODELS = dbt_project_dir() / "models"
BRIDGE_SCHEMA = [
    ("venue", "VARCHAR"),
    ("market_id", "VARCHAR"),
    ("event_id", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("event_ticker", "VARCHAR"),
    ("membership_observation_id", "VARCHAR"),
    ("membership_source_kind", "VARCHAR"),
    ("membership_received_at", "TIMESTAMP WITH TIME ZONE"),
    ("inferred", "BOOLEAN"),
]


def _observation(
    observation_id,
    market_id,
    day,
    *,
    source="market_direct",
    mode="explicit",
    events=(),
    enclosing=None,
    venue=VENUE,
    usable=True,
    assets=None,
):
    observed_at = START + timedelta(days=day)
    refs = [
        {
            "event_id": event_id,
            "observation_id": observation_id,
            "source_kind": source,
            "received_at": observed_at.isoformat(),
        }
        for event_id in events
    ]
    assets = assets or (str(int(market_id) * 100 + 1), str(int(market_id) * 100 + 2))
    outcomes = [
        {"asset_kind": "ctf_token", "asset_id": asset_id, "condition_id": "0x" + "a" * 64}
        for asset_id in assets
    ]
    return (
        observation_id,
        venue,
        market_id,
        source,
        observed_at,
        START + timedelta(days=10),
        enclosing,
        mode,
        usable,
        None if usable else "ambiguous asset identifiers",
        json.dumps(outcomes if usable else []),
        json.dumps(refs),
    )


def _observations():
    return [
        # Source priority outranks recency, including explicit memberships.
        _observation("10-direct-old", "10", 1, events=("1001", "1002")),
        _observation("10-embedded-new", "10", 4, source="event_embedded", events=("1009",)),
        # An explicit empty or invalid current value blocks historical inference.
        _observation(
            "20-history", "20", 2, source="event_embedded", mode="missing", enclosing="2001"
        ),
        _observation("20-current", "20", 5),
        _observation(
            "30-history", "30", 2, source="event_embedded", mode="missing", enclosing="3001"
        ),
        _observation("30-current", "30", 5, mode="invalid"),
        # Missing current references infer from all historical enclosing observations.
        _observation("40-current", "40", 5, mode="missing"),
        _observation("40-direct-old", "40", 1, mode="missing", enclosing="4001"),
        _observation(
            "40-embedded-new", "40", 4, source="event_embedded", mode="missing", enclosing="4001"
        ),
        _observation("40-b-a", "40", 3, source="event_embedded", mode="missing", enclosing="4002"),
        _observation("40-b-z", "40", 3, source="event_embedded", mode="missing", enclosing="4002"),
        # Equal receipts use observation ID; equal native IDs in another venue do not conflict.
        _observation("50-a", "50", 3, events=("5001",)),
        _observation("50-z", "50", 3, events=("5002",)),
        _observation("foreign50-current", "50", 3, venue="other", events=("5003",)),
        # Invalid and conflicting native identities retain their event memberships.
        _observation("60-current", "60", 3, usable=False, events=("6001",)),
        _observation("70-current", "70", 3, assets=("9001", "9002"), events=("7001",)),
        _observation("80-current", "80", 3, assets=("9001", "9002"), events=("8001",)),
    ]


def _render(path, *, old_bridge=False):
    def ref(name):
        return "markets_current" if old_bridge and name == "int_market_selected" else name

    return (
        Environment(undefined=StrictUndefined)
        .from_string(path.read_text())
        .render(ref=ref, config=lambda **kwargs: "")
    )


@pytest.fixture(params=[False, True], ids=["source-order", "reverse-source-order"])
def selected_views(request):
    with duckdb.connect(
        ":memory:",
        config={
            "memory_limit": "2GB",
            "threads": 2,
            "temp_directory": "",
            "max_temp_directory_size": "0B",
        },
    ) as connection:
        connection.execute(
            "CREATE TABLE observations (observation_id VARCHAR PRIMARY KEY, "
            "venue VARCHAR NOT NULL, market_id VARCHAR NOT NULL, source_kind VARCHAR NOT NULL, "
            "observed_at TIMESTAMPTZ NOT NULL, batch_loaded_at TIMESTAMPTZ NOT NULL, "
            "enclosing_event_id VARCHAR, membership_mode VARCHAR NOT NULL, usable BOOLEAN NOT NULL, "
            "identity_error VARCHAR, outcomes_json JSON NOT NULL, event_refs_json JSON NOT NULL)"
        )
        observations = _observations()
        connection.executemany(
            "INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            list(reversed(observations)) if request.param else observations,
        )
        connection.execute(
            "CREATE VIEW stg_gamma__market_observations AS SELECT * FROM observations"
        )
        for directory, model in (
            ("staging", "stg_gamma__market_event_refs"),
            ("core", "int_market_selected"),
            ("core", "int_market_outcomes"),
            ("core", "markets_current"),
            ("core", "quarantine_market_outcomes"),
        ):
            sql = _render(MODELS / directory / f"{model}.sql")
            connection.execute(f"CREATE VIEW {model} AS {sql}")
        bridge = MODELS / "core" / "market_event_bridge.sql"
        connection.execute(
            "CREATE VIEW bridge_current_owner AS " + _render(bridge, old_bridge=True)
        )
        connection.execute("CREATE VIEW bridge_selected_owner AS " + _render(bridge))
        yield connection


def _membership(
    venue, market_id, event_id, observation_id, day, *, inferred=False, source="market_direct"
):
    return (
        venue,
        market_id,
        event_id,
        None,
        None,
        observation_id,
        source,
        START + timedelta(days=day),
        inferred,
    )


def test_bridge_schema_rows_and_provenance_survive_selected_owner_join(selected_views):
    expected = Counter(
        [
            _membership(VENUE, "10", "1001", "10-direct-old", 1),
            _membership(VENUE, "10", "1002", "10-direct-old", 1),
            _membership(VENUE, "40", "4001", "40-direct-old", 1, inferred=True),
            _membership(VENUE, "40", "4002", "40-b-z", 3, inferred=True, source="event_embedded"),
            _membership(VENUE, "50", "5002", "50-z", 3),
            _membership("other", "50", "5003", "foreign50-current", 3),
            _membership(VENUE, "60", "6001", "60-current", 3),
            _membership(VENUE, "70", "7001", "70-current", 3),
            _membership(VENUE, "80", "8001", "80-current", 3),
        ]
    )
    results = []
    for relation in ("bridge_current_owner", "bridge_selected_owner"):
        schema = selected_views.execute(f"DESCRIBE {relation}").fetchall()
        assert [(column[0], column[1]) for column in schema] == BRIDGE_SCHEMA
        rows = Counter(selected_views.execute(f"SELECT * FROM {relation}").fetchall())
        assert rows == expected
        assert sum(rows.values()) == 9
        assert len({row[:3] for row in rows}) == 9
        results.append((schema, rows))
    assert results[0] == results[1]


def test_selected_and_current_owners_stay_one_to_one_while_conflicts_are_quarantined(
    selected_views,
):
    expected_owners = Counter(
        [
            (VENUE, "10", "10-direct-old"),
            (VENUE, "20", "20-current"),
            (VENUE, "30", "30-current"),
            (VENUE, "40", "40-current"),
            (VENUE, "50", "50-z"),
            ("other", "50", "foreign50-current"),
            (VENUE, "60", "60-current"),
            (VENUE, "70", "70-current"),
            (VENUE, "80", "80-current"),
        ]
    )
    for relation in ("int_market_selected", "markets_current"):
        assert (
            Counter(
                selected_views.execute(
                    f"SELECT venue, market_id, observation_id FROM {relation}"
                ).fetchall()
            )
            == expected_owners
        )
    assert selected_views.execute(
        "SELECT venue, market_id, observation_id, identity_error "
        "FROM quarantine_market_outcomes ORDER BY venue, market_id"
    ).fetchall() == [
        (VENUE, "60", "60-current", "ambiguous asset identifiers"),
        (VENUE, "70", "70-current", "native identity has conflicting owners"),
        (VENUE, "80", "80-current", "native identity has conflicting owners"),
    ]
    assert selected_views.execute(
        "SELECT venue, usable FROM markets_current WHERE market_id = '50' ORDER BY venue"
    ).fetchall() == [("other", True), (VENUE, True)]


def test_bridge_still_requires_current_market_relationship_validation():
    schema = yaml.safe_load((MODELS / "core" / "schema.yml").read_text())
    bridge = next(model for model in schema["models"] if model["name"] == "market_event_bridge")
    market_id = next(column for column in bridge["columns"] if column["name"] == "market_id")
    assert {
        "relationships": {"arguments": {"to": "ref('markets_current')", "field": "market_id"}}
    } in market_id["tests"]
