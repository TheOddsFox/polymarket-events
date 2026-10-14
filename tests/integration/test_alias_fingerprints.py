"""Only proved identity views may share fingerprints; evidence stays independently bound."""

from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, copy_built
from oddsfox_catalogue import semantics
from oddsfox_catalogue.certification import BuildInvalid, assert_build_valid
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.semantics import (
    OPERATIONAL_RELATIONS,
    SEMANTIC_RELATIONS,
    bounded_connection,
    published_snapshot,
    relation_schema,
    semantic_query,
    warehouse_snapshot,
)

ALIASES = {
    "history.int_event_semantic": "staging.stg_gamma__event_observations",
    "history.int_market_semantic": "staging.stg_gamma__market_observations",
    "history.market_history": "history.int_market_semantic",
}


def _reference(settings: Settings) -> dict:
    with bounded_connection(settings) as connection:
        return {
            "schemas": {
                relation: relation_schema(connection, relation)
                for relation in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)
            },
            "relations": {
                relation: semantic_fingerprint(connection, semantic_query(connection, relation))
                for relation in SEMANTIC_RELATIONS
            },
        }


class ObservedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = []

    def execute(self, sql, *args, **kwargs):
        self.statements.append(sql.strip().upper())
        return self.connection.execute(sql, *args, **kwargs)

    def begin(self):
        self.statements.append("BEGIN")
        self.connection.begin()
        return self

    def commit(self):
        self.statements.append("COMMIT")
        self.connection.commit()
        return self

    def rollback(self):
        self.statements.append("ROLLBACK")
        self.connection.rollback()
        return self

    def __getattr__(self, name):
        return getattr(self.connection, name)


def _assert_snapshot(settings: Settings, monkeypatch, *, reused: set[str]) -> dict:
    expected = _reference(settings)
    calls, connections = [], []

    @contextmanager
    def observed_connection(settings):
        with bounded_connection(settings) as connection:
            observed = ObservedConnection(connection)
            connections.append(observed)
            yield observed

    def fingerprint(connection, query, *args, **kwargs):
        assert connection is connections[0]
        calls.append(query.rsplit(" FROM ", 1)[1])
        return semantic_fingerprint(connection, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(semantics, "bounded_connection", observed_connection)
        patch.setattr(semantics, "semantic_fingerprint", fingerprint)
        actual = warehouse_snapshot(settings)
    assert actual == expected
    assert set(actual["relations"]) == set(SEMANTIC_RELATIONS)
    assert set(actual["schemas"]) == set(SEMANTIC_RELATIONS) | set(OPERATIONAL_RELATIONS)
    assert set(calls) == set(SEMANTIC_RELATIONS) - reused
    assert len(calls) == len(SEMANTIC_RELATIONS) - len(reused)
    assert len(connections) == 1
    statements = connections[0].statements
    assert statements[0].startswith("BEGIN")
    assert sum(sql.startswith("BEGIN") for sql in statements) == 1
    assert sum(sql.startswith(("COMMIT", "ROLLBACK")) for sql in statements) == 1
    assert statements[-1].startswith(("COMMIT", "ROLLBACK"))
    return actual


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("fingerprint-aliases")
    build_warehouse(root)
    return root


@pytest.fixture
def settings(built: Path, tmp_path: Path) -> Settings:
    return copy_built(built, tmp_path)


def test_only_the_three_proved_aliases_skip_hashing_and_keep_exact_snapshot(
    settings: Settings, monkeypatch
) -> None:
    actual = _assert_snapshot(settings, monkeypatch, reused=set(ALIASES))
    for alias, source in ALIASES.items():
        assert actual["relations"][alias] == actual["relations"][source]
        assert actual["schemas"][alias] == actual["schemas"][source]


@pytest.mark.parametrize("alteration", ["filter", "projection", "value"])
def test_changed_view_definition_cannot_reuse_even_with_identical_fixture_rows(
    settings: Settings, monkeypatch, alteration: str
) -> None:
    before = _reference(settings)
    target = "history.int_market_semantic"
    source = ALIASES[target]
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        assert connection.execute(
            f"select count(*) from {source} where observation_id = 'absent-observation'"
        ).fetchone() == (0,)
        if alteration == "filter":
            query = f"select * from {source} where observation_id <> 'absent-observation'"
        elif alteration == "projection":
            columns = ", ".join('"' + column["name"] + '"' for column in before["schemas"][target])
            query = f"select {columns} from {source}"
        else:
            query = (
                "select * replace (case when observation_id = 'absent-observation' "
                f"then 'changed' else question end as question) from {source}"
            )
        connection.execute(f"create or replace view {target} as {query}")
    actual = _assert_snapshot(settings, monkeypatch, reused={"history.int_event_semantic"})
    assert actual == before, "unchanged fixture rows must not substitute for proving view identity"


@pytest.mark.parametrize("target", list(ALIASES))
def test_equal_content_table_is_never_an_alias(
    settings: Settings, monkeypatch, target: str
) -> None:
    before = _reference(settings)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute(f"create temp table saved_alias as select * from {target}")
        connection.execute(f"drop view {target}")
        connection.execute(f"create table {target} as select * from saved_alias")
    invalid = {target}
    if target == "history.int_market_semantic":
        invalid.add("history.market_history")
    actual = _assert_snapshot(settings, monkeypatch, reused=set(ALIASES) - invalid)
    assert actual["relations"] == before["relations"]


@pytest.mark.parametrize(
    "alteration", ["extra-column", "column-order", "processing-type", "column-aliases"]
)
def test_full_ordered_schema_must_match_before_reusing_a_digest(
    settings: Settings, monkeypatch, alteration: str
) -> None:
    target = "history.market_history"
    source = ALIASES[target]
    before = _reference(settings)
    query = {
        "extra-column": f"select *, 7::integer as extra_evidence from {source}",
        "column-order": f"select question, * exclude (question) from {source}",
        "processing-type": f"select * replace (batch_loaded_at::varchar as batch_loaded_at) from {source}",
        "column-aliases": f"select * from {source}",
    }[alteration]
    definition = target
    if alteration == "column-aliases":
        names = [column["name"] for column in before["schemas"][target]]
        names[0], names[1] = names[1], names[0]
        definition += " (" + ", ".join('"' + name + '"' for name in names) + ")"
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute(f"create or replace view {definition} as {query}")
        if alteration == "column-aliases":
            connection.execute(f"create temp view independent_identity as select * from {source}")
            actual_sql = connection.execute(
                "select sql from duckdb_views() where schema_name='history' and view_name='market_history'"
            ).fetchone()[0]
            expected_sql = connection.execute(
                "select sql from duckdb_views() where temporary and view_name='independent_identity'"
            ).fetchone()[0]
            assert actual_sql.split(" AS ", 1)[1] == expected_sql.split(" AS ", 1)[1]
    actual = _assert_snapshot(settings, monkeypatch, reused=set(ALIASES) - {target})
    assert actual["schemas"][target] != before["schemas"][target]
    if alteration == "processing-type":
        assert actual["relations"][target] == before["relations"][target]
    else:
        assert actual["relations"][target] != before["relations"][target]


@pytest.mark.parametrize("mutation", ["nonpublic-history", "nonwinning-observation"])
def test_hidden_evidence_changes_are_detected_while_public_projection_is_unchanged(
    settings: Settings, monkeypatch, mutation: str
) -> None:
    before = _reference(settings)
    public = published_snapshot(settings)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        if mutation == "nonpublic-history":
            connection.execute(
                "create or replace view history.market_history as "
                "select * replace ('unpublished evidence changed' as description) "
                "from history.int_market_semantic"
            )
            reused = set(ALIASES) - {"history.market_history"}
            changed = "history.market_history"
        else:
            candidate = connection.execute(
                "select observation_id from bronze.market_observations_values "
                "where observation_id not in "
                "(select observation_id from core.int_market_selection_state) limit 1"
            ).fetchone()
            assert candidate is not None, "fixture must retain at least one losing observation"
            connection.execute(
                "update bronze.market_observations_values set payload_hash='changed losing evidence' "
                "where observation_id=?",
                [candidate[0]],
            )
            reused = set(ALIASES)
            changed = "history.int_market_semantic"
    actual = _assert_snapshot(settings, monkeypatch, reused=reused)
    assert actual["relations"][changed] != before["relations"][changed]
    assert published_snapshot(settings) == public
    with pytest.raises(BuildInvalid, match="changed"):
        assert_build_valid(settings)
