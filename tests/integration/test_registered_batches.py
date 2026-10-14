"""Registered batch progress must commit with its model or leave both untouched."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue.dbt_runner import DBT_DIR, run_dbt_at


@dataclass
class BatchProject:
    project: Path
    warehouse: Path
    work: Path

    def sql(self, statement: str) -> list[tuple]:
        with duckdb.connect(str(self.warehouse)) as connection:
            return connection.execute(statement).fetchall()

    def run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return run_dbt_at(
            ["run", *args],
            project_dir=self.project,
            profiles_dir=self.project,
            warehouse=self.warehouse,
            work_dir=self.work,
        )

    def snapshot(self, model: str = "parent") -> tuple[list[tuple], list[tuple]]:
        return (
            self.sql(f"select * from main.{model} order by observation_id"),
            self.sql(
                f"select * from main.__checkpoint_{model} "
                "order by record_kind, batch_id, processing_version"
            ),
        )

    def relation_state(self, identifier: str):
        with duckdb.connect(str(self.warehouse)) as connection:
            kind = connection.execute(
                "select table_type from information_schema.tables "
                "where table_schema='main' and table_name=?",
                [identifier],
            ).fetchone()
            if kind is None:
                return None
            columns = connection.execute(
                "select column_name, data_type, is_nullable from information_schema.columns "
                "where table_schema='main' and table_name=? order by ordinal_position",
                [identifier],
            ).fetchall()
            definition = connection.execute(
                "select sql from duckdb_views() where not temporary "
                "and schema_name='main' and view_name=?",
                [identifier],
            ).fetchone()
            rows = connection.execute(f"select * from main.{identifier} order by all").fetchall()
            return kind[0], columns, rows, definition

    def batch(self, batch_id: str, observed_at: str, amount: int | None) -> None:
        with duckdb.connect(str(self.warehouse)) as connection:
            connection.execute(
                "insert into bronze.batch_registry values (?, 'loaded', ?)",
                [batch_id, observed_at],
            )
            if amount is not None:
                connection.execute(
                    "insert into bronze.observations values (?, ?, ?)",
                    [f"observation-{batch_id}", batch_id, amount],
                )


def _assert_ok(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]


def _assert_error(result: subprocess.CompletedProcess[str], message: str) -> None:
    assert result.returncode != 0, result.stdout
    assert message in result.stdout + result.stderr, result.stdout[-6000:] + result.stderr[-3000:]


def _project(root: Path) -> BatchProject:
    project = root / "project"
    (project / "models").mkdir(parents=True)
    (project / "macros").mkdir()
    shutil.copyfile(DBT_DIR / "macros/registered_batches.sql", project / "macros/batches.sql")
    shutil.copyfile(DBT_DIR / "profiles.yml", project / "profiles.yml")
    (project / "dbt_project.yml").write_text(
        "name: registered_batches_test\nversion: '1.0'\nconfig-version: 2\n"
        "profile: oddsfox_catalogue\nmodel-paths: [models]\nmacro-paths: [macros]\n"
    )
    (project / "models/sources.yml").write_text(
        "version: 2\nsources:\n  - name: bronze\n    schema: bronze\n"
        "    tables:\n      - name: batch_registry\n      - name: observations\n"
    )
    for model in ("parent", "child", "sibling"):
        dependencies = "['parent']" if model == "child" else "[]"
        source = "ref('parent')" if model == "child" else "source('bronze', 'observations')"
        (project / f"models/{model}.sql").write_text(
            """{{ config(materialized='incremental', incremental_strategy='delete+insert',
    unique_key='observation_id',
    pre_hook="{{ registered_batches_begin(version=var('processing_version', '1'), dependencies=DEPENDENCIES) }}",
    post_hook=["{{ registered_batches_finish(version=var('processing_version', '1')) }}",
        "{% if var('fail_after_finish', false) %}select error('injected failure after finish');{% endif %}"]) }}
select o.* from {{ SOURCE }} as o
where {{ registered_batches_predicate('o.batch_id') }}
""".replace("DEPENDENCIES", dependencies).replace("SOURCE", source)
        )
    result = BatchProject(project, root / "warehouse.duckdb", root / "work")
    result.sql("create schema bronze")
    result.sql(
        "create table bronze.batch_registry "
        "(batch_id varchar primary key, status varchar, observed_at timestamp)"
    )
    result.sql(
        "create table bronze.observations "
        "(observation_id varchar, batch_id varchar, amount integer)"
    )
    return result


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> BatchProject:
    result = _project(tmp_path_factory.mktemp("registered-batches"))
    result.batch("newer", "2026-10-12", 20)
    _assert_ok(result.run())
    return result


@pytest.fixture
def batches(built: BatchProject, tmp_path: Path) -> BatchProject:
    project = tmp_path / "project"
    shutil.copytree(built.project, project)
    warehouse = tmp_path / "warehouse.duckdb"
    shutil.copyfile(built.warehouse, warehouse)
    return BatchProject(project, warehouse, tmp_path / "work")


@pytest.mark.parametrize("full_refresh", [False, True], ids=["incremental", "full-refresh"])
def test_post_hook_failure_rolls_back_target_and_checkpoint(
    batches: BatchProject, full_refresh: bool
) -> None:
    before = batches.snapshot()
    batches.batch("next", "2026-10-13", 30)
    args = ["--select", "parent", *(["--full-refresh"] if full_refresh else [])]
    failed = batches.run(*args, "--vars", '{"fail_after_finish": true}')
    _assert_error(failed, "injected failure after finish")
    assert batches.snapshot() == before

    _assert_ok(batches.run(*args))
    rows, checkpoint = batches.snapshot()
    assert rows == [("observation-newer", "newer", 20), ("observation-next", "next", 30)]
    assert checkpoint == [("batch", "newer", "1"), ("batch", "next", "1"), ("metadata", "", "1")]


def test_empty_registry_and_loaded_batch_without_observations(tmp_path: Path) -> None:
    batches = _project(tmp_path)
    _assert_ok(batches.run("--select", "parent"))
    assert batches.snapshot() == ([], [("metadata", "", "1")])

    batches.batch("empty", "2026-10-12", None)
    _assert_ok(batches.run("--select", "parent"))
    assert batches.snapshot() == ([], [("batch", "empty", "1"), ("metadata", "", "1")])


def test_late_older_batch_is_consumed_once_and_matches_full_refresh(batches: BatchProject) -> None:
    batches.batch("older", "2020-01-01", 10)
    batches.sql("insert into bronze.batch_registry values ('not-loaded', 'failed', '2026-10-14')")
    batches.sql("insert into bronze.observations values ('excluded', 'not-loaded', 999)")
    _assert_ok(batches.run("--select", "parent"))
    expected = (
        [("observation-newer", "newer", 20), ("observation-older", "older", 10)],
        [("batch", "newer", "1"), ("batch", "older", "1"), ("metadata", "", "1")],
    )
    assert batches.snapshot() == expected
    _assert_ok(batches.run("--select", "parent"))
    assert batches.snapshot() == expected
    _assert_ok(batches.run("--select", "parent", "--full-refresh"))
    assert batches.snapshot() == expected


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("drop table main.__checkpoint_parent", "Missing catalogue checkpoint"),
        (
            "update main.__checkpoint_parent set processing_version = 'old'",
            "Invalid catalogue checkpoint",
        ),
        (
            "update main.__checkpoint_parent set record_kind = 'unexpected' where record_kind = 'batch'",
            "Invalid catalogue checkpoint",
        ),
        (
            "delete from main.__checkpoint_parent where record_kind = 'metadata'",
            "Invalid catalogue checkpoint",
        ),
        (
            "insert into main.__checkpoint_parent values ('batch', 'unknown', '1')",
            "Invalid catalogue checkpoint",
        ),
        (
            "insert into main.__checkpoint_parent values ('metadata', 'extra', '1')",
            "Invalid catalogue checkpoint",
        ),
    ],
    ids=[
        "missing",
        "wrong-version",
        "record-kind",
        "no-metadata",
        "unknown-batch",
        "extra-metadata",
    ],
)
def test_invalid_checkpoint_fails_before_target_mutation(
    batches: BatchProject, corruption: str, message: str
) -> None:
    batches.sql(corruption)
    batches.batch("next", "2026-10-13", 30)
    before_target = batches.sql("select * from main.parent order by observation_id")
    missing = corruption.startswith("drop")
    before_checkpoint = None if missing else batches.snapshot()[1]
    _assert_error(batches.run("--select", "parent"), message)
    assert batches.sql("select * from main.parent order by observation_id") == before_target
    if missing:
        assert (
            batches.sql(
                "select table_name from information_schema.tables "
                "where table_schema = 'main' and table_name = '__checkpoint_parent'"
            )
            == []
        )
    else:
        assert batches.snapshot()[1] == before_checkpoint


@pytest.mark.parametrize(
    "corruption", ["duplicate", "null-version", "null-kind", "null-batch", "bad-schema"]
)
def test_legacy_checkpoint_tables_fail_closed(batches: BatchProject, corruption: str) -> None:
    batches.sql("alter table main.__checkpoint_parent rename to old_checkpoint")
    batches.sql("create table main.__checkpoint_parent as select * from main.old_checkpoint")
    if corruption == "duplicate":
        for column in ("record_kind", "batch_id", "processing_version"):
            batches.sql(f"alter table main.__checkpoint_parent alter column {column} set not null")
        batches.sql(
            "insert into main.__checkpoint_parent "
            "select * from main.old_checkpoint where record_kind = 'batch'"
        )
    elif corruption == "null-version":
        batches.sql(
            "update main.__checkpoint_parent set processing_version = null where record_kind = 'batch'"
        )
    elif corruption in ("null-kind", "null-batch"):
        column = "record_kind" if corruption == "null-kind" else "batch_id"
        batches.sql(
            f"update main.__checkpoint_parent set {column} = null where record_kind = 'batch'"
        )
    else:
        batches.sql("alter table main.__checkpoint_parent drop column record_kind")
    batches.batch("next", "2026-10-13", 30)
    before_target = batches.sql("select * from main.parent order by observation_id")
    before_checkpoint = batches.sql("select * from main.__checkpoint_parent order by all")
    result = batches.run("--select", "parent")
    if corruption == "bad-schema":
        assert result.returncode != 0, result.stdout
    elif corruption == "duplicate":
        _assert_error(result, "Invalid catalogue checkpoint; run --full-refresh")
    else:
        _assert_error(result, "Invalid catalogue checkpoint")
    assert batches.sql("select * from main.parent order by observation_id") == before_target
    assert batches.sql("select * from main.__checkpoint_parent order by all") == before_checkpoint


def test_missing_target_rebuilds_all_batches_despite_orphan_checkpoint(
    batches: BatchProject,
) -> None:
    batches.sql("drop table main.parent")
    batches.batch("older", "2020-01-01", 10)
    _assert_ok(batches.run("--select", "parent"))
    assert batches.snapshot() == (
        [("observation-newer", "newer", 20), ("observation-older", "older", 10)],
        [("batch", "newer", "1"), ("batch", "older", "1"), ("metadata", "", "1")],
    )


def test_full_refresh_resets_only_selected_checkpoint(batches: BatchProject) -> None:
    siblings = {model: batches.snapshot(model) for model in ("child", "sibling")}
    batches.sql("update main.__checkpoint_parent set processing_version = 'obsolete'")
    batches.batch("next", "2026-10-13", 30)
    _assert_ok(
        batches.run("--select", "parent", "--full-refresh", "--vars", '{"processing_version": "2"}')
    )
    assert batches.snapshot()[1] == [
        ("batch", "newer", "2"),
        ("batch", "next", "2"),
        ("metadata", "", "2"),
    ]
    assert {model: batches.snapshot(model) for model in siblings} == siblings


def test_child_rejects_stale_parent_then_builds_after_ancestors(batches: BatchProject) -> None:
    before = batches.snapshot("child")
    batches.batch("next", "2026-10-13", 30)
    _assert_error(batches.run("--select", "child"), "Stale catalogue dependency parent")
    assert batches.snapshot("child") == before
    _assert_ok(batches.run("--select", "+child"))
    assert batches.snapshot("child") == batches.snapshot("parent")
    incremental = batches.snapshot("child")
    _assert_ok(batches.run("--select", "+child", "--full-refresh"))
    assert batches.snapshot("child") == incremental


@pytest.mark.parametrize("checkpoint", ["missing", "valid-table"])
def test_existing_view_target_requires_explicit_full_refresh_without_mutation(
    batches: BatchProject, checkpoint: str
) -> None:
    batches.sql("drop table main.parent")
    batches.sql(
        "create view main.parent as select 'retained-view-observation'::varchar as observation_id, "
        "'retained-view-batch'::varchar as batch_id, -50::integer as amount"
    )
    if checkpoint == "missing":
        batches.sql("drop table main.__checkpoint_parent")
    batches.batch("next", "2026-10-13", 30)
    before = (batches.relation_state("parent"), batches.relation_state("__checkpoint_parent"))
    _assert_error(batches.run("--select", "parent"), "--full-refresh --select parent")
    assert (
        batches.relation_state("parent"),
        batches.relation_state("__checkpoint_parent"),
    ) == before

    _assert_ok(batches.run("--select", "parent", "--full-refresh"))
    assert batches.relation_state("parent")[0] == "BASE TABLE"
    assert batches.snapshot() == (
        [("observation-newer", "newer", 20), ("observation-next", "next", 30)],
        [("batch", "newer", "1"), ("batch", "next", "1"), ("metadata", "", "1")],
    )


@pytest.mark.parametrize("checkpoint", ["missing", "valid-table"])
def test_failed_explicit_refresh_preserves_existing_view_and_checkpoint(
    batches: BatchProject, checkpoint: str
) -> None:
    batches.sql("drop table main.parent")
    batches.sql(
        "create view main.parent as select 'retained-view-observation'::varchar as observation_id, "
        "'retained-view-batch'::varchar as batch_id, -50::integer as amount"
    )
    if checkpoint == "missing":
        batches.sql("drop table main.__checkpoint_parent")
    batches.batch("next", "2026-10-13", 30)
    before = (batches.relation_state("parent"), batches.relation_state("__checkpoint_parent"))
    result = batches.run(
        "--select", "parent", "--full-refresh", "--vars", '{"fail_after_finish": true}'
    )
    _assert_error(result, "injected failure after finish")
    assert (
        batches.relation_state("parent"),
        batches.relation_state("__checkpoint_parent"),
    ) == before


@pytest.mark.parametrize(
    "corruption",
    ["integer-version", "nullable-version", "missing-column", "view", "reordered", "extra-column"],
)
def test_incompatible_checkpoint_schema_requires_refresh_before_mutation_and_is_repairable(
    batches: BatchProject, corruption: str
) -> None:
    checkpoint = "main.__checkpoint_parent"
    batches.sql(f"alter table {checkpoint} rename to retained_checkpoint")
    if corruption == "view":
        batches.sql(f"create view {checkpoint} as select * from main.retained_checkpoint")
    else:
        columns = {
            "record_kind": "varchar not null",
            "batch_id": "varchar not null",
            "processing_version": "varchar not null",
        }
        if corruption == "integer-version":
            columns["processing_version"] = "integer not null"
        elif corruption == "nullable-version":
            columns["processing_version"] = "varchar"
        elif corruption == "reordered":
            columns = {"processing_version": columns.pop("processing_version"), **columns}
        elif corruption == "extra-column":
            columns["extra_claim"] = "varchar default 'retained'"
        declaration = ", ".join(f"{name} {kind}" for name, kind in columns.items())
        batches.sql(
            f"create table {checkpoint} ({declaration}, primary key(record_kind, batch_id))"
        )
        batches.sql(
            f"insert into {checkpoint}(record_kind, batch_id, processing_version) "
            "select record_kind, batch_id, processing_version from main.retained_checkpoint"
        )
        if corruption == "missing-column":
            batches.sql(
                f"alter table {checkpoint} rename column processing_version to version_claim"
            )
    batches.batch("next", "2026-10-13", 30)
    before = (batches.relation_state("parent"), batches.relation_state("__checkpoint_parent"))
    _assert_error(batches.run("--select", "parent"), "--full-refresh --select parent")
    assert (
        batches.relation_state("parent"),
        batches.relation_state("__checkpoint_parent"),
    ) == before
    _assert_ok(batches.run("--select", "parent", "--full-refresh"))
    repaired = batches.relation_state("__checkpoint_parent")
    assert repaired[0] == "BASE TABLE"
    assert repaired[1] == [
        ("record_kind", "VARCHAR", "NO"),
        ("batch_id", "VARCHAR", "NO"),
        ("processing_version", "VARCHAR", "NO"),
    ]
    assert batches.snapshot() == (
        [("observation-newer", "newer", 20), ("observation-next", "next", 30)],
        [("batch", "newer", "1"), ("batch", "next", "1"), ("metadata", "", "1")],
    )
