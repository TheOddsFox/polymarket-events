"""The packaged profile must constrain each real dbt-duckdb connection."""

from __future__ import annotations

import json
from contextvars import copy_context
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments.local import LocalEnvironment
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt_common.context import set_invocation_context

from oddsfox_catalogue import dbt_runner
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.dbt_runner import dbt_environment
from oddsfox_catalogue.resources import dbt_project_dir


def _profile(settings: Settings, *, threads: int | None = 2):
    settings.temporary_dir.mkdir(parents=True)
    settings.warehouse_path.parent.mkdir(parents=True)
    env = dbt_environment(settings)
    if threads is None:
        env.pop("CATALOGUE_DUCKDB_THREADS")
    else:
        env["CATALOGUE_DUCKDB_THREADS"] = str(threads)
    raw = load_yaml_text(dbt_project_dir().joinpath("profiles.yml").read_text())

    def render():
        set_invocation_context(env)
        return Profile.render_profile(
            raw["oddsfox_catalogue"], "oddsfox_catalogue", None, ProfileRenderer()
        )[1]

    rendered = copy_context().run(render)
    scheduler_threads = rendered.pop("threads")
    rendered.pop("type")
    return scheduler_threads, DuckDBCredentials.from_dict(rendered)


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_packaged_profile_constrains_engine_with_single_model_scheduler(tmp_path, threads):
    settings = Settings(tmp_path)
    scheduler_threads, credentials = _profile(settings, threads=threads)
    env = LocalEnvironment(credentials)
    handle = None
    try:
        handle = env.handle()
        actual = (
            handle.cursor()
            .execute(
                "SELECT current_setting('threads'), current_setting('memory_limit'), "
                "current_setting('temp_directory'), current_setting('max_temp_directory_size')"
            )
            .fetchone()
        )
        assert actual == (threads or 1, "1.8 GiB", str(settings.temporary_dir), "8.0 GiB")
        assert scheduler_threads == 1
    finally:
        if handle is not None:
            handle.close()
        env.close()


@pytest.mark.parametrize("scheduler", [None, 2])
def test_tiny_real_dbt_scheduler_override_preserves_two_thread_engine(
    tmp_path: Path, monkeypatch, scheduler
):
    project = tmp_path / "project"
    models = project / "models"
    models.mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: scheduler_probe\nversion: '1.0'\nconfig-version: 2\n"
        "profile: oddsfox_catalogue\nmodels:\n  scheduler_probe:\n    +materialized: table\n"
    )
    (models / "scheduler_probe.sql").write_text(
        "SELECT current_setting('threads')::INTEGER AS engine_threads, "
        "current_setting('memory_limit') AS engine_memory_limit\n"
    )
    actual_run = dbt_runner.subprocess.run
    calls = []

    def bounded_run(*args, **kwargs):
        calls.append(True)
        return actual_run(*args, **kwargs, timeout=90)

    monkeypatch.setattr(dbt_runner.subprocess, "run", bounded_run)
    warehouse, work = tmp_path / "tiny.duckdb", tmp_path / "work"
    arguments = ["run"] + ([] if scheduler is None else ["--threads", str(scheduler)])
    result = dbt_runner.run_dbt_at(
        arguments,
        project_dir=project,
        profiles_dir=dbt_project_dir(),
        warehouse=warehouse,
        work_dir=work,
        memory_limit="32MiB",
        max_temp_bytes=32 * 1024**2,
        threads=2,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]
    assert calls == [True]
    recorded = json.loads((work / "target" / "run_results.json").read_text())
    assert recorded["args"].get("threads") == scheduler
    assert f"Concurrency: {scheduler or 1} threads" in result.stdout
    assert len(recorded["results"]) == 1 and recorded["results"][0]["status"] == "success"
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        assert connection.execute("SELECT * FROM main.scheduler_probe").fetchall() == [
            (2, "32.0 MiB")
        ]


@pytest.mark.parametrize("legacy_settings", [False, True])
def test_second_adapter_cursor_after_spill(tmp_path: Path, legacy_settings):
    settings = Settings(tmp_path)
    settings = replace(
        settings,
        load=replace(settings.load, duckdb_memory_limit="32MiB", duckdb_threads=1),
        capture=replace(settings.capture, max_temp_bytes=256 * 1024**2),
    )
    _, credentials = _profile(settings, threads=1)
    if legacy_settings:
        # Reproduce the old profile's per-cursor SETs at the same finite limits.
        credentials.settings = dict(credentials.config_options)
        credentials.settings.pop("threads")
        credentials.config_options = {"threads": 1}
    env = LocalEnvironment(credentials)
    first = second = None
    try:
        first = env.handle()
        first.cursor().execute(
            "CREATE TABLE spill_source AS SELECT i, hash(i) AS sort_key FROM range(2000000) t(i)"
        )
        first.cursor().execute("SET enable_profiling = 'json'")
        profile = tmp_path / "spill-profile.json"
        first.cursor().execute("SET profiling_output = ?", [str(profile)])
        first.cursor().execute(
            "CREATE TABLE spill_sorted AS SELECT i, sort_key FROM spill_source ORDER BY sort_key"
        )
        assert first.cursor().fetchone() == (2000000,)
        spill_bytes = json.loads(profile.read_text())["system_peak_temp_dir_size"]
        assert 0 < spill_bytes < settings.capture.max_temp_bytes, "query must actually spill"
        first.cursor().execute("SET enable_profiling = 'no_output'")
        if legacy_settings:
            with pytest.raises(duckdb.NotImplementedException, match="Cannot switch temporary"):
                env.handle()
        else:
            second = env.handle()
            assert second.cursor().execute(
                "SELECT count(*), sum(i) FROM spill_sorted"
            ).fetchone() == (
                2000000,
                2000000 * 1999999 // 2,
            )
            assert second.cursor().execute(
                "SELECT current_setting('threads'), current_setting('memory_limit'), "
                "current_setting('temp_directory'), current_setting('max_temp_directory_size')"
            ).fetchone() == (1, "32.0 MiB", str(settings.temporary_dir), "256.0 MiB")
    finally:
        if second is not None:
            second.close()
        if first is not None:
            first.close()
        env.close()


def test_invalid_memory_limit_fails_before_adapter_opens_warehouse(tmp_path):
    settings = Settings(tmp_path)
    settings = replace(settings, load=replace(settings.load, duckdb_memory_limit="not-a-limit"))
    _, credentials = _profile(settings)
    env = LocalEnvironment(credentials)
    try:
        with pytest.raises(duckdb.ParserException, match="Memory must have a number"):
            env.handle()
        assert not settings.warehouse_path.exists()
        assert not list(settings.temporary_dir.iterdir())
    finally:
        env.close()
