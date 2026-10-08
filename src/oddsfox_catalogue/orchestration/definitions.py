"""Dagster definitions for the catalogue pipeline.

Asset graph::

    raw/gamma_pages  ->  bronze/* (4 tables)  ->  dbt models  ->  publish/published_release

Every step runs in the same process (in_process_executor), under the run lock, so a
single host never runs two writers at once. Runs are queued by QueuedRunCoordinator
(see ops/dagster.yaml), so schedules cannot overlap.
"""

import json
import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetSelection,
    Definitions,
    Failure,
    MaterializeResult,
    RunFailureSensorContext,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    asset,
    define_asset_job,
    in_process_executor,
    multi_asset,
    run_failure_sensor,
    sensor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.config import Settings, load_settings
from oddsfox_catalogue.dbt_runner import DBT_DIR, assert_warehouse_released, run_dbt_at
from oddsfox_catalogue.ids import utc_now
from oddsfox_catalogue.pipeline import capture_stage, load_stage, publish_stage
from oddsfox_catalogue.publish import PublishBlocked

BRONZE_TABLES = (
    "event_observations",
    "market_observations",
    "quarantined_records",
    "batch_registry",
)
RAW_KEY = AssetKey(["raw", "gamma_pages"])
PUBLISHED_KEY = AssetKey(["publish", "published_release"])
# dbt marts that the published release reads. dagster-dbt keys models as <schema>/<name>.
PUBLISHED_INPUTS = (AssetKey(["marts", "mart_event_catalogue"]),)


def alerts_path(settings: Settings) -> Path:
    return settings.state_dir / "alerts.log"


def append_alert(settings: Settings, message: str) -> None:
    path = alerts_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{stamp} {message}\n")


def _dbt_project(settings: Settings) -> DbtProject:
    """Parse the project once so the manifest exists before definitions are built."""
    target = settings.state_dir / "dbt" / "target"
    result = run_dbt_at(
        ["parse"],
        project_dir=DBT_DIR,
        profiles_dir=DBT_DIR,
        warehouse=settings.warehouse_path,
        work_dir=settings.state_dir / "dbt",
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt parse failed:\n{result.stdout[-2000:]}")
    return DbtProject(
        project_dir=DBT_DIR,
        profiles_dir=DBT_DIR,
        target_path=target,
    )


def build_definitions(
    settings: Settings,
    *,
    transport: httpx.BaseTransport | None = None,
) -> Definitions:
    """Build the Dagster definitions for one project root. ``transport`` is for tests only."""
    # dbt reads the warehouse location from the environment. Dagster runs in this process,
    # so the variable is set once here and inherited by every dbt subprocess.
    os.environ["CATALOGUE_WAREHOUSE"] = str(settings.warehouse_path)
    project = _dbt_project(settings)

    @asset(
        key=RAW_KEY,
        group_name="raw",
        description="Immutable Gamma pages for one capture batch.",
        config_schema={"mode": str},
    )
    def gamma_pages(context: AssetExecutionContext) -> MaterializeResult:
        mode = context.op_execution_context.op_config["mode"]
        summary = capture_stage(settings, mode, transport=transport)
        if summary.status != "captured":
            raise Failure(f"capture ended with status {summary.status} for {summary.batch_id}")
        return MaterializeResult(
            metadata={
                "batch_id": summary.batch_id,
                "mode": mode,
                "pages_written": summary.pages_written,
                "records": summary.records,
            }
        )

    @multi_asset(
        outs={
            name: AssetOut(key=AssetKey(["bronze", name]), group_name="bronze", is_required=True)
            for name in BRONZE_TABLES
        },
        deps=[RAW_KEY],
        can_subset=False,
    )
    def bronze_tables(context: AssetExecutionContext) -> Iterator[MaterializeResult]:
        summary = load_stage(settings)
        for name in BRONZE_TABLES:
            yield MaterializeResult(
                asset_key=AssetKey(["bronze", name]),
                metadata={
                    "pages_loaded": summary.pages_loaded,
                    "event_rows": summary.event_rows,
                    "market_rows": summary.market_rows,
                    "quarantined": summary.quarantined,
                    "batches_registered": ", ".join(summary.batches_registered),
                },
            )

    @dbt_assets(manifest=project.manifest_path, project=project)
    def catalogue_dbt(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
        assert_warehouse_released(settings.warehouse_path)
        yield from dbt.cli(["build"], context=context).stream()

    @asset(
        key=PUBLISHED_KEY,
        group_name="publish",
        deps=list(PUBLISHED_INPUTS),
        description="Parquet release written only after a passing dbt build.",
    )
    def published_release(context: AssetExecutionContext) -> MaterializeResult:
        try:
            release = publish_stage(settings, now=utc_now)
        except PublishBlocked as exc:
            raise Failure(str(exc)) from exc
        return MaterializeResult(
            metadata={"release_id": release.release_id, "path": str(release.path)}
        )

    dbt_resource = DbtCliResource(
        project_dir=DBT_DIR,
        profiles_dir=DBT_DIR,
        target_path=settings.state_dir / "dbt" / "target",
        dbt_executable=str(Path(sys.executable).with_name("dbt")),
    )

    def job_config(mode: str | None) -> dict[str, Any] | None:
        if mode is None:
            return None
        return {"ops": {"raw__gamma_pages": {"config": {"mode": mode}}}}

    everything = AssetSelection.all()
    dbt_only = AssetSelection.assets(*_dbt_model_keys(project))
    bootstrap = define_asset_job(
        "bootstrap",
        selection=everything,
        config=job_config("bootstrap"),
        executor_def=in_process_executor,
    )
    daily_refresh = define_asset_job(
        "daily_refresh",
        selection=everything,
        config=job_config("daily"),
        executor_def=in_process_executor,
    )
    weekly_reconcile = define_asset_job(
        "weekly_reconcile",
        selection=everything,
        config=job_config("reconcile"),
        executor_def=in_process_executor,
    )
    replay = define_asset_job(
        "replay",
        selection=AssetSelection.assets(*_bronze_keys())
        | dbt_only
        | AssetSelection.assets(PUBLISHED_KEY),
        executor_def=in_process_executor,
    )
    validate = define_asset_job("validate", selection=dbt_only, executor_def=in_process_executor)
    publish = define_asset_job(
        "publish", selection=AssetSelection.assets(PUBLISHED_KEY), executor_def=in_process_executor
    )

    daily_schedule = ScheduleDefinition(
        name="daily_refresh_schedule",
        job=daily_refresh,
        cron_schedule=settings.schedule.daily_cron,
        execution_timezone="UTC",
    )
    weekly_schedule = ScheduleDefinition(
        name="weekly_reconcile_schedule",
        job=weekly_reconcile,
        cron_schedule=settings.schedule.weekly_cron,
        execution_timezone="UTC",
    )

    @run_failure_sensor(name="alert_on_run_failure")
    def alert_on_run_failure(context: RunFailureSensorContext) -> None:
        append_alert(
            settings,
            f"run {context.dagster_run.run_id} ({context.dagster_run.job_name}) failed: "
            f"{context.failure_event.message}",
        )

    @sensor(name="missed_run_alert", job=daily_refresh, minimum_interval_seconds=3600)
    def missed_run_alert(context) -> SkipReason | RunRequest | None:
        if not settings.ledger_path.exists():
            return SkipReason("no ledger yet")
        ledger = Ledger(settings.ledger_path)
        try:
            batches = ledger.list_batches("captured") + ledger.list_batches("loaded")
        finally:
            ledger.close()
        if not batches:
            return SkipReason("no captured batches yet")
        newest = max(batch["started_at"] for batch in batches)
        started = datetime.fromisoformat(newest.replace("Z", "+00:00"))
        age_hours = (utc_now() - started).total_seconds() / 3600
        if age_hours > 30:
            append_alert(settings, f"no capture for {age_hours:.1f}h (newest {newest})")
        return SkipReason(f"newest capture {age_hours:.1f}h old")

    return Definitions(
        assets=[gamma_pages, bronze_tables, catalogue_dbt, published_release],
        jobs=[bootstrap, daily_refresh, weekly_reconcile, replay, validate, publish],
        schedules=[daily_schedule, weekly_schedule],
        sensors=[alert_on_run_failure, missed_run_alert],
        resources={"dbt": dbt_resource},
        executor=in_process_executor,
    )


def _bronze_keys() -> list[AssetKey]:
    return [AssetKey(["bronze", name]) for name in BRONZE_TABLES]


def _dbt_model_keys(project: DbtProject) -> list[AssetKey]:
    """Asset keys of every dbt model, as dagster-dbt names them."""
    manifest = json.loads(Path(project.manifest_path).read_text(encoding="utf-8"))
    keys: list[AssetKey] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") == "model":
            keys.append(AssetKey([node["schema"], node["name"]]))
    return keys


def _module_definitions() -> Definitions:
    return build_definitions(load_settings())


defs = None
if os.environ.get("CATALOGUE_DAGSTER_LAZY") != "1":
    defs = _module_definitions()
