"""Real builds bind all semantic relations and refuse stale, partial and changed state."""

from dataclasses import replace

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, copy_built
from fakes.fake_gamma import FakeGamma
from fakes.harness import build_runtime, make_settings
from fakes.world import demo_world
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.certification import BuildInvalid, assert_build_valid, read_build_validity
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending
from oddsfox_catalogue.pipeline import dbt_stage
from oddsfox_catalogue.semantics import SEMANTIC_RELATIONS


@pytest.fixture(scope="module")
def certified_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("certified_catalogue")
    build_warehouse(root)
    return root


def test_complete_build_certifies_all_declared_semantics(certified_root, tmp_path):
    settings = copy_built(certified_root, tmp_path)
    receipt = assert_build_valid(settings)
    assert set(receipt["binding"]["warehouse"]["relations"]) == set(SEMANTIC_RELATIONS)
    assert set(receipt["binding"]["published"]) == {
        "events",
        "markets",
        "outcomes",
        "event_tags",
        "event_series",
        "market_event_bridge",
        "quarantine",
    }
    assert receipt["binding"]["capture"]["batch_ids"]


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE history.market_metrics SET volume='99999'",
        "UPDATE history.market_history SET question='externally changed'",
        "DELETE FROM core.market_event_bridge",
        "UPDATE bronze.event_observations SET payload=json_merge_patch(payload, '{\"external\": true}')",
        "ALTER TABLE core.events_current ADD COLUMN externally_added INTEGER",
    ],
)
def test_external_changes_to_every_layer_block_publication(certified_root, tmp_path, sql):
    settings = copy_built(certified_root, tmp_path)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute(sql)
    with pytest.raises(BuildInvalid, match="changed"):
        assert_build_valid(settings)


def test_processing_metadata_and_invocation_snapshots_are_not_semantic(certified_root, tmp_path):
    settings = copy_built(certified_root, tmp_path)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute(
            "UPDATE bronze.batch_registry SET loaded_at=current_timestamp, load_id='new-operational-id'"
        )
        connection.execute(
            "INSERT INTO marts.catalogue_snapshots SELECT * FROM marts.catalogue_snapshots"
        )
    assert_build_valid(settings)


def test_changed_quality_requires_new_certification(certified_root, tmp_path):
    settings = copy_built(certified_root, tmp_path)
    changed = replace(settings, quality=replace(settings.quality, quarantine_max_ratio=0.0))
    with pytest.raises(BuildInvalid, match="changed"):
        assert_build_valid(changed)


def test_subset_build_stays_dirty_and_uses_a_fresh_target(certified_root, tmp_path):
    settings = copy_built(certified_root, tmp_path)
    result = dbt_stage(settings, ["build", "--select", "events_current"])
    assert result.returncode == 0, result.stdout[-3000:]
    assert read_build_validity(settings)["status"] == "dirty"
    with pytest.raises(BuildInvalid, match="dirty"):
        assert_build_valid(settings)
    assert result.target_path.exists()
    assert len(list((settings.state_dir / "dbt" / "invocations").glob("*/target"))) == 2


def test_new_unloaded_capture_cannot_reuse_the_previous_build(certified_root, tmp_path):
    settings = copy_built(certified_root, tmp_path)
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    try:
        run_capture(runtime, "selected", market_ids=["10"])
    finally:
        runtime.ledger.close()
    with pytest.raises(BuildInvalid, match="unregistered"):
        assert_build_valid(settings)


def test_partial_load_invalidates_before_the_first_dlt_write(certified_root, tmp_path, monkeypatch):
    settings = copy_built(certified_root, tmp_path)
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    try:
        run_capture(runtime, "selected", market_ids=["10"])
    finally:
        runtime.ledger.close()

    def fail_before_write(*args):
        assert read_build_validity(settings)["status"] == "dirty"
        raise RuntimeError("interrupted first dlt write")

    monkeypatch.setattr("oddsfox_catalogue.load.runner._run_resource", fail_before_write)
    with Ledger(settings.ledger_path) as ledger, pytest.raises(RuntimeError, match="interrupted"):
        load_pending(LoadRuntime(settings=settings, ledger=ledger))
    with pytest.raises(BuildInvalid, match="dirty"):
        assert_build_valid(settings)


def test_successfully_absent_selected_capture_can_certify_empty_catalogue(tmp_path):
    runtime, _ = build_runtime(tmp_path, FakeGamma(demo_world()))
    try:
        run_capture(runtime, "selected", market_ids=["999999"])
    finally:
        runtime.ledger.close()
    settings = make_settings(tmp_path)
    with Ledger(settings.ledger_path) as ledger:
        load_pending(LoadRuntime(settings=settings, ledger=ledger))
    built = dbt_stage(settings, ["build"])
    assert built.returncode == 0, built.stdout[-3000:]
    receipt = assert_build_valid(settings)
    assert receipt["binding"]["published"]["events"]["rows"] == 0
    assert receipt["binding"]["published"]["markets"]["rows"] == 0


@pytest.mark.parametrize("damage", ["deleted", "disabled"])
def test_real_build_cannot_omit_or_disable_a_required_test(
    certified_root, tmp_path, monkeypatch, damage
):
    import json

    from oddsfox_catalogue.dbt_runner import run_dbt

    settings = copy_built(certified_root, tmp_path)

    def tampered_build(settings, args):
        result = run_dbt(settings, args)
        assert result.returncode == 0, result.stdout[-3000:]
        manifest_path = result.target_path / "manifest.json"
        results_path = result.target_path / "run_results.json"
        manifest = json.loads(manifest_path.read_bytes())
        results = json.loads(results_path.read_bytes())
        test_ids = [
            key for key, node in manifest["nodes"].items() if node["resource_type"] == "test"
        ]
        assert len(test_ids) > 1
        omitted = test_ids[0]
        if damage == "deleted":
            del manifest["nodes"][omitted]
        else:
            manifest["nodes"][omitted]["config"]["enabled"] = False
        results["results"] = [row for row in results["results"] if row["unique_id"] != omitted]
        assert any(row["status"] == "pass" for row in results["results"])
        manifest_path.write_text(json.dumps(manifest))
        results_path.write_text(json.dumps(results))
        return result

    monkeypatch.setattr("oddsfox_catalogue.pipeline.run_dbt", tampered_build)
    result = dbt_stage(settings, ["build"])
    assert result.returncode == 1
    assert "required model/test inventory" in result.stdout or "disabled catalogue" in result.stdout
    assert read_build_validity(settings)["status"] == "dirty"
    with pytest.raises(BuildInvalid, match="dirty"):
        assert_build_valid(settings)
