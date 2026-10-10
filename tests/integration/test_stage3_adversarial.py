"""Corrupt execution artifacts cannot escape the CLI or destroy a valid publication."""

import json
import subprocess
from copy import deepcopy

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, copy_built
from fakes.harness import FIXED_NOW
from oddsfox_catalogue.certification import prepare_dbt_execution, read_build_validity
from oddsfox_catalogue.cli import main
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.publish import CURRENT_POINTER, current_release, publish_release
from oddsfox_catalogue.rebuild import rebuild_and_verify


@pytest.fixture(scope="module")
def certified_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("adversarial_recovery")
    build_warehouse(root)
    return root


@pytest.mark.parametrize(
    "damage",
    [
        "manifest_metadata",
        "result_metadata",
        "args",
        "node_config",
        "node_resource_type",
        "node_name",
        "result_identity",
    ],
)
def test_malformed_artifacts_report_a_controlled_failure_and_preserve_publication(
    certified_root, tmp_path, monkeypatch, capsys, damage
):
    settings = copy_built(certified_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW)
    pointer_bytes = (settings.published_dir / CURRENT_POINTER).read_bytes()
    source_target = next((settings.state_dir / "dbt/invocations").glob("*/target"))
    manifest = deepcopy(json.loads((source_target / "manifest.json").read_bytes()))
    results = deepcopy(json.loads((source_target / "run_results.json").read_bytes()))

    def corrupt_successful_build(actual_settings, args):
        target = actual_settings.state_dir / "dbt/invocations/corrupt/target"
        prepare_dbt_execution(actual_settings, target)
        target.mkdir(parents=True)
        manifest["metadata"]["invocation_started_at"] = iso_utc(utc_now())
        if damage == "manifest_metadata":
            manifest["metadata"] = None
        elif damage == "result_metadata":
            results["metadata"] = []
        elif damage == "args":
            results["args"] = []
        elif damage in {"node_config", "node_resource_type", "node_name"}:
            node = next(
                node for node in manifest["nodes"].values() if node["resource_type"] == "model"
            )
            if damage == "node_config":
                node["config"] = False
            else:
                node["resource_type" if damage == "node_resource_type" else "name"] = []
        else:
            results["results"][0]["unique_id"] = []
        (target / "manifest.json").write_text(json.dumps(manifest))
        (target / "run_results.json").write_text(json.dumps(results))
        result = subprocess.CompletedProcess(args, 0, "dbt reported success\n", "")
        result.target_path = target
        return result

    monkeypatch.setattr("oddsfox_catalogue.cli.load_settings", lambda: settings)
    monkeypatch.setattr("oddsfox_catalogue.pipeline.run_dbt", corrupt_successful_build)
    assert main(["dbt", "build"]) in {1, 3}
    output = capsys.readouterr()
    assert "Traceback" not in output.out + output.err
    assert read_build_validity(settings)["status"] == "dirty"
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == pointer_bytes
    assert current_release(settings)["release_id"] == first.release_id


def test_raw_rebuild_detects_quarantine_content_and_observation_history_schema_drift(
    certified_root, tmp_path
):
    settings = copy_built(certified_root, tmp_path)
    with duckdb.connect(str(settings.warehouse_path)) as connection:
        connection.execute(
            "CREATE OR REPLACE VIEW core.quarantine_market_outcomes AS "
            "SELECT venue, market_id, observation_id, identity_error "
            "FROM core.int_market_outcomes WHERE NOT usable "
            "UNION ALL SELECT 'polymarket', '10', 'external', 'invented identity failure'"
        )
        connection.execute("ALTER TABLE history.market_history ADD COLUMN unexpected VARCHAR")
    report = rebuild_and_verify(settings)
    assert not report.matched
    for relation in (
        "core.quarantine_market_outcomes:",
        "published:quarantine:",
        "schema:history.market_history:",
    ):
        assert any(problem.startswith(relation) for problem in report.mismatches), relation
