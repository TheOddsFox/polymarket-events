"""Certification rejects incomplete execution and changed certified inputs."""

import hashlib
import json
from copy import deepcopy

import pytest

from fakes.harness import make_settings
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.certification import (
    BuildInvalid,
    _node_inventory,
    _validate_artifacts,
    assert_build_valid,
    certify_build,
    mark_dirty,
    prepare_dbt_execution,
    read_build_validity,
)
from oddsfox_catalogue.dbt_runner import is_selected_dbt_command
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.semantics import (
    OPERATIONAL_RELATIONS,
    SEMANTIC_RELATIONS,
    canonical_json_chunks,
    json_descriptor,
)


def artifacts(settings):
    target = settings.state_dir / "dbt" / "invocations" / "fresh" / "target"
    nodes = {}
    for relation in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS):
        if relation.startswith("bronze."):
            continue
        name = relation.split(".")[1]
        nodes["model." + name] = {"name": name, "resource_type": "model", "config": {}}
    nodes["test.required"] = {"name": "required", "resource_type": "test", "config": {}}
    nodes["test.second"] = {"name": "second", "resource_type": "test", "config": {}}
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "invocation_id": "fresh",
        },
        "nodes": nodes,
    }
    results = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "invocation_id": "fresh",
        },
        "args": {"which": "build", "vars": {}},
        "results": [
            {"unique_id": key, "status": "success" if value["resource_type"] == "model" else "pass"}
            for key, value in nodes.items()
        ],
    }
    return target, manifest, results


@pytest.fixture(autouse=True)
def trusted_preflight(monkeypatch, tmp_path):
    _, manifest, _ = artifacts(make_settings(tmp_path))
    monkeypatch.setattr(
        "oddsfox_catalogue.certification._parse_required_nodes",
        lambda *_: _node_inventory(deepcopy(manifest)),
    )


def write_artifacts(target, manifest, results):
    target.mkdir(parents=True, exist_ok=True)
    manifest["metadata"].setdefault("invocation_started_at", iso_utc(utc_now()))
    (target / "manifest.json").write_text(json.dumps(manifest))
    (target / "run_results.json").write_text(json.dumps(results))


def test_status_inspection_does_not_create_state(tmp_path):
    settings = make_settings(tmp_path)
    assert read_build_validity(settings) is None
    assert not settings.state_dir.exists()


@pytest.mark.parametrize(
    "flag",
    [
        "--select",
        "--select=fqn:*",
        "--models",
        "--models=fqn:*",
        "-s",
        "-sfqn:*",
        "-m",
        "-mfqn:*",
        "--exclude",
        "--exclude=x",
        "--selector",
        "--selector=x",
    ],
)
def test_explicit_cli_selection_never_certifies(flag):
    assert is_selected_dbt_command(["build", flag])
    assert not is_selected_dbt_command(["build", "--full-refresh"])


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "skipped",
        "warned",
        "failed",
        "invocation",
        "schema",
        "run",
        "deferred",
        "empty",
        "missing-model",
    ],
)
def test_execution_artifacts_must_prove_every_required_node(tmp_path, mutation):
    settings = make_settings(tmp_path)
    target, manifest, results = artifacts(settings)
    prepare_dbt_execution(settings, target)
    if mutation == "missing":
        results["results"].pop()
    elif mutation == "duplicate":
        results["results"].append(deepcopy(results["results"][0]))
    elif mutation in {"skipped", "warned", "failed"}:
        results["results"][-1]["status"] = mutation
    elif mutation == "invocation":
        results["metadata"]["invocation_id"] = "old"
    elif mutation == "schema":
        results["metadata"]["dbt_schema_version"] = "future"
    elif mutation == "run":
        results["args"]["which"] = "run"
    elif mutation in {"deferred", "empty"}:
        results["args"]["defer" if mutation == "deferred" else "empty"] = True
    elif mutation == "missing-model":
        manifest["nodes"].pop(next(iter(manifest["nodes"])))
    write_artifacts(target, manifest, results)
    with pytest.raises(BuildInvalid):
        _validate_artifacts(settings, target)


@pytest.mark.parametrize(
    "damage", ["deleted", "disabled", "severity", "checksum", "enabled-type", "vars"]
)
def test_required_test_inventory_is_independent_of_build_artifacts(tmp_path, damage):
    settings = make_settings(tmp_path)
    target, manifest, results = artifacts(settings)
    prepare_dbt_execution(settings, target)
    if damage == "deleted":
        del manifest["nodes"]["test.required"]
    elif damage == "disabled":
        manifest["nodes"]["test.required"]["config"]["enabled"] = False
    elif damage == "severity":
        manifest["nodes"]["test.required"]["config"]["severity"] = "warn"
    elif damage == "checksum":
        manifest["nodes"]["test.required"]["checksum"] = {"name": "sha256", "checksum": "different"}
    elif damage == "enabled-type":
        manifest["nodes"]["test.required"]["config"]["enabled"] = 1
    else:
        results["args"]["vars"] = {"projection_version": "changed"}
    if damage in {"deleted", "disabled"}:
        results["results"] = [
            row for row in results["results"] if row["unique_id"] != "test.required"
        ]
    write_artifacts(target, manifest, results)
    with pytest.raises(BuildInvalid):
        _validate_artifacts(settings, target)


def test_receipt_detects_changes_and_tampering(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    target, manifest, results = artifacts(settings)
    prepare_dbt_execution(settings, target)
    write_artifacts(target, manifest, results)
    binding = {"effective_vars": {}, "warehouse": "same"}
    monkeypatch.setattr("oddsfox_catalogue.certification._binding", lambda *_: deepcopy(binding))
    monkeypatch.setattr("oddsfox_catalogue.pipeline.open_event_drop_warning", lambda *_: None)
    certify_build(settings, target)
    assert assert_build_valid(settings)["binding"] == binding
    binding["warehouse"] = "external edit"
    with pytest.raises(BuildInvalid, match="changed"):
        assert_build_valid(settings)
    binding["warehouse"] = "same"
    with Ledger(settings.ledger_path) as ledger:
        row = ledger.build_validity()
        ledger.set_build_validity(
            "valid",
            row["updated_at"],
            payload_json=row["payload_json"] + " ",
            payload_sha256=row["payload_sha256"],
        )
    with pytest.raises(BuildInvalid, match="checksum"):
        assert_build_valid(settings)


def test_reused_successful_artifacts_cannot_certify_a_new_mutation(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    target, manifest, results = artifacts(settings)
    prepare_dbt_execution(settings, target)
    manifest["metadata"]["invocation_started_at"] = "2000-01-01T00:00:00Z"
    write_artifacts(target, manifest, results)
    with pytest.raises(BuildInvalid, match="predate"):
        certify_build(settings, target)
    with pytest.raises(BuildInvalid, match="fresh"):
        prepare_dbt_execution(settings, target)
    other = settings.state_dir / "dbt" / "new" / "target"
    prepare_dbt_execution(settings, other)
    with pytest.raises(BuildInvalid, match="different prepared"):
        certify_build(settings, target)


def test_changed_sources_cannot_certify_completed_artifacts(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    target, manifest, results = artifacts(settings)
    prepare_dbt_execution(settings, target)
    write_artifacts(target, manifest, results)
    monkeypatch.setattr("oddsfox_catalogue.certification._model_revision", lambda *_: "changed")
    with pytest.raises(BuildInvalid, match="changed during"):
        certify_build(settings, target)


def test_failed_or_interrupted_mutation_leaves_previous_receipt_dirty(tmp_path):
    settings = make_settings(tmp_path)
    mark_dirty(settings, "before load")
    assert read_build_validity(settings)["status"] == "dirty"
    with pytest.raises(BuildInvalid, match="dirty"):
        assert_build_valid(settings)


def test_canonical_descriptor_has_exact_bounded_bytes():
    value = {"z": [1, None], "a": "test"}
    payload = b"".join(canonical_json_chunks(value))
    assert payload == b'{"a":"test","z":[1,null]}\n'
    assert json_descriptor(value) == {
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    with pytest.raises(ValueError, match="size limit"):
        list(canonical_json_chunks(value, max_bytes=len(payload) - 1))


def test_preflight_failure_never_starts_a_mutating_build(tmp_path, monkeypatch):
    import subprocess

    from oddsfox_catalogue.dbt_runner import run_dbt

    settings = make_settings(tmp_path)
    calls = []
    monkeypatch.undo()

    def fail_parse(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "parse failed", "")

    monkeypatch.setattr("oddsfox_catalogue.dbt_runner.run_dbt_at", fail_parse)
    with pytest.raises(BuildInvalid, match="preflight parse failed"):
        run_dbt(settings, ["build", '--vars={"projection_version":"v2"}', "--full-refresh"])
    assert len(calls) == 1
    assert calls[0][:2] == ["parse", "--no-partial-parse"]
    assert "--full-refresh" not in calls[0]
    assert json.loads(calls[0][-1]) == {"projection_version": "v2"}
    assert read_build_validity(settings)["status"] == "dirty"
