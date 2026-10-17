from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import duckdb
import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("optimization_campaign", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


def package(root: Path, **files: str) -> Path:
    root.mkdir(parents=True)
    for name, content in {"__init__.py": "", **files}.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return root


def inventory(path: Path) -> dict[str, str]:
    return campaign.bench.source_hashes(path)


def test_campaign_budget_is_shared_and_keeps_holdback_and_shutdown_reserve():
    default = campaign.bench.Budget()
    campaign_budget = campaign.bench.Budget(limit_s=1800)
    assert default.limit_s == 600
    assert campaign_budget.limit_s == 1800
    campaign_budget.holdback_s = 100
    assert campaign_budget.remaining <= 1685
    with pytest.raises(TimeoutError, match="1800-second"):
        campaign.bench.Budget(inherited=1785, limit_s=1800).admit(1)


def test_manifest_accepts_only_data_entries_and_complete_hash_inventory(tmp_path):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    path = tmp_path / "manifest.json"
    entry = {"path": str(source), "source_hashes": inventory(source)}
    path.write_text(json.dumps({"baseline": entry, "binding": entry}))
    manifest = campaign._load_manifest(path)
    assert campaign._resolve_entry(manifest["baseline"], path)[1] == inventory(source)

    entry["command"] = ["python", "evil.py"]
    path.write_text(json.dumps({"baseline": entry, "binding": entry}))
    with pytest.raises(ValueError, match="only path and source_hashes"):
        campaign._resolve_entry(campaign._load_manifest(path)["baseline"], path)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(source_hashes={"__init__.py": "0" * 64}),
        lambda value: value.update(source_hashes={"../outside.py": "0" * 64}),
        lambda value: value.update(path=""),
    ],
)
def test_manifest_rejects_incomplete_or_unsafe_package_entries(tmp_path, mutate):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    entry = {"path": str(source), "source_hashes": inventory(source)}
    mutate(entry)
    with pytest.raises(ValueError):
        campaign._resolve_entry(entry, tmp_path / "manifest.json")


def test_manifest_rejects_duplicate_json_keys(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text('{"baseline":{},"baseline":{},"binding":{}}')
    with pytest.raises(ValueError, match="duplicate key"):
        campaign._load_manifest(path)


def test_output_nested_in_source_is_rejected_before_any_evidence_is_created(tmp_path):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    entry = {"path": str(source), "source_hashes": inventory(source)}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"baseline": entry, "binding": entry}))
    output = source / "evidence"
    args = campaign.parser().parse_args(["--manifest", str(manifest_path), "--output", str(output)])
    with pytest.raises(ValueError, match="disjoint"):
        campaign.run(args)
    assert not output.exists()
    assert inventory(source) == entry["source_hashes"]


def test_secondary_ownership_allows_only_its_assigned_file():
    binding = {"__init__.py": "a", "current_store.py": "b", "current_sync.py": "c"}
    assert campaign._validate_variant_ownership(
        binding,
        {**binding, "current_store.py": "changed"},
        "current_store.py",
    )
    with pytest.raises(ValueError, match="outside"):
        campaign._validate_variant_ownership(
            binding,
            {**binding, "current_store.py": "changed", "current_sync.py": "changed"},
            "current_store.py",
        )


def test_optional_stage_budget_uses_binding_side_runtime_with_safety_margin():
    samples = [
        {"category": "baseline", "wall_s": 200},
        {"category": "binding", "wall_s": 100},
        {"category": "binding", "wall_s": 120},
        {"category": "baseline", "wall_s": 220},
        {"category": "baseline", "wall_s": 210},
        {"category": "binding", "wall_s": 110},
    ]
    assert campaign._optional_stage_estimate(samples) == 120 * 1.25 * 7


def test_freezing_isolated_packages_does_not_mutate_inputs(tmp_path):
    baseline = package(tmp_path / "baseline", **{"current_store.py": "VALUE = 1\n"})
    binding = package(tmp_path / "binding", **{"current_store.py": "VALUE = 2\n"})
    outcome = package(tmp_path / "outcome", **{"current_store.py": "VALUE = 3\n"})
    entries = {
        name: {"path": str(path), "source_hashes": inventory(path)}
        for name, path in (
            ("baseline", baseline),
            ("binding", binding),
            ("outcome_grouping", outcome),
        )
    }
    before = {path: inventory(path) for path in (baseline, binding, outcome)}
    frozen = campaign._freeze_packages(
        {
            name: campaign._resolve_entry(entry, tmp_path / "manifest.json")
            for name, entry in entries.items()
        },
        tmp_path / "evidence",
        campaign.bench.Budget(limit_s=1800),
    )
    assert frozen["provenance"]["outcome_grouping"]["eligible"] is True
    assert {path: inventory(path) for path in (baseline, binding, outcome)} == before
    assert inventory(frozen["packages"]["outcome_grouping"]) == inventory(outcome)


def _private_snapshot(
    identity_value="candidate",
    response="run-a",
    origin="http://127.0.0.1:1",
    unresolved=False,
):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE sync_state (key VARCHAR, value JSON)")
    state = {
        "store_contract": "catalogue_store.v4",
        "projection_version": 1,
        "generation": 7,
        "full_pass": {"cursor_after": "100", "completed_at": "2026-10-14T00:00:00+00:00"},
        "fetch_pending": None,
        "bulk_policy": {
            "origin": origin,
            "lanes": {"events": {"supported": True, "passes": [None]}},
        },
        "discovery": {"lanes": {"events": {"frontier": 10}}},
        "reconcile": {"turn": 2},
        "last_publish_at": "2026-10-14T00:00:00+00:00",
        "previous_publish_seconds": 4.0,
        "last_run": {
            "operation": "reconcile",
            "status": "published",
            "published": True,
            "publication": {
                "release_id": "release-volatile",
                "relations": {"events": {"sha256": response, "bytes": 100}},
            },
            "http_attempts": 10,
            "downloaded_bytes": 1000,
            "peak_rss_bytes": 5000,
            "elapsed_s": 1.2,
            "timings": {"acquire_apply_s": 1.0},
            "requests": {"total_latency_s": 2.0},
        },
    }
    for key, value in state.items():
        conn.execute("INSERT INTO sync_state VALUES (?, ?)", [key, json.dumps(value)])
    conn.execute(
        "CREATE TABLE _receipts (kind VARCHAR, venue VARCHAR, id VARCHAR, receipt VARCHAR, response VARCHAR)"
    )
    conn.execute(
        "INSERT INTO _receipts VALUES ('market','polymarket','1','2026-10-14T00:00:00Z',?)",
        [response],
    )
    conn.execute(
        "CREATE TABLE _membership (venue VARCHAR, event_id VARCHAR, receipt VARCHAR, response VARCHAR, unresolved BOOLEAN, latest_receipt VARCHAR, latest_response VARCHAR)"
    )
    conn.execute(
        "INSERT INTO _membership VALUES ('polymarket','1','2026-10-14T00:00:00Z',?, ?, '2026-10-14T00:00:00Z',?)",
        [response, unresolved, response],
    )
    conn.execute(
        "CREATE TABLE _identity (venue VARCHAR, market_id VARCHAR, candidate JSON, accepted JSON, invalidated_at VARCHAR, invalidated_response VARCHAR)"
    )
    conn.execute(
        "INSERT INTO _identity VALUES ('polymarket', '1', ?, NULL, NULL, ?)",
        [json.dumps([{"asset_id": identity_value}]), response],
    )
    conn.execute(
        "CREATE TABLE _identity_claims (venue VARCHAR, market_id VARCHAR, role VARCHAR, asset_kind VARCHAR, asset_id VARCHAR)"
    )
    conn.execute("INSERT INTO _identity_claims VALUES ('polymarket','1','candidate','token','9')")
    conn.execute("CREATE TABLE _parent_repairs (event_id VARCHAR)")
    conn.execute("INSERT INTO _parent_repairs VALUES ('1')")
    conn.execute(
        "CREATE TABLE _reconcile_attempts (kind VARCHAR, id VARCHAR, last_attempt TIMESTAMPTZ)"
    )
    conn.execute("INSERT INTO _reconcile_attempts VALUES ('events','1','2026-10-14T00:00:00Z')")
    return conn


def test_private_oracle_normalizes_only_generated_refs_and_measurements():
    first = _private_snapshot(response="capture-1", origin="http://127.0.0.1:1001")
    second = _private_snapshot(response="capture-2", origin="http://127.0.0.1:1002")
    assert campaign.bench.canonical_hash(
        campaign.bench.private_state_snapshot(first)
    ) == campaign.bench.canonical_hash(campaign.bench.private_state_snapshot(second))
    changed_identity = _private_snapshot(identity_value="different")
    assert campaign.bench.canonical_hash(
        campaign.bench.private_state_snapshot(first)
    ) != campaign.bench.canonical_hash(campaign.bench.private_state_snapshot(changed_identity))
    changed_generation = _private_snapshot()
    changed_generation.execute("UPDATE sync_state SET value='8' WHERE key='generation'")
    assert campaign.bench.canonical_hash(
        campaign.bench.private_state_snapshot(first)
    ) != campaign.bench.canonical_hash(campaign.bench.private_state_snapshot(changed_generation))
    changed_membership = _private_snapshot(unresolved=True)
    assert campaign.bench.canonical_hash(
        campaign.bench.private_state_snapshot(first)
    ) != campaign.bench.canonical_hash(campaign.bench.private_state_snapshot(changed_membership))
    for conn in (first, second, changed_identity, changed_generation, changed_membership):
        conn.close()


def test_worker_semantic_hash_includes_private_state():
    base = {
        "tables": {},
        "state": {"generation": 4},
        "private_state": {"_identity_claims": [{"asset_id": "1"}]},
        "receipt_state": {},
        "publication_verified": True,
        "publication": {"generation": 4},
    }
    changed = {**base, "private_state": {"_identity_claims": [{"asset_id": "2"}]}}
    first = campaign.bench.canonical_hash(campaign.bench.semantic_snapshot(base, []))
    second = campaign.bench.canonical_hash(campaign.bench.semantic_snapshot(changed, []))
    assert first != second


def test_frozen_synthetic_smoke_workers_have_equivalent_private_and_public_state(tmp_path):
    seed = tmp_path / "seed"
    campaign.bench.create_seed(seed, *campaign.bench.PROFILES["smoke"])
    runtime = tmp_path / "runtime" / "oddsfox_catalogue"
    runtime.parent.mkdir()
    campaign.bench.copy_runtime(runtime)
    first_output, second_output = tmp_path / "sample-first", tmp_path / "sample-second"
    first_output.mkdir()
    second_output.mkdir()
    budget = campaign.bench.Budget()
    first = campaign.bench.run_sample(
        seed,
        first_output,
        runtime,
        budget,
        "smoke",
        "first",
    )
    second = campaign.bench.run_sample(
        seed,
        second_output,
        runtime,
        budget,
        "smoke",
        "second",
    )
    assert first["semantic_hash"] == second["semantic_hash"]


def test_four_http_accounting_counters_reject_any_candidate_increase():
    from test_full_pass_benchmark import row

    baseline = [
        row(pair, runtime_bytes=100, offered_bytes=200, reserved_attempts=12) for pair in (1, 2, 3)
    ]
    for key in (
        "http_attempts",
        "downloaded_bytes",
        "runtime_reserved_attempts",
        "runtime_downloaded_bytes",
    ):
        candidate = [
            row(pair, acquire=18, runtime_bytes=100, offered_bytes=200, reserved_attempts=12)
            for pair in (1, 2, 3)
        ]
        for sample in candidate:
            sample[key] += 1
        assert campaign.bench.comparison(baseline, candidate, "perf")["status"] == "deferred"


def test_missing_pairs_and_inconsistent_private_semantics_remain_unqualified():
    from test_full_pass_benchmark import row

    baseline = [row(pair) for pair in (1, 2, 3)]
    candidate = [row(pair, acquire=18) for pair in (1, 2)]
    assert (
        campaign.bench.comparison(baseline, candidate, "perf")["status"] == "insufficient_evidence"
    )
    candidate.append(row(3, acquire=18, semantic="private-state-diff"))
    assert campaign.bench.comparison(baseline, candidate, "perf")["status"] == "invalid"


def test_incomplete_combination_falls_back_to_binding_and_integrity_failure_retains_none():
    assert campaign._failure_retention(True, integrity_failure=False) == ["binding"]
    assert campaign._failure_retention(True, integrity_failure=True) == []
    assert campaign._failure_retention(False, integrity_failure=False) == []


def test_source_integrity_failure_after_primary_gate_retains_no_binding(tmp_path, monkeypatch):
    baseline = package(tmp_path / "baseline", **{"current_store.py": "VALUE = 1\n"})
    binding = package(tmp_path / "binding", **{"current_store.py": "VALUE = 2\n"})
    manifest = tmp_path / "manifest.json"
    entries = {
        name: {"path": str(path), "source_hashes": inventory(path)}
        for name, path in (("baseline", baseline), ("binding", binding))
    }
    manifest.write_text(json.dumps(entries))
    output = tmp_path / "evidence"

    monkeypatch.setattr(campaign, "_campaign_provenance", lambda _budget: {})
    monkeypatch.setattr(
        campaign,
        "_preflight",
        lambda _paths, _budget: {
            name: (path, hashes)
            for name, (path, hashes) in (
                (name, (Path(entry["path"]), entry["source_hashes"]))
                for name, entry in entries.items()
            )
        },
    )
    monkeypatch.setattr(
        campaign,
        "_freeze_packages",
        lambda _resolved, _output, _budget: {
            "packages": {"baseline": baseline, "binding": binding},
            "provenance": {
                name: {"path": str(path), "source_hashes": inventory(path)}
                for name, path in (("baseline", baseline), ("binding", binding))
            },
        },
    )
    monkeypatch.setattr(campaign.bench, "build_dataset", lambda *_args: (None, None, {"events": 1}))
    monkeypatch.setattr(
        campaign.bench,
        "supervised_prepare",
        lambda *_args, **_kwargs: {
            "status": "prepared",
            "source_hashes": inventory(baseline),
            "distribution": {"events": 1},
            "seed_sha256": "seed-hash",
        },
    )

    def qualified_primary(*_args, **_kwargs):
        report = _args[-1]
        report["stages"]["binding_vs_baseline"] = {
            "samples": [{"category": "binding", "wall_s": 1}] * 3
        }
        return {"status": "qualified"}

    monkeypatch.setattr(campaign, "_compare_stage", qualified_primary)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)

    def changed_input(*_args):
        raise campaign.CampaignIntegrityError("source changed after primary comparison")

    monkeypatch.setattr(campaign, "_integrity_check", changed_input)
    args = campaign.parser().parse_args(["--manifest", str(manifest), "--output", str(output)])

    assert campaign.run(args) == 1
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "failed"
    assert report["retained"] == []


@pytest.mark.parametrize("unsafe", ["symlink", "fifo"])
def test_source_package_rejects_symlink_and_special_entries(tmp_path, unsafe):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    file = source / "unsafe.py"
    if unsafe == "symlink":
        outside = tmp_path / "outside.py"
        outside.write_text("secret")
        file.symlink_to(outside)
    elif hasattr(os, "mkfifo"):
        os.mkfifo(file)
    else:
        pytest.skip("FIFO is unavailable")
    with pytest.raises(ValueError, match="symlink or special"):
        campaign.bench.validate_tree(source, python_only=True)
