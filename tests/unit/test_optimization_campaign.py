from __future__ import annotations

import importlib.util
import json
import os
import signal
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


def test_warmup_aliases_identical_runtime_workload_without_rewarming(tmp_path, monkeypatch):
    runtime = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    calls = []

    def stage_sample(_seed, _output, _package, _budget, _label, workload=None):
        calls.append(workload)
        return {"wall_s": 1.0, "workload": workload}

    monkeypatch.setattr(campaign, "_stage_sample", stage_sample)
    report = {}
    warmed = set()
    budget = campaign.bench.Budget()

    campaign._warm(
        "baseline",
        runtime,
        tmp_path / "seed-a",
        tmp_path / "evidence",
        budget,
        warmed,
        report,
        "closed_any",
    )
    campaign._warm(
        "closed_any",
        runtime,
        tmp_path / "seed-a",
        tmp_path / "evidence",
        budget,
        warmed,
        report,
        "closed_any",
    )
    campaign._warm(
        "baseline",
        runtime,
        tmp_path / "seed-b",
        tmp_path / "evidence",
        budget,
        warmed,
        report,
        "discovery_dense",
    )

    assert calls == ["closed_any", "discovery_dense"]
    assert report["warmups"]["baseline:closed_any"] == report["warmups"]["closed_any:closed_any"]
    assert report["warmups"]["baseline:discovery_dense"]["sample"]["workload"] == (
        "discovery_dense"
    )


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


def _v3_manifest_entry(path):
    return {"path": str(path), "source_hashes": inventory(path)}


def test_v3_manifest_requires_all_candidates_and_sorted_exact_combination_keys(tmp_path):
    baseline = package(
        tmp_path / "baseline",
        **{
            "current_store.py": "BASE = 1\n",
            "current_reconcile.py": "BASE = 1\n",
            "current_sync.py": "BASE = 1\n",
            "current_runtime.py": "BASE = 1\n",
        },
    )
    candidate = package(
        tmp_path / "candidate",
        **{
            "current_store.py": "BASE = 1\n",
            "current_reconcile.py": "BASE = 1\n",
            "current_sync.py": "BASE = 1\n",
            "current_runtime.py": "BASE = 1\n",
        },
    )
    entries = {name: _v3_manifest_entry(candidate) for name in campaign.V3_CANDIDATES}
    path = tmp_path / "manifest-v3.json"
    path.write_text(
        json.dumps(
            {
                "version": 3,
                "baseline": _v3_manifest_entry(baseline),
                "candidates": entries,
                "combinations": {"claims+queue": _v3_manifest_entry(candidate)},
            }
        )
    )
    manifest = campaign._load_manifest(path)
    assert manifest["_version"] == 3
    assert manifest["_combination:claims+queue"] == _v3_manifest_entry(candidate)

    for key in ("queue+claims", "claims+claims", "claims+projection"):
        path.write_text(
            json.dumps(
                {
                    "version": 3,
                    "baseline": _v3_manifest_entry(baseline),
                    "candidates": entries,
                    "combinations": {key: _v3_manifest_entry(candidate)},
                }
            )
        )
        with pytest.raises(ValueError, match="combination keys"):
            campaign._load_manifest(path)


def test_v3_manifest_rejects_unknown_fields_and_missing_candidates(tmp_path):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    entry = _v3_manifest_entry(source)
    candidates = {name: entry for name in campaign.V3_CANDIDATES}
    path = tmp_path / "manifest-v3.json"
    for data, message in (
        (
            {"version": 3, "baseline": entry, "candidates": candidates, "command": "run"},
            "unknown fields",
        ),
        (
            {"version": 3, "baseline": entry, "candidates": {"claims": entry}},
            "requires claims",
        ),
    ):
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match=message):
            campaign._load_manifest(path)


def test_v3_ownership_allows_only_owned_added_files_and_never_removals():
    baseline = {"__init__.py": "a", "current_store.py": "b"}
    candidate = {**baseline, "current_claim_indexes.py": "new"}
    assert campaign._validate_v3_ownership(
        baseline, candidate, campaign.V3_OWNERSHIP["claims"], "claims"
    )
    with pytest.raises(ValueError, match="outside its ownership"):
        campaign._validate_v3_ownership(
            baseline,
            {**candidate, "unrelated.py": "bad"},
            campaign.V3_OWNERSHIP["claims"],
            "claims",
        )
    with pytest.raises(ValueError, match="removes"):
        campaign._validate_v3_ownership(
            baseline, {"__init__.py": "a"}, campaign.V3_OWNERSHIP["claims"], "claims"
        )


def test_v3_combination_ownership_is_union_of_feature_surfaces():
    baseline = {"current_sync.py": "a", "current_store.py": "b"}
    combination = {"current_sync.py": "c", "current_store.py": "d"}
    allowed = campaign.V3_OWNERSHIP["claims"] | campaign.V3_OWNERSHIP["closed_any"]
    assert campaign._validate_v3_ownership(baseline, combination, allowed, "combo")
    with pytest.raises(ValueError, match="outside its ownership"):
        campaign._validate_v3_ownership(
            baseline, {**combination, "current_reconcile.py": "bad"}, allowed, "combo"
        )


def test_v3_freezing_validates_owned_additions_and_exact_combination(tmp_path):
    base_files = {
        "current_store.py": "store = 1\n",
        "current_reconcile.py": "queue = 1\n",
        "current_sync.py": "sync = 1\n",
        "current_runtime.py": "runtime = 1\n",
        "cli.py": "cli = 1\n",
    }
    baseline = package(tmp_path / "baseline", **base_files)
    candidates = {}
    for name, filename in (
        ("claims", "current_store.py"),
        ("queue", "current_reconcile.py"),
        ("closed_any", "current_sync.py"),
        ("keyset", "current_sync.py"),
    ):
        files = {**base_files, filename: f"{name} = 2\n"}
        if name in {"claims", "keyset"}:
            files["current_claim_indexes.py" if name == "claims" else "current_keyset.py"] = (
                "owned = True\n"
            )
        candidates[name] = package(tmp_path / name, **files)
    combo = package(
        tmp_path / "combo",
        **{
            **base_files,
            "current_store.py": "claims = 2\n",
            "current_reconcile.py": "queue = 2\n",
        },
    )
    resolved = {
        "baseline": (baseline, inventory(baseline)),
        **{name: (path, inventory(path)) for name, path in candidates.items()},
        "_combination:claims+queue": (combo, inventory(combo)),
    }
    frozen = campaign._freeze_v3_packages(
        resolved, tmp_path / "evidence", campaign.bench.Budget(limit_s=10800)
    )
    assert all(frozen["provenance"][name]["eligible"] for name in candidates)
    assert frozen["provenance"]["_combination:claims+queue"]["eligible"]
    assert inventory(baseline) == resolved["baseline"][1]

    incomplete_combo = package(
        tmp_path / "incomplete-combo",
        **{**base_files, "current_store.py": "claims = 2\n"},
    )
    bad_resolved = {
        **resolved,
        "_combination:claims+queue": (incomplete_combo, inventory(incomplete_combo)),
    }
    with pytest.raises(ValueError, match="no changed files for declared features: queue"):
        campaign._freeze_v3_packages(
            bad_resolved, tmp_path / "bad-evidence", campaign.bench.Budget(limit_s=10800)
        )


def test_v3_requires_its_long_shared_budget(tmp_path):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    entry = _v3_manifest_entry(source)
    manifest = tmp_path / "manifest-v3.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 3,
                "baseline": entry,
                "candidates": {name: entry for name in campaign.V3_CANDIDATES},
            }
        )
    )
    args = campaign.parser().parse_args(
        ["--manifest", str(manifest), "--output", str(tmp_path / "evidence")]
    )
    with pytest.raises(ValueError, match="10,800-second"):
        campaign.run(args, budget=campaign.bench.Budget(limit_s=1800))


def test_v3_workloads_and_fallback_follow_declared_priority():
    assert campaign._v3_workloads("claims") == (None,)
    assert campaign._v3_workloads("queue") == (None,)
    assert campaign._v3_workloads("closed_any") == ("closed_any",)
    assert campaign._v3_workloads("keyset") == (
        "discovery_sparse",
        "discovery_dense",
        "discovery_tail",
    )
    report = {"qualified_candidates": {"closed_any": {}, "queue": {}}}
    assert campaign._best_v3_candidate(report) == "queue"


def test_v3_api_seed_verification_covers_both_databases_and_metadata(tmp_path):
    root = tmp_path / "api-seeds" / "closed_any"
    root.mkdir(parents=True)
    metadata = {}
    for component in ("provider", "workspace"):
        database = root / component / "catalogue.duckdb"
        database.parent.mkdir()
        database.write_bytes(component.encode())
        metadata[f"{component}_seed_sha256"] = campaign._digest_bounded(
            database, campaign.bench.Budget(limit_s=10800)
        )
    manifest = root / "api-seed.json"
    manifest.write_text('{"workload":"closed_any"}\n')
    metadata["_manifest_sha256"] = campaign._digest_bounded(
        manifest, campaign.bench.Budget(limit_s=10800)
    )
    campaign._verify_api_seeds(
        {"closed_any": metadata},
        tmp_path,
        campaign.bench.Budget(limit_s=10800),
    )
    manifest.write_text('{"workload":"changed"}\n')
    with pytest.raises(campaign.CampaignIntegrityError, match="manifest changed"):
        campaign._verify_api_seeds(
            {"closed_any": metadata},
            tmp_path,
            campaign.bench.Budget(limit_s=10800),
        )


def test_v3_claim_resource_failure_does_not_abort_queue_candidate(tmp_path, monkeypatch):
    baseline = package(tmp_path / "baseline", **{"current_reconcile.py": "BASE = 1\n"})
    claims = package(
        tmp_path / "claims",
        **{
            "current_reconcile.py": "BASE = 1\n",
            "current_store.py": "CLAIMS = 1\n",
        },
    )
    queue = package(tmp_path / "queue", **{"current_reconcile.py": "QUEUE = 1\n"})
    closed_any = package(tmp_path / "closed-any", **{"current_sync.py": "CLOSED_ANY = 1\n"})
    output = tmp_path / "evidence"
    output.mkdir()
    report = {"deferred": {}, "stages": {}}
    packages = {
        "baseline": baseline,
        "claims": claims,
        "queue": queue,
        "closed_any": closed_any,
    }
    provenance = {name: {"eligible": name != "baseline"} for name in packages}
    monkeypatch.setattr(
        campaign.bench,
        "claim_resource_diagnostics",
        lambda *_args: {"status": "insufficient_evidence", "resource_gate": False},
        raising=False,
    )
    monkeypatch.setattr(
        campaign.bench,
        "prepare_api_seed",
        lambda *_args: (_ for _ in ()).throw(OSError("fixture preparation unavailable")),
    )
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(
        campaign,
        "_stage_sample",
        lambda *_args, **_kwargs: {"wall_s": 2.0},
    )
    monkeypatch.setattr(
        campaign.bench,
        "comparison",
        lambda *_args: {"status": "qualified", "saved_s": 2.0},
    )

    campaign._run_v3(
        tmp_path / "seed",
        output,
        campaign.bench.Budget(limit_s=10800),
        set(),
        report,
        packages,
        provenance,
        "seed-hash",
        {},
    )

    assert report["claim_resource_diagnostics"]["resource_gate"] is False
    assert "claims" in report["deferred"]
    assert report["api_seed_errors"] == {"closed_any": "OSError"}
    assert "closed_any" in report["deferred"]
    assert report["qualified_candidates"] == {
        "queue": {"comparisons": [{"status": "qualified", "saved_s": 2.0}], "workloads": [None]}
    }


def test_v3_api_candidate_uses_supervised_fresh_seed_and_workload_gate(tmp_path, monkeypatch):
    baseline = package(tmp_path / "baseline", **{"current_sync.py": "BASE = 1\n"})
    closed_any = package(tmp_path / "closed-any", **{"current_sync.py": "CANDIDATE = 1\n"})
    output = tmp_path / "evidence"
    output.mkdir()
    report = {"deferred": {}, "stages": {}}
    packages = {"baseline": baseline, "closed_any": closed_any}
    provenance = {
        "baseline": {"eligible": False},
        "closed_any": {"eligible": True},
    }
    prepared = []

    def prepare_api_seed(root, workload, _budget, runtime_package):
        assert not root.exists()
        assert runtime_package == baseline
        prepared.append(workload)
        for component in ("provider", "workspace"):
            database = root / component / "catalogue.duckdb"
            database.parent.mkdir(parents=True)
            database.write_bytes(component.encode())
        metadata_path = root / "api-seed.json"
        metadata_path.write_text('{"workload":"closed_any"}\n')
        return {
            "status": "prepared",
            "runtime_source_hashes": campaign._hash_package(
                runtime_package, campaign.bench.Budget()
            ),
            "provider_seed_sha256": campaign._digest_bounded(
                root / "provider" / "catalogue.duckdb", campaign.bench.Budget()
            ),
            "workspace_seed_sha256": campaign._digest_bounded(
                root / "workspace" / "catalogue.duckdb", campaign.bench.Budget()
            ),
        }

    monkeypatch.setattr(campaign.bench, "prepare_api_seed", prepare_api_seed)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(campaign, "_stage_sample", lambda *_args, **_kwargs: {"wall_s": 2.0})
    monkeypatch.setattr(
        campaign.bench,
        "api_comparison",
        lambda *_args: {"status": "qualified", "saved_s": 1.0},
    )

    campaign._run_v3(
        tmp_path / "unused-full-pass-seed",
        output,
        campaign.bench.Budget(limit_s=10800),
        set(),
        report,
        packages,
        provenance,
        "unused-seed-hash",
        {},
    )

    assert prepared == ["closed_any"]
    assert report["retained"] == ["closed_any"]
    assert report["qualified_candidates"]["closed_any"]["workloads"] == ["closed_any"]


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


def test_fallback_retention_requires_fresh_integrity_and_harness_checks(tmp_path, monkeypatch):
    calls = []

    def check(*_args):
        calls.append("integrity")

    def harness(*_args):
        calls.append("harness")

    monkeypatch.setattr(campaign, "_integrity_check", check)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", harness)
    assert campaign._failure_retention(True, {}, {}, tmp_path, "seed", {}, object()) == ["binding"]
    assert calls == ["integrity", "harness"]
    assert campaign._failure_retention(False, {}, {}, tmp_path, "seed", {}, object()) == []

    def invalid(*_args):
        raise campaign.CampaignIntegrityError("changed")

    monkeypatch.setattr(campaign, "_integrity_check", invalid)
    assert campaign._failure_retention(True, {}, {}, tmp_path, "seed", {}, object()) == []


@pytest.mark.parametrize("now,expected", [(80, ["binding"]), (85, [])])
def test_fallback_releases_optional_holdback_but_keeps_shutdown_reserve(
    tmp_path, monkeypatch, now, expected
):
    budget = campaign.bench.Budget(limit_s=100)
    budget.started = 0
    budget.holdback_s = 15
    monkeypatch.setattr(campaign.bench.time, "monotonic", lambda: now)
    monkeypatch.setattr(
        campaign,
        "_integrity_check",
        lambda *_args: budget.admit(0.001),
    )
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    report = {"retained": ["outcome_grouping"]}
    campaign._record_failure_retention(report, True, {}, {}, tmp_path, "seed", {}, budget)
    assert report["retained"] == expected
    assert budget.holdback_s == 0


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


def test_early_timeout_before_harness_provenance_keeps_partial_report(tmp_path, monkeypatch):
    baseline = package(tmp_path / "baseline", **{"current_store.py": "VALUE = 1\n"})
    binding = package(tmp_path / "binding", **{"current_store.py": "VALUE = 2\n"})
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                name: {"path": str(path), "source_hashes": inventory(path)}
                for name, path in (("baseline", baseline), ("binding", binding))
            }
        )
    )

    def timeout_before_provenance(_budget):
        raise TimeoutError("budget expired")

    monkeypatch.setattr(campaign, "_campaign_provenance", timeout_before_provenance)
    args = campaign.parser().parse_args(
        ["--manifest", str(manifest), "--output", str(tmp_path / "evidence")]
    )
    assert campaign.run(args) == 0
    report = json.loads((tmp_path / "evidence" / "report.json").read_text())
    assert report["status"] == "insufficient_evidence"
    assert report["retained"] == []


@pytest.mark.parametrize(
    "failure,mutate,expected_status,expected_retained,expected_code",
    [
        ("timeout", "binding", "insufficient_evidence", [], None),
        ("timeout", "seed", "insufficient_evidence", [], None),
        ("timeout", "harness", "insufficient_evidence", [], None),
        ("timeout", None, "insufficient_evidence", ["binding"], None),
        ("timeout", "budget", "insufficient_evidence", [], None),
        ("keyboard", None, "partial", ["binding"], 130),
        ("term", None, "partial", ["binding"], 128 + signal.SIGTERM),
        ("timeout", "verify_term", "partial", [], 128 + signal.SIGTERM),
    ],
)
def test_optional_failure_revalidates_qualified_binding_before_fallback(
    tmp_path, monkeypatch, failure, mutate, expected_status, expected_retained, expected_code
):
    baseline = package(tmp_path / "baseline", **{"current_store.py": "VALUE = 1\n"})
    binding = package(tmp_path / "binding", **{"current_store.py": "VALUE = 2\n"})
    grouping = package(tmp_path / "grouping", **{"current_store.py": "VALUE = 3\n"})
    manifest = tmp_path / "manifest.json"
    entries = {
        name: {"path": str(path), "source_hashes": inventory(path)}
        for name, path in (
            ("baseline", baseline),
            ("binding", binding),
            ("outcome_grouping", grouping),
        )
    }
    manifest.write_text(json.dumps(entries))
    output = tmp_path / "evidence"
    harness_state = {"changed": False}
    monkeypatch.setattr(
        campaign,
        "_campaign_provenance",
        lambda _budget: {"changed": harness_state["changed"]},
    )
    monkeypatch.setattr(campaign.bench, "build_dataset", lambda *_args: (None, None, {"events": 1}))

    def prepare(_argv, output, _budget, _log, *, package_root):
        seed = output / "seed" / "catalogue.duckdb"
        seed.parent.mkdir(parents=True)
        seed.write_bytes(b"seed")
        return {
            "status": "prepared",
            "source_hashes": inventory(package_root),
            "distribution": {"events": 1},
            "seed_sha256": campaign._digest_bounded(seed, campaign.bench.Budget()),
        }

    monkeypatch.setattr(campaign.bench, "supervised_prepare", prepare)
    comparisons = 0

    def compare(
        _stage, _left_name, _left, _right_name, _right, _seed, _output, _budget, _warmed, report
    ):
        nonlocal comparisons
        comparisons += 1
        if comparisons == 1:
            report["stages"]["binding_vs_baseline"] = {
                "samples": [{"category": "binding", "wall_s": 1}] * 3
            }
            return {"status": "qualified"}
        if mutate == "binding":
            target = output / "inputs" / "binding" / "oddsfox_catalogue" / "current_store.py"
            target.write_text("VALUE = 999\n")
        elif mutate == "seed":
            (output / "seed" / "catalogue.duckdb").write_bytes(b"changed")
        elif mutate == "harness":
            harness_state["changed"] = True
        if failure == "keyboard":
            raise KeyboardInterrupt
        if failure == "term":
            raise campaign.bench.Terminated(signal.SIGTERM)
        raise TimeoutError("optional comparison exhausted its budget")

    monkeypatch.setattr(campaign, "_compare_stage", compare)
    if mutate in {"budget", "verify_term"}:
        integrity = campaign._integrity_check
        checks = 0

        def exhaust_fallback(*args):
            nonlocal checks
            checks += 1
            if checks == 2:
                if mutate == "verify_term":
                    raise campaign.bench.Terminated(signal.SIGTERM)
                raise TimeoutError("no time remains for fallback verification")
            return integrity(*args)

        monkeypatch.setattr(campaign, "_integrity_check", exhaust_fallback)
    args = campaign.parser().parse_args(["--manifest", str(manifest), "--output", str(output)])

    assert campaign.run(args) == (expected_code if expected_code is not None else 0)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == expected_status
    assert report["retained"] == expected_retained
    if mutate == "budget":
        assert report["error"].startswith("optional comparison")


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
    with pytest.raises(ValueError, match="symlink or special|link or reparse"):
        campaign.bench.validate_tree(source, python_only=True)
