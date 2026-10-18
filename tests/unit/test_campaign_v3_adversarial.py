from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("campaign_v3_adversarial", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
benchmark = campaign.bench


def package(path, **files):
    path.mkdir(parents=True)
    for name, content in {"__init__.py": "", **files}.items():
        (path / name).write_text(content)
    return path


def entry(path):
    return {"path": str(path), "source_hashes": benchmark.source_hashes(path)}


def samples(workload="closed_any", runtime=20):
    return [
        {
            "pair": pair,
            "settled_units": 1,
            "measurement_version": benchmark.MEASUREMENT_VERSION,
            "workload": workload,
            "oracle_pass": True,
            "oracle_fingerprint": "verified-complete-work",
            "completed_work": {"events": 1000, "markets": 4040, "pending": 0},
            "runtime_sequence_wall_s": runtime,
            "wall_s": 100,
            "peak_rss_bytes": 1000,
            "worker_peak_before_oracle_bytes": 900,
            "http_attempts": 100,
            "downloaded_bytes": 1000,
            "runtime_reserved_attempts": 100,
            "runtime_downloaded_bytes": 1000,
        }
        for pair in (1, 2, 3)
    ]


@pytest.mark.parametrize("field", ["command", "environment", "prepare", "timeout"])
def test_v3_manifest_rejects_executable_configuration_at_package_boundary(tmp_path, field):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    value = entry(source)
    value[field] = "not executable configuration"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 3,
                "baseline": value,
                "candidates": {name: entry(source) for name in campaign.V3_CANDIDATES},
            }
        )
    )
    loaded = campaign._load_manifest(manifest)
    with pytest.raises(ValueError, match="only path and source_hashes"):
        campaign._resolve_entry(loaded["baseline"], manifest)


@pytest.mark.parametrize(
    "hashes",
    [
        {"__init__.py": "A" * 64},
        {"__init__.py": "a" * 63},
        {"../outside.py": "a" * 64},
        {"__init__.py": 123},
        {"__init__.py": "0" * 64},
    ],
)
def test_v3_manifest_does_not_trust_malformed_or_tampered_hashes(tmp_path, hashes):
    source = package(tmp_path / "runtime")
    with pytest.raises(ValueError):
        campaign._resolve_entry(
            {"path": str(source), "source_hashes": hashes}, tmp_path / "manifest.json"
        )


def test_combination_cannot_claim_queue_when_only_store_changed(tmp_path):
    base = {"current_store.py": "store = 1\n", "current_reconcile.py": "queue = 1\n"}
    baseline = package(tmp_path / "baseline", **base)
    incomplete = package(tmp_path / "incomplete", **{**base, "current_store.py": "store = 2\n"})
    resolved = {
        "baseline": (baseline, benchmark.source_hashes(baseline)),
        "_combination:claims+queue": (incomplete, benchmark.source_hashes(incomplete)),
    }
    with pytest.raises(ValueError, match="feature|queue"):
        campaign._freeze_v3_packages(
            resolved, tmp_path / "evidence", benchmark.Budget(limit_s=10800)
        )


@pytest.mark.parametrize(
    "counter",
    ["http_attempts", "runtime_reserved_attempts", "downloaded_bytes", "runtime_downloaded_bytes"],
)
def test_closed_any_rejects_each_per_pair_http_regression_even_when_medians_improve(counter):
    baseline, candidate = samples(), samples(runtime=17)
    candidate[0][counter] += 1
    if counter == "http_attempts":
        candidate[0]["runtime_reserved_attempts"] += 1
    if counter == "runtime_downloaded_bytes":
        candidate[0]["downloaded_bytes"] += 1
    candidate[1][counter] = 0
    if counter == "runtime_reserved_attempts":
        candidate[1]["http_attempts"] = 0
    if counter == "downloaded_bytes":
        candidate[1]["runtime_downloaded_bytes"] = 0
    assert benchmark.api_comparison(baseline, candidate, "closed_any")["status"] == "deferred"


@pytest.mark.parametrize("counter", ["http_attempts", "runtime_reserved_attempts"])
def test_keyset_envelope_allowance_never_applies_to_attempts(counter):
    baseline = samples("discovery_sparse")
    candidate = samples("discovery_sparse", 17)
    candidate[0][counter] += 1
    if counter == "http_attempts":
        candidate[0]["runtime_reserved_attempts"] += 1
    assert benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == "deferred"


@pytest.mark.parametrize("counter", ["downloaded_bytes", "runtime_downloaded_bytes"])
def test_keyset_envelope_growth_boundary_is_checked_for_each_byte_counter(counter):
    baseline = samples("discovery_sparse")
    candidate = samples("discovery_sparse", 17)
    candidate[0][counter] = 1050
    if counter == "runtime_downloaded_bytes":
        candidate[0]["downloaded_bytes"] = 1050
    assert (
        benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == "qualified"
    )
    candidate[0][counter] = 1051
    if counter == "runtime_downloaded_bytes":
        candidate[0]["downloaded_bytes"] = 1051
    assert benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == "deferred"


@pytest.mark.parametrize("pair", [None, 1, True, "3"])
def test_missing_duplicate_or_invalid_pair_identity_cannot_qualify(pair):
    baseline, candidate = samples(), samples(runtime=17)
    candidate[2]["pair"] = pair
    assert benchmark.api_comparison(baseline, candidate, "closed_any")["status"] == (
        "insufficient_evidence"
    )


def test_api_comparison_rejects_wrong_workload_and_failed_oracle():
    baseline, candidate = samples(), samples(runtime=17)
    candidate[0]["workload"] = "discovery_sparse"
    with pytest.raises(ValueError, match="workload differs"):
        benchmark.api_comparison(baseline, candidate, "closed_any")
    candidate[0]["workload"] = "closed_any"
    candidate[0]["oracle_pass"] = False
    with pytest.raises(ValueError, match="oracle failed"):
        benchmark.api_comparison(baseline, candidate, "closed_any")


def test_api_comparison_does_not_accept_absent_completed_work_from_both_sides():
    baseline, candidate = samples(), samples(runtime=17)
    for row in baseline + candidate:
        row.pop("completed_work")
    with pytest.raises(ValueError, match="completed work|incomplete|unreported"):
        benchmark.api_comparison(baseline, candidate, "closed_any")


@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), "100"])
def test_api_comparison_rejects_invalid_counter_values(value):
    baseline, candidate = samples(), samples(runtime=17)
    candidate[0]["http_attempts"] = value
    with pytest.raises(ValueError, match="metric|counter"):
        benchmark.api_comparison(baseline, candidate, "closed_any")


def test_v3_budget_deducts_preparation_holdback_and_shutdown_without_reset(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(benchmark.time, "monotonic", lambda: clock[0])
    budget = benchmark.Budget(limit_s=10800)
    clock[0] += 1000
    budget.holdback_s = 200
    assert budget.remaining == 9585
    budget.admit(9585)
    with pytest.raises(TimeoutError, match="10800-second"):
        budget.admit(9585.001)
    clock[0] += 9585
    assert budget.remaining == 0
    budget.holdback_s = 0
    assert budget.remaining == 200


def test_claim_speed_is_not_measured_without_installation_resource_proof(tmp_path, monkeypatch):
    report = {"deferred": {}}
    monkeypatch.setattr(benchmark, "_save", lambda *_args: None)

    def must_not_measure(*_args, **_kwargs):
        pytest.fail("steady-state speed cannot replace claim installation resource evidence")

    monkeypatch.setattr(campaign, "_compare_stage", must_not_measure)
    monkeypatch.setattr(
        benchmark,
        "claim_resource_diagnostics",
        lambda *_args: {
            "status": "deferred",
            "resource_gate": False,
            "reason": "unproven resource model",
        },
    )
    (tmp_path / "evidence").mkdir()
    campaign._run_v3(
        tmp_path / "seed",
        tmp_path / "evidence",
        benchmark.Budget(limit_s=10800),
        set(),
        report,
        {"baseline": tmp_path / "baseline", "claims": tmp_path / "claims"},
        {"claims": {"eligible": True}},
        "seed-hash",
        {},
    )
    assert report["retained"] == []
    assert report["deferred"]["claims"] == "unproven resource model"
    assert report["status"] == "insufficient_evidence"


def test_combination_must_pass_union_of_qualified_workloads(tmp_path, monkeypatch):
    report = {"deferred": {}}
    observed = []
    monkeypatch.setattr(benchmark, "_save", lambda *_args: None)
    monkeypatch.setattr(
        benchmark,
        "prepare_api_seed",
        lambda *_args, **_kwargs: {
            "status": "prepared",
            "provider_seed_sha256": "s",
            "workspace_seed_sha256": "s",
            "runtime_source_hashes": {"__init__.py": "hash"},
        },
        raising=False,
    )
    monkeypatch.setattr(campaign, "_digest_bounded", lambda *_args: "s")
    monkeypatch.setattr(campaign, "_hash_package", lambda *_args: {"__init__.py": "hash"})
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(campaign, "_verify_api_seeds", lambda *_args: None)

    def warm(name, _package, _seed, _output, _budget, _warmed, current, workload=None):
        key = name if workload is None else f"{name}:{workload}"
        current.setdefault("warmups", {})[key] = {"sample": {"wall_s": 1}}

    def compare(stage, *_args, workload=None, **_kwargs):
        observed.append((stage, workload))
        return {"status": "qualified"}

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    output = tmp_path / "evidence"
    output.mkdir()
    campaign._run_v3(
        tmp_path / "seed",
        output,
        benchmark.Budget(limit_s=10800),
        set(),
        report,
        {
            "baseline": tmp_path / "baseline",
            "queue": tmp_path / "queue",
            "closed_any": tmp_path / "closed_any",
            "_combination:closed_any+queue": tmp_path / "combo",
        },
        {
            "queue": {"eligible": True},
            "closed_any": {"eligible": True},
            "_combination:closed_any+queue": {"eligible": True},
        },
        "seed-hash",
        {},
    )
    combined_workloads = [
        workload for stage, workload in observed if stage.startswith("final_combination")
    ]
    assert combined_workloads == [None, "closed_any"]
    assert report["retained"] == ["queue", "closed_any", "combined"]


def test_equal_unfinished_work_does_not_qualify_as_complete_api_work():
    baseline, candidate = samples(), samples(runtime=17)
    for row in baseline + candidate:
        row["completed_work"]["pending"] = 1
    with pytest.raises(ValueError, match="completed work|pending|incomplete|unreported"):
        benchmark.api_comparison(baseline, candidate, "closed_any")


def test_empty_oracle_fingerprint_is_not_evidence_of_equivalence():
    baseline, candidate = samples(), samples(runtime=17)
    for row in baseline + candidate:
        row["oracle_fingerprint"] = ""
    with pytest.raises(ValueError, match="fingerprints"):
        benchmark.api_comparison(baseline, candidate, "closed_any")


@pytest.fixture
def oracle_catalogue(tmp_path):
    import duckdb

    root = tmp_path / "catalogue"
    benchmark.create_seed(root, 10, 41)
    metadata = {
        "workload": "discovery_dense",
        "expected_counts": {},
        "expected_public_fingerprints": {},
        "expected_private_fingerprints": {},
        "expected_event_ids": [],
        "expected_market_ids": [],
        "expected_refreshed_market_ids": [],
        "expected_absent_market_ids": [],
        "expected_parent_repairs": [],
        "receipt_cutoff": benchmark.BASE_TIME.isoformat(),
    }
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        for table in (
            "events",
            "markets",
            "event_markets",
            "outcomes",
            "_identity",
            "_identity_claims",
        ):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            category = "private" if table.startswith("_") else "public"
            metadata[f"expected_{category}_fingerprints"][table] = benchmark.canonical_hash(rows)
            if category == "public":
                metadata["expected_counts"][table] = len(rows)
        metadata["expected_event_ids"] = [
            str(row[0])
            for row in conn.execute(
                "SELECT event_id FROM events ORDER BY try_cast(event_id AS HUGEINT)"
            ).fetchall()
        ]
        metadata["expected_market_ids"] = [
            str(row[0])
            for row in conn.execute(
                "SELECT market_id FROM markets ORDER BY try_cast(market_id AS HUGEINT)"
            ).fetchall()
        ]
        metadata["expected_workspace_market_ids"] = list(metadata["expected_market_ids"])
        conn.execute("UPDATE _receipts SET receipt='2026-10-14T00:00:01Z'")
        conn.execute(
            "UPDATE _membership SET receipt='2026-10-14T00:00:01Z',latest_receipt='2026-10-14T00:00:01Z'"
        )
        frontiers = {
            lane: {"frontier": max(map(int, metadata[key])), "highwater": None, "phase": "tail"}
            for lane, key in (("events", "expected_event_ids"), ("markets", "expected_market_ids"))
        }
        metadata["expected_discovery_frontiers"] = frontiers
        metadata["expected_receipt_keys"] = [
            list(row)
            for row in conn.execute("SELECT kind,venue,id FROM _receipts ORDER BY ALL").fetchall()
        ]
        metadata["expected_membership_keys"] = [
            list(row)
            for row in conn.execute(
                "SELECT venue,event_id FROM _membership ORDER BY ALL"
            ).fetchall()
        ]
        metadata["expected_generation"] = json.loads(
            conn.execute("SELECT value FROM sync_state WHERE key='generation'").fetchone()[0]
        )
        metadata["expected_attempt_ids"] = []
        conn.execute(
            "INSERT INTO sync_state VALUES ('discovery',?)",
            [json.dumps({"version": 2, "next_lane": 0, "lanes": frontiers, "pending": None})],
        )
    seed = tmp_path / "seed"
    seed.mkdir()
    (seed / "api-seed.json").write_text(json.dumps(metadata))
    assert benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)["oracle_pass"]
    return root, seed


@pytest.mark.parametrize(
    "sql,check",
    [
        (
            "DELETE FROM _identity_claims WHERE role='candidate' AND rowid=(SELECT min(rowid) FROM _identity_claims WHERE role='candidate')",
            "identity_and_claims",
        ),
        (
            "UPDATE _identity SET candidate='[]' WHERE market_id=(SELECT min(market_id) FROM _identity)",
            "identity_and_claims",
        ),
        (
            "UPDATE _membership SET unresolved=TRUE WHERE event_id=(SELECT min(event_id) FROM _membership)",
            "receipt_coverage",
        ),
        (
            "UPDATE _receipts SET id='99999999999999999999' WHERE kind='market' AND id=(SELECT min(id) FROM _receipts WHERE kind='market')",
            "receipt_coverage",
        ),
        (
            "INSERT INTO sync_state VALUES ('fetch_pending', '{\"raw_id\":\"pinned\"}')",
            "no_pending_work",
        ),
        ("INSERT INTO _parent_repairs VALUES ('99999999999999999999')", "parent_repairs"),
    ],
)
def test_api_oracle_rejects_private_state_corruption(oracle_catalogue, sql, check):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(sql)
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"][check] is False


def test_unchanged_tail_seed_starts_from_completed_numeric_frontiers(tmp_path, monkeypatch):
    import duckdb

    create_seed = benchmark.create_seed

    def small_seed(root, _events, _markets, **kwargs):
        return create_seed(root, 10, 41, **kwargs)

    monkeypatch.setattr(benchmark, "create_seed", small_seed)
    root = tmp_path / "tail"
    benchmark.create_api_seed(root, "discovery_tail")
    with duckdb.connect(str(root / "workspace" / "catalogue.duckdb"), read_only=True) as conn:
        row = conn.execute("SELECT value FROM sync_state WHERE key='discovery'").fetchone()
        assert row is not None, "unchanged-tail measurement must not bootstrap from frontier zero"
        discovery = json.loads(row[0])
        assert discovery["version"] == 2
        assert discovery["pending"] is None
        for lane, table, column in (
            ("events", "events", "event_id"),
            ("markets", "markets", "market_id"),
        ):
            maximum = conn.execute(
                f"SELECT max(try_cast({column} AS HUGEINT)) FROM {table}"
            ).fetchone()[0]
            state = discovery["lanes"][lane]
            assert state["frontier"] == maximum
            assert state["highwater"] is None
            assert state["phase"] == "tail"


@pytest.mark.parametrize("warning", ["cleanup_deferred", "cleanup_warnings"])
def test_api_speed_cannot_qualify_by_deferring_publication_maintenance(warning):
    baseline, candidate = samples(), samples(runtime=17)
    candidate[0][warning] = True if warning == "cleanup_deferred" else ["cleanup cutoff"]
    result = benchmark.api_comparison(baseline, candidate, "closed_any")
    assert result["status"] == "deferred"
    assert "maintenance" in result["reason"]


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE _receipts SET receipt='2026-10-13T23:59:59Z' WHERE id=(SELECT min(id) FROM _receipts WHERE kind='market') AND kind='market'",
        "UPDATE _membership SET receipt='2026-10-13T23:59:59Z' WHERE event_id=(SELECT min(event_id) FROM _membership)",
    ],
)
def test_api_oracle_requires_fresh_receipts_for_fixed_discovery_work(oracle_catalogue, sql):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(sql)
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["receipt_coverage"] is False


@pytest.fixture
def absence_catalogue(oracle_catalogue):
    import duckdb

    from oddsfox_catalogue.capture.writer import write_page
    from oddsfox_catalogue.current_rows import project_response

    root, seed = oracle_catalogue
    metadata_path = seed / "api-seed.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.update(
        workload="closed_any",
        expected_refreshed_market_ids=metadata["expected_market_ids"],
        expected_absent_market_ids=["99999999999999999999"],
    )
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        benchmark.apply(
            conn,
            project_response(
                [benchmark.make_market("99999999999999999999", "Known absent market")],
                "markets",
                received_at="2026-10-13T00:00:00Z",
                response_id="seed-absent",
            ),
        )
        metadata["expected_workspace_market_ids"].append("99999999999999999999")
        metadata["expected_workspace_market_ids"].sort(key=int)
        for table in ("events", "markets", "event_markets", "outcomes"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            metadata["expected_public_fingerprints"][table] = benchmark.canonical_hash(rows)
            metadata["expected_counts"][table] = len(rows)
        for table in ("_identity", "_identity_claims"):
            columns = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
            rows = [
                dict(zip(columns, row, strict=True))
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            ]
            metadata["expected_private_fingerprints"][table] = benchmark.canonical_hash(rows)
        metadata["expected_receipt_keys"] = [
            list(row)
            for row in conn.execute("SELECT kind,venue,id FROM _receipts ORDER BY ALL").fetchall()
        ]
        metadata["expected_generation"] = json.loads(
            conn.execute("SELECT value FROM sync_state WHERE key='generation'").fetchone()[0]
        )
        conn.execute(
            "UPDATE _receipts SET receipt='2026-10-14T00:00:01Z',response='absence-proof' WHERE kind='market' AND venue='polymarket' AND id='99999999999999999999'"
        )
        benchmark.set_state(
            conn,
            "api_benchmark_omission_progress",
            {"complete": True, "ids": metadata["expected_absent_market_ids"]},
        )
    metadata_path.write_text(json.dumps(metadata))
    raw_dir = root / "raw" / "absence-proof"
    raw_dir.mkdir(parents=True)
    written = write_page(
        raw_dir,
        1,
        b'{"error":"not found"}',
        {
            "request": {"endpoint": "/markets/99999999999999999999", "params": {}},
            "status": 404,
            "received_at": "2026-10-14T00:00:01Z",
        },
    )
    from oddsfox_catalogue.current_config import CurrentSettings
    from oddsfox_catalogue.runlock import run_lock

    settings = CurrentSettings(root)
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn, run_lock(settings.run_lock_path):
        benchmark._publish(settings, conn, time.monotonic() + 1200)
        benchmark.set_state(conn, "last_publish_at", benchmark.BASE_TIME.isoformat())
        conn.execute("CHECKPOINT")
    oracle = benchmark.api_workload_oracle(root, seed, "closed_any", 1)
    assert oracle["oracle_pass"], oracle["oracle_checks"]
    return root, seed, written.manifest_path


@pytest.mark.parametrize(
    "mutation",
    [
        {"status": 200},
        {"request": {"endpoint": "/markets/1", "params": {}}},
        {"received_at": "2026-10-13T00:00:00Z"},
    ],
)
def test_api_oracle_binds_confirmed_absence_to_native_request_and_receipt(
    absence_catalogue, mutation
):
    root, seed, manifest_path = absence_catalogue
    manifest = json.loads(manifest_path.read_text())
    manifest.update(mutation)
    manifest_path.write_text(json.dumps(manifest))
    result = benchmark.api_workload_oracle(root, seed, "closed_any", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["absence_manifests"] is False


def test_api_oracle_rejects_known_absence_without_fresh_native_404(absence_catalogue):
    import duckdb

    root, seed, _manifest_path = absence_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(
            "UPDATE _receipts SET receipt='2026-10-13T00:00:00Z' "
            "WHERE kind='market' AND venue='polymarket' AND id='99999999999999999999'"
        )
    result = benchmark.api_workload_oracle(root, seed, "closed_any", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["receipt_coverage"] is False


def test_api_equivalence_allows_intended_request_count_changes_only():
    baseline, candidate = samples("discovery_sparse"), samples("discovery_sparse", 17)
    for old, new in zip(baseline, candidate, strict=True):
        old["settled_units"] = 100
        new["settled_units"] = 10
        old["completed_work"]["request_or_page_count"] = 100
        new["completed_work"]["request_or_page_count"] = 10
    assert (
        benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == "qualified"
    )
    candidate[0]["completed_work"]["events"] -= 1
    with pytest.raises(ValueError, match="completed work"):
        benchmark.api_comparison(baseline, candidate, "discovery_sparse")


@pytest.mark.parametrize(
    "key,value,check",
    [
        (
            "fetch_pending",
            {"identity": {"lane": "markets"}, "remaining": [{"id": ["1"]}], "raw_id": "pinned"},
            "no_pending_work",
        ),
        (
            "keyset_pending",
            {"endpoint": "/events/keyset", "cursor": "opaque", "raw_id": "pinned"},
            "no_pending_work",
        ),
        (
            "keyset_discovery",
            {"lanes": {"events": {"passes": {"all": {"done": False}}}}},
            "keyset_progress",
        ),
    ],
)
def test_api_oracle_rejects_unfinished_legacy_and_keyset_work(oracle_catalogue, key, value, check):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute("INSERT INTO sync_state VALUES (?,?)", [key, json.dumps(value)])
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"][check] is False


def test_api_oracle_rejects_terminal_keyset_pass_with_saved_cursor(oracle_catalogue):
    import duckdb

    root, seed = oracle_catalogue
    metadata = json.loads((seed / "api-seed.json").read_text())
    event_highwater = max(map(int, metadata["expected_event_ids"]))
    market_highwater = max(map(int, metadata["expected_market_ids"]))
    state = {
        "version": 1,
        "next_lane": 0,
        "lanes": {
            "events": {
                "passes": {
                    "all": {
                        "highwater": event_highwater,
                        "cursor": "still-pending",
                        "last_id": event_highwater,
                        "page_limit": 100,
                        "done": True,
                    }
                }
            },
            "markets": {
                "passes": {
                    name: {
                        "highwater": market_highwater,
                        "cursor": None,
                        "last_id": market_highwater,
                        "page_limit": 100,
                        "done": True,
                    }
                    for name in ("open", "closed")
                }
            },
        },
    }
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute("INSERT INTO sync_state VALUES ('keyset_discovery',?)", [json.dumps(state)])
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["keyset_progress"] is False


def test_api_oracle_rejects_publication_generation_gap(oracle_catalogue):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(
            "UPDATE sync_state SET value=to_json(CAST(value AS BIGINT)+1) WHERE key='generation'"
        )
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["generation"] is False


def test_api_oracle_requires_completed_discovery_frontiers(oracle_catalogue):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        discovery = json.loads(
            conn.execute("SELECT value FROM sync_state WHERE key='discovery'").fetchone()[0]
        )
        discovery["lanes"]["events"]["frontier"] -= 1
        conn.execute("UPDATE sync_state SET value=? WHERE key='discovery'", [json.dumps(discovery)])
    assert benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)["oracle_pass"] is False


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO _receipts VALUES ('market','other-venue','1','2026-10-14T00:00:01Z','foreign')",
        "INSERT INTO _membership VALUES ('other-venue','1','2026-10-14T00:00:01Z','foreign',FALSE,'2026-10-14T00:00:01Z','foreign')",
    ],
)
def test_api_oracle_rejects_unexpected_receipt_namespaces(oracle_catalogue, sql):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(sql)
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["receipt_coverage"] is False


def test_api_oracle_rejects_unexpected_parent_attempt(oracle_catalogue):
    import duckdb

    root, seed = oracle_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(
            "CREATE TABLE _reconcile_attempts (kind VARCHAR,id VARCHAR,last_attempt TIMESTAMPTZ)"
        )
        conn.execute("INSERT INTO _reconcile_attempts VALUES ('native','1','2026-10-14T00:00:01Z')")
    result = benchmark.api_workload_oracle(root, seed, "discovery_dense", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["attempts"] is False


@pytest.mark.parametrize(
    "marker",
    [
        None,
        {"complete": False, "ids": ["99999999999999999999"]},
        {"complete": True, "ids": []},
        {"complete": True, "ids": ["1"]},
    ],
)
def test_api_oracle_requires_exact_terminal_omission_progress(absence_catalogue, marker):
    import duckdb

    root, seed, _manifest = absence_catalogue
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        if marker is None:
            conn.execute("DELETE FROM sync_state WHERE key='api_benchmark_omission_progress'")
        else:
            benchmark.set_state(conn, "api_benchmark_omission_progress", marker)
    result = benchmark.api_workload_oracle(root, seed, "closed_any", 1)
    assert result["oracle_pass"] is False
    assert result["oracle_checks"]["omission_progress"] is False
