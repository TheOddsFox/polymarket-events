"""Focused tests for V6 campaign ownership and evidence gates."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("campaign_v6", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


def _package(path: Path, files: dict[str, str]) -> dict[str, str]:
    path.mkdir(parents=True)
    for name, contents in files.items():
        destination = path / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(contents)
    return campaign.bench.source_hashes(path)


def test_v6_freezing_accepts_only_owned_claim_index_module_addition(tmp_path):
    baseline = tmp_path / "baseline"
    base_hashes = _package(
        baseline,
        {"__init__.py": "", "current_store.py": "BASE = 1\n", "cli.py": "CLI = 1\n"},
    )
    candidate = tmp_path / "index-candidate"
    candidate_hashes = _package(
        candidate,
        {
            "__init__.py": "",
            "current_store.py": "BASE = 2\n",
            "cli.py": "CLI = 2\n",
            "current_claim_indexes.py": "INDEX = 'idx_identity_claims_asset_id'\n",
        },
    )
    output = tmp_path / "evidence"
    output.mkdir()
    frozen = campaign._freeze_v6_packages(
        {
            "baseline": (baseline, base_hashes),
            "claim_asset_index": (candidate, candidate_hashes),
        },
        output,
        campaign.bench.Budget(limit_s=60),
    )
    assert frozen["provenance"]["claim_asset_index"]["eligible"] is True
    assert "current_claim_indexes.py" in campaign._hash_package(
        frozen["packages"]["claim_asset_index"]
    )


def test_v6_combination_must_differ_from_each_declared_independent_package(tmp_path):
    baseline = tmp_path / "baseline"
    base_hashes = _package(baseline, {"current_store.py": "BASE = 1\n"})
    staged = tmp_path / "staged"
    staged_hashes = _package(staged, {"current_store.py": "STAGED = 1\n"})
    columnar = tmp_path / "columnar"
    columnar_hashes = _package(columnar, {"current_store.py": "COLUMNAR = 1\n"})
    combination = tmp_path / "combination"
    combination_hashes = _package(combination, {"current_store.py": "STAGED = 1\n"})
    output = tmp_path / "evidence"
    output.mkdir()
    with pytest.raises(ValueError, match="is identical to its staged_state_keys package"):
        campaign._freeze_v6_packages(
            {
                "baseline": (baseline, base_hashes),
                "staged_state_keys": (staged, staged_hashes),
                "columnar_renewals": (columnar, columnar_hashes),
                "_combination:columnar_renewals+staged_state_keys": (
                    combination,
                    combination_hashes,
                ),
            },
            output,
            campaign.bench.Budget(limit_s=60),
        )


def test_v6_synthetic_guard_rejects_any_http_counter_increase(monkeypatch, tmp_path):
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    rows = []
    for pair in (1, 2, 3):
        rows.extend(
            [
                {
                    "category": "baseline",
                    "pair": pair,
                    "semantic_hash": "same",
                    "cleanup_deferred": False,
                    "acquire_apply_s": 10.0,
                    "wall_s": 12.0,
                    "peak_rss_bytes": 100,
                    "worker_peak_before_oracle_bytes": 90,
                    "http_attempts": 1,
                    "downloaded_bytes": 10,
                    "runtime_reserved_attempts": 1,
                    "runtime_downloaded_bytes": 10,
                },
                {
                    "category": "candidate",
                    "pair": pair,
                    "semantic_hash": "same",
                    "cleanup_deferred": False,
                    "acquire_apply_s": 10.0,
                    "wall_s": 12.0,
                    "peak_rss_bytes": 100,
                    "worker_peak_before_oracle_bytes": 90,
                    "http_attempts": 1,
                    "downloaded_bytes": 11 if pair == 2 else 10,
                    "runtime_reserved_attempts": 1,
                    "runtime_downloaded_bytes": 10,
                },
            ]
        )

    def compare(*_args, **_kwargs):
        return {"status": "deferred", "reason": "no speed improvement"}

    monkeypatch.setattr(campaign, "_compare_stage", compare)
    report = {"stages": {"candidate_synthetic_guard_vs_baseline": {"samples": rows}}}
    result = campaign._v6_synthetic_guard(
        "candidate",
        candidate,
        baseline,
        tmp_path / "seed",
        tmp_path / "output",
        campaign.bench.Budget(limit_s=60),
        set(),
        report,
    )
    assert result["status"] == "failed"
    assert result["http_counters_nonincrease"] is False


def test_v6_source_inventory_requires_exact_regular_tree(tmp_path):
    source = tmp_path / "catalogue-source"
    source.mkdir()
    db = source / "catalogue.duckdb"
    db.write_bytes(b"database")
    digest = hashlib.sha256(db.read_bytes()).hexdigest()
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    entry = {"path": str(source), "source_hashes": {"catalogue.duckdb": digest}}
    resolved, actual = campaign._validate_v6_source_entry(entry, manifest)
    assert resolved == source
    assert actual == entry["source_hashes"]
    (source / "unlisted.toml").write_text("changed")
    with pytest.raises(ValueError, match="does not match"):
        campaign._validate_v6_source_entry(entry, manifest)


def test_v6_source_inventory_excludes_only_regular_root_writer_lock(tmp_path):
    source = tmp_path / "catalogue-source"
    source.mkdir()
    (source / "catalogue.duckdb").write_bytes(b"database")
    lock = source / ".catalogue.lock"
    lock.write_text("pid=123\n")
    (source / "nested").mkdir()
    (source / "nested" / ".catalogue.lock").write_text("catalogue data")
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    expected = campaign._hash_catalogue_source(source)
    assert set(expected) == {"catalogue.duckdb", "nested/.catalogue.lock"}
    lock.write_text("pid=456\n")
    actual, verified = campaign._validate_v6_source_entry(
        {"path": str(source), "source_hashes": expected}, manifest
    )
    assert actual == source
    assert verified == expected
    lock.unlink()
    lock.symlink_to(source / "catalogue.duckdb")
    with pytest.raises(ValueError, match="symlink"):
        campaign._hash_catalogue_source(source)


def test_v6_sample_supervises_tiny_fixed_worker_with_sibling_target(tmp_path, monkeypatch):
    import duckdb

    bench = campaign.bench
    source = tmp_path / "source"
    bench.create_seed(source, 4, 20)
    with duckdb.connect(str(source / "catalogue.duckdb"), read_only=True) as conn:
        closed_id = conn.execute(
            "SELECT event_id FROM events WHERE closed IS TRUE ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        active_id = conn.execute(
            "SELECT event_id FROM events WHERE closed IS FALSE ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        orphan_ids = [
            row[0]
            for row in conn.execute(
                "SELECT market_id FROM markets ANTI JOIN event_markets USING(venue, market_id) "
                "ORDER BY market_id"
            ).fetchall()
        ]
        counts = {
            table: conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            for table in ("events", "markets", "event_markets", "outcomes", "_identity_claims")
        }
    requests = tmp_path / "requests.json"
    requests.write_text(
        json.dumps(
            {
                "requests": {
                    "closed_event_batches": [[closed_id]],
                    "active_event_batches": [[active_id]],
                    "orphan_market_ids": orphan_ids,
                    "counts": counts,
                },
                "domain_clock": "2026-10-30T01:00:00+00:00",
            }
        )
    )
    package = Path(bench.oddsfox_catalogue.__file__).resolve().parent
    monkeypatch.setenv("FULL_PASS_EXPECTED_PACKAGE", str(package))
    output = tmp_path / "evidence"
    sample_output = output / "tiny-sample"

    sample = campaign._v6_sample(
        source,
        requests,
        sample_output,
        package,
        bench.Budget(limit_s=120),
        "tiny-driver-sample",
    )

    assert sample["publication_verified"] is True
    assert sample["fixed_receipt_coverage"] is True
    assert sample["completed_work"]["closed_batches_requested"] == 1
    assert sample["completed_work"]["active_batches_requested"] == 1
    assert sample["settled_units"] == 3
    target_id = hashlib.sha256(str(sample_output).encode()).hexdigest()[:16]
    assert (output / f"fixed-root-{target_id}").is_dir()


def _measured_v6_worker_result(package: Path) -> dict:
    return {
        "status": "measured",
        "runtime_source": str(package),
        "acquire_apply_s": 1.0,
        "runtime_sequence_wall_s": 1.0,
        "wall_s": 2.0,
        "peak_rss_bytes": 100,
        "worker_peak_before_oracle_bytes": 90,
        "http_attempts": 1,
        "downloaded_bytes": 10,
        "runtime_reserved_attempts": 1,
        "runtime_downloaded_bytes": 10,
        "source_stats": {},
        "publication_verified": True,
        "semantic_hash": "state",
        "settled_requests": [],
        "settled_units": 1,
        "completed_work": {},
        "classification_signature": {},
        "capture_units": [],
        "scheduler_cursor_progression": "fixed-work",
        "publication_noop": True,
    }


def test_v6_worker_supervision_uses_campaign_budget_not_legacy_cap(tmp_path, monkeypatch):
    package = tmp_path / "runtime"
    package.mkdir()
    captured = {}

    def prepare(command, output, budget, log, *, package_root):
        captured.update(
            command=command,
            output=output,
            remaining=budget.remaining,
            log=log,
            package_root=package_root,
        )
        return _measured_v6_worker_result(package)

    monkeypatch.setattr(campaign.bench, "supervised_prepare", prepare)
    monkeypatch.setattr(
        campaign.bench,
        "supervised",
        lambda *_args, **_kwargs: pytest.fail("legacy worker cap used for V6 fixed workload"),
    )
    monkeypatch.setattr(campaign.bench, "validate_source_accounting", lambda *_args: None)
    budget = campaign.bench.Budget(limit_s=2000)

    row = campaign._v6_worker_result(["worker"], tmp_path, package, budget, "fullsize-warmup")

    assert row["status"] == "measured"
    assert captured["package_root"] == package
    assert captured["remaining"] > 1200
    assert captured["log"] == tmp_path / "fullsize-warmup.log"


def test_legacy_supervisor_keeps_1200_second_whole_worker_cap(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    script = repo / "scripts" / "verify-installed-workflow.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import json\n"
        "def run_command(argv, cwd, env, log, *, timeout, max_output_bytes):\n"
        "    log.write_text(json.dumps({'status': 'measured', 'timeout': timeout}))\n"
    )
    package = tmp_path / "runtime"
    package.mkdir()
    monkeypatch.setattr(campaign.bench, "REPO", repo)
    monkeypatch.setattr(campaign.bench, "base_environment", lambda: {})

    result = campaign.bench.supervised(
        ["worker"], tmp_path, package, campaign.bench.Budget(limit_s=2000), tmp_path / "legacy.log"
    )

    assert result == {"status": "measured", "timeout": 1200}


def test_v6_timeout_marks_later_candidates_not_attempted(tmp_path, monkeypatch):
    packages = {name: tmp_path / name for name in ("baseline", *campaign.V6_CANDIDATES)}
    for package in packages.values():
        package.mkdir()
    package_names = []

    def fixed_warm(name, _source, _requests, _output, _package, _budget, _report, _warmed):
        package_names.append(name)
        return {"wall_s": 1.0}

    def synthetic_warm(name, _package, _seed, _output, _budget, _warmed, report):
        report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1.0}}

    fixed_calls = []

    def timeout_comparison(stage, left_name, *_args, **_kwargs):
        fixed_calls.append((stage, left_name))
        raise TimeoutError("first candidate exhausted its allocated comparison budget")

    monkeypatch.setattr(campaign, "_v6_warm", fixed_warm)
    monkeypatch.setattr(campaign, "_warm", synthetic_warm)
    monkeypatch.setattr(campaign, "_v6_fixed_comparison", timeout_comparison)
    output = tmp_path / "campaign"
    output.mkdir()
    report = {
        "stages": {},
        "warmups": {},
        "catalogue_source": {"request_manifest_sha256": "frozen"},
    }
    provenance = {name: {"eligible": True} for name in campaign.V6_CANDIDATES}

    campaign._v6_run(
        tmp_path / "seed",
        "seed-hash",
        tmp_path / "source-snapshot",
        tmp_path / "requests.json",
        output,
        campaign.bench.Budget(limit_s=2000),
        set(),
        report,
        packages,
        provenance,
        {},
    )

    timed_out_candidate = campaign.V6_CANDIDATES[1]
    unvisited_candidate = campaign.V6_CANDIDATES[2]
    assert fixed_calls == [(f"{timed_out_candidate}_fixed_catalogue_vs_baseline", "baseline")]
    assert report["stages"][f"{timed_out_candidate}_fixed_catalogue_vs_baseline"]["status"] == (
        "incomplete"
    )
    assert "not_attempted" in report["deferred"][unvisited_candidate]
    assert f"{unvisited_candidate}_fixed" not in package_names
