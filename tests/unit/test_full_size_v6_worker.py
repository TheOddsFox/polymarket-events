"""Focused correctness tests for the V6 full-size worker primitives."""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-full-pass.py"
SPEC = importlib.util.spec_from_file_location("benchmark_full_pass_v6", SCRIPT)
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)


def test_market_replacement_restores_full_namespaced_multiset():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE _identity_claims "
        "(venue VARCHAR, market_id VARCHAR, role VARCHAR, asset_kind VARCHAR, asset_id VARCHAR)"
    )
    rows = [
        ("polymarket", "42", "candidate", "condition", "asset-a"),
        ("polymarket", "42", "accepted", "condition", "asset-a"),
        ("polymarket", "42", "candidate", "token", "asset-b"),
        ("other", "42", "accepted", "condition", "other-asset"),
        ("polymarket", "42", "candidate", "token", "asset-b"),
    ]
    conn.executemany("INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)", rows)

    replaced = bench._replace_market_claims_diagnostic(conn, "polymarket", "42")

    assert bench._claim_multiset(replaced) == bench._claim_multiset(rows[:3] + rows[4:])
    assert conn.execute(
        "SELECT venue, market_id, role, asset_kind, asset_id FROM _identity_claims "
        "WHERE venue='other' AND market_id='42'"
    ).fetchall() == [rows[3]]
    assert (
        conn.execute(
            "SELECT count(*) FROM _identity_claims WHERE venue='polymarket' AND market_id='42'"
        ).fetchone()[0]
        == 4
    )
    conn.close()


@pytest.mark.parametrize("nested", [False, True])
def test_claim_resource_rejects_output_inside_source_without_writing(tmp_path, nested):
    source = tmp_path / "source"
    source.mkdir()
    (source / "catalogue.duckdb").write_bytes(b"read-only source")
    (source / "metadata.json").write_text('{"stable":true}\n')
    output = source / "evidence" if nested else source
    if nested:
        output.mkdir()
    before = bench._regular_tree_hashes(source)

    with pytest.raises(ValueError, match="disjoint"):
        bench.claim_resource_worker(source, tmp_path / "target", output)

    assert bench._regular_tree_hashes(source) == before
    assert not (source / "claim-resource.json").exists()
    assert not (output / "claim-resource.json").exists()


def test_claim_resource_preserves_failure_evidence_for_safe_output(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "catalogue.duckdb").write_bytes(b"read-only source")
    output = tmp_path / "evidence"
    output.mkdir()
    report_path = output / "claim-resource.json"
    monkeypatch.setattr(
        bench.oddsfox_catalogue, "current_claim_indexes", SimpleNamespace(), raising=False
    )
    monkeypatch.setenv("FULL_PASS_EXPECTED_PACKAGE", str(tmp_path / "different-runtime"))

    status = bench.claim_resource_worker(source, tmp_path / "target", output)

    assert status == 3
    report = json.loads(report_path.read_text())
    assert report["status"] == "failed"
    assert "different runtime" in report["message"]
    assert (source / "catalogue.duckdb").read_bytes() == b"read-only source"


def test_catalogue_copy_admission_checks_retained_and_free_space(monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "catalogue.duckdb").write_bytes(b"1234567890")
    target = tmp_path / "target"
    limits = SimpleNamespace(max_retained_bytes=10, free_reserve_bytes=5)
    monkeypatch.setattr(bench.shutil, "disk_usage", lambda _path: SimpleNamespace(free=14))

    with pytest.raises(ValueError, match="free-space reserve"):
        bench._admit_catalogue_copy(source, target, limits)
    assert not target.exists()

    monkeypatch.setattr(bench.shutil, "disk_usage", lambda _path: SimpleNamespace(free=15))
    result = bench._admit_catalogue_copy(source, target, limits)
    assert result["copy_bytes"] == 10

    (source / "catalogue.duckdb").write_bytes(b"12345678901")
    with pytest.raises(ValueError, match="retained limit"):
        bench._admit_catalogue_copy(source, target, limits)


def test_oracle_spill_limit_uses_only_remaining_allowance():
    assert bench._remaining_spill_bytes(100, 35) == 65
    with pytest.raises(ValueError, match="temporary storage allowance exhausted"):
        bench._remaining_spill_bytes(100, 100)
    with pytest.raises(ValueError, match="temporary storage allowance exhausted"):
        bench._remaining_spill_bytes(100, 101)


def test_catalogue_hash_rejects_lock_symlink_before_excluding_lock_name(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    external = tmp_path / "external"
    external.write_text("do not follow")
    (source / ".catalogue.lock").symlink_to(external)

    with pytest.raises(ValueError, match="symlink"):
        bench._regular_tree_hashes(source)

    assert external.read_text() == "do not follow"


def test_observation_clock_includes_latest_membership_and_identity_invalidation():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE _receipts(receipt TIMESTAMPTZ)")
    conn.execute("CREATE TABLE _membership(receipt TIMESTAMPTZ, latest_receipt TIMESTAMPTZ)")
    conn.execute("CREATE TABLE _identity(invalidated_at TIMESTAMPTZ)")
    conn.execute("CREATE TABLE _reconcile_attempts(last_attempt TIMESTAMPTZ)")
    conn.execute("INSERT INTO _receipts VALUES ('2020-01-01T00:00:00+00:00')")
    conn.execute(
        "INSERT INTO _membership VALUES ('2020-01-02T00:00:00+00:00', '2020-01-03T00:00:00+00:00')"
    )
    conn.execute("INSERT INTO _identity VALUES ('2020-01-04T00:00:00+00:00')")
    conn.execute("INSERT INTO _reconcile_attempts VALUES ('2020-01-05T00:00:00+00:00')")

    assert bench.catalogue_observation_clock(
        conn, now=datetime(2020, 1, 1, tzinfo=UTC)
    ) == datetime(2020, 1, 5, 0, 0, 0, 1, tzinfo=UTC)
    conn.close()


def test_fixed_worker_rejects_publication_that_exceeds_operation_deadline(monkeypatch):
    monkeypatch.setattr(bench.time, "monotonic", lambda: 10.0)

    with pytest.raises(TimeoutError, match="production deadline before standalone verification"):
        bench._require_operation_deadline(10.0, "standalone verification")


def test_fixed_worker_allows_standalone_verification_before_operation_deadline(monkeypatch):
    monkeypatch.setattr(bench.time, "monotonic", lambda: 9.9)

    bench._require_operation_deadline(10.0, "standalone verification")


def test_fixed_worker_warmup_exposes_direct_attribution(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"
    bench.create_seed(source, 4, 20)
    with duckdb.connect(str(source / "catalogue.duckdb"), read_only=True) as conn:
        closed = conn.execute(
            "SELECT event_id FROM events WHERE closed IS TRUE ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        active = conn.execute(
            "SELECT event_id FROM events WHERE closed IS FALSE ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        orphan_ids = [
            row[0]
            for row in conn.execute(
                "SELECT market_id FROM markets ANTI JOIN event_markets "
                "USING(venue,market_id) ORDER BY market_id"
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
                    "closed_event_batches": [[closed]],
                    "active_event_batches": [[active]],
                    "orphan_market_ids": orphan_ids,
                    "counts": counts,
                },
                "domain_clock": "2026-10-30T01:00:00+00:00",
            }
        )
    )
    output = tmp_path / "evidence"
    output.mkdir()
    package = Path(bench.oddsfox_catalogue.__file__).resolve().parent
    monkeypatch.setenv("FULL_PASS_EXPECTED_PACKAGE", str(package))

    code = bench.fixed_catalogue_worker(
        source, tmp_path / "target", output, "warm-attribution", requests, attribution=True
    )

    report = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert code == 0, report
    diagnostics = report["attribution_diagnostics"]
    assert diagnostics["mode"] == "warmup_only_direct_unprofiled"
    assert diagnostics["statement_count"] > 0
    assert diagnostics["sql_execute_wall_s"] >= 0
    assert diagnostics["result_materialization_wall_s"] >= 0
    assert diagnostics["parameter_scalar_count"] > 0
    assert diagnostics["nested_timings_overlap"] is True
    assert "native_profiles" in diagnostics
