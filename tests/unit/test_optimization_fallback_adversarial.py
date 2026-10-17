from __future__ import annotations

import io
import json
import signal

import pytest
from test_optimization_campaign import campaign, inventory, package


@pytest.mark.parametrize(
    "failure,status,exit_code",
    [
        (TimeoutError("provenance budget exhausted"), "insufficient_evidence", 0),
        (ValueError("provenance unavailable"), "failed", 1),
        (KeyboardInterrupt(), "partial", 130),
        (campaign.bench.Terminated(signal.SIGTERM), "partial", 128 + signal.SIGTERM),
    ],
)
def test_initial_provenance_failure_preserves_report_and_operator_exit(
    tmp_path, monkeypatch, failure, status, exit_code
):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    entry = {"path": str(source), "source_hashes": inventory(source)}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"baseline": entry, "binding": entry}))
    output = tmp_path / "evidence"

    def fail_before_provenance_exists(_budget):
        raise failure

    monkeypatch.setattr(campaign, "_campaign_provenance", fail_before_provenance_exists)
    args = campaign.parser().parse_args(["--manifest", str(manifest), "--output", str(output)])

    assert campaign.run(args) == exit_code
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == status
    assert report["retained"] == []
    assert report["stages"] == {}
    if isinstance(failure, Exception):
        assert str(failure) in report["error"]


@pytest.mark.parametrize(
    "failure,status,exit_code",
    [
        (TimeoutError("queue comparison exhausted"), "insufficient_evidence", 0),
        (KeyboardInterrupt(), "partial", 130),
        (campaign.bench.Terminated(signal.SIGTERM), "partial", 128 + signal.SIGTERM),
    ],
)
def test_later_optional_failure_discards_previously_qualified_secondary(
    tmp_path, monkeypatch, failure, status, exit_code
):
    entries = {}
    for name, store, queue in (
        ("baseline", 1, 1),
        ("binding", 2, 1),
        ("outcome_grouping", 3, 1),
        ("queue", 2, 2),
    ):
        source = package(
            tmp_path / name,
            **{
                "current_store.py": f"VALUE = {store}\n",
                "current_reconcile.py": f"VALUE = {queue}\n",
            },
        )
        entries[name] = {"path": str(source), "source_hashes": inventory(source)}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(entries))
    output = tmp_path / "evidence"
    monkeypatch.setattr(campaign, "_campaign_provenance", lambda _budget: {"source_hashes": {}})
    monkeypatch.setattr(campaign.bench, "build_dataset", lambda *_args: (None, None, {"events": 1}))

    def prepare(_argv, output, budget, _log, *, package_root):
        seed = output / "seed" / "catalogue.duckdb"
        seed.parent.mkdir(parents=True)
        seed.write_bytes(b"immutable seed")
        return {
            "status": "prepared",
            "source_hashes": inventory(package_root),
            "distribution": {"events": 1},
            "seed_sha256": campaign._digest_bounded(seed, budget),
        }

    def compare(stage, *_args):
        report = _args[-1]
        report["stages"][stage] = {
            "status": "running",
            "samples": [{"category": "binding", "wall_s": 1}] * 3,
        }
        if stage == "queue_vs_binding":
            assert report["retained"] == ["binding", "outcome_grouping"]
            raise failure
        report["stages"][stage]["status"] = "qualified"
        return {"status": "qualified"}

    monkeypatch.setattr(campaign.bench, "supervised_prepare", prepare)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    args = campaign.parser().parse_args(["--manifest", str(manifest), "--output", str(output)])

    assert campaign.run(args) == exit_code
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == status
    assert report["retained"] == ["binding"]
    assert report["stages"]["outcome_grouping_vs_binding"]["status"] == "qualified"
    assert report["stages"]["queue_vs_binding"]["status"] == "running"
    assert "final_combination" not in report


@pytest.mark.parametrize("remaining,expected_reads", [(0.5, 1), (0.0, 0)])
def test_integrity_hash_stops_before_reading_in_shutdown_reserve(
    monkeypatch, remaining, expected_reads
):
    now = [0.0]
    monkeypatch.setattr(campaign.time, "monotonic", lambda: now[0])
    budget = campaign.bench.Budget(limit_s=1800)
    cutoff = budget.limit_s - campaign.bench.SHUTDOWN_RESERVE_S
    now[0] = cutoff - remaining
    reads = []

    class SlowStream(io.BytesIO):
        def read(self, size=-1):
            reads.append(now[0])
            now[0] += 0.5
            return super().read(size)

    class HashInput:
        def open(self, mode):
            assert mode == "rb"
            return SlowStream(b"x" * (2 * 1024 * 1024))

    with pytest.raises(TimeoutError, match="budget is exhausted"):
        campaign._digest_bounded(HashInput(), budget)

    assert len(reads) == expected_reads
    assert all(start < cutoff for start in reads)
    assert budget.elapsed <= cutoff
