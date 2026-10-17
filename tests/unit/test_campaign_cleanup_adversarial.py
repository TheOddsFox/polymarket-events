"""Adversarial boundaries for benchmark qualification and committed publication."""

from __future__ import annotations

import json
import signal
from dataclasses import replace

import duckdb
import pytest
from test_full_pass_benchmark import row
from test_optimization_campaign import campaign, inventory, package

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import apply, initialize
from oddsfox_catalogue.signals import Terminated


class RecordingConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = []

    def execute(self, query, *args, **kwargs):
        self.statements.append(query)
        return self.connection.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.connection, name)


@pytest.fixture
def publication_store(tmp_path):
    settings = CurrentSettings(
        tmp_path / "root", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    with duckdb.connect(str(settings.store_path)) as connection:
        initialize(connection)
        apply(
            connection,
            ResponseRows(
                "2026-01-01T00:00:00Z",
                "00000000000000000000000000000001",
                events=[dict(venue="polymarket", event_id="1", title="committed")],
            ),
        )
        yield settings, RecordingConnection(connection)


@pytest.mark.parametrize("path", ["fresh", "fast_noop", "verified_noop"])
@pytest.mark.parametrize("stop", [KeyboardInterrupt(), Terminated(signal.SIGTERM)])
def test_operator_stop_after_commit_is_not_replaced_by_rollback_failure(
    publication_store, monkeypatch, path, stop
):
    settings, connection = publication_store
    if path != "fresh":
        publisher.publish(settings, connection)
        if path == "verified_noop":
            monkeypatch.setattr(publisher, "_release_bytes_match", lambda *_args: False)
    connection.statements.clear()

    def interrupt_retention(*_args, **_kwargs):
        raise stop

    monkeypatch.setattr(publisher, "_retain_raw", interrupt_retention)
    with pytest.raises(type(stop)) as caught:
        publisher.publish(settings, connection)

    assert caught.value is stop
    transaction_statements = [
        query for query in connection.statements if query in {"COMMIT", "ROLLBACK"}
    ]
    assert transaction_statements == ["COMMIT"]
    assert publisher.verify_current(settings, connection=connection)["valid"] is True


@pytest.mark.parametrize("path", ["fresh", "fast_noop", "verified_noop"])
def test_commit_failure_before_execution_still_rolls_back_transaction(
    publication_store, monkeypatch, path
):
    settings, connection = publication_store
    if path != "fresh":
        publisher.publish(settings, connection)
        if path == "verified_noop":
            monkeypatch.setattr(publisher, "_release_bytes_match", lambda *_args: False)
    connection.statements.clear()
    execute = connection.execute
    failure = RuntimeError("commit was never executed")

    def fail_before_commit(query, *args, **kwargs):
        if query == "COMMIT":
            connection.statements.append(query)
            raise failure
        return execute(query, *args, **kwargs)

    monkeypatch.setattr(connection, "execute", fail_before_commit)
    with pytest.raises(RuntimeError) as caught:
        publisher.publish(settings, connection)
    assert caught.value is failure
    assert [query for query in connection.statements if query in {"COMMIT", "ROLLBACK"}] == [
        "COMMIT",
        "ROLLBACK",
    ]
    connection.connection.execute("BEGIN TRANSACTION")
    connection.connection.execute("ROLLBACK")


@pytest.mark.parametrize("path", ["fresh", "fast_noop", "verified_noop"])
def test_mandatory_admission_cleanup_failure_remains_fatal(publication_store, monkeypatch, path):
    settings, connection = publication_store
    if path != "fresh":
        publisher.publish(settings, connection)
        if path == "verified_noop":
            monkeypatch.setattr(publisher, "_release_bytes_match", lambda *_args: False)
    pointer = settings.published_dir / "current.json"
    prior_pointer = pointer.read_bytes() if pointer.exists() else None
    connection.statements.clear()
    failure = OSError("mandatory cleanup failed")

    def fail_mandatory_cleanup(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(publisher, "_cleanup", fail_mandatory_cleanup)
    with pytest.raises(OSError) as caught:
        publisher.publish(settings, connection)
    assert caught.value is failure
    assert [query for query in connection.statements if query in {"COMMIT", "ROLLBACK"}] == [
        "ROLLBACK"
    ]
    assert (pointer.read_bytes() if pointer.exists() else None) == prior_pointer


def _samples(acquire=20):
    return [
        {
            **row(
                pair,
                acquire=acquire,
                rss=10_000,
                offered_bytes=200,
                reserved_attempts=20,
            ),
            "measurement_version": 2,
            "worker_setup_wall_s": 1,
            "runtime_sequence_wall_s": 25,
            "oracle_wall_s": 4,
            "worker_peak_before_oracle_bytes": 1000,
        }
        for pair in (1, 2, 3)
    ]


def test_new_metrics_cannot_be_scored_against_implicitly_legacy_samples():
    legacy = [row(pair) for pair in (1, 2, 3)]
    result = campaign.bench.comparison(legacy, _samples(acquire=18), "perf")
    assert result["status"] == "invalid"
    assert "fresh baseline" in result["reason"]


@pytest.mark.parametrize("version", [True, False, 2.0, "2", None, 3])
def test_measurement_version_is_an_exact_supported_integer(version):
    candidate = _samples(acquire=18)
    candidate[0]["measurement_version"] = version
    with pytest.raises(ValueError, match="measurement version"):
        campaign.bench.comparison(_samples(), candidate, "perf")


@pytest.mark.parametrize("preoracle_peak,expected", [(1050, "qualified"), (1051, "deferred")])
def test_oracle_memory_cannot_hide_a_preoracle_memory_regression(preoracle_peak, expected):
    candidate = _samples(acquire=18)
    for sample in candidate:
        sample["worker_peak_before_oracle_bytes"] = preoracle_peak
    assert campaign.bench.comparison(_samples(), candidate, "perf")["status"] == expected


@pytest.mark.parametrize(
    "counter",
    ["http_attempts", "downloaded_bytes", "runtime_reserved_attempts", "runtime_downloaded_bytes"],
)
def test_http_increase_in_one_pair_cannot_be_offset_by_another_pair(counter):
    candidate = _samples(acquire=18)
    candidate[0][counter] += 1
    candidate[1][counter] -= 1
    assert campaign.bench.comparison(_samples(), candidate, "perf")["status"] == "deferred"


@pytest.mark.parametrize("side", ["baseline", "candidate"])
def test_one_deferred_cleanup_sample_cannot_establish_equivalent_work(side):
    samples = {"baseline": _samples(), "candidate": _samples(acquire=18)}
    samples[side][1]["cleanup_deferred"] = True
    result = campaign.bench.comparison(samples["baseline"], samples["candidate"], "perf")
    assert result["status"] == "deferred"
    assert "maintenance" in result["reason"]


def _v2_manifest(tmp_path):
    source = package(tmp_path / "runtime", **{"current_store.py": "VALUE = 1\n"})
    entry = {"path": str(source), "source_hashes": inventory(source)}
    return {"version": 2, "baseline": entry, "candidates": {"outcome_grouping": entry}}


def test_v2_manifest_has_no_binding_prerequisite(tmp_path):
    manifest = _v2_manifest(tmp_path)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    loaded = campaign._load_manifest(path)
    paths = campaign._preflight_paths(loaded, path, tmp_path / "evidence")
    assert set(paths) == {"baseline", "outcome_grouping"}


@pytest.mark.parametrize("location", ["top", "candidate"])
def test_v2_manifest_rejects_executable_configuration_before_creating_output(tmp_path, location):
    manifest = _v2_manifest(tmp_path)
    if location == "top":
        manifest["command"] = ["python", "untrusted.py"]
    else:
        manifest["candidates"]["outcome_grouping"] = {
            **manifest["baseline"],
            "command": ["python", "untrusted.py"],
        }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    output = tmp_path / "evidence"
    args = campaign.parser().parse_args(["--manifest", str(path), "--output", str(output)])
    with pytest.raises(ValueError):
        campaign.run(args)
    assert not output.exists()


@pytest.mark.parametrize("version", [True, False, 2.0, "2", None, 3])
def test_v2_manifest_rejects_ambiguous_or_unsupported_versions(tmp_path, version):
    manifest = _v2_manifest(tmp_path)
    manifest["version"] = version
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        campaign._load_manifest(path)


@pytest.mark.parametrize("name", ["binding", "projection", "urgency", "__import__"])
def test_v2_manifest_accepts_only_owned_current_candidates(tmp_path, name):
    manifest = _v2_manifest(tmp_path)
    manifest["candidates"][name] = manifest["baseline"]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        campaign._load_manifest(path)


def test_v2_evaluates_first_candidate_before_spending_second_warmup(tmp_path, monkeypatch):
    calls = []

    def warm(name, _package, _seed, _output, _budget, _warmed, report):
        calls.append(f"warm-{name}")
        report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1}}

    def compare(stage, _left_name, _left, right_name, *_args):
        report = _args[-1]
        calls.append(f"score-{right_name}")
        report["stages"][stage] = {"samples": []}
        return {"status": "deferred"}

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    packages = {name: tmp_path / name for name in ("baseline", "outcome_grouping", "queue")}
    provenance = {name: {"eligible": True} for name in packages}
    report = {"stages": {}}
    campaign._run_v2(
        tmp_path / "seed",
        tmp_path,
        campaign.bench.Budget(limit_s=1800),
        set(),
        report,
        packages,
        provenance,
        "seed-hash",
        {},
    )
    assert calls == [
        "warm-baseline",
        "warm-outcome_grouping",
        "score-outcome_grouping",
        "warm-queue",
        "score-queue",
    ]


@pytest.mark.parametrize("limit,scored", [(37.5, True), (37.499, False)])
def test_v2_scoring_admission_preserves_integrity_and_shutdown_reserves(
    tmp_path, monkeypatch, limit, scored
):
    monkeypatch.setattr(campaign.time, "monotonic", lambda: 0.0)
    calls = []

    def warm(name, _package, _seed, _output, _budget, _warmed, report):
        report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1}}

    def compare(stage, *_args):
        report = _args[-1]
        calls.append(stage)
        report["stages"][stage] = {"samples": []}
        return {"status": "deferred"}

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    packages = {name: tmp_path / name for name in ("baseline", "outcome_grouping")}
    report = {"stages": {}}
    campaign._run_v2(
        tmp_path / "seed",
        tmp_path,
        campaign.bench.Budget(limit_s=limit),
        set(),
        report,
        packages,
        {"outcome_grouping": {"eligible": True}},
        "seed-hash",
        {},
    )
    assert bool(calls) is scored
    assert report["admission"]["outcome_grouping"]["projected_scored_seconds"] == 7.5


def test_v2_second_warmup_and_score_cannot_spend_combination_reserve(tmp_path, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(campaign.time, "monotonic", lambda: now[0])
    calls = []

    def warm(name, _package, _seed, _output, budget, _warmed, report):
        if name == "queue":
            assert budget.holdback_s == 140
        report.setdefault("warmups", {})[name] = {
            "sample": {"wall_s": 100 if name == "queue" else 1}
        }

    def compare(stage, *_args):
        report = _args[-1]
        calls.append(stage)
        now[0] = 100
        report["stages"][stage] = {"samples": []}
        return {"status": "qualified", "saved_s": 2}

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(campaign, "_integrity_check", lambda *_args: None)
    monkeypatch.setattr(campaign, "_hash_package", lambda *_args: {})
    packages = {name: tmp_path / name for name in ("baseline", "outcome_grouping", "queue")}
    report = {"stages": {}}
    campaign._run_v2(
        tmp_path / "seed",
        tmp_path,
        campaign.bench.Budget(limit_s=500),
        set(),
        report,
        packages,
        {name: {"eligible": True} for name in packages},
        "seed-hash",
        {},
    )
    assert calls == ["outcome_grouping_vs_baseline"]
    assert report["retained"] == ["outcome_grouping"]
    assert report["admission"]["queue"]["combination_reserve_seconds"] == 125
    assert "insufficient_evidence" in report["deferred"]["queue"]


def test_v2_combination_can_consume_its_reserve_without_spending_integrity_or_shutdown(
    tmp_path, monkeypatch
):
    now = [0.0]
    monkeypatch.setattr(campaign.time, "monotonic", lambda: now[0])
    budget = campaign.bench.Budget(limit_s=460)
    packages = {name: tmp_path / name for name in ("baseline", "outcome_grouping", "queue")}
    combined = tmp_path / "final"
    combined_hashes = {"current_store.py": "combined"}
    calls = []

    def warm(name, _package, _seed, _output, shared_budget, _warmed, report):
        if name == "final_combination":
            assert shared_budget.holdback_s == 15
            shared_budget.admit(10)
            now[0] += 10
            calls.append("combination-warm")
        report.setdefault("warmups", {})[name] = {"sample": {"wall_s": 1}}

    def compare(stage, *_args):
        report = _args[-1]
        now[0] = {
            "outcome_grouping_vs_baseline": 100,
            "queue_vs_baseline": 300,
            "final_combination_vs_baseline": 330,
        }[stage]
        calls.append(stage)
        report["stages"][stage] = {"samples": []}
        return {"status": "qualified", "saved_s": 2}

    def assemble(_packages, _candidates, _output, shared_budget):
        assert shared_budget.holdback_s == 15
        shared_budget.admit(10)
        now[0] += 10
        calls.append("assemble")
        return combined, combined_hashes

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_compare_stage", compare)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assemble_v2_combination", assemble)
    monkeypatch.setattr(campaign, "_hash_package", lambda *_args: combined_hashes)
    report = {"stages": {}}
    campaign._run_v2(
        tmp_path / "seed",
        tmp_path,
        budget,
        set(),
        report,
        packages,
        {name: {"eligible": True} for name in packages},
        "seed-hash",
        {},
    )
    assert calls == [
        "outcome_grouping_vs_baseline",
        "queue_vs_baseline",
        "assemble",
        "combination-warm",
        "final_combination_vs_baseline",
    ]
    assert report["retained"] == ["outcome_grouping", "queue", "combined"]
    assert report["status"] == "qualified"
    assert budget.holdback_s == 15
    assert budget.remaining == 100


@pytest.mark.parametrize("failure", [None, TimeoutError("validation budget expired")])
def test_v2_fallback_needs_fresh_integrity_and_harness_validation(tmp_path, monkeypatch, failure):
    calls = []
    monkeypatch.setattr(campaign.time, "monotonic", lambda: 0.0)
    budget = campaign.bench.Budget(limit_s=1800)
    budget.holdback_s = 140

    def integrity(*_args):
        assert budget.holdback_s == 0
        calls.append("integrity")
        if failure is not None:
            raise failure

    monkeypatch.setattr(campaign, "_integrity_check", integrity)
    monkeypatch.setattr(
        campaign, "_assert_campaign_provenance", lambda *_args: calls.append("harness")
    )
    report = {
        "retained": ["combined"],
        "qualified_candidates": {
            name: {"comparison": {"saved_s": 2}} for name in ("queue", "outcome_grouping")
        },
    }
    campaign._retain_best_v2_candidate(report, {}, {}, tmp_path, "seed", {}, budget)
    assert report["retained"] == (["outcome_grouping"] if failure is None else [])
    assert calls == (["integrity", "harness"] if failure is None else ["integrity"])
    assert budget.holdback_s == 0


@pytest.mark.parametrize("tampered", [False, True])
def test_v2_timeout_spends_reserved_integrity_time_then_returns_valid_fallback_exit(
    tmp_path, monkeypatch, tampered
):
    now = [0.0]
    monkeypatch.setattr(campaign.time, "monotonic", lambda: now[0])
    budget = campaign.bench.Budget(limit_s=1800)
    entries = {}
    for name, value in (("baseline", 1), ("outcome_grouping", 2)):
        source = package(tmp_path / name, **{"current_store.py": f"VALUE = {value}\n"})
        entries[name] = {"path": str(source), "source_hashes": inventory(source)}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 2,
                "baseline": entries["baseline"],
                "candidates": {"outcome_grouping": entries["outcome_grouping"]},
            }
        )
    )
    output = tmp_path / "evidence"
    provenance_calls = []

    def provenance(shared_budget):
        shared_budget.admit(0.001)
        provenance_calls.append(now[0])
        if now[0] > 0:
            now[0] += 4
        return {"source_hashes": {"harness": "unchanged"}}

    def prepare(_argv, output, shared_budget, _log, *, package_root):
        seed = output / "seed" / "catalogue.duckdb"
        seed.parent.mkdir(parents=True)
        seed.write_bytes(b"immutable seed")
        return {
            "status": "prepared",
            "source_hashes": inventory(package_root),
            "distribution": {"events": 1},
            "seed_sha256": campaign._digest_bounded(seed, shared_budget),
        }

    def exhausted_optional_stage(seed, _output, shared_budget, _warmed, report, *_args):
        report["qualified_candidates"] = {"outcome_grouping": {"comparison": {"saved_s": 2}}}
        shared_budget.holdback_s = 140
        now[0] = 1775
        if tampered:
            (seed / "catalogue.duckdb").write_bytes(b"changed seed")
        raise TimeoutError("optional comparison could not finish")

    original_integrity = campaign._integrity_check
    integrity_calls = []

    def consume_integrity_reserve(*args):
        shared_budget = args[-1]
        assert shared_budget is budget
        assert shared_budget.holdback_s == 0
        shared_budget.admit(5)
        integrity_calls.append(now[0])
        original_integrity(*args)
        now[0] += 5

    monkeypatch.setattr(campaign, "_campaign_provenance", provenance)
    monkeypatch.setattr(campaign.bench, "build_dataset", lambda *_args: (None, None, {"events": 1}))
    monkeypatch.setattr(campaign.bench, "supervised_prepare", prepare)
    monkeypatch.setattr(campaign, "_run_v2", exhausted_optional_stage)
    monkeypatch.setattr(campaign, "_integrity_check", consume_integrity_reserve)
    args = campaign.parser().parse_args(["--manifest", str(manifest), "--output", str(output)])

    assert campaign.run(args, budget=budget) == 0
    report = json.loads((output / "report.json").read_text())
    assert report["retained"] == ([] if tampered else ["outcome_grouping"])
    assert report["status"] == (
        "insufficient_evidence" if tampered else "qualified_individual_combination_deferred"
    )
    assert integrity_calls == [1775]
    assert provenance_calls == ([0] if tampered else [0, 1780])
    assert budget.holdback_s == 0
    assert now[0] < 1785
