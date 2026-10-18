from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-full-pass.py"
SPEC = importlib.util.spec_from_file_location("full_pass_api_workloads", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def rows(workload: str, runtime: float, *, candidate=False) -> list[dict]:
    result = []
    for pair in (1, 2, 3):
        result.append(
            {
                "pair": pair,
                "measurement_version": benchmark.MEASUREMENT_VERSION,
                "workload": workload,
                "oracle_pass": True,
                "oracle_fingerprint": "expected-content",
                "completed_work": {
                    "events": 1000,
                    "markets": 4040,
                    "pending": 0,
                    "request_or_page_count": 17 if candidate else 20,
                },
                "settled_units": 17 if candidate else 20,
                "runtime_sequence_wall_s": runtime,
                "wall_s": 100,
                "peak_rss_bytes": 1000,
                "worker_peak_before_oracle_bytes": 900,
                "http_attempts": 10,
                "downloaded_bytes": 100,
                "runtime_reserved_attempts": 10,
                "runtime_downloaded_bytes": 100,
                "candidate": candidate,
            }
        )
    return result


@pytest.mark.parametrize("workload", ["closed_any", "discovery_sparse"])
def test_api_primary_requires_ten_percent_and_one_second(workload):
    result = benchmark.api_comparison(
        rows(workload, 20), rows(workload, 17, candidate=True), workload
    )
    assert result["status"] == "qualified"
    assert result["saved_s"] == 3


@pytest.mark.parametrize("workload", ["discovery_dense", "discovery_tail"])
def test_api_guard_workloads_require_equivalent_work_and_no_material_regression(workload):
    result = benchmark.api_comparison(
        rows(workload, 20), rows(workload, 21, candidate=True), workload
    )
    assert result["status"] == "qualified"

    slower = benchmark.api_comparison(
        rows(workload, 20), rows(workload, 21.01, candidate=True), workload
    )
    assert slower["status"] == "deferred"


def test_api_comparison_rejects_missing_pairs_and_wrong_oracle():
    incomplete = rows("closed_any", 20)[:2]
    assert benchmark.api_comparison(incomplete, rows("closed_any", 17), "closed_any")["status"] == (
        "insufficient_evidence"
    )

    candidate = rows("closed_any", 17, candidate=True)
    candidate[1]["oracle_fingerprint"] = "different"
    with pytest.raises(ValueError, match="inconsistent oracle fingerprints"):
        benchmark.api_comparison(rows("closed_any", 20), candidate, "closed_any")

    old = rows("closed_any", 20)
    old[0].pop("measurement_version")
    assert benchmark.api_comparison(old, rows("closed_any", 17), "closed_any")["status"] == (
        "invalid"
    )


def test_closed_any_keeps_strict_request_and_byte_counters():
    candidate = rows("closed_any", 17, candidate=True)
    candidate[0]["http_attempts"] += 1
    candidate[0]["runtime_reserved_attempts"] += 1
    result = benchmark.api_comparison(rows("closed_any", 20), candidate, "closed_any")
    assert result["status"] == "deferred"
    assert not result["http_attempts_nonincreasing"]

    candidate = rows("closed_any", 17, candidate=True)
    candidate[0]["downloaded_bytes"] += 1
    result = benchmark.api_comparison(rows("closed_any", 20), candidate, "closed_any")
    assert result["status"] == "deferred"
    assert not result["downloaded_bytes_within_gate"]


def test_keyset_allows_only_five_percent_envelope_byte_growth():
    baseline = rows("discovery_sparse", 20)
    candidate = rows("discovery_sparse", 17, candidate=True)
    candidate[0]["downloaded_bytes"] = 105
    candidate[0]["runtime_downloaded_bytes"] = 105
    assert benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == (
        "qualified"
    )
    candidate[1]["downloaded_bytes"] = 106
    assert benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == (
        "deferred"
    )


def test_api_comparison_requires_all_safety_metrics_to_stay_within_five_percent():
    candidate = rows("closed_any", 17, candidate=True)
    candidate[0]["worker_peak_before_oracle_bytes"] = 946
    assert benchmark.api_comparison(rows("closed_any", 20), candidate, "closed_any")["status"] == (
        "qualified"
    )
    candidate[1]["worker_peak_before_oracle_bytes"] = 951
    assert benchmark.api_comparison(rows("closed_any", 20), candidate, "closed_any")["status"] == (
        "deferred"
    )


def test_api_comparison_ignores_scheduler_unit_count_but_requires_fixed_work():
    baseline = rows("discovery_sparse", 20)
    candidate = rows("discovery_sparse", 17, candidate=True)
    assert benchmark.api_comparison(baseline, candidate, "discovery_sparse")["status"] == (
        "qualified"
    )
    candidate[0]["completed_work"]["pending"] = 1
    with pytest.raises(ValueError, match="inconsistent completed work"):
        benchmark.api_comparison(baseline, candidate, "discovery_sparse")


def test_api_comparison_rejects_deferred_publication_maintenance():
    candidate = rows("closed_any", 17, candidate=True)
    candidate[0]["cleanup_deferred"] = True
    result = benchmark.api_comparison(rows("closed_any", 20), candidate, "closed_any")
    assert result["status"] == "deferred"
    assert "maintenance" in result["reason"]


def test_attempt_ledger_requires_exact_committed_known_unit_and_receipt_time():
    metadata = {
        "expected_event_ids": ["100", "101"],
        "expected_market_ids": ["200"],
        "expected_workspace_market_ids": ["200"],
        "receipt_cutoff": benchmark.BASE_TIME.isoformat(),
        "seed_attempts": [],
    }
    receipt = benchmark.BASE_TIME + timedelta(seconds=5)
    receipt_times = {("events", "100"): receipt, ("events", "101"): receipt}
    ledger = [
        {
            "requested_kind": "events",
            "requested_ids": ["100"],
            "native_request": False,
            "kind": "events",
            "ids": ["100"],
            "timestamp": (benchmark.BASE_TIME + timedelta(seconds=4)).isoformat(),
        }
    ]
    observed = [("events", "100", benchmark.BASE_TIME + timedelta(seconds=4))]
    assert benchmark.api_attempt_ledger_valid(metadata, observed, ledger, receipt_times)

    assert not benchmark.api_attempt_ledger_valid(metadata, [], ledger, receipt_times)
    extra = observed + [("events", "101", benchmark.BASE_TIME + timedelta(seconds=4))]
    assert not benchmark.api_attempt_ledger_valid(metadata, extra, ledger, receipt_times)
    changed = [("events", "100", benchmark.BASE_TIME + timedelta(seconds=3))]
    assert not benchmark.api_attempt_ledger_valid(metadata, changed, ledger, receipt_times)

    extra_ledger_id = [{**ledger[0], "ids": ["100", "200"], "requested_ids": ["100"]}]
    assert not benchmark.api_attempt_ledger_valid(
        metadata, observed, extra_ledger_id, receipt_times
    )


def test_attempt_ledger_rejects_uncommitted_split_and_bad_time_bounds():
    metadata = {
        "expected_event_ids": ["100"],
        "expected_market_ids": [],
        "expected_workspace_market_ids": [],
        "receipt_cutoff": benchmark.BASE_TIME.isoformat(),
        "seed_attempts": [],
    }
    receipt_times = {("events", "100"): benchmark.BASE_TIME + timedelta(seconds=2)}
    # A failed or unfinished split never returns from fetch_unit, so it creates
    # no ledger entry and cannot justify a durable attempt row.
    assert benchmark.api_attempt_ledger_valid(metadata, [], [], receipt_times)
    late = [
        {
            "requested_kind": "events",
            "requested_ids": ["100"],
            "native_request": False,
            "kind": "events",
            "ids": ["100"],
            "timestamp": (benchmark.BASE_TIME + timedelta(seconds=3)).isoformat(),
        }
    ]
    observed = [("events", "100", benchmark.BASE_TIME + timedelta(seconds=3))]
    assert not benchmark.api_attempt_ledger_valid(metadata, observed, late, receipt_times)
    naive = [{**late[0], "timestamp": datetime(2026, 10, 13).isoformat()}]
    assert not benchmark.api_attempt_ledger_valid(metadata, observed, naive, receipt_times)


def test_native_attempt_kind_cannot_be_used_for_an_event_lookup():
    metadata = {
        "expected_event_ids": ["100"],
        "expected_market_ids": ["100"],
        "expected_workspace_market_ids": ["100"],
        "receipt_cutoff": benchmark.BASE_TIME.isoformat(),
        "seed_attempts": [],
    }
    timestamp = benchmark.BASE_TIME + timedelta(seconds=1)
    receipt_times = {("events", "100"): timestamp, ("markets", "100"): timestamp}
    ledger = [
        {
            "requested_kind": "events",
            "requested_ids": ["100"],
            "native_request": True,
            "kind": "native",
            "ids": ["100"],
            "timestamp": timestamp.isoformat(),
        }
    ]
    observed = [("native", "100", timestamp)]
    assert not benchmark.api_attempt_ledger_valid(metadata, observed, ledger, receipt_times)


def test_attempt_ledger_enforces_the_scheduler_retry_cooldown():
    first = benchmark.BASE_TIME + timedelta(seconds=1)
    cutoff = benchmark.BASE_TIME.isoformat()
    metadata = {
        "expected_event_ids": ["100"],
        "expected_market_ids": [],
        "expected_workspace_market_ids": [],
        "receipt_cutoff": cutoff,
        "seed_attempts": [["events", "100", first.isoformat()]],
    }
    receipt_times = {("events", "100"): first + timedelta(hours=1)}
    ledger = [
        {
            "requested_kind": "events",
            "requested_ids": ["100"],
            "native_request": False,
            "kind": "events",
            "ids": ["100"],
            "timestamp": (first + timedelta(minutes=59)).isoformat(),
        }
    ]
    observed = [
        ("events", "100", first + timedelta(minutes=59)),
    ]
    assert not benchmark.api_attempt_ledger_valid(metadata, observed, ledger, receipt_times)
    ledger[0]["timestamp"] = (first + timedelta(hours=1)).isoformat()
    observed[0] = ("events", "100", first + timedelta(hours=1))
    assert benchmark.api_attempt_ledger_valid(metadata, observed, ledger, receipt_times)


def test_failed_or_unfinished_fetch_does_not_enter_settled_attempt_ledger():
    ledger = []

    def failed_fetch(*_args, **_kwargs):
        raise RuntimeError("split response did not settle")

    with pytest.raises(RuntimeError, match="did not settle"):
        benchmark.call_and_record_attempt(
            failed_fetch,
            ledger,
            object(),
            object(),
            object(),
            "events",
            ["100"],
            attempts=("events", ["100"], benchmark.BASE_TIME),
        )
    assert ledger == []

    result = benchmark.call_and_record_attempt(
        lambda *_args, **_kwargs: {"responses": 1},
        ledger,
        object(),
        object(),
        object(),
        "events",
        ["100"],
        attempts=("events", ["100"], benchmark.BASE_TIME),
    )
    assert result == {"responses": 1}
    assert len(ledger) == 1


class FakeClaimConnection:
    def __init__(self, *, fail_population_batch=None, fail_index=None):
        self.statements = []
        self.batch_sizes = []
        self.fail_population_batch = fail_population_batch
        self.fail_index = fail_index
        self.population_calls = 0
        self.committed_batches = 0

    def execute(self, statement):
        self.statements.append(statement)
        if statement.startswith("CREATE INDEX") and statement.startswith(
            f"CREATE INDEX idx_identity_claims_{self.fail_index}"
        ):
            raise RuntimeError("index creation failed")
        if statement == "COMMIT":
            self.committed_batches += 1
        return self

    def executemany(self, statement, rows):
        assert statement == "INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)"
        self.population_calls += 1
        self.batch_sizes.append(len(rows))
        if self.population_calls == self.fail_population_batch:
            raise RuntimeError("population batch failed")


def test_claim_fixture_population_commits_bounded_batches():
    conn = FakeClaimConnection()
    reports = []
    result = benchmark.populate_claim_fixture(
        conn, 20_005, batch_size=10_000, on_batch=reports.append
    )

    assert conn.batch_sizes == [10_000, 10_000, 5]
    assert conn.committed_batches == 3
    assert [report["completed_rows"] for report in reports] == [10_000, 20_000, 20_005]
    assert result == {
        "status": "population_complete",
        "requested_rows": 20_005,
        "completed_rows": 20_005,
        "completed_batches": 3,
        "batch_size": 10_000,
    }
    rows = benchmark.claim_fixture_rows(0, 4)
    assert [row[2] for row in rows] == ["candidate", "accepted", "candidate", "accepted"]
    assert {row[3] for row in rows} == {"clob", "position"}
    assert all(0 < int(row[4]) < 2**256 for row in rows)


def test_claim_population_failure_preserves_prior_commits_and_failure_evidence(tmp_path):
    conn = FakeClaimConnection(fail_population_batch=2)
    evidence = tmp_path / "claim-progress.json"

    with pytest.raises(RuntimeError, match="population batch failed"):
        benchmark.populate_claim_fixture(
            conn,
            25_000,
            batch_size=10_000,
            on_batch=lambda report: benchmark.atomic_write_json(evidence, report),
        )

    report = __import__("json").loads(evidence.read_text())
    assert report["status"] == "population_failed"
    assert report["completed_rows"] == 10_000
    assert report["completed_batches"] == 1
    assert report["failed_batch"] == {"start": 10_000, "end_exclusive": 20_000}
    assert conn.committed_batches == 1
    assert conn.statements[-2:] == ["BEGIN TRANSACTION", "ROLLBACK"]


def test_claim_population_failure_callback_preserves_original_operator_interrupt():
    class InterruptingConnection(FakeClaimConnection):
        def executemany(self, statement, rows):
            super().executemany(statement, rows)
            raise KeyboardInterrupt()

    def broken_evidence(_report):
        raise OSError("cannot persist diagnostic progress")

    conn = InterruptingConnection()
    with pytest.raises(KeyboardInterrupt):
        benchmark.populate_claim_fixture(conn, 1, batch_size=1, on_batch=broken_evidence)
    assert conn.statements[-2:] == ["BEGIN TRANSACTION", "ROLLBACK"]


def test_claim_population_failure_does_not_suppress_operator_interrupt_from_callback():
    conn = FakeClaimConnection(fail_population_batch=1)

    def interrupted_evidence(_report):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        benchmark.populate_claim_fixture(conn, 1, batch_size=1, on_batch=interrupted_evidence)


def test_postcommit_progress_callback_errors_remain_visible():
    conn = FakeClaimConnection()

    def broken_progress(_report):
        raise OSError("cannot persist progress")

    with pytest.raises(OSError, match="cannot persist progress"):
        benchmark.populate_claim_fixture(conn, 1, batch_size=1, on_batch=broken_progress)
    assert conn.committed_batches == 1
    assert conn.statements[-1] == "COMMIT"


def test_claim_indexes_are_built_atomically_and_rollback_on_partial_failure():
    conn = FakeClaimConnection()
    benchmark.build_claim_fixture_indexes(conn)
    assert conn.statements == [
        "BEGIN TRANSACTION",
        "CREATE INDEX idx_identity_claims_asset_id ON _identity_claims(asset_id)",
        "CREATE INDEX idx_identity_claims_market_id ON _identity_claims(market_id)",
        "COMMIT",
    ]

    failed = FakeClaimConnection(fail_index="market_id")
    with pytest.raises(RuntimeError, match="index creation failed"):
        benchmark.build_claim_fixture_indexes(failed)
    assert failed.statements == [
        "BEGIN TRANSACTION",
        "CREATE INDEX idx_identity_claims_asset_id ON _identity_claims(asset_id)",
        "CREATE INDEX idx_identity_claims_market_id ON _identity_claims(market_id)",
        "ROLLBACK",
    ]


def test_closed_any_seed_phantom_branch_uses_shared_set_state(monkeypatch, tmp_path):
    def create_empty_seed(root, *_args, **_kwargs):
        benchmark.validate_root(root, initialize=True)
        settings = benchmark.CurrentSettings(root)
        with benchmark.connect_store(settings) as conn:
            benchmark.initialize(conn)
        return {"events": 0, "markets": 0, "closed_events": 0, "urgent_active_events": 0}

    monkeypatch.setattr(benchmark, "create_seed", create_empty_seed)
    monkeypatch.setattr(benchmark, "_publish", lambda *_args, **_kwargs: ({"noop": True}, 0.0))
    monkeypatch.setattr(
        benchmark,
        "verify_current",
        lambda *_args, **_kwargs: {"store_generation": 1, "published_generation": 1},
    )

    # The tiny fixture intentionally lacks the 9,900 provider rows required by
    # the completed workload. Reaching this later validation proves the setup
    # branch created the phantom rows and called set_state without requiring a
    # full-scale seed build.
    with pytest.raises(ValueError, match="closed-any cohort selection is incomplete"):
        benchmark.create_api_seed(tmp_path / "closed-any", "closed_any")


def test_gamma_receipt_clock_tracks_shared_api_workload_clock():
    class Client:
        def __init__(self, *args, **kwargs):
            self.now = kwargs["now"]

    domain_clock = {"now": benchmark.BASE_TIME}
    client_factory = benchmark.frozen_gamma_factory(Client, domain_clock)
    client = client_factory().now

    assert client() == benchmark.BASE_TIME + timedelta(microseconds=1)
    domain_clock["now"] = benchmark.BASE_TIME + timedelta(hours=2)
    assert client() == benchmark.BASE_TIME + timedelta(hours=2, microseconds=1)


def test_closed_any_omissions_precede_native_404s_and_resume_from_durable_progress():
    absent_ids = [str(value) for value in range(1, 101)]

    class Store:
        def __init__(self):
            self.state = {}

        def get_state(self, _conn, key):
            return self.state.get(key)

    class Sync:
        def __init__(self):
            self.current_store = Store()
            self.calls = []
            self.receipt_ids = []
            self.raise_after_ten_native = True

        def fetch_unit(self, _settings, _conn, _client, kind, ids, *, progress):
            self.calls.append(("list", kind, list(ids), progress))
            self.current_store.state.update(progress)
            return {"responses": 1}, list(ids)

        def fetch_apply(self, _settings, _conn, _client, kind, ids, *, native, absence, progress):
            assert kind == "markets"
            assert native is True and absence is True
            market_id = ids[0]
            self.calls.append(("native", kind, list(ids), progress))
            self.receipt_ids.append(market_id)
            self.current_store.state.update(progress)
            if len(self.receipt_ids) == 10 and self.raise_after_ten_native:
                self.raise_after_ten_native = False
                raise RuntimeError("simulated interruption after durable progress")

    sync = Sync()
    clock = {"now": benchmark.BASE_TIME}
    with pytest.raises(RuntimeError, match="simulated interruption"):
        benchmark.settle_closed_any_absences(sync, object(), object(), object(), absent_ids, clock)

    assert sync.calls[0][0:3] == ("list", "markets", absent_ids)
    assert sync.calls[0][3] == {
        "api_benchmark_omission_progress": {"complete": True, "ids": absent_ids}
    }
    assert [call[2][0] for call in sync.calls[1:]] == absent_ids[:10]
    assert sync.current_store.get_state(None, "api_benchmark_absence_progress") == {
        "next_absent": 10
    }

    settled = benchmark.settle_closed_any_absences(
        sync, object(), object(), object(), absent_ids, clock
    )
    assert settled == 90
    assert sync.calls[10][0] == "native"
    assert [call[2][0] for call in sync.calls[11:]] == absent_ids[10:]
    assert sync.receipt_ids == absent_ids[:10] + absent_ids[10:]
    assert sync.current_store.get_state(None, "api_benchmark_omission_progress") == {
        "complete": True,
        "ids": absent_ids,
    }
    assert sync.current_store.get_state(None, "api_benchmark_absence_progress") == {
        "next_absent": 100
    }


@pytest.mark.parametrize(("workload", "stride"), [("discovery_dense", 1), ("discovery_sparse", 10)])
def test_discovery_api_seed_validates_dense_and_sparse_source_cohorts(
    monkeypatch, tmp_path, workload, stride
):
    create_seed = benchmark.create_seed

    def create_small_seed(root, event_count, market_count, **kwargs):
        if event_count == 0 and market_count == 0:
            return create_seed(root, event_count, market_count, **kwargs)
        assert event_count == 1000 and market_count == 4040
        return create_seed(
            root,
            10,
            40,
            entity_id_base=kwargs.get("entity_id_base", 0),
            entity_id_stride=kwargs.get("entity_id_stride", 1),
            publish=kwargs.get("publish", True),
        )

    monkeypatch.setattr(benchmark, "create_seed", create_small_seed)
    metadata = benchmark.create_api_seed(tmp_path / workload, workload)

    assert metadata["distribution"]["events"] == 10
    assert metadata["distribution"]["markets"] == 40
    assert metadata["distribution"]["id_stride"] == stride
    assert metadata["distribution"]["urgent_active_events"] == 1
    assert metadata["distribution"]["closed_events"] == 9


def test_discovery_interruption_latches_through_caught_stop_and_repair():
    class Stop(Exception):
        pass

    stop = benchmark.SettledUnitStop(50, Stop)
    for _ in range(49):
        stop.before_unit()
        stop.record(1)
    stop.record(0)  # A keyset completion transition without an HTTP response.
    assert stop.settled == 49
    stop.before_unit()
    stop.record(1)

    # The discovery scheduler catches the first stop; runtime then attempts a
    # parent repair before returning from the same supervised operation.
    with pytest.raises(Stop):
        try:
            stop.before_unit()
        except Stop:
            assert stop.triggered
            stop.before_unit()
    assert stop.settled == 50

    stop.disable()  # The resumed invocation is permitted to make progress.
    stop.before_unit()
    stop.record(1)
    assert stop.settled == 51
