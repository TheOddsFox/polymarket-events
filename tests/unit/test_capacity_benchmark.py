"""Small deterministic checks for the capacity evidence harness."""

import importlib.util
import json
from argparse import Namespace
from inspect import signature
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from oddsfox_catalogue.current_config import CurrentSettings

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/benchmark-capacity.py"
spec = importlib.util.spec_from_file_location("capacity_benchmark", SCRIPT)
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


def test_audit_reservation_matches_enforced_provider_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(
        capacity.accounting, "aggregate_usage", lambda _: {"logical_owned_bytes": 0}
    )
    monkeypatch.setattr(capacity.shutil, "disk_usage", lambda _: SimpleNamespace(free=2**50))
    measured = capacity.capacity([tmp_path], tmp_path, tmp_path)
    assert measured["reserved_bytes"] == (
        capacity.CurrentLimits().max_retained_bytes
        + capacity.MAX_AUDIT_BYTES
        + capacity.ANCILLARY_BYTES
    )
    assert signature(capacity.CapacitySource).parameters["max_audit_bytes"].default == (
        capacity.MAX_AUDIT_BYTES
    )


def test_child_environment(monkeypatch):
    for key in (
        "CATALOGUE_MAX_REQUESTS",
        "CATALOGUE_CONFIG",
        "PYTHONHOME",
        "PYTHONPATH",
        "VIRTUAL_ENV",
    ):
        monkeypatch.setenv(key, "unsafe")
    env = capacity.child_environment("http://127.0.0.1:1234")
    assert {k for k in env if k.startswith("CATALOGUE_")} == {
        "CATALOGUE_GAMMA_BASE_URL",
        "CATALOGUE_GAMMA_ALLOW_LOOPBACK",
    }
    assert all(k not in env for k in ("PYTHONHOME", "PYTHONPATH", "VIRTUAL_ENV"))
    assert capacity.CurrentLimits().max_duration_s == 1200


def make_store(root):
    root.mkdir()
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute("CREATE TABLE events(venue VARCHAR,event_id VARCHAR,closed BOOLEAN)")
        conn.execute("CREATE TABLE markets(venue VARCHAR,market_id VARCHAR,closed BOOLEAN)")
        conn.execute(
            "CREATE TABLE _membership(venue VARCHAR,event_id VARCHAR,receipt TIMESTAMPTZ,unresolved BOOLEAN)"
        )
        conn.execute(
            "CREATE TABLE _receipts(kind VARCHAR,venue VARCHAR,id VARCHAR,receipt TIMESTAMPTZ)"
        )
        conn.execute("CREATE TABLE sync_state(key VARCHAR,value JSON)")
        conn.execute("CREATE TABLE event_markets(id VARCHAR)")
        conn.execute("CREATE TABLE outcomes(id VARCHAR)")
        conn.execute("INSERT INTO events VALUES ('polymarket','1',false),('polymarket','2',false)")
        conn.execute("INSERT INTO _membership VALUES ('polymarket','1','2026-01-01',false)")


def test_receipt_oracle_requires_complete_strict_renewal(tmp_path):
    seed, candidate = tmp_path / "seed", tmp_path / "candidate"
    make_store(seed)
    make_store(candidate)
    initial = capacity.snapshot(seed, candidate, tmp_path)["active_events"]
    assert initial["renewed"] == 0
    assert initial["pending_unrenewed"] == 2
    assert initial["missing_or_unresolved"] == 1
    with duckdb.connect(str(candidate / "catalogue.duckdb")) as conn:
        conn.execute("UPDATE _membership SET receipt='2026-02-01',unresolved=true")
    assert capacity.snapshot(seed, candidate, tmp_path)["active_events"]["renewed"] == 0
    with duckdb.connect(str(candidate / "catalogue.duckdb")) as conn:
        conn.execute("UPDATE _membership SET unresolved=false")
    assert capacity.snapshot(seed, candidate, tmp_path)["active_events"]["renewed"] == 1


def test_process_failure_keeps_report(tmp_path, monkeypatch):
    seed, root, output = tmp_path / "seed", tmp_path / "root", tmp_path / "output"
    seed.mkdir()
    root.mkdir()
    args = Namespace(
        seed_root=seed,
        root=root,
        output=output,
        executable=tmp_path / "mock-executable",
        account_root=[seed],
        invocations=1,
        backup_every=0,
        cadence_seconds=0,
        max_total_seconds=4000,
        characterize=True,
    )
    monkeypatch.setattr(capacity, "paths", lambda _: (seed, root, output, [seed, root, output]))
    monkeypatch.setattr(capacity, "snapshot", lambda *_: {})
    monkeypatch.setattr(capacity, "capacity", lambda *_: {})
    monkeypatch.setattr(capacity, "load_current_settings", lambda *_, **__: CurrentSettings(root))

    class Source:
        url = "http://127.0.0.1:1234"
        controls = {"open_event_id": "1"}
        stats = {}

        def __init__(self, *_):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    monkeypatch.setattr(capacity, "CapacitySource", Source)

    def fail(argv, cwd, env, log, timeout, max_output_bytes):
        log.write_text("failure evidence")
        raise ValueError("child failed")

    monkeypatch.setattr(capacity.installed, "run_command", fail)
    assert capacity.run(args) == 1
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "failed"
    assert report["sustained_acceptance"] is False
    assert Path(report["steps"][0]["log"]).read_text() == "failure evidence"


def test_disjoint_paths_and_cadence():
    assert not capacity.disjoint(Path("/a"), Path("/a/b"))
    with pytest.raises(ValueError, match="true scheduler"):
        capacity.run(
            Namespace(
                invocations=1,
                backup_every=0,
                cadence_seconds=0,
                max_total_seconds=4000,
                characterize=False,
            )
        )


def test_all_markets_are_one_cold_cohort_and_missing_events_overdue(tmp_path):
    seed, candidate = tmp_path / "seed", tmp_path / "candidate"
    for root in (seed, candidate):
        make_store(root)
        with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
            conn.execute(
                "INSERT INTO markets VALUES ('polymarket','10',false),('polymarket','11',true),('polymarket','12',NULL)"
            )
    report = capacity.snapshot(seed, candidate, tmp_path)
    assert "active_markets" not in report
    assert report["cold_markets"]["cohort"] == 3
    assert report["cold_markets"]["due"] == 3
    assert report["cold_markets"]["overdue"] == 3
    assert report["active_events"]["due"] == 2
    assert report["active_events"]["overdue"] == 2


def test_snapshot_with_real_source_open(tmp_path):
    from fakes.world import make_event, make_market
    from oddsfox_catalogue.current_rows import project_response
    from oddsfox_catalogue.current_store import apply, initialize

    seed, candidate = tmp_path / "seed", tmp_path / "candidate"
    for root in (seed, candidate):
        root.mkdir()
        with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
            initialize(conn)
            apply(
                conn,
                project_response(
                    [
                        make_event("1", "Open", markets=[make_market("10", "Open")]),
                        make_event(
                            "2",
                            "Closed",
                            closed=True,
                            markets=[make_market("11", "Closed", closed=True)],
                        ),
                    ],
                    "events",
                    received_at="2026-01-01T00:00:00Z",
                    response_id="seed",
                ),
            )
    with capacity.CapacitySource(seed, tmp_path / "audit", delay_s=0) as source:
        assert source.controls["open_event_id"] == "1"
        assert capacity.snapshot(seed, candidate, tmp_path)["cold_markets"]["cohort"] == 2


def test_tiny_characterization_with_real_commands(tmp_path, monkeypatch):
    from fakes.world import make_event, make_market
    from oddsfox_catalogue.current_config import validate_root
    from oddsfox_catalogue.current_rows import project_response
    from oddsfox_catalogue.current_store import apply, initialize

    executable = SCRIPT.parents[1] / ".venv/bin/catalogue"
    if not executable.is_file():
        pytest.skip("local catalogue entrypoint unavailable")
    seed, root, output = tmp_path / "seed", tmp_path / "candidate", tmp_path / "evidence"
    for selected in (seed, root):
        validate_root(selected, initialize=True)
        with duckdb.connect(str(selected / "catalogue.duckdb")) as conn:
            initialize(conn)
            apply(
                conn,
                project_response(
                    [
                        make_event("1", "Open", markets=[make_market("10", "Open")]),
                        make_event(
                            "2",
                            "Closed",
                            closed=True,
                            markets=[make_market("11", "Closed", closed=True)],
                        ),
                    ],
                    "events",
                    received_at="2026-01-01T00:00:00Z",
                    response_id="seed",
                ),
            )
    source_type = capacity.CapacitySource
    monkeypatch.setattr(capacity, "CapacitySource", lambda *a: source_type(*a, delay_s=0))
    # The integration measures tiny fixture behavior, not operator disk capacity.
    monkeypatch.setattr(capacity, "capacity", lambda *_: {})
    args = Namespace(
        seed_root=seed,
        root=root,
        output=output,
        executable=executable,
        account_root=[seed],
        invocations=1,
        backup_every=0,
        cadence_seconds=0,
        max_total_seconds=4000,
        characterize=True,
    )
    assert capacity.run(args) == 0
    report = json.loads((output / "report.json").read_text())
    assert report["characterization_complete"] is True
    assert [s["name"] for s in report["steps"]] == ["probe", "refresh", "reconcile", "verify"]
    assert report["source"]["http_attempts"] > 0
    assert report["sustained_acceptance"] is False


def test_initial_query_requires_capacity_admission(tmp_path, monkeypatch):
    seed, root, output = tmp_path / "seed", tmp_path / "root", tmp_path / "evidence"
    seed.mkdir()
    root.mkdir()
    monkeypatch.setattr(capacity, "paths", lambda _: (seed, root, output, [seed, root, output]))

    def denied(*_):
        raise ValueError("no capacity")

    monkeypatch.setattr(capacity, "capacity", denied)
    monkeypatch.setattr(capacity, "snapshot", lambda *_: pytest.fail("query before admission"))
    args = Namespace(
        seed_root=seed,
        root=root,
        output=output,
        executable=tmp_path / "mock-executable",
        account_root=[seed],
        invocations=1,
        backup_every=0,
        cadence_seconds=0,
        max_total_seconds=4000,
        characterize=True,
    )
    assert capacity.run(args) == 1
    assert json.loads((output / "report.json").read_text())["error"] == "ValueError: no capacity"


def test_backup_refused_before_any_child(monkeypatch):
    monkeypatch.setattr(
        capacity.installed, "run_command", lambda *_a, **_kw: pytest.fail("child invoked")
    )
    with pytest.raises(ValueError, match="explicitly accounted external"):
        capacity.run(
            Namespace(
                invocations=1,
                backup_every=1,
                cadence_seconds=0,
                max_total_seconds=4000,
                characterize=True,
            )
        )
    assert capacity.parser().get_default("backup_every") == 0


def test_log_budget_counts_failed_logs(tmp_path):
    (tmp_path / "failed.log").write_bytes(b"x" * 10)
    assert (
        capacity.log_budget(tmp_path) == capacity.ANCILLARY_BYTES - 2 * capacity.REPORT_BYTES - 10
    )
