from __future__ import annotations

import contextlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from oddsfox_catalogue.signals import Terminated

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-full-pass.py"
SPEC = importlib.util.spec_from_file_location("benchmark_attribution_v5", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def _context_connection():
    @contextlib.contextmanager
    def opened():
        connection = duckdb.connect(":memory:")
        try:
            yield connection
        finally:
            connection.close()

    return opened()


def test_attribution_proxy_preserves_chaining_and_separates_fetch_counts():
    metrics = benchmark.new_attribution_metrics()
    with benchmark.AttributedConnection(_context_connection(), metrics) as conn:
        returned = conn.execute("SELECT ?", ["hello"])
        assert returned is conn
        assert conn.fetchone() == ("hello",)
        conn.execute("SELECT unnest(?)", [["a", "b"]])
        assert conn.fetchall() == [("a",), ("b",)]
    assert metrics["statement_count"] == 2
    assert metrics["fetchone_count"] == 1
    assert metrics["fetchall_count"] == 1
    assert metrics["parameter_scalar_count"] == 1
    assert metrics["parameter_list_element_count"] == 2


def test_attribution_proxy_propagates_execute_and_fetch_exceptions():
    metrics = benchmark.new_attribution_metrics()

    @contextlib.contextmanager
    def faulty_connection():
        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, query, _parameters=None):
                if "bad" in query:
                    raise RuntimeError("execute failure")
                return self

            def fetchmany(self, _size=1):
                raise RuntimeError("fetch failure")

        yield Connection()

    with benchmark.AttributedConnection(faulty_connection(), metrics) as conn:
        with pytest.raises(RuntimeError, match="execute failure"):
            conn.execute("bad query")
        with pytest.raises(RuntimeError, match="fetch failure"):
            conn.execute("SELECT 1").fetchmany(2)
    assert metrics["statement_count"] == 2
    assert metrics["fetchmany_count"] == 1


def test_attribution_context_close_cannot_suppress_operator_termination():
    class SuppressingContext:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return True

    metrics = benchmark.new_attribution_metrics()
    proxy = benchmark.AttributedConnection(SuppressingContext(), metrics)
    proxy._finish_statement = lambda: (_ for _ in ()).throw(Terminated(15))
    with pytest.raises(Terminated), proxy:
        pass


def test_sql_capture_excludes_copy_and_insert_select_from_lookup_classification():
    metrics = benchmark.new_attribution_metrics()
    with benchmark.AttributedConnection(_context_connection(), metrics) as conn:
        conn.execute("CREATE TABLE events(event_id VARCHAR)")
        conn.execute("INSERT INTO events SELECT '1'")
        conn.execute("SELECT * FROM events WHERE event_id=?", ["1"]).fetchall()
    assert set(metrics["captured_statements"]) == {"shared_lookup"}
    assert metrics["captured_statements"]["shared_lookup"]["sql"].startswith("SELECT")


def test_v5_attribution_worker_command_has_no_cprofile_argument(tmp_path):
    argv = benchmark.worker_argv(
        tmp_path / "seed",
        tmp_path / "root",
        tmp_path,
        "perf",
        "warm-v5",
        50,
        None,
        attribution=True,
    )
    assert "--_attribution" in argv
    assert "--_profile" not in argv


def test_native_profiles_replay_actual_bounded_queries_and_rollback_receipt(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    database = root / "catalogue.duckdb"
    with duckdb.connect(str(database)) as conn:
        conn.execute(
            "CREATE TABLE _identity_claims(venue VARCHAR, market_id VARCHAR, role VARCHAR, "
            "asset_kind VARCHAR, asset_id VARCHAR)"
        )
        conn.execute("INSERT INTO _identity_claims VALUES ('v','1','accepted','token','a')")
        conn.execute(
            "CREATE TABLE _receipts(kind VARCHAR, venue VARCHAR, id VARCHAR, "
            "receipt TIMESTAMPTZ, response VARCHAR, PRIMARY KEY(kind,venue,id))"
        )
    metrics = benchmark.new_attribution_metrics()

    # Capture the statements against the same on-disk synthetic schema the
    # native diagnostic will clone and replay.
    @contextlib.contextmanager
    def open_database():
        connection = duckdb.connect(str(database))
        try:
            yield connection
        finally:
            connection.close()

    with benchmark.AttributedConnection(open_database(), metrics) as conn:
        conn.execute(
            "SELECT venue, market_id, role, asset_kind, asset_id "
            "FROM _identity_claims WHERE asset_id IN (?)",
            ["a"],
        ).fetchall()
        conn.execute(
            "INSERT INTO _receipts VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(kind,venue,id) DO UPDATE SET "
            "receipt=excluded.receipt,response=excluded.response",
            ["event", "v", "id-2", "2026-10-14T00:00:00+00:00", "synthetic"],
        )
    assert set(metrics["captured_statements"]) == {"shared_lookup", "receipt_write"}
    captured_receipt = metrics["captured_statements"]["receipt_write"]
    assert captured_receipt["sql"].startswith("INSERT INTO _receipts VALUES (?, ?, ?, ?, ?)")
    result = benchmark._warmup_native_profiles(
        root,
        tmp_path / "profiles",
        "native-test",
        metrics["captured_statements"],
        SimpleNamespace(
            limits=SimpleNamespace(
                duckdb_memory_bytes=256 * 1024 * 1024,
                duckdb_threads=2,
                max_temp_bytes=1024 * 1024 * 1024,
            )
        ),
    )
    assert result["status"] == "captured", result
    assert result["receipt_write_rolled_back"] is True
    with duckdb.connect(str(database), read_only=True) as conn:
        assert conn.execute("SELECT count(*) FROM _receipts").fetchone()[0] == 1


def test_v5_attribution_warmup_requires_the_diagnostic_contract(tmp_path, monkeypatch):
    output = tmp_path / "evidence"
    output.mkdir()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    budget = benchmark.Budget(limit_s=10)
    valid = {
        "status": "complete",
        "measurement_version": benchmark.MEASUREMENT_VERSION,
        "acquire_apply_s": 1.0,
        "publish_verify_s": 0.1,
        "wall_s": 1.1,
        "peak_rss_bytes": 1,
        "http_attempts": 0,
        "downloaded_bytes": 0,
        "runtime_reserved_attempts": 0,
        "runtime_downloaded_bytes": 0,
        "source_stats": {"faults": 0, "http_attempts": 0, "downloaded_bytes": 0},
        "runtime_source": str(runtime.resolve()),
        "warmup_diagnostics": {
            "version": 1,
            "attribution_mode": "direct_unprofiled",
        },
    }
    monkeypatch.setattr(benchmark, "supervised", lambda *_args, **_kwargs: dict(valid))
    sample = benchmark.run_sample(
        tmp_path / "seed",
        output,
        runtime,
        budget,
        "smoke",
        "warm",
        warmup=True,
        attribution=True,
    )
    assert sample["profile_files"] == []
    assert sample["profile_summary"] is None

    valid["warmup_diagnostics"] = {"version": 0, "attribution_mode": "cprofile"}
    with pytest.raises(ValueError, match="direct unprofiled attribution"):
        benchmark.run_sample(
            tmp_path / "seed",
            output,
            runtime,
            budget,
            "smoke",
            "bad-warm",
            warmup=True,
            attribution=True,
        )


def test_native_profile_cleanup_does_not_mask_operator_termination(tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    with duckdb.connect(str(root / "catalogue.duckdb")):
        pass
    signal = Terminated(15)

    def stop_connect(*_args, **_kwargs):
        raise signal

    original_unlink = Path.unlink

    def failing_unlink(path, *args, **kwargs):
        if path.name == "diagnostic.duckdb":
            raise PermissionError("synthetic cleanup failure")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(benchmark.duckdb, "connect", stop_connect)
    monkeypatch.setattr(Path, "unlink", failing_unlink)
    settings = SimpleNamespace(
        limits=SimpleNamespace(
            duckdb_memory_bytes=256 * 1024 * 1024,
            duckdb_threads=2,
            max_temp_bytes=1024 * 1024 * 1024,
        )
    )
    with pytest.raises(Terminated) as raised:
        benchmark._warmup_native_profiles(
            root,
            tmp_path / "profiles",
            "native-stop",
            {"shared_lookup": {}, "receipt_write": {}},
            settings,
        )
    assert raised.value is signal


def test_scheduler_diagnostics_are_supervised_worker_logic_and_keep_seed_immutable(
    tmp_path, capsys, monkeypatch
):
    monkeypatch.setenv(
        "FULL_PASS_EXPECTED_PACKAGE", str((benchmark.REPO / "src" / "oddsfox_catalogue").resolve())
    )
    seed = tmp_path / "seed"
    original_create_seed = benchmark.create_seed
    original_create_seed(seed, 2, 8)

    def create_small_seed(root, *_args, **kwargs):
        return original_create_seed(root, 2, 8, **kwargs)

    monkeypatch.setattr(benchmark, "create_seed", create_small_seed)
    before = benchmark.digest(seed / "catalogue.duckdb")
    diagnostic_root = tmp_path / "diagnostics"
    diagnostic_root.mkdir()

    assert benchmark.scheduler_diagnostics_worker(seed, diagnostic_root) == 0
    result = __import__("json").loads(capsys.readouterr().out)
    assert result["status"] == "measured"
    assert [scale["scale"] for scale in result["scales"]] == [
        "2 events / 8 markets",
        "2 events / 8 markets",
    ]
    for scale in result["scales"]:
        assert set(scale["measurements"]) == {
            "urgent_events_due",
            "residual_markets_due",
            "full_pass_remaining",
        }
        assert all(
            measurement["repetitions"] == 3 and measurement["explain_analyze"]
            for measurement in scale["measurements"].values()
        )
    assert benchmark.digest(seed / "catalogue.duckdb") == before
