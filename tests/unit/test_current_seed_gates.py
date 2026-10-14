"""A private staged seed never becomes a usable catalogue before completion."""

from contextlib import contextmanager

import duckdb
import pytest

from oddsfox_catalogue import cli, current_backup, current_import, current_publish, current_runtime
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import SCHEMAS, CurrentError, ResponseRows

STAGES = ("baseline", "evidence", "verify")


@pytest.fixture
def target(tmp_path):
    settings = CurrentSettings(tmp_path / "current")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
    return settings


@pytest.fixture(params=[*STAGES, None], ids=[*STAGES, "json-null"])
def pending(target, request):
    marker = (
        None
        if request.param is None
        else {
            "version": 1,
            "stage": request.param,
            "source": {
                "release_path": str(target.root.parent / "legacy"),
                "manifest_sha256": "a" * 64,
                "raw_root": str(target.root.parent / "raw"),
            },
        }
    )
    with connect_store(target) as conn:
        if request.param != "baseline":
            conn.execute(
                "INSERT INTO events (venue,event_id,title) VALUES ('polymarket','1','partial')"
            )
        store.set_state(conn, "seed_pending", marker)
    return target, marker


def snapshot(settings):
    with connect_store(settings) as conn:
        return {
            name: conn.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall()
            for (name,) in conn.execute("SHOW TABLES").fetchall()
        }


@pytest.mark.parametrize(
    "operation", ["refresh", "reconcile", "probe", "init", "publish", "backup", "verify", "current"]
)
def test_pending_seed_blocks_canonical_operations_before_side_effects(
    pending, monkeypatch, operation
):
    settings, _ = pending
    before = snapshot(settings)

    def forbidden(*args, **kwargs):
        pytest.fail("partial seed reached mutation, retention, publication or HTTP")

    monkeypatch.setattr(current_runtime, "GammaClient", forbidden)
    for module, names in (
        (current_backup, ("prune", "create_backup")),
        (current_publish, ("publish", "verify_current")),
    ):
        for name in names:
            monkeypatch.setattr(module, name, forbidden)
    with pytest.raises(CurrentError, match="seed import incomplete"):
        current_runtime.execute(settings, operation, {"allow_seeding": True}, 1000)
    assert snapshot(settings) == before
    assert not settings.published_dir.exists()
    assert not settings.backups_dir.exists()


def test_pending_seed_status_reports_exact_private_progress(pending):
    settings, marker = pending
    before = snapshot(settings)
    result = current_runtime.execute(settings, "status", {}, 1000)
    assert result["status"] == "seeding"
    assert result["seed_pending"] == marker
    assert result["generation"] == 0
    assert result["discovery"] is None
    assert snapshot(settings) == before


def test_default_validation_blocks_seed_and_explicit_allow_still_checks_integrity(pending):
    settings, marker = pending
    with connect_store(settings) as conn:
        with pytest.raises(CurrentError, match="seed import incomplete"):
            store.validate_store(conn)
        if marker is None:
            return
        assert store.validate_store(conn, allow_seeding=True)["valid"]
        conn.execute("""INSERT INTO _identity_claims VALUES
            ('polymarket','2','candidate','clob_token','11'),
            ('polymarket','2','candidate','clob_token','11')""")
        with pytest.raises(CurrentError, match="integrity"):
            store.validate_store(conn, allow_seeding=True)


@pytest.mark.parametrize("operation", ["publish", "backup", "apply"])
def test_direct_helpers_cannot_expose_or_mutate_pending_seed(pending, operation):
    settings, _ = pending
    before = snapshot(settings)
    with pytest.raises(CurrentError, match="seed import incomplete"):
        if operation == "backup":
            current_backup.create_backup(settings)
        else:
            with connect_store(settings) as conn:
                if operation == "publish":
                    current_publish.publish(settings, conn)
                else:
                    store.apply(
                        conn, ResponseRows("2026-10-14T00:00:00Z", "unsafe"), {"unsafe": True}
                    )
    assert snapshot(settings) == before
    assert not (settings.published_dir / "current.json").exists()
    assert not settings.backups_dir.exists()


@pytest.mark.parametrize("operation", ["verify-backup", "restore"])
def test_independent_recovery_remains_available_while_current_seed_is_pending(
    pending, monkeypatch, operation
):
    settings, _ = pending
    before = snapshot(settings)
    options = {
        "backup": settings.root.parent / "backup",
        "destination": settings.root.parent / "restored",
    }
    calls = []

    def recover(*args, **kwargs):
        calls.append((args, kwargs))
        return {"valid": True}

    def forbidden(*args, **kwargs):
        pytest.fail("independent recovery opened the existing catalogue")

    monkeypatch.setattr(current_runtime, "connect_store", forbidden)
    monkeypatch.setattr(current_backup, "verify_backup", recover)
    monkeypatch.setattr(current_backup, "restore_backup", recover)
    assert current_runtime.execute(settings, operation, options, 1000) == {"valid": True}
    expected = (
        (options["backup"],)
        if operation == "verify-backup"
        else (options["backup"], options["destination"])
    )
    assert calls == [(expected, {"deadline": 1000})]
    assert snapshot(settings) == before


@pytest.mark.parametrize("fail_phase", [None, 1], ids=["complete", "phase-error"])
def test_import_driver_closes_each_phase_and_opens_fresh_publication_connection(
    target, monkeypatch, fail_phase
):
    connections, events = [], []
    phases = []

    @contextmanager
    def tracked(settings):
        for previous in connections:
            with pytest.raises(duckdb.ConnectionException, match="closed"):
                previous.execute("SELECT 1")
        with connect_store(settings) as conn:
            index = len(connections)
            connections.append(conn)
            events.append(("open", index))
            yield conn
        events.append(("close", index))

    def phase(settings, conn, **options):
        index = len(phases)
        assert conn is connections[index]
        assert len(connections) == index + 1
        phases.append(index)
        events.append(("phase", index))
        if index == fail_phase:
            raise CurrentError("phase failed")
        if index < 2:
            store.set_state(conn, "seed_pending", {"stage": STAGES[index + 1]})
            return {"complete": False, "phase": STAGES[index + 1]}
        conn.execute("DELETE FROM sync_state WHERE key='seed_pending'")
        store.set_state(conn, "seed", {"frontiers": {"events": 3, "markets": 4}})
        store.set_state(conn, "generation", 1)
        return {"valid": True, "complete": True, "phase": "complete"}

    def publish(settings, conn, **options):
        assert phases == [0, 1, 2]
        assert len(connections) == 4 and conn is connections[3]
        assert store.get_state(conn, "seed_pending") is None
        events.append(("publish", 3))
        return {"release_id": "complete", "noop": False}

    monkeypatch.setattr(current_runtime, "connect_store", tracked)
    monkeypatch.setattr(current_import, "import_release", phase)
    monkeypatch.setattr(current_publish, "publish", publish)
    if fail_phase is None:
        result = current_runtime.execute(target, "import-release", {}, 1000)
        assert result["complete"] is True
        assert result["publication"]["release_id"] == "complete"
        expected = [(event, index) for index in range(3) for event in ("open", "phase", "close")]
        expected += [("open", 3), ("publish", 3), ("close", 3)]
        assert events == expected
    else:
        with pytest.raises(CurrentError, match="phase failed"):
            current_runtime.execute(target, "import-release", {}, 1000)
        assert phases == [0, 1]
    for conn in connections:
        with pytest.raises(duckdb.ConnectionException, match="closed"):
            conn.execute("SELECT 1")


@pytest.mark.parametrize("table", ["_membership", "_identity_claims"])
def test_failed_schema_creation_is_atomic_and_retryable_on_reopen(tmp_path, table):
    path = tmp_path / "fresh.duckdb"
    with duckdb.connect(str(path)) as conn:

        class FailingConnection:
            def execute(self, query, params=None):
                if query.lstrip().startswith("CREATE TABLE") and table in query:
                    assert conn.execute("SHOW TABLES").fetchall()
                    raise RuntimeError("DDL interrupted")
                return conn.execute(query) if params is None else conn.execute(query, params)

        with pytest.raises(RuntimeError, match="DDL interrupted"):
            store.initialize(FailingConnection())
        assert conn.execute("SHOW TABLES").fetchall() == []
    with duckdb.connect(str(path)) as conn:
        assert conn.execute("SHOW TABLES").fetchall() == []
        store.initialize(conn)
        assert set(SCHEMAS) <= {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
        assert store.validate_store(conn)["valid"]


def test_unknown_existing_schema_is_never_adopted_or_modified(tmp_path):
    path = tmp_path / "unknown.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE unrelated AS SELECT 42 AS item")
    with duckdb.connect(str(path)) as conn:
        with pytest.raises(CurrentError, match="unrecognized"):
            store.initialize(conn)
        assert conn.execute("SHOW TABLES").fetchall() == [("unrelated",)]
        assert conn.execute("SELECT * FROM unrelated").fetchall() == [(42,)]


def test_json_null_seed_marker_cannot_be_restarted_as_pristine_import(target):
    with connect_store(target) as conn:
        store.set_state(conn, "seed_pending", None)
    before = snapshot(target)
    with pytest.raises(CurrentError, match="seed_pending|marker"):
        current_runtime.execute(
            target,
            "import-release",
            {
                "release_path": target.root.parent / "legacy",
                "manifest_sha256": "a" * 64,
                "raw_root": target.root.parent / "raw",
            },
            1000,
        )
    assert snapshot(target) == before


@pytest.mark.parametrize("argv", [["publish", "--allow-seeding"], ["--allow-seeding", "publish"]])
def test_cli_exposes_no_seed_validation_bypass(argv):
    with pytest.raises(SystemExit) as error:
        cli.build_parser().parse_args(argv)
    assert error.value.code == 2
