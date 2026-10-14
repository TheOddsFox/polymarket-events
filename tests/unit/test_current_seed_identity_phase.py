"""Fresh phase-C identity checks quarantine provisional seeds atomically."""

import hashlib
import json
from datetime import UTC, datetime

import pytest
from test_current_import import add_event_page, release_fixture

from oddsfox_catalogue import current_backup, current_import, current_publish, current_runtime
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store
from oddsfox_catalogue.current_contract import CurrentError, ResponseRows


def snapshot(settings):
    with connect_store(settings) as conn:
        return {
            name: conn.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall()
            for (name,) in conn.execute("SHOW TABLES").fetchall()
        }


def phase(seed, *, connection_wrapper=None):
    settings, release, options, _ = seed
    # Each import phase owns a real, fresh file connection, as the runtime does.
    with connect_store(settings) as conn:
        if connection_wrapper is not None:
            conn = connection_wrapper(conn)
        return current_import.import_release(settings, conn, release, **options)


@pytest.fixture
def provisional(tmp_path):
    release, _, fixture_conn = release_fixture(tmp_path, usable=True)
    fixture_conn.close()
    raw_root = tmp_path / "raw"
    checksum = add_event_page(
        release,
        raw_root,
        [
            {
                "id": "1",
                "markets": [
                    {
                        "id": "3",
                        "question": "required supplemental market",
                        "version": "v1",
                        "conditionId": "0x" + "3" * 64,
                        "outcomes": ["Yes"],
                        # The immutable baseline market 2 already owns 123.
                        "clobTokenIds": ["123"],
                    }
                ],
            }
        ],
    )
    coverage = json.loads((release / "coverage.json").read_text())
    response_id = "legacy-event:" + coverage["units"][0]["page_id"]
    seed = (
        CurrentSettings(tmp_path / "current"),
        release,
        {"manifest_sha256": checksum, "raw_root": raw_root},
        response_id,
    )
    assert phase(seed) == {"complete": False, "phase": "evidence"}
    assert phase(seed) == {"complete": False, "phase": "verify"}
    return seed


def assert_provisional(seed):
    settings, _, _, response_id = seed
    with connect_store(settings) as conn:
        assert conn.execute(
            "SELECT market_id,usable,identity_error FROM markets ORDER BY market_id"
        ).fetchall() == [("2", True, None), ("3", True, None)]
        assert conn.execute(
            "SELECT market_id,asset_id FROM outcomes ORDER BY market_id"
        ).fetchall() == [("2", "123"), ("3", "123")]
        assert conn.execute("SELECT * FROM event_markets").fetchall() == [("polymarket", "1", "3")]
        assert conn.execute(
            "SELECT market_id,role,asset_kind,asset_id FROM _identity_claims ORDER BY ALL"
        ).fetchall() == [
            (mid, role, "ctf_token", "123")
            for mid in ("2", "3")
            for role in ("accepted", "candidate")
        ]
        assert conn.execute(
            "SELECT market_id,accepted IS NOT NULL,invalidated_at FROM _identity ORDER BY ALL"
        ).fetchall() == [("2", True, None), ("3", True, None)]
        assert conn.execute(
            "SELECT id,receipt,response FROM _receipts WHERE kind='market' ORDER BY id"
        ).fetchall() == [
            ("2", datetime(2026, 10, 12, tzinfo=UTC), "legacy-baseline:obs"),
            ("3", datetime(2026, 10, 13, tzinfo=UTC), response_id),
        ]
        assert conn.execute(
            "SELECT market_id,question FROM markets ORDER BY market_id"
        ).fetchall() == [
            ("2", "seed market"),
            ("3", "required supplemental market"),
        ]
        assert store.get_state(conn, "seed_pending")["stage"] == "verify"
        assert store.generation(conn) == 0
        for key in ("seed", "events_frontier", "markets_frontier", "discovery"):
            assert store.get_state(conn, key) is None


def test_evidence_commits_provisional_identity_with_original_receipts(provisional):
    assert_provisional(provisional)
    settings = provisional[0]
    result = current_runtime.execute(settings, "status", {}, 1000)
    assert result["status"] == "seeding"
    assert result["generation"] == 0
    assert result["seed_pending"]["stage"] == "verify"
    assert not (settings.published_dir / "current.json").exists()


@pytest.mark.parametrize(
    "operation", ["refresh", "reconcile", "probe", "init", "publish", "backup", "verify", "current"]
)
def test_provisional_conflicting_seed_blocks_runtime_before_side_effects(
    provisional, monkeypatch, operation
):
    settings = provisional[0]
    before = snapshot(settings)

    def forbidden(*args, **kwargs):
        pytest.fail("provisional seed reached retention, a canonical helper, or HTTP")

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


@pytest.mark.parametrize("operation", ["validate", "apply", "publish", "backup"])
def test_provisional_conflicting_seed_blocks_direct_helpers(provisional, operation):
    settings = provisional[0]
    before = snapshot(settings)
    with pytest.raises(CurrentError, match="seed import incomplete"):
        if operation == "backup":
            current_backup.create_backup(settings)
        else:
            with connect_store(settings) as conn:
                if operation == "validate":
                    store.validate_store(conn)
                elif operation == "publish":
                    current_publish.publish(settings, conn)
                else:
                    store.apply(
                        conn,
                        ResponseRows("2026-10-14T00:00:00Z", "unsafe"),
                        {"unsafe": True},
                    )
    assert snapshot(settings) == before
    assert not settings.published_dir.exists()
    assert not settings.backups_dir.exists()


def assert_quarantined(conn):
    assert conn.execute(
        "SELECT market_id,usable,identity_error FROM markets ORDER BY market_id"
    ).fetchall() == [
        ("2", False, "asset_ownership_conflict"),
        ("3", False, "asset_ownership_conflict"),
    ]
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)


@pytest.mark.parametrize("fault", ["after-quarantine", "validation", "commit"])
def test_verify_failure_restores_provisional_seed_and_retry_never_replays_raw(
    provisional, monkeypatch, fault
):
    settings, _, _, _ = provisional
    assert_provisional(provisional)
    before = snapshot(settings)
    calls = []

    def forbidden(*args, **kwargs):
        pytest.fail("phase C or its retry reread raw evidence")

    monkeypatch.setattr(current_import, "_event_pages", forbidden)
    checks = current_import._identity_checks
    validate = store.validate_store

    def checked_quarantine(conn):
        calls.append("quarantine")
        checks(conn)
        assert_quarantined(conn)
        # Quarantine must preserve reservation evidence and acquisition stamps.
        for name in ("_identity_claims", "_receipts", "_membership", "event_markets"):
            assert conn.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall() == before[name]
        if fault == "after-quarantine":
            raise RuntimeError("injected after quarantine")

    def failed_validation(conn, *, allow_seeding=False):
        calls.append("validation")
        assert allow_seeding is True
        assert_quarantined(conn)
        assert validate(conn, allow_seeding=True)["valid"]
        raise CurrentError("injected validation failure")

    class FailedCommit:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def execute(self, sql, *args, **kwargs):
            if sql == "COMMIT":
                calls.append("commit")
                assert_quarantined(self.conn)
                assert store.generation(self.conn) == 1
                assert store.get_state(self.conn, "seed") is not None
                assert store.get_state(self.conn, "events_frontier") == 3
                assert store.get_state(self.conn, "markets_frontier") == 4
                assert store.get_state(self.conn, "discovery") is not None
                assert (
                    self.conn.execute(
                        "SELECT 1 FROM sync_state WHERE key='seed_pending'"
                    ).fetchall()
                    == []
                )
                raise RuntimeError("injected commit failure")
            return self.conn.execute(sql, *args, **kwargs)

    with monkeypatch.context() as faults:
        faults.setattr(current_import, "_identity_checks", checked_quarantine)
        if fault == "validation":
            faults.setattr(store, "validate_store", failed_validation)
        with pytest.raises(CurrentError, match="seed verify failed: injected"):
            phase(provisional, connection_wrapper=FailedCommit if fault == "commit" else None)

    assert calls == ["quarantine", *([fault] if fault in {"validation", "commit"} else [])]
    assert snapshot(settings) == before
    assert_provisional(provisional)
    result = phase(provisional)
    assert result["complete"] is True
    assert result["generation"] == 1
    assert result["seed"]["frontiers"] == {"events": 3, "markets": 4}
    assert result["seed"]["supplement"] == {
        "policy": "verified-event-required-markets-v1",
        "markets": 1,
        "market_ids_sha256": hashlib.sha256(b"3\n").hexdigest(),
    }
    with connect_store(settings) as conn:
        assert_quarantined(conn)
        for name in ("_identity_claims", "_receipts", "_membership", "event_markets"):
            assert conn.execute(f'SELECT * FROM "{name}" ORDER BY ALL').fetchall() == before[name]
        assert conn.execute(
            "SELECT i.market_id,i.invalidated_at=r.receipt,i.invalidated_response=r.response,"
            "i.accepted IS NOT NULL FROM _identity i JOIN _receipts r "
            "ON r.kind='market' AND r.venue=i.venue AND r.id=i.market_id ORDER BY i.market_id"
        ).fetchall() == [("2", True, True, True), ("3", True, True, True)]
        assert store.generation(conn) == 1
        assert conn.execute("SELECT 1 FROM sync_state WHERE key='seed_pending'").fetchall() == []
        assert store.get_state(conn, "seed") == result["seed"]
        assert store.get_state(conn, "events_frontier") == 3
        assert store.get_state(conn, "markets_frontier") == 4
        discovery = store.get_state(conn, "discovery")
        assert discovery["version"] == 2
        assert discovery["pending"] is None
        assert discovery["lanes"] == {
            lane: {"frontier": frontier, "highwater": None, "phase": "tail"}
            for lane, frontier in (("events", 3), ("markets", 4))
        }
        assert store.validate_store(conn)["valid"]
        current_publish.publish(settings, conn)
    assert current_publish.verify_current(settings)["valid"]
