"""Adversarial recovery checks use real publication and durable store state."""

import json
import signal
from dataclasses import replace

import pytest

from oddsfox_catalogue import current_publish, current_runtime, current_store, current_sync
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError, ResponseRows
from oddsfox_catalogue.signals import Terminated


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(
        tmp_path / "root", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.apply(
            conn,
            ResponseRows(
                "2026-01-01T00:00:00Z",
                "initial",
                events=[{"venue": "polymarket", "event_id": "1", "title": "unchanged"}],
            ),
        )
    return settings


def _state(conn):
    return conn.execute("SELECT key,value FROM sync_state ORDER BY key").fetchall()


@pytest.mark.parametrize("previous_seconds", [800, 915])
@pytest.mark.parametrize(
    "full_pass",
    [None, {"pass_started_at": "2026-01-01T00:00:00Z", "cursor_after": 99, "completed_at": None}],
)
def test_full_pass_guard_preserves_pending_work_and_existing_progress(
    catalogue, monkeypatch, previous_seconds, full_pass
):
    settings = catalogue
    with connect_store(settings) as conn:
        current_store.set_state(conn, "previous_publish_seconds", previous_seconds)
        current_store.set_state(conn, "full_pass", full_pass)
        # The frozen request must not be inspected, rewritten or resumed by a blocked run.
        current_store.set_state(conn, "discovery", {"pending": {"raw_id": "a" * 32}})
        before = _state(conn)

    def forbidden(*_args, **_kwargs):
        pytest.fail("impossible publication reservation must block before acquisition")

    monkeypatch.setattr(current_runtime, "GammaClient", forbidden)
    monkeypatch.setattr(current_sync, "resume_pending", forbidden)
    with pytest.raises(CurrentError, match="1200\\.00s.*publish --recalibrate"):
        current_runtime.execute(settings, "reconcile", {"full_pass": True}, deadline=10**20)

    with connect_store(settings) as conn:
        assert _state(conn) == before
        assert conn.execute("SELECT title FROM events").fetchall() == [("unchanged",)]
        assert conn.execute("SELECT count(*) FROM _receipts").fetchone()[0] == 1


@pytest.mark.parametrize("fallback_noop", [False, True])
def test_recalibration_bypasses_both_noop_routes_and_replaces_only_publication_measurement(
    catalogue, monkeypatch, fallback_noop
):
    settings = catalogue
    with connect_store(settings) as conn:
        first = current_publish.publish(settings, conn)
        current_store.set_state(conn, "previous_publish_seconds", 915)
        current_store.set_state(conn, "last_publish_at", "2026-01-01T00:00:00Z")
        domain_before = {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            for table in ("events", "_receipts", "_membership", "_identity", "_identity_claims")
        }
        generation = current_store.generation(conn)
        if fallback_noop:
            # Force the full verification path while leaving the release genuinely valid.
            monkeypatch.setattr(current_publish, "_release_bytes_match", lambda *_: False)
        assert current_publish.publish(settings, conn)["noop"] is True
        assert current_store.get_state(conn, "previous_publish_seconds") == 915

        result, measured = current_runtime._publish(settings, conn, None, recalibrate=True)

        assert result["noop"] is False
        assert result["release_id"] != first["release_id"]
        pointer = json.loads((settings.published_dir / "current.json").read_text())
        assert pointer["previous_release_id"] == first["release_id"]
        assert current_store.get_state(conn, "previous_publish_seconds") == measured
        assert current_store.generation(conn) == generation
        for table, rows in domain_before.items():
            assert conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() == rows
        assert current_publish.verify_current(settings, connection=conn)["valid"]


@pytest.mark.parametrize("failure", ["export", "operator", "deadline"])
def test_failed_recalibration_preserves_both_protected_releases_and_saved_estimate(
    catalogue, monkeypatch, failure
):
    settings = catalogue
    with connect_store(settings) as conn:
        first = current_publish.publish(settings, conn)
        second = current_publish.publish(settings, conn, force=True)
        current_store.set_state(conn, "previous_publish_seconds", 915)
        current_store.set_state(conn, "last_publish_at", "2026-01-01T00:00:00Z")
        before_state = _state(conn)
        pointer = settings.published_dir / "current.json"
        before_pointer = pointer.read_bytes()
        expected = Terminated if failure == "operator" else CurrentError
        deadline = -1 if failure == "deadline" else None
        if failure != "deadline":

            def fail_export(*_args, **_kwargs):
                if failure == "operator":
                    raise Terminated(signal.SIGTERM)
                raise CurrentError("injected export failure")

            monkeypatch.setattr(current_publish, "_export", fail_export)
        with pytest.raises(expected):
            current_runtime._publish(settings, conn, deadline, recalibrate=True)
        assert pointer.read_bytes() == before_pointer
        assert _state(conn) == before_state
        for release in (first, second):
            assert (settings.published_dir / "releases" / release["release_id"]).is_dir()
        assert current_publish.verify_current(settings, connection=conn)["valid"]


def test_forced_publication_still_rejects_corrupt_live_state(catalogue):
    settings = catalogue
    with connect_store(settings) as conn:
        current_publish.publish(settings, conn)
        current_store.set_state(conn, "previous_publish_seconds", 915)
        pointer = settings.published_dir / "current.json"
        before_pointer = pointer.read_bytes()
        conn.execute("UPDATE events SET event_id='invalid'")
        with pytest.raises(CurrentError):
            current_runtime._publish(settings, conn, None, recalibrate=True)
        assert pointer.read_bytes() == before_pointer
        assert current_store.get_state(conn, "previous_publish_seconds") == 915
    assert current_publish.verify_current(settings)["valid"]
