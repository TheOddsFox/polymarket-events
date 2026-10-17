"""A forced pass publishes on completion or on the cadence, and exits 3 otherwise."""

import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_runtime import execute
from oddsfox_catalogue.current_store import apply, get_state, initialize, set_state


class _Client:
    def __init__(self, *_args, **kwargs):
        self.deadline = kwargs["deadline"]
        self.stats = self

    requests = 0
    downloaded_bytes = 0

    def as_dict(self):
        return {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def _settings(tmp_path):
    settings = CurrentSettings(
        tmp_path / "root",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=60),
    )
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        initialize(conn)
        apply(
            conn,
            ResponseRows(
                "2026-01-01T00:00:00Z",
                "1",
                events=[dict(venue="polymarket", event_id="1", title="open")],
            ),
        )
    return settings


def test_full_pass_cadence(tmp_path, monkeypatch):
    from oddsfox_catalogue import current_reconcile, current_runtime

    settings = _settings(tmp_path)
    published = []
    seen = {}

    def fake_reconcile(settings, conn, client, *, deadline):
        seen["gap"] = deadline - client.deadline
        if seen.get("complete"):
            state = get_state(conn, "full_pass")
            state["completed_at"] = datetime.now(UTC).isoformat()
            set_state(conn, "full_pass", state)
        return {"responses": 0, "source_complete": False}

    def fake_publish(settings, conn, deadline):
        published.append(True)
        set_state(conn, "last_publish_at", datetime.now(UTC).isoformat())
        return {"noop": True}, 0.0

    monkeypatch.setattr(current_runtime, "GammaClient", _Client)
    monkeypatch.setattr(current_runtime, "_publish", fake_publish)
    monkeypatch.setattr(current_reconcile, "reconcile", fake_reconcile)
    with connect_store(settings) as conn:
        set_state(conn, "last_publish_at", datetime.now(UTC).isoformat())
        set_state(conn, "previous_publish_seconds", 100)
    result = execute(settings, "reconcile", {"full_pass": True}, time.monotonic() + 1000)
    assert result["published"] is False
    assert result["full_pass"]["started_at"]
    assert published == []
    assert seen["gap"] == 150

    with connect_store(settings) as conn:
        set_state(
            conn,
            "last_publish_at",
            (datetime.now(UTC) - timedelta(seconds=21601)).isoformat(),
        )
    execute(settings, "reconcile", {"full_pass": True}, time.monotonic() + 1000)
    assert published == [True]
    assert seen["gap"] == 150
    published.clear()
    seen["complete"] = True
    with connect_store(settings) as conn:
        set_state(conn, "last_publish_at", datetime.now(UTC).isoformat())
        set_state(conn, "full_pass", None)
    execute(settings, "reconcile", {"full_pass": True}, time.monotonic() + 1000)
    assert published == [True]


def test_completion_after_deferred_cadence_keeps_measured_publish_reserve(tmp_path, monkeypatch):
    from oddsfox_catalogue import current_publish, current_reconcile, current_runtime

    settings = _settings(tmp_path)
    now = [100.0]
    seen = {}
    monkeypatch.setattr(current_runtime.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(current_runtime, "GammaClient", _Client)

    def complete_pass(settings, conn, client, *, deadline):
        apply(
            conn,
            ResponseRows(
                "2026-01-02T00:00:00Z",
                "completion-reserve",
                events=[dict(venue="polymarket", event_id="1", title="updated")],
            ),
        )
        seen["reserve"] = deadline - client.deadline
        state = get_state(conn, "full_pass")
        state["completed_at"] = datetime.now(UTC).isoformat()
        set_state(conn, "full_pass", state)
        now[0] = client.deadline - 1
        return {"responses": 1, "source_complete": False}

    monkeypatch.setattr(current_reconcile, "reconcile", complete_pass)
    with connect_store(settings) as conn:
        set_state(conn, "last_publish_at", datetime.now(UTC).isoformat())
        set_state(conn, "previous_publish_seconds", 100)
        set_state(conn, "full_pass", None)

    original_export = current_publish._export
    exports = [0]

    def slow_export(conn, table, path):
        original_export(conn, table, path)
        if exports[0] == 0:
            now[0] += 100
        exports[0] += 1

    monkeypatch.setattr(current_publish, "_export", slow_export)
    result = execute(settings, "reconcile", {"full_pass": True}, 1100)

    assert seen == {"reserve": 150}
    assert result["published"] is True
    assert exports[0] > 1
