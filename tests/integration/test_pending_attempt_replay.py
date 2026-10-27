"""Pending fetch replay preserves scheduler retry fairness."""

import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import httpx
import pytest

from oddsfox_catalogue import current_reconcile, current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import CaptureStopped, GammaClient

NOW = datetime(2026, 10, 27, tzinfo=UTC)


def event():
    return {"id": "1", "active": True, "closed": False, "archived": False}


def market():
    return {
        "id": "7",
        "version": "v1",
        "conditionId": "0x" + "7".zfill(64),
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["71", "72"],
    }


@pytest.fixture
def setup(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(":memory:")
    current_store.initialize(conn)
    current_reconcile._initialize(conn)
    set_bulk_policy(settings, conn)
    yield settings, conn
    conn.close()


def client(settings, handler):
    return GammaClient(settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None)


def set_bulk_policy(settings, conn, *, market_passes=(None,)):
    current_store.set_state(
        conn,
        "bulk_policy",
        {
            **current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {
                lane: {
                    "supported": True,
                    "passes": list(market_passes if lane == "markets" else (None,)),
                }
                for lane in current_sync.LANES
            },
        },
    )


def seed_event(conn):
    current_store.apply(
        conn,
        project_response(
            [event()],
            "events",
            received_at=(NOW - timedelta(hours=2)).isoformat(),
            response_id="seed-event",
        ),
    )


def seed_market(conn):
    current_store.apply(
        conn,
        project_response(
            [market()],
            "markets",
            received_at=(NOW - timedelta(hours=2)).isoformat(),
            response_id="seed-market",
        ),
    )


def leave_pending(settings, conn, lane, ids, *, native=False, attempts):
    with (
        client(settings, lambda _: (_ for _ in ()).throw(CaptureStopped("stop"))) as source,
        pytest.raises(CaptureStopped),
    ):
        current_sync.fetch_unit(settings, conn, source, lane, ids, native=native, attempts=attempts)


@pytest.mark.parametrize("interrupted", [False, True])
def test_interrupted_reconcile_resume_keeps_the_one_hour_attempt_cooldown(
    setup, monkeypatch, interrupted
):
    settings, conn = setup
    seed_event(conn)
    monkeypatch.setattr(current_reconcile, "_now", lambda: NOW)
    if interrupted:
        leave_pending(settings, conn, "events", ["1"], attempts=("events", ["1"], NOW))
        pending = current_store.get_state(conn, "fetch_pending")
        assert set(pending["identity"]) == {
            "lane",
            "ids",
            "native",
            "progress",
            "policy",
            "certificate",
            "passes",
            "api_certificate_version",
        }
        assert pending["attempts"] == ["events", ["1"], NOW.isoformat()]

    calls = []
    with client(
        settings, lambda request: calls.append(request) or httpx.Response(200, json=[])
    ) as source:
        current_reconcile.reconcile(settings, conn, source, deadline=time.monotonic() + 30)

    assert [request.url.path for request in calls] == ["/events"]
    assert conn.execute("SELECT kind,id,last_attempt FROM _reconcile_attempts").fetchall() == [
        ("events", "1", NOW)
    ]


@pytest.mark.parametrize(
    "metadata",
    [
        ["events", ["2"], NOW.isoformat()],
        ["markets", ["1"], NOW.isoformat()],
        ["unknown", ["1"], NOW.isoformat()],
        ["events", ["1"], "2026-10-27T00:00:00"],
        ["events", ["1"], "not-a-timestamp"],
        ["events", ["1", "1"], NOW.isoformat()],
        ["events", ["1"]],
    ],
)
def test_resume_rejects_malformed_attempt_metadata_before_http(setup, metadata):
    settings, conn = setup
    seed_event(conn)
    leave_pending(settings, conn, "events", ["1"], attempts=("events", ["1"], NOW))
    pending = current_store.get_state(conn, "fetch_pending")
    pending["attempts"] = metadata
    current_store.set_state(conn, "fetch_pending", pending)

    with (
        client(settings, lambda _: pytest.fail("invalid checkpoint made HTTP")) as source,
        pytest.raises(CurrentError, match="incompatible pending repair"),
    ):
        current_sync.resume_pending(settings, conn, source)

    assert current_store.get_state(conn, "fetch_pending")["attempts"] == metadata
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (0,)


def test_legacy_pending_without_attempt_metadata_resumes_without_inventing_attempt(setup):
    settings, conn = setup
    leave_pending(settings, conn, "events", ["1"], attempts=None)
    pending = current_store.get_state(conn, "fetch_pending")
    pending.pop("attempts", None)
    current_store.set_state(conn, "fetch_pending", pending)
    calls = []

    with client(
        settings, lambda request: calls.append(request) or httpx.Response(200, json=[event()])
    ) as source:
        current_sync.resume_pending(settings, conn, source)

    assert [request.url.path for request in calls] == ["/events"]
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (0,)


def test_direct_retry_can_upgrade_legacy_pending_when_caller_supplies_attempt(setup):
    settings, conn = setup
    seed_event(conn)
    leave_pending(settings, conn, "events", ["1"], attempts=None)
    pending = current_store.get_state(conn, "fetch_pending")
    pending.pop("attempts", None)
    current_store.set_state(conn, "fetch_pending", pending)

    with client(settings, lambda _: httpx.Response(200, json=[])) as source:
        current_sync.fetch_unit(
            settings, conn, source, "events", ["1"], attempts=("events", ["1"], NOW)
        )

    assert conn.execute("SELECT kind,id,last_attempt FROM _reconcile_attempts").fetchall() == [
        ("events", "1", NOW)
    ]


@pytest.mark.parametrize("conflicting", [False, True])
def test_direct_retry_restores_saved_attempts_and_rejects_replacement(setup, conflicting):
    settings, conn = setup
    seed_event(conn)
    leave_pending(settings, conn, "events", ["1"], attempts=("events", ["1"], NOW))
    pending = current_store.get_state(conn, "fetch_pending")
    calls = []
    attempts = ("events", ["1"], NOW + timedelta(minutes=1)) if conflicting else None
    with client(
        settings, lambda request: calls.append(request) or httpx.Response(200, json=[])
    ) as source:
        if conflicting:
            with pytest.raises(CurrentError, match="resume the pending repair"):
                current_sync.fetch_unit(settings, conn, source, "events", ["1"], attempts=attempts)
            assert calls == []
            assert current_store.get_state(conn, "fetch_pending") == pending
            assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (0,)
        else:
            current_sync.fetch_unit(settings, conn, source, "events", ["1"], attempts=attempts)
            assert len(calls) == 1
            assert current_store.get_state(conn, "fetch_pending") is None
            assert conn.execute(
                "SELECT kind,id,last_attempt FROM _reconcile_attempts"
            ).fetchall() == [("events", "1", NOW)]


def test_attempt_is_committed_only_with_final_omitting_pass(setup):
    settings, conn = setup
    seed_market(conn)
    policy = current_store.get_state(conn, "bulk_policy")
    policy["lanes"]["markets"]["passes"] = [False, True]
    current_store.set_state(conn, "bulk_policy", policy)

    def interrupt_second_pass(request):
        if request.url.params.get("closed") == "true":
            raise CaptureStopped("stop between passes")
        return httpx.Response(200, json=[])

    with (
        client(settings, interrupt_second_pass) as source,
        pytest.raises(CaptureStopped),
    ):
        current_sync.fetch_unit(
            settings,
            conn,
            source,
            "markets",
            ["7"],
            progress={"checkpoint": {"done": True}},
            attempts=("markets", ["7"], NOW),
        )

    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["attempts"] == ["markets", ["7"], NOW.isoformat()]
    assert current_store.get_state(conn, "checkpoint") is None
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (0,)

    with client(settings, lambda _: httpx.Response(200, json=[])) as source:
        current_sync.resume_pending(settings, conn, source)

    assert current_store.get_state(conn, "fetch_pending") is None
    assert current_store.get_state(conn, "checkpoint") == {"done": True}
    assert conn.execute("SELECT kind,id,last_attempt FROM _reconcile_attempts").fetchall() == [
        ("markets", "7", NOW)
    ]


def test_native_attempt_kind_survives_interrupted_confirmation(setup):
    settings, conn = setup
    seed_market(conn)
    leave_pending(settings, conn, "markets", ["7"], native=True, attempts=("native", ["7"], NOW))
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["attempts"] == ["native", ["7"], NOW.isoformat()]

    with client(settings, lambda _: httpx.Response(404)) as source:
        current_sync.resume_pending(settings, conn, source)

    assert conn.execute("SELECT kind,id,last_attempt FROM _reconcile_attempts").fetchall() == [
        ("native", "7", NOW)
    ]


def test_raw_response_replay_after_apply_failure_commits_attempt_without_new_http(
    setup, monkeypatch
):
    settings, _memory = setup
    conn = duckdb.connect(str(settings.store_path))
    current_store.initialize(conn)
    current_reconcile._initialize(conn)
    set_bulk_policy(settings, conn)
    seed_event(conn)
    monkeypatch.setattr(current_reconcile, "_now", lambda: NOW)

    original_apply = current_store.apply
    first_calls = []

    def fail_apply(*_args, **_kwargs):
        raise RuntimeError("crash after durable response")

    with client(
        settings,
        lambda request: first_calls.append(request) or httpx.Response(200, json=[]),
    ) as source:
        monkeypatch.setattr(current_store, "apply", fail_apply)
        with pytest.raises(RuntimeError, match="durable response"):
            current_sync.fetch_unit(
                settings,
                conn,
                source,
                "events",
                ["1"],
                attempts=("events", ["1"], NOW),
            )

    pending = current_store.get_state(conn, "fetch_pending")
    manifest = settings.raw_dir / pending["raw_id"] / "p000001.manifest.json"
    assert manifest.is_file()
    assert len(first_calls) == 1
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (0,)
    conn.close()

    conn = duckdb.connect(str(settings.store_path))
    monkeypatch.setattr(current_store, "apply", original_apply)
    resumed_calls = []
    try:
        with client(
            settings,
            lambda request: (
                resumed_calls.append(request) or pytest.fail("durable response replay made HTTP")
            ),
        ) as source:
            current_reconcile.reconcile(settings, conn, source, deadline=time.monotonic() + 30)

        assert resumed_calls == []
        assert current_store.get_state(conn, "fetch_pending") is None
        assert conn.execute("SELECT kind,id,last_attempt FROM _reconcile_attempts").fetchall() == [
            ("events", "1", NOW)
        ]
    finally:
        conn.close()


def test_malformed_caller_attempts_fail_before_pending_raw_or_http(setup):
    settings, conn = setup
    calls = []

    with (
        client(
            settings, lambda request: calls.append(request) or pytest.fail("unexpected HTTP")
        ) as source,
        pytest.raises(CurrentError, match="invalid scheduler attempt metadata"),
    ):
        current_sync.fetch_unit(
            settings, conn, source, "events", ["1"], attempts=("events", ["2"], NOW)
        )

    assert calls == []
    assert current_store.get_state(conn, "fetch_pending") is None
    assert not settings.raw_dir.exists() or not any(settings.raw_dir.iterdir())
