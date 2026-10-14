"""Discovery is bounded, durable and never interprets omissions as deletion."""

import time
from dataclasses import replace

import duckdb
import httpx
import pytest

from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.gamma.http import GammaClient


def event(key, closed=False):
    return {
        "id": str(key),
        "active": not closed,
        "closed": closed,
        "archived": False,
        "markets": [],
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
    current_store.set_state(
        conn,
        "bulk_policy",
        {
            **current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {k: {"supported": True, "passes": [None]} for k in current_sync.LANES},
        },
    )
    yield settings, conn
    conn.close()


def client(settings, handler, **kwargs):
    return GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None, **kwargs
    )


def test_independent_tails_empty_omissions_resume(setup):
    settings, conn = setup
    calls = []

    def handler(request):
        calls.append(request)
        lane = request.url.path[1:]
        if "id" not in request.url.params:
            return httpx.Response(200, json=[event(103 if lane == "events" else 2)])
        return httpx.Response(200, json=[])

    result = current_sync.discover(
        settings, conn, client(settings, handler), deadline=time.monotonic() + 30
    )
    assert result["frontiers"]["events"]["frontier"] == 103
    assert result["frontiers"]["markets"]["frontier"] == 2
    assert [r.url.path for r in calls[:4]] == ["/events", "/markets", "/events", "/markets"]
    assert all(len(r.url.params.get_list("id")) <= 100 for r in calls)
    assert conn.execute("SELECT count(*) FROM _receipts").fetchone()[0] == 2
    calls.clear()
    current_sync.discover(settings, conn, client(settings, handler), deadline=time.monotonic() + 30)
    assert len(calls) == 2  # Independent high-water probes, no old known-ID sweep.


def test_policy_guard(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", {})
    with pytest.raises(CurrentError, match="certificate"):
        current_sync.discover(
            settings,
            conn,
            client(settings, lambda _: pytest.fail("HTTP")),
            deadline=time.monotonic() + 30,
        )


def test_mismatch_has_no_checkpoint(setup):
    settings, conn = setup

    def handler(request):
        return httpx.Response(200, json=[event(2 if "id" not in request.url.params else 999)])

    with pytest.raises(CurrentError, match="unrequested"):
        current_sync.discover(
            settings, conn, client(settings, handler), deadline=time.monotonic() + 30
        )
    state = current_store.get_state(conn, "discovery")
    assert state["pending"] is None
    assert state["lanes"]["events"]["frontier"] == 0
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["identity"]["lane"] == "events"
    assert pending["identity"]["ids"] == [1, 2]
    assert pending["identity"]["progress"]["discovery"]["lanes"]["events"]["frontier"] == 2
    assert pending["remaining"] == [
        {"endpoint": "/events", "params": current_sync._window_params([1, 2], "events", None)}
    ]


def test_crash_after_raw_replays_without_http(setup, monkeypatch):
    settings, conn = setup
    apply = current_store.apply
    monkeypatch.setattr(
        current_store, "apply", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError())
    )
    with pytest.raises(RuntimeError):
        current_sync.discover(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=[])),
            deadline=time.monotonic() + 30,
        )
    assert list(settings.raw_dir.glob("*/p000001.manifest.json"))
    monkeypatch.setattr(current_store, "apply", apply)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=[])

    current_sync.discover(settings, conn, client(settings, handler), deadline=time.monotonic() + 30)
    assert len(calls) == 1


def test_probe_open_closed_absent_and_membership(setup):
    settings, _ = setup

    def handler(request):
        if request.url.path.endswith("/3"):
            return httpx.Response(404)
        if request.url.path.rsplit("/", 1)[-1].isdigit():
            key = int(request.url.path.rsplit("/", 1)[-1])
            return httpx.Response(200, json=event(key, key == 2))
        if "id" not in request.url.params:
            return httpx.Response(200, json=[event(2, True)])
        return httpx.Response(200, json=[event(1), event(2, True)])

    controls = {"open": [1], "closed": [2], "absent": [3]}
    certificate = current_sync.probe_policy(
        settings, client(settings, handler), event_ids=controls, market_ids=controls
    )
    assert all(v["supported"] for v in certificate["lanes"].values()), certificate


def test_oversized_windows_split_without_native_fallback(setup):
    settings, conn = setup
    calls = []

    def handler(request):
        ids = request.url.params.get_list("id")
        calls.append(request)
        if not ids:
            return httpx.Response(200, json=[event(4)])
        if len(ids) > 1:
            return httpx.Response(200, content=b" " * 2000)
        return httpx.Response(200, json=[])

    current_sync.discover(
        settings,
        conn,
        client(settings, handler, max_body_bytes=1024),
        deadline=time.monotonic() + 30,
    )
    assert current_store.get_state(conn, "discovery")["lanes"]["events"]["frontier"] == 4
    assert all(r.url.path in {"/events", "/markets"} for r in calls)


def test_native_repair_404_does_not_delete(setup):
    settings, conn = setup
    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, lambda _: httpx.Response(200, json=event(1))),
        "events",
        [1],
        native=True,
    )
    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, lambda _: httpx.Response(404)),
        "events",
        [1],
        native=True,
        progress={"repair_done": True},
    )
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    assert current_store.get_state(conn, "repair_done") is True
    assert current_store.get_state(conn, "fetch_pending") is None


def test_native_repair_durable_replay(setup, monkeypatch):
    settings, conn = setup
    apply = current_store.apply

    def crash(*a, **kw):
        raise RuntimeError("after raw")

    monkeypatch.setattr(current_store, "apply", crash)
    with pytest.raises(RuntimeError):
        current_sync.fetch_apply(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=event(1))),
            "events",
            [1],
            native=True,
        )
    monkeypatch.setattr(current_store, "apply", apply)
    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, lambda _: pytest.fail("replay made HTTP request")),
        "events",
        [1],
        native=True,
    )
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1
