"""Frozen lifecycle passes preserve atomic scheduling and bounded replay."""

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
        "closed": closed,
        "active": not closed,
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
    certificate = {
        **current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {
            lane: {"supported": True, "passes": [False, True]} for lane in current_sync.LANES
        },
    }
    current_store.set_state(conn, "bulk_policy", certificate)
    yield settings, conn
    conn.close()


def client(settings, handler, **kwargs):
    return GammaClient(
        settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None, **kwargs
    )


def test_probe_fallback_and_native_parity(setup):
    settings, _ = setup
    calls = []

    def handler(request):
        calls.append(request)
        path = request.url.path
        if path.endswith("/3"):
            return httpx.Response(404)
        if path.rsplit("/", 1)[-1].isdigit():
            key = int(path.rsplit("/", 1)[-1])
            return httpx.Response(200, json=event(key, key == 2))
        closed = request.url.params.get("closed") == "true"
        if "id" not in request.url.params:
            return httpx.Response(200, json=[event(2 if closed else 1, closed)])
        return httpx.Response(200, json=[event(2 if closed else 1, closed)])

    controls = {"open": [1], "closed": [2], "absent": [3]}
    result = current_sync.probe_policy(
        settings, client(settings, handler), event_ids=controls, market_ids=controls
    )
    assert all(v["supported"] and v["passes"] == [False, True] for v in result["lanes"].values()), (
        result
    )
    assert len(calls) == 16


@pytest.mark.parametrize("mismatch", ["identity", "lifecycle", "membership"])
def test_probe_rejects_projection_mismatch(setup, mismatch):
    settings, _ = setup

    def handler(request):
        if request.url.path.endswith("/3"):
            return httpx.Response(404)
        if request.url.path.rsplit("/", 1)[-1].isdigit():
            key = int(request.url.path.rsplit("/", 1)[-1])
            return httpx.Response(200, json=event(key, key == 2))
        row = event(1)
        if mismatch == "identity":
            row["id"] = "3"
        elif mismatch == "lifecycle":
            row["closed"] = True
        else:
            row.pop("markets")
        return httpx.Response(200, json=[row])

    controls = {"open": [1], "closed": [2], "absent": [3]}
    result = current_sync.probe_policy(
        settings, client(settings, handler), event_ids=controls, market_ids=controls
    )
    assert result["lanes"]["events"]["supported"] is False


def test_first_pass_commits_rows_not_frontier_then_raw_replay(setup, monkeypatch):
    settings, conn = setup
    apply = current_store.apply
    count = 0

    def crash_second(*args, **kwargs):
        nonlocal count
        count += 1
        if count == 2:
            raise RuntimeError("crash after second raw")
        return apply(*args, **kwargs)

    monkeypatch.setattr(current_store, "apply", crash_second)
    calls = []

    def handler(request):
        calls.append(request)
        closed = request.url.params.get("closed") == "true"
        return httpx.Response(200, json=[event(1, closed)])

    with pytest.raises(RuntimeError):
        current_sync.fetch_apply(
            settings, conn, client(settings, handler), "events", [1], progress={"frontier": 1}
        )
    assert current_store.get_state(conn, "frontier") is None
    assert conn.execute("select closed from events").fetchone() == (False,)
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["remaining"][0]["params"]["closed"] is True
    monkeypatch.setattr(current_store, "apply", apply)
    result = current_sync.resume_pending(
        settings, conn, client(settings, lambda _: pytest.fail("raw replay made HTTP"))
    )
    assert result["requests"] == 0
    assert current_store.get_state(conn, "frontier") == 1
    assert conn.execute("select closed from events").fetchone() == (True,)
    assert len(calls) == 2


def test_split_preserves_filter_and_final_checkpoint(setup):
    settings, conn = setup
    calls = []

    def handler(request):
        params = request.url.params
        calls.append((params.get("closed"), params.get_list("id")))
        if len(params.get_list("id")) > 1:
            return httpx.Response(200, content=b" " * 2000)
        assert current_store.get_state(conn, "finished") is None
        return httpx.Response(200, json=[])

    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, handler, max_body_bytes=1024),
        "events",
        [1, 2],
        progress={"finished": True},
    )
    assert calls == [
        ("false", ["1", "2"]),
        ("false", ["1"]),
        ("false", ["2"]),
        ("true", ["1", "2"]),
        ("true", ["1"]),
        ("true", ["2"]),
    ]
    assert current_store.get_state(conn, "finished") is True


def test_empty_pass_omission_preserves_retained_rows(setup):
    settings, conn = setup
    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, lambda _: httpx.Response(200, json=event(1, True))),
        "events",
        [1],
        native=True,
    )
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=[])

    result = current_sync.fetch_apply(settings, conn, client(settings, handler), "events", [1])
    assert result["responses"] == len(calls) == 2
    assert conn.execute("select closed from events").fetchone() == (True,)


def test_discovery_heads_and_window_commit_only_all_passes(setup, monkeypatch):
    settings, conn = setup
    apply = current_store.apply

    def crash_closed_window(*args, **kwargs):
        pending = current_store.get_state(conn, "fetch_pending")
        if pending and pending["remaining"][0]["params"].get("closed") is True:
            raise RuntimeError("closed pass crash")
        return apply(*args, **kwargs)

    monkeypatch.setattr(current_store, "apply", crash_closed_window)

    def handler(request):
        closed = request.url.params.get("closed") == "true"
        return httpx.Response(
            200, json=[event(2 if closed else 1, closed)] if "id" not in request.url.params else []
        )

    with pytest.raises(RuntimeError):
        current_sync.discover(
            settings, conn, client(settings, handler), deadline=time.monotonic() + 30
        )
    state = current_store.get_state(conn, "discovery")
    assert state["pending"] is None
    assert state["lanes"]["events"]["highwater"] == 2
    assert state["lanes"]["events"]["frontier"] == 0


def test_old_policy_and_old_pending_rejected(setup):
    settings, conn = setup
    certificate = current_store.get_state(conn, "bulk_policy")
    current_store.set_state(conn, "bulk_policy", {**certificate, "version": 1})
    with pytest.raises(CurrentError, match="certificate"):
        current_sync.fetch_apply(
            settings, conn, client(settings, lambda _: pytest.fail("HTTP")), "events", [1]
        )
    current_store.set_state(conn, "bulk_policy", certificate)
    current_store.set_state(
        conn,
        "fetch_pending",
        {
            "identity": {"lane": "events", "ids": [1], "native": False, "progress": {}},
            "remaining": [[1]],
            "raw_id": "a" * 32,
        },
    )
    with pytest.raises(CurrentError, match="incompatible"):
        current_sync.resume_pending(settings, conn, client(settings, lambda _: pytest.fail("HTTP")))


def test_nullable_lifecycle_is_retained_without_coverage_claim(setup):
    settings, conn = setup
    calls = []

    def handler(request):
        calls.append(request)
        row = event(1)
        row["closed"] = None
        return httpx.Response(200, json=[row])

    current_sync.fetch_apply(settings, conn, client(settings, handler), "events", [1])
    assert len(calls) == 2
    assert conn.execute("select closed from events").fetchone() == (None,)


def test_opposite_boolean_lifecycle_rejected(setup):
    settings, conn = setup
    with pytest.raises(CurrentError, match="lifecycle"):
        current_sync.fetch_apply(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=[event(1, True)])),
            "events",
            [1],
        )


def test_highwater_pass_heads_resume_before_exposing_bound(setup):
    settings, conn = setup
    calls = []

    def handler(request):
        calls.append(request)
        closed = request.url.params.get("closed") == "true"
        return httpx.Response(200, json=[event(5 if closed else 2, closed)])

    current_sync.discover(
        settings, conn, client(settings, handler, max_requests=1), deadline=time.monotonic() + 30
    )
    state = current_store.get_state(conn, "discovery")
    assert state["lanes"]["events"]["highwater"] is None
    assert state["pending"]["heads"] == [2]
    assert state["pending"]["maximum"] == 2
    assert state["pending"]["remaining"][0]["params"]["closed"] is True
    current_sync.discover(
        settings, conn, client(settings, handler, max_requests=1), deadline=time.monotonic() + 30
    )
    state = current_store.get_state(conn, "discovery")
    assert state["lanes"]["events"]["highwater"] == 5
    assert state["lanes"]["events"]["frontier"] == 0
    assert [r.url.params.get("closed") for r in calls] == ["false", "true"]


def test_pending_highwater_tamper_rejected_before_http(setup):
    settings, conn = setup
    current_sync.discover(
        settings,
        conn,
        client(settings, lambda _: httpx.Response(200, json=[event(2)]), max_requests=1),
        deadline=time.monotonic() + 30,
    )
    state = current_store.get_state(conn, "discovery")
    state["pending"]["maximum"] = 99
    current_store.set_state(conn, "discovery", state)
    with pytest.raises(CurrentError, match="incompatible"):
        current_sync.discover(
            settings,
            conn,
            client(settings, lambda _: pytest.fail("HTTP")),
            deadline=time.monotonic() + 30,
        )
