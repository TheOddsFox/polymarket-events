"""Cold keyset application must repair changed identities before receipts hide them."""

import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import httpx
import pytest

from fakes.world import make_market
from oddsfox_catalogue import current_keyset, current_reconcile, current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import GammaClient

NOW = datetime.now(UTC)


def event(markets):
    return {"id": "7", "active": False, "closed": True, "archived": False, "markets": markets}


@pytest.fixture
def catalogue(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(":memory:")
    current_store.initialize(conn)
    current_reconcile._initialize(conn)
    current_store.set_state(
        conn,
        "bulk_policy",
        {
            "version": 3,
            "policy": current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
            "capabilities": {
                "keyset_discovery": {
                    "supported": True,
                    "page_limit": 100,
                    "order": "id-ascending",
                    "lifecycle_coverage": "events-all-markets-open-and-closed",
                    "proof": "two-page-native-parity",
                }
            },
        },
    )
    monkeypatch.setattr(current_reconcile, "_now", lambda: NOW)
    yield settings, conn
    conn.close()


def seed(conn, keys):
    for key in keys:
        current_store.apply(
            conn,
            project_response(
                [make_market(str(key), "original", closed=True, seed=int(key))],
                "markets",
                received_at=(NOW - timedelta(days=1)).isoformat(),
                response_id=f"seed-{key}",
                native_market_ids=[str(key)],
            ),
        )


def http_client(settings, handler):
    return GammaClient(settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None)


def prepare_page(settings, conn, lane, highwater):
    state = {
        "turn": 0,
        "cold_slot": 0,
        "windows": {lane: {"keyset": True, "started_at": NOW.timestamp()}},
    }
    high = {"id": str(highwater), "closed": True}
    if lane == "events":
        high = event([])
    if lane == "markets":
        current_keyset.cold_page(
            settings,
            conn,
            http_client(settings, lambda _: httpx.Response(200, json=[])),
            lane,
            reconcile_state=state,
            now=NOW,
        )
        current_keyset.cold_page(settings, conn, object(), lane, reconcile_state=state, now=NOW)
    current_keyset.cold_page(
        settings,
        conn,
        http_client(settings, lambda _: httpx.Response(200, json=[high])),
        lane,
        reconcile_state=state,
        now=NOW,
    )
    return state


def one_cold_page(monkeypatch, lane, state):
    selected = []

    def cold(*_args, **_kwargs):
        if selected:
            return None
        selected.append(True)
        return lane, [], 0, state["windows"][lane]

    monkeypatch.setattr(current_reconcile, "_cold", cold)
    monkeypatch.setattr(current_reconcile, "_initialize", lambda _conn: None)
    # Use the exact frozen state supplied to the page, independent of unrelated scheduling.
    return selected


@pytest.mark.parametrize("lane", ["markets", "events"])
def test_fresh_cold_page_repairs_changed_recent_closed_market(catalogue, monkeypatch, lane):
    settings, conn = catalogue
    seed(conn, [11])
    changed = make_market("11", "changed", closed=True, seed=111)
    state = prepare_page(settings, conn, lane, 11)
    current_store.set_state(conn, "reconcile", state)
    one_cold_page(monkeypatch, lane, state)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/markets/11":
            return httpx.Response(200, json=changed)
        assert request.url.path == f"/{lane}/keyset"
        return httpx.Response(200, json={lane: [event([changed]) if lane == "events" else changed]})

    result = current_reconcile.reconcile(
        settings, conn, http_client(settings, handler), deadline=time.monotonic() + 60
    )
    assert calls == [f"/{lane}/keyset", "/markets/11"]
    assert result["responses"] == 2
    assert conn.execute("SELECT usable FROM markets WHERE market_id='11'").fetchone() == (True,)
    assert conn.execute("SELECT id FROM _reconcile_attempts WHERE kind='native'").fetchall() == [
        ("11",)
    ]


@pytest.mark.parametrize("lane", ["markets", "events"])
def test_captured_pending_page_replays_without_refetch_and_repairs(catalogue, monkeypatch, lane):
    settings, conn = catalogue
    seed(conn, [11])
    changed = make_market("11", "changed", closed=True, seed=111)
    state = prepare_page(settings, conn, lane, 11)
    apply = current_store.apply

    def crash(*_args, **_kwargs):
        raise RuntimeError("crash before application")

    with monkeypatch.context() as interrupted:
        interrupted.setattr(current_store, "apply", crash)
        with pytest.raises(RuntimeError, match="before application"):
            current_keyset.cold_page(
                settings,
                conn,
                http_client(
                    settings,
                    lambda _: httpx.Response(
                        200, json={lane: [event([changed]) if lane == "events" else changed]}
                    ),
                ),
                lane,
                reconcile_state=state,
                now=NOW,
            )
    assert current_store.apply is apply
    assert current_store.get_state(conn, "keyset_pending") is not None
    monkeypatch.setattr(current_reconcile, "_cold", lambda *_a, **_k: None)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        assert request.url.path == "/markets/11", "durable page must not be fetched twice"
        return httpx.Response(200, json=changed)

    result = current_reconcile.reconcile(
        settings, conn, http_client(settings, handler), deadline=time.monotonic() + 60
    )
    assert calls == ["/markets/11"]
    assert result["responses"] == 2
    assert current_store.get_state(conn, "keyset_pending") is None
    assert conn.execute("SELECT usable FROM markets WHERE market_id='11'").fetchone() == (True,)


@pytest.mark.parametrize("elapsed,expected", [(3599, []), (3600, ["/markets/11"])])
def test_native_repair_cooldown_boundary(catalogue, elapsed, expected):
    settings, conn = catalogue
    seed(conn, [11])
    changed = make_market("11", "changed", closed=True, seed=111)
    current_store.apply(
        conn,
        project_response([changed], "markets", received_at=NOW.isoformat(), response_id="changed"),
    )
    conn.execute(
        "INSERT INTO _reconcile_attempts VALUES ('native', '11', ?)",
        [NOW - timedelta(seconds=elapsed)],
    )
    calls = []
    count = current_reconcile.repair_identities(
        settings,
        conn,
        http_client(
            settings,
            lambda request: calls.append(request.url.path) or httpx.Response(200, json=changed),
        ),
        "markets",
        ["11"],
        NOW,
        time.monotonic() + 60,
    )
    assert calls == expected
    assert count == len(expected)


def test_nested_repairs_are_bounded_and_operator_failure_propagates(catalogue):
    settings, conn = catalogue
    keys = range(1, 102)
    seed(conn, keys)
    changed = {
        str(key): make_market(str(key), "changed", closed=True, seed=1000 + key) for key in keys
    }
    current_store.apply(
        conn,
        project_response(
            [event(list(changed.values()))],
            "events",
            received_at=NOW.isoformat(),
            response_id="nested",
        ),
    )
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=changed[request.url.path.rsplit("/", 1)[-1]])

    assert (
        current_reconcile.repair_identities(
            settings,
            conn,
            http_client(settings, handler),
            "events",
            ["7"],
            NOW,
            time.monotonic() + 60,
        )
        == 100
    )
    assert len(calls) == len(set(calls)) == 100
    assert conn.execute("SELECT count(*) FROM markets WHERE NOT usable").fetchone() == (1,)

    class Interrupted:
        def get(self, *_args, **_kwargs):
            raise KeyboardInterrupt("operator")

    with pytest.raises(KeyboardInterrupt, match="operator"):
        current_reconcile.repair_identities(
            settings, conn, Interrupted(), "events", ["7"], NOW, time.monotonic() + 60
        )


def test_canonical_runtime_repairs_initial_durable_page_before_scheduler(catalogue, monkeypatch):
    from contextlib import contextmanager

    from oddsfox_catalogue import current_runtime

    settings, conn = catalogue
    seed(conn, [11])
    changed = make_market("11", "changed", closed=True, seed=111)
    state = prepare_page(settings, conn, "events", 7)
    with monkeypatch.context() as crash:
        crash.setattr(
            current_store, "apply", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("crash"))
        )
        with pytest.raises(RuntimeError, match="crash"):
            current_keyset.cold_page(
                settings,
                conn,
                http_client(
                    settings, lambda _: httpx.Response(200, json={"events": [event([changed])]})
                ),
                "events",
                reconcile_state=state,
                now=NOW,
            )
    assert current_store.get_state(conn, "keyset_pending")["kind"] == "page"
    calls = []

    def handler(request):
        calls.append(request.url.path)
        assert request.url.path == "/markets/11", "runtime must replay captured bytes"
        return httpx.Response(200, json=changed)

    @contextmanager
    def connection(_settings):
        yield conn

    def scheduled(*_args, **_kwargs):
        assert conn.execute("SELECT usable FROM markets").fetchone() == (True,)
        return {"responses": 0}

    monkeypatch.setattr(current_runtime, "connect_store", connection)
    monkeypatch.setattr(
        current_runtime, "GammaClient", lambda *_a, **_k: http_client(settings, handler)
    )
    monkeypatch.setattr(current_runtime, "_publish", lambda *_a: ({"noop": True}, 0.0))
    monkeypatch.setattr(current_reconcile, "reconcile", scheduled)
    result = current_runtime.execute(settings, "reconcile", {}, time.monotonic() + 60)
    assert calls == ["/markets/11"]
    assert result["responses"] >= 1
    assert current_store.get_state(conn, "keyset_pending") is None


@pytest.mark.parametrize("expired", [False, True])
def test_repair_checks_stop_before_each_native_unit(catalogue, monkeypatch, expired):
    settings, conn = catalogue
    seed(conn, [11, 12])
    changed = [make_market(str(key), "changed", closed=True, seed=100 + key) for key in [11, 12]]
    current_store.apply(
        conn,
        project_response(changed, "markets", received_at=NOW.isoformat(), response_id="changed"),
    )
    clock = [10.0 if expired else 9.0]
    calls = []

    def fetch(*_args, **kwargs):
        calls.append(kwargs["attempts"][1])
        clock[0] = 10.0
        return {"responses": 1}

    monkeypatch.setattr(current_reconcile.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(current_reconcile, "fetch_apply", fetch)
    count = current_reconcile.repair_identities(
        settings, conn, object(), "markets", ["11", "12"], NOW, 10.0
    )
    assert calls == ([] if expired else [["11"]])
    assert count == (0 if expired else 1)


@pytest.mark.parametrize("kind", ["highwater", "empty", "above_highwater", "bootstrap"])
def test_only_accepted_reconciliation_page_ids_nominate_repair(catalogue, kind):
    settings, conn = catalogue
    state = {"windows": {"events": {"keyset": True, "started_at": NOW.timestamp()}}}
    reconciliation = None if kind == "bootstrap" else state
    result = current_keyset.cold_page(
        settings,
        conn,
        http_client(settings, lambda _: httpx.Response(200, json=[event([])])),
        "events",
        reconcile_state=reconciliation,
        now=NOW,
    )
    assert "identity_repairs" not in result
    if kind == "highwater":
        return
    rows = [] if kind == "empty" else [event([])]
    if kind == "above_highwater":
        rows[0]["id"] = "8"
    result = current_keyset.cold_page(
        settings,
        conn,
        http_client(settings, lambda _: httpx.Response(200, json={"events": rows})),
        "events",
        reconcile_state=reconciliation,
        now=NOW,
    )
    assert "identity_repairs" not in result
    if kind == "above_highwater":
        assert conn.execute("SELECT count(*) FROM events").fetchone() == (0,)


def test_repair_metadata_contains_only_accepted_top_level_ids(catalogue):
    settings, conn = catalogue
    state = prepare_page(settings, conn, "events", 7)
    accepted = event([make_market("11", "nested", closed=True)])
    beyond = {**event([]), "id": "8"}
    result = current_keyset.cold_page(
        settings,
        conn,
        http_client(settings, lambda _: httpx.Response(200, json={"events": [accepted, beyond]})),
        "events",
        reconcile_state=state,
        now=NOW,
    )
    assert result["identity_repairs"] == {"lane": "events", "ids": ["7"]}


@pytest.mark.parametrize("failure", ["capture", "budget"])
def test_partial_repair_stop_keeps_committed_page_and_native_counts(
    catalogue, monkeypatch, failure
):
    from oddsfox_catalogue.gamma.http import CaptureStopped, RequestBudgetExceeded

    settings, conn = catalogue
    seed(conn, [11, 12])
    changed = [make_market(str(key), "changed", closed=True, seed=100 + key) for key in [11, 12]]
    state = prepare_page(settings, conn, "events", 7)
    current_store.set_state(conn, "reconcile", state)
    one_cold_page(monkeypatch, "events", state)
    native = []
    error = CaptureStopped if failure == "capture" else RequestBudgetExceeded

    def fetch(*_args, **kwargs):
        native.append(kwargs["attempts"][1])
        if len(native) == 2:
            raise error("stop after first native response")
        return {"responses": 1}

    monkeypatch.setattr(current_reconcile, "fetch_apply", fetch)
    result = current_reconcile.reconcile(
        settings,
        conn,
        http_client(settings, lambda _: httpx.Response(200, json={"events": [event(changed)]})),
        deadline=time.monotonic() + 60,
    )
    assert native == [["11"], ["12"]]
    assert result["responses"] == 2, "one settled page and one settled native response"
    assert result["cold_batches"] == 1
    assert current_store.get_state(conn, "keyset_pending") is None
    assert conn.execute("SELECT count(*) FROM event_markets").fetchone() == (2,)


@pytest.mark.parametrize("failure", [RuntimeError("native failure"), KeyboardInterrupt("operator")])
def test_genuine_and_operator_errors_remain_visible_after_partial_repair(
    catalogue, monkeypatch, failure
):
    settings, conn = catalogue
    seed(conn, [11, 12])
    changed = [make_market(str(key), "changed", closed=True, seed=100 + key) for key in [11, 12]]
    current_store.apply(
        conn,
        project_response(changed, "markets", received_at=NOW.isoformat(), response_id="changed"),
    )
    calls = []
    counter = {"responses": 3}

    def fetch(*_args, **_kwargs):
        calls.append(True)
        if len(calls) == 2:
            raise failure
        return {"responses": 1}

    monkeypatch.setattr(current_reconcile, "fetch_apply", fetch)
    with pytest.raises(type(failure), match=str(failure)):
        current_reconcile.repair_identities(
            settings,
            conn,
            object(),
            "markets",
            ["11", "12"],
            NOW,
            time.monotonic() + 60,
            response_counter=counter,
        )
    assert counter["responses"] == 4


@pytest.mark.parametrize("restart", ["cursor", "oversized"])
def test_restarted_unapplied_page_does_not_nominate_repairs(catalogue, monkeypatch, restart):
    from types import SimpleNamespace

    from oddsfox_catalogue.gamma.http import CaptureStopped, CursorExpired, ResponseTooLarge

    settings, conn = catalogue
    state = prepare_page(settings, conn, "events", 7)

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("page not received")

    with pytest.raises(CaptureStopped):
        current_keyset.cold_page(
            settings, conn, Interrupted(), "events", reconcile_state=state, now=NOW
        )
    assert current_store.get_state(conn, "keyset_pending")["kind"] == "page"
    error = CursorExpired if restart == "cursor" else ResponseTooLarge

    def failed_read(*_args):
        raise error("retry with a new page")

    monkeypatch.setattr(current_keyset, "_read_pending", failed_read)
    result = current_keyset.resume_pending(settings, conn, SimpleNamespace())
    assert "identity_repairs" not in result
    assert result["responses"] == 0
    assert result["cursor_restart" if restart == "cursor" else "page_restart"] is True
    assert current_store.get_state(conn, "keyset_pending") is None
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone() == (0,)


def test_first_repair_initializes_scheduler_attempt_table(catalogue):
    settings, conn = catalogue
    seed(conn, [11])
    changed = make_market("11", "changed", closed=True, seed=111)
    current_store.apply(
        conn,
        project_response([changed], "markets", received_at=NOW.isoformat(), response_id="changed"),
    )
    conn.execute("DROP TABLE _reconcile_attempts")
    calls = []
    count = current_reconcile.repair_identities(
        settings,
        conn,
        http_client(
            settings,
            lambda request: calls.append(request.url.path) or httpx.Response(200, json=changed),
        ),
        "markets",
        ["11"],
        NOW,
        time.monotonic() + 60,
    )
    assert calls == ["/markets/11"]
    assert count == 1
    assert conn.execute("SELECT kind,id FROM _reconcile_attempts").fetchall() == [("native", "11")]
