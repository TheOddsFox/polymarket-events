"""Adversarial catalogue.v3 invariants using synthetic, offline responses only."""

import json
import time
from dataclasses import replace
from itertools import permutations
from pathlib import Path

import duckdb
import httpx
import pytest

from oddsfox_catalogue import current_publish, current_store, current_sync
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import SCHEMAS, CurrentError, columns
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import GammaClient


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        gamma=GammaSettings(base_url="http://127.0.0.1:9", requests_per_second=10),
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
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        },
    )
    yield settings, conn
    conn.close()


def market(key="2", assets=("11", "12"), **fields):
    return {
        "id": key,
        "version": "v1",
        "conditionId": "0x" + "a" * 64,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": list(assets),
        "active": True,
        "closed": False,
        "archived": False,
        **fields,
    }


def packet(payload, lane="markets", day=1, *, native=(), response_id=None):
    return project_response(
        payload,
        lane,
        received_at=f"2026-10-{day:02d}T00:00:00Z",
        response_id=response_id or f"response-{day}",
        native_market_ids=native,
    )


def client(settings, handler):
    return GammaClient(settings.gamma, transport=httpx.MockTransport(handler), sleep=lambda _: None)


def no_http(request):
    pytest.fail(f"durable pending response unexpectedly made HTTP request: {request.url}")


def state_snapshot(conn):
    tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    return {
        table: conn.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall() for table in tables
    }


def crash_after_raw(settings, conn, monkeypatch):
    original = current_store.apply

    def crash(*args, **kwargs):
        raise RuntimeError("crash after durable raw")

    monkeypatch.setattr(current_store, "apply", crash)
    with pytest.raises(RuntimeError, match="durable raw"):
        current_sync.fetch_apply(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=market())),
            "markets",
            [2],
            native=True,
            progress={"repair_cursor": "finished"},
        )
    monkeypatch.setattr(current_store, "apply", original)
    work = current_store.get_state(conn, "fetch_pending")
    return settings.raw_dir / work["raw_id"]


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("lane", ["events", "markets"])
@pytest.mark.parametrize("lifecycle", [{}, {"active": None, "closed": None, "archived": None}])
def test_nullable_source_lifecycle_survives_sync(catalogue, lane, lifecycle, native):
    settings, conn = catalogue
    raw = {"id": "2", "markets": []} if lane == "events" else market()
    for key in ("active", "closed", "archived"):
        raw.pop(key, None)
    raw.update(lifecycle)
    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, lambda _: httpx.Response(200, json=raw if native else [raw])),
        lane,
        [2],
        native=native,
    )
    assert conn.execute(f"SELECT active,closed,archived FROM {lane}").fetchall() == [
        (None, None, None)
    ]
    assert current_store.get_state(conn, "fetch_pending") is None


@pytest.mark.parametrize("bad", [False, 0, -1, 2**256, "01", "1e2", 1.5, None])
def test_invalid_native_asset_never_nominates_outcomes(catalogue, bad):
    _, conn = catalogue
    current_store.apply(conn, packet(market(assets=(bad, "12"))))
    assert conn.execute("SELECT usable FROM markets").fetchall() == [(False,)]
    assert conn.execute("SELECT * FROM outcomes").fetchall() == []
    assert current_store.validate_store(conn)["valid"]


@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize("encoded", [False, True])
def test_uint256_assets_remain_exact_through_store_and_parquet(catalogue, version, encoded):
    settings, conn = catalogue
    values = [2**255 + 1, 2**256 - 1]
    expected = [str(value) for value in values]
    assets = json.dumps(expected) if encoded else values
    field = "positionIds" if version == "v2" else "clobTokenIds"
    raw = market(version=version, **{field: assets})
    current_store.apply(conn, packet(raw))
    result = current_publish.publish(settings, conn)
    path = Path(result["path"]) / "outcomes.parquet"
    assert conn.execute(
        "SELECT asset_id FROM read_parquet(?) ORDER BY outcome_index", [str(path)]
    ).fetchall() == [(value,) for value in expected]
    assert current_publish.verify_current(settings, connection=conn)["valid"]


@pytest.mark.parametrize("ordering", list(permutations((1, 2, 3))))
def test_late_complete_empty_and_new_unknown_preserve_ordered_membership(catalogue, ordering):
    _, conn = catalogue
    responses = {
        1: packet({"id": "1", "markets": [market()]}, "events", day=1),
        2: packet({"id": "1", "markets": []}, "events", day=2),
        3: packet({"id": "1"}, "events", day=3),
    }
    for day in ordering:
        current_store.apply(conn, responses[day])
    assert conn.execute("SELECT * FROM event_markets").fetchall() == []
    assert current_store.summary(conn)["unresolved_memberships"] == 1
    assert current_store.validate_store(conn)["valid"]


def test_changed_embedded_map_reserves_both_maps_until_new_native_confirmation(catalogue):
    _, conn = catalogue
    current_store.apply(conn, packet(market(), day=1))
    current_store.apply(conn, packet(market(assets=("21", "22")), day=3))
    current_store.apply(conn, packet(market("3", ("11", "31")), day=4))
    current_store.apply(conn, packet(market("4", ("21", "41")), day=5))
    assert conn.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (0,)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    current_store.apply(conn, packet(market(), day=2, native=[2]))
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    # Moving competitors releases their claims, but does not confirm market 2.
    current_store.apply(conn, packet(market("3", ("51", "52")), day=6, native=[3]))
    current_store.apply(conn, packet(market("4", ("61", "62")), day=7, native=[4]))
    assert conn.execute("SELECT usable FROM markets WHERE market_id='2'").fetchone() == (False,)
    current_store.apply(conn, packet(market(assets=("21", "22")), day=8, native=[2]))
    assert conn.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (3,)
    assert current_store.validate_store(conn)["valid"]


def test_stale_conflicting_snapshot_cannot_reserve_new_assets(catalogue):
    _, conn = catalogue
    current_store.apply(conn, packet(market(), day=3, native=[2]))
    current_store.apply(conn, packet(market("3", ("21", "22")), day=4, native=[3]))
    before = state_snapshot(conn)
    current_store.apply(conn, packet(market("3"), day=2, native=[3]))
    assert state_snapshot(conn) == before


def test_progress_serialization_failure_rolls_back_peer_invalidation_and_all_internal_state(
    catalogue,
):
    _, conn = catalogue
    current_store.apply(conn, packet(market()), {"repair_cursor": "before"})
    before = state_snapshot(conn)
    conflicting = packet({"id": "1", "markets": [market("3")]}, "events", day=2)
    conflicting.parent_event_ids = {"9"}
    with pytest.raises(TypeError):
        current_store.apply(conn, conflicting, {"repair_cursor": "after", "invalid": object()})
    assert state_snapshot(conn) == before
    assert current_store.validate_store(conn)["valid"]


@pytest.mark.parametrize("omission", ["bulk", "native404"])
def test_source_omission_keeps_market_identity_ownership_and_membership(catalogue, omission):
    settings, conn = catalogue
    current_store.apply(conn, packet({"id": "1", "markets": [market()]}, "events"))
    before = state_snapshot(conn)
    response = httpx.Response(200, json=[]) if omission == "bulk" else httpx.Response(404)
    current_sync.fetch_apply(
        settings,
        conn,
        client(settings, lambda _: response),
        "markets",
        [2],
        native=omission == "native404",
        progress={"repair_cursor": "finished"},
    )
    after = state_snapshot(conn)
    assert {k: v for k, v in after.items() if k != "sync_state"} == {
        k: v for k, v in before.items() if k != "sync_state"
    }
    assert current_store.generation(conn) == 1
    current_store.apply(conn, packet(market("3"), day=2))
    assert conn.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (0,)


def test_pending_repair_request_identity_is_frozen(catalogue, monkeypatch):
    settings, conn = catalogue
    crash_after_raw(settings, conn, monkeypatch)
    before = state_snapshot(conn)
    with pytest.raises(CurrentError, match="pending"):
        current_sync.fetch_apply(
            settings,
            conn,
            client(settings, no_http),
            "markets",
            [2],
            native=True,
            progress={"repair_cursor": "different"},
        )
    assert state_snapshot(conn) == before
    current_sync.resume_pending(settings, conn, client(settings, no_http))
    assert current_store.get_state(conn, "repair_cursor") == "finished"
    assert current_store.get_state(conn, "fetch_pending") is None
    assert conn.execute("SELECT usable FROM markets").fetchone() == (True,)


@pytest.mark.parametrize("corruption", ["request", "body", "manifest"])
def test_corrupt_pending_raw_fails_closed_without_refetch_or_checkpoint(
    catalogue, monkeypatch, corruption
):
    settings, conn = catalogue
    raw_dir = crash_after_raw(settings, conn, monkeypatch)
    before = state_snapshot(conn)
    manifest = raw_dir / "p000001.manifest.json"
    if corruption == "request":
        value = json.loads(manifest.read_text())
        value["request"]["endpoint"] = "/markets/999"
        manifest.write_text(json.dumps(value))
    elif corruption == "body":
        (raw_dir / "p000001.json.gz").write_bytes(b"damaged raw body")
    else:
        manifest.write_text("{broken JSON")
    with pytest.raises((CurrentError, ValueError)):
        current_sync.resume_pending(settings, conn, client(settings, no_http))
    assert state_snapshot(conn) == before
    assert current_store.get_state(conn, "repair_cursor") is None


def test_empty_publication_has_full_schemas_and_noop_keeps_pointer(catalogue):
    settings, conn = catalogue
    result = current_publish.publish(settings, conn)
    for relation, schema in SCHEMAS.items():
        path = Path(result["path"]) / f"{relation}.parquet"
        actual = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        assert tuple((row[0], row[1]) for row in actual) == schema
        assert conn.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone() == (0,)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    current_store.apply(conn, packet([], day=2), {"empty_scan": True})
    assert current_publish.publish(settings, conn)["noop"]
    assert pointer.read_bytes() == before


def test_null_fields_and_content_generation_ignore_new_receipt_and_discarded_metadata(catalogue):
    settings, conn = catalogue
    raw = {"id": "1", "title": None, "markets": [market(question=None, slug=None)]}
    first = packet(raw, "events")
    for relation in ("events", "markets", "outcomes"):
        assert all(set(row) == set(columns(relation)) for row in getattr(first, relation))
    current_store.apply(conn, first)
    release = current_publish.publish(settings, conn)
    raw["volume"] = "999999999999999999999"
    raw["markets"][0]["volume"] = "999999999999999999999"
    current_store.apply(conn, packet(raw, "events", day=2))
    assert current_store.generation(conn) == 1
    assert current_publish.publish(settings, conn)["release_id"] == release["release_id"]
    raw["title"] = "Now known"
    current_store.apply(conn, packet(raw, "events", day=3))
    assert current_store.generation(conn) == 2
    assert conn.execute("SELECT question,slug FROM markets").fetchone() == (None, None)
    assert current_publish.publish(settings, conn)["generation"] == 2


@pytest.mark.parametrize("field", ["active", "closed", "archived"])
@pytest.mark.parametrize("value", [0, "false", []])
def test_nonboolean_lifecycle_rejected_without_data_or_checkpoint(catalogue, field, value):
    settings, conn = catalogue
    raw = market(**{field: value})
    with pytest.raises(CurrentError):
        current_sync.fetch_apply(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=raw)),
            "markets",
            [2],
            native=True,
            progress={"repair_cursor": "finished"},
        )
    assert conn.execute("SELECT count(*) FROM markets").fetchone() == (0,)
    assert current_store.get_state(conn, "repair_cursor") is None
    assert not list(settings.raw_dir.glob("*/p000001.manifest.json"))


@pytest.mark.parametrize("closed", ["missing", None])
def test_probe_with_unknown_control_lifecycle_is_unsupported(catalogue, closed):
    settings, _ = catalogue

    def handler(request):
        if request.url.path.endswith("/3"):
            return httpx.Response(404)
        key = request.url.path.rsplit("/", 1)[-1]
        assert key in {"1", "2"}
        raw = market(key)
        if closed == "missing":
            raw.pop("closed")
        else:
            raw["closed"] = closed
        return httpx.Response(200, json=raw)

    controls = {"open": [1], "closed": [2], "absent": [3]}
    certificate = current_sync.probe_policy(
        settings, client(settings, handler), event_ids=controls, market_ids=controls
    )
    assert all(lane["supported"] is False for lane in certificate["lanes"].values())


def test_pending_repair_batch_tamper_cannot_consume_different_raw(catalogue, monkeypatch):
    settings, conn = catalogue
    crash_after_raw(settings, conn, monkeypatch)
    work = current_store.get_state(conn, "fetch_pending")
    work["remaining"] = [{"endpoint": "/markets/3", "params": {}}]
    current_store.set_state(conn, "fetch_pending", work)
    before = state_snapshot(conn)
    with pytest.raises(CurrentError):
        current_sync.resume_pending(settings, conn, client(settings, no_http))
    assert state_snapshot(conn) == before


@pytest.mark.parametrize("corruption", ["request", "body", "manifest"])
def test_discovery_raw_corruption_never_advances_or_refetches(catalogue, monkeypatch, corruption):
    settings, conn = catalogue
    original = current_store.apply

    def crash(*args, **kwargs):
        raise RuntimeError("crash after durable raw")

    monkeypatch.setattr(current_store, "apply", crash)
    with pytest.raises(RuntimeError, match="durable raw"):
        current_sync.discover(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=[])),
            deadline=time.monotonic() + 30,
        )
    monkeypatch.setattr(current_store, "apply", original)
    pending = current_store.get_state(conn, "discovery")["pending"]
    raw_dir = settings.raw_dir / pending["raw_id"]
    manifest = raw_dir / "p000001.manifest.json"
    if corruption == "request":
        receipt = json.loads(manifest.read_text())
        receipt["request"]["params"]["ascending"] = True
        manifest.write_text(json.dumps(receipt))
    elif corruption == "body":
        (raw_dir / "p000001.json.gz").write_bytes(b"damaged raw body")
    else:
        manifest.write_text("{broken JSON")
    before = state_snapshot(conn)
    with pytest.raises((CurrentError, ValueError)):
        current_sync.discover(
            settings, conn, client(settings, no_http), deadline=time.monotonic() + 30
        )
    assert state_snapshot(conn) == before


@pytest.mark.parametrize(
    "field", ["kind", "maximum", "policy", "certificate", "endpoint", "params"]
)
def test_discovery_pending_request_fields_are_bound_to_raw_receipt(catalogue, monkeypatch, field):
    settings, conn = catalogue
    original = current_store.apply

    def crash(*args, **kwargs):
        raise RuntimeError("crash after durable raw")

    monkeypatch.setattr(current_store, "apply", crash)
    with pytest.raises(RuntimeError, match="durable raw"):
        current_sync.discover(
            settings,
            conn,
            client(settings, lambda _: httpx.Response(200, json=[])),
            deadline=time.monotonic() + 30,
        )
    monkeypatch.setattr(current_store, "apply", original)
    state = current_store.get_state(conn, "discovery")
    pending = state["pending"]
    if field == "endpoint":
        pending["remaining"][0]["endpoint"] = "/markets"
    elif field == "params":
        pending["remaining"][0]["params"]["limit"] = 2
    else:
        pending[field] = {
            "kind": "window",
            "maximum": 999,
            "policy": {**current_sync.POLICY, "version": -1},
            "certificate": {},
        }[field]
    current_store.set_state(conn, "discovery", state)
    before = state_snapshot(conn)
    with pytest.raises(CurrentError, match="receipt mismatch|incompatible pending discovery"):
        current_sync.discover(
            settings, conn, client(settings, no_http), deadline=time.monotonic() + 30
        )
    assert state_snapshot(conn) == before
