"""Frozen lifecycle passes preserve atomic scheduling and bounded replay."""

import time
from dataclasses import replace
from types import SimpleNamespace

import duckdb
import httpx
import pytest

from fakes.world import make_market
from oddsfox_catalogue import current_keyset, current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.gamma.http import (
    CaptureStopped,
    GammaClient,
    RequestBudgetExceeded,
    RetriesExhausted,
)


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
    settings, conn = setup
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


def test_keyset_probe_is_disabled_when_event_highwater_is_only_lifecycle_split(setup, monkeypatch):
    settings, conn = setup
    controls = {"open": [1], "closed": [2], "absent": []}
    base = {
        "lanes": {
            "events": {"supported": True, "passes": [False, True]},
            "markets": {"supported": True, "passes": [None]},
        }
    }
    monkeypatch.setattr(current_sync, "probe_policy", lambda *_a, **_k: base)
    called = []
    monkeypatch.setattr(current_sync, "_probe_keyset", lambda *_a: called.append(True))
    result = current_sync.probe_api_optimizations(
        settings,
        conn,
        object(),
        event_ids=controls,
        market_ids=controls,
        market_closed_any=False,
        keyset_discovery=True,
    )
    assert result["lanes"]["events"]["passes"] == [False, True]
    assert result["capabilities"]["keyset_discovery"]["supported"] is False
    assert "all-lifecycle event coverage" in result["capabilities"]["keyset_discovery"]["error"]
    assert called == []


def test_keyset_runtime_rejects_forged_split_event_highwater_before_http(setup):
    settings, conn = setup
    certificate = _keyset_certificate(settings)
    certificate["lanes"]["events"]["passes"] = [False, True]
    current_store.set_state(conn, "bulk_policy", certificate)

    class UnexpectedRequest:
        def get(self, *_args):
            pytest.fail("split-lifecycle certificate must be rejected before HTTP")

    with pytest.raises(CurrentError, match="all-lifecycle event highwater"):
        current_keyset.cold_page(
            settings,
            conn,
            UnexpectedRequest(),
            "events",
            reconcile_state={"windows": {"events": {"started_at": 1.0}}},
            now=1.0,
        )


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


def _v3_certificate(settings):
    return {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {
            "market_closed_any": {
                "supported": True,
                "controls": ["11", "12", "13"],
                "lifecycle_coverage": "known-controls-only",
            }
        },
    }


def test_v3_closed_any_is_used_for_bounded_market_repairs(setup):
    settings, conn = setup
    market = make_market("11", "synthetic", version="v1")
    current_store.set_state(conn, "bulk_policy", _v3_certificate(settings))
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=[market])

    current_sync.fetch_apply(settings, conn, client(settings, handler), "markets", [11])
    assert len(seen) == 1
    assert seen[0].url.path == "/markets"
    assert seen[0].url.params.get("closed") == "any"
    assert seen[0].url.params.get_list("id") == ["11"]


def test_v3_any_pending_repair_survives_json_roundtrip_and_replays_exactly(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _v3_certificate(settings))

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("test stop")

    with pytest.raises(CaptureStopped):
        current_sync.fetch_apply(settings, conn, Interrupted(), "markets", [11])
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["identity"]["passes"] == ["any"]
    current_sync.validate_fetch_pending(settings, conn, pending)
    seen = []
    market = make_market("11", "synthetic", version="v1")

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=[market])

    current_sync.resume_pending(settings, conn, client(settings, handler))
    assert len(seen) == 1
    assert seen[0].url.params.get("closed") == "any"
    assert current_store.get_state(conn, "fetch_pending") is None


def test_v2_pending_repair_replays_with_its_original_request(setup):
    settings, conn = setup
    certificate = {
        **current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
    }
    current_store.set_state(conn, "bulk_policy", certificate)

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("test stop")

    with pytest.raises(CaptureStopped):
        current_sync.fetch_apply(settings, conn, Interrupted(), "markets", [11])
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["identity"]["certificate"] == certificate
    current_sync.validate_fetch_pending(settings, conn, pending)

    market = make_market("11", "synthetic", version="v1")
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=[market])

    current_sync.resume_pending(settings, conn, client(settings, handler))
    assert len(seen) == 1
    assert seen[0].url.path == "/markets"
    assert seen[0].url.params.get("closed") is None
    assert current_store.get_state(conn, "fetch_pending") is None


def test_v3_native_pending_is_fenced_from_v2_readers(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _v3_certificate(settings))

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("test stop")

    with pytest.raises(CaptureStopped):
        current_sync.fetch_apply(settings, conn, Interrupted(), "markets", ["11"], native=True)
    pending = current_store.get_state(conn, "fetch_pending")
    assert pending["identity"]["policy"] == {
        **current_sync.POLICY,
        "acquisition_certificate_version": 3,
    }
    # The previous reader required the exact v2 policy, including for native
    # requests without a stored request certificate.
    assert pending["identity"]["policy"] != current_sync.POLICY

    def baseline_v2_reader(identity):
        if identity["policy"] != current_sync.POLICY:
            raise CurrentError("incompatible pending repair policy")

    with pytest.raises(CurrentError, match="incompatible pending repair policy"):
        baseline_v2_reader(pending["identity"])


@pytest.mark.parametrize(
    ("state_key", "pending_value"),
    [
        ("fetch_pending", {"kind": "repair"}),
        ("discovery", {"pending": {"kind": "highwater"}}),
        ("keyset_pending", {"kind": "page"}),
    ],
)
def test_optional_reprobe_refuses_all_pending_acquisition_kinds(
    setup, monkeypatch, state_key, pending_value
):
    settings, conn = setup
    current_store.set_state(conn, state_key, pending_value)
    monkeypatch.setattr(
        current_sync,
        "probe_policy",
        lambda *_args, **_kwargs: pytest.fail("pending work must fence probe requests"),
    )

    with pytest.raises(CurrentError, match="work is pending"):
        current_sync.probe_api_optimizations(
            settings,
            conn,
            object(),
            event_ids={"open": ["1"], "closed": ["2"], "absent": ["3"]},
            market_ids={"open": ["11"], "closed": ["12"], "absent": ["13"]},
        )


def _keyset_certificate(settings):
    return {
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
    }


def test_keyset_cold_pages_resume_in_order_and_stop_at_frozen_highwater(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))
    # The largest ID is closed; the certified unfiltered highwater must still
    # cover it even if open events have a lower maximum.
    rows = [event(key, closed=(key == 4)) for key in range(1, 5)]
    calls = []

    def handler(request):
        calls.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/events":
            return httpx.Response(200, json=[rows[-1]])
        cursor = request.url.params.get("after_cursor")
        if cursor is None:
            return httpx.Response(200, json={"events": rows[:2], "next_cursor": "cursor-1"})
        return httpx.Response(200, json={"events": rows[2:]})

    reconcile = {"windows": {"events": {"started_at": 1.0}}}
    from datetime import UTC, datetime

    for _ in range(3):
        result = current_keyset.cold_page(
            settings,
            conn,
            client(settings, handler),
            "events",
            reconcile_state=reconcile,
            now=datetime.now(UTC),
        )
    assert result["complete"] is True
    assert [
        row[0] for row in conn.execute("SELECT event_id FROM events ORDER BY event_id").fetchall()
    ] == [
        "1",
        "2",
        "3",
        "4",
    ]
    assert (
        current_store.get_state(conn, "keyset_discovery")["lanes"]["events"]["passes"]["all"][
            "highwater"
        ]
        == 4
    )
    assert [path for path, _ in calls] == ["/events", "/events/keyset", "/events/keyset"]
    assert calls[-1][1]["after_cursor"] == "cursor-1"


def test_keyset_pending_rejects_parameter_changes_and_preserves_exact_request(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("test stop")

    from datetime import UTC, datetime

    reconcile = {"windows": {"events": {"started_at": 1.0}}}
    with pytest.raises(CaptureStopped):
        current_keyset.cold_page(
            settings,
            conn,
            Interrupted(),
            "events",
            reconcile_state=reconcile,
            now=datetime.now(UTC),
        )
    pending = current_store.get_state(conn, "keyset_pending")
    assert pending["request"] == {
        "endpoint": "/events",
        "params": {"limit": 1, "order": "id", "ascending": False},
    }
    pending["request"]["params"]["offset"] = 1
    with pytest.raises(CurrentError, match="keyset"):
        current_keyset.validate_pending(settings, conn, pending)


def test_malformed_fresh_keyset_page_is_not_pinned_and_can_be_retried(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))
    import json
    from datetime import UTC, datetime

    class Response:
        status = 200
        received_at = datetime.now(UTC)

        def __init__(self, value):
            self.json = value
            self.body = json.dumps(value).encode()

    class Highwater:
        def get(self, *_args):
            return Response([event(2)])

    reconcile = {"windows": {"events": {"started_at": 1.0}}}
    current_keyset.cold_page(
        settings, conn, Highwater(), "events", reconcile_state=reconcile, now=datetime.now(UTC)
    )

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("leave a pending page")

    with pytest.raises(CaptureStopped):
        current_keyset.cold_page(
            settings,
            conn,
            Interrupted(),
            "events",
            reconcile_state=reconcile,
            now=datetime.now(UTC),
        )
    pending = current_store.get_state(conn, "keyset_pending")
    raw_manifest = settings.raw_dir / pending["raw_id"] / "p000001.manifest.json"

    class Malformed:
        def get(self, *_args):
            return Response({"events": [event(2), event(1)]})

    with pytest.raises(CurrentError, match="increasing"):
        current_keyset.resume_pending(settings, conn, Malformed())
    assert not raw_manifest.exists()
    assert current_store.get_state(conn, "keyset_pending") is not None

    class Corrected:
        def get(self, *_args):
            return Response({"events": [event(1), event(2)]})

    current_keyset.resume_pending(settings, conn, Corrected())
    assert current_store.get_state(conn, "keyset_pending") is None
    assert raw_manifest.is_file()


def test_corrupt_durable_keyset_body_is_rejected_before_replay_network(setup, monkeypatch):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))
    from datetime import UTC, datetime

    def handler(request):
        if request.url.path == "/events":
            return httpx.Response(200, json=[event(2)])
        return httpx.Response(200, json={"events": [event(1), event(2)]})

    reconcile = {"windows": {"events": {"started_at": 1.0}}}
    current_keyset.cold_page(
        settings,
        conn,
        client(settings, handler),
        "events",
        reconcile_state=reconcile,
        now=datetime.now(UTC),
    )
    monkeypatch.setattr(
        current_store,
        "apply",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("stop after capture")),
    )
    with pytest.raises(RuntimeError, match="stop after capture"):
        current_keyset.cold_page(
            settings,
            conn,
            client(settings, handler),
            "events",
            reconcile_state=reconcile,
            now=datetime.now(UTC),
        )

    pending = current_store.get_state(conn, "keyset_pending")
    body = settings.raw_dir / pending["raw_id"] / "p000001.json.gz"
    body.write_bytes(b"tampered")
    calls = []

    class MustNotFetch:
        def get(self, *args):
            calls.append(args)
            raise AssertionError("corrupt durable response reached network")

    with pytest.raises(ValueError, match="checksum"):
        current_keyset.resume_pending(settings, conn, MustNotFetch())
    assert calls == []
    assert current_store.get_state(conn, "keyset_pending") == pending


@pytest.mark.parametrize("bad_id", ["/tmp/outside", "../escape", "not-a-uuid", None])
def test_keyset_pending_rejects_unsafe_raw_identity_before_network(setup, bad_id):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("test stop")

    from datetime import UTC, datetime

    with pytest.raises(CaptureStopped):
        current_keyset.cold_page(
            settings,
            conn,
            Interrupted(),
            "events",
            reconcile_state={"windows": {"events": {"started_at": 1.0}}},
            now=datetime.now(UTC),
        )
    pending = current_store.get_state(conn, "keyset_pending")
    pending["raw_id"] = bad_id
    current_store.set_state(conn, "keyset_pending", pending)
    calls = []

    class MustNotFetch:
        def get(self, *args):
            calls.append(args)
            raise AssertionError("unsafe raw identity reached network")

    with pytest.raises(CurrentError, match="pending keyset"):
        current_keyset.resume_pending(settings, conn, MustNotFetch())
    assert calls == []


def test_keyset_pending_rejects_symlink_raw_directory_before_network(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))

    class Interrupted:
        def get(self, *_args):
            raise CaptureStopped("test stop")

    from datetime import UTC, datetime

    with pytest.raises(CaptureStopped):
        current_keyset.cold_page(
            settings,
            conn,
            Interrupted(),
            "events",
            reconcile_state={"windows": {"events": {"started_at": 1.0}}},
            now=datetime.now(UTC),
        )
    pending = current_store.get_state(conn, "keyset_pending")
    outside = settings.root.parent / "outside-raw"
    outside.mkdir()
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    (settings.raw_dir / pending["raw_id"]).symlink_to(outside, target_is_directory=True)
    calls = []

    class MustNotFetch:
        def get(self, *args):
            calls.append(args)
            raise AssertionError("symlink raw identity reached network")

    with pytest.raises(CurrentError, match="pending keyset"):
        current_keyset.resume_pending(settings, conn, MustNotFetch())
    assert calls == []
    assert list(outside.iterdir()) == []


def test_keyset_parser_handles_empty_cursor_and_validates_above_highwater(setup):
    settings, _conn = setup
    del settings
    empty, cursor, last, crossed = current_keyset._parse(
        SimpleNamespace(status=200, json={"events": [], "next_cursor": "next"}),
        "events",
        after=None,
        previous_id=4,
        highwater=10,
        closed=None,
    )
    assert (empty, cursor, last, crossed) == ([], "next", 4, False)
    rows, cursor, last, crossed = current_keyset._parse(
        SimpleNamespace(status=200, json={"events": [event(9), event(11)]}),
        "events",
        after="next",
        previous_id=8,
        highwater=10,
        closed=None,
    )
    assert [row["id"] for row in rows] == ["9"]
    assert cursor is None and last == 9 and crossed is True


def test_optional_keyset_503_disables_capability_but_budget_stop_propagates(setup, monkeypatch):
    settings, conn = setup
    base = {"lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES}}
    monkeypatch.setattr(current_sync, "probe_policy", lambda *_args, **_kwargs: base)

    class FailedOptional:
        def get(self, *_args):
            raise RetriesExhausted("/events/keyset: HTTP 503")

    certificate = current_sync.probe_api_optimizations(
        settings,
        conn,
        FailedOptional(),
        event_ids={"open": ["1"], "closed": ["2"], "absent": []},
        market_ids={"open": ["1"], "closed": ["2"], "absent": []},
        market_closed_any=False,
        keyset_discovery=True,
    )
    assert certificate["capabilities"]["keyset_discovery"]["supported"] is False

    class StoppedOptional:
        def get(self, *_args):
            raise RequestBudgetExceeded("test deadline")

    with pytest.raises(RequestBudgetExceeded):
        current_sync.probe_api_optimizations(
            settings,
            conn,
            StoppedOptional(),
            event_ids={"open": ["1"], "closed": ["2"], "absent": []},
            market_ids={"open": ["1"], "closed": ["2"], "absent": []},
            market_closed_any=False,
            keyset_discovery=True,
        )


def test_keyset_probe_cannot_certify_empty_page_over_known_control(setup):
    settings, _conn = setup
    del settings

    class EmptyKeyset:
        def get(self, *_args):
            return SimpleNamespace(status=200, json={"events": []})

    with pytest.raises(CurrentError, match="keyset control query"):
        current_sync._probe_keyset_pass(EmptyKeyset(), "events", None, ["1"])


def test_keyset_probe_checks_known_controls_beyond_unfiltered_sample(setup):
    settings, _conn = setup
    del settings
    from datetime import UTC, datetime

    class Response:
        status = 200
        received_at = datetime.now(UTC)

        def __init__(self, value):
            self.json = value

    calls = []

    class RecentControls:
        def get(self, path, params):
            calls.append((path, params))
            if path == "/events":
                return Response(event(int(path.rsplit("/", 1)[-1])))
            if path == "/events/keyset":
                if "id" in params:
                    if "after_cursor" in params:
                        return Response({"events": []})
                    return Response({"events": [event(1000)], "next_cursor": "filtered-terminal"})
                cursor = params.get("after_cursor")
                if cursor is None:
                    return Response(
                        {"events": [event(i) for i in range(1, 11)], "next_cursor": "p2"}
                    )
                return Response({"events": [event(i) for i in range(11, 21)]})
            if path.startswith("/events/"):
                return Response(event(int(path.rsplit("/", 1)[-1])))
            raise AssertionError((path, params))

    current_sync._probe_keyset_pass(RecentControls(), "events", None, ["1000"])
    assert any(path == "/events/keyset" and params.get("id") == ["1000"] for path, params in calls)
    assert any(params.get("after_cursor") == "filtered-terminal" for _, params in calls)


def test_keyset_bootstrap_completes_all_lifecycle_passes_before_advancing_frontier(setup):
    settings, conn = setup
    current_store.set_state(conn, "bulk_policy", _keyset_certificate(settings))
    import json
    from datetime import UTC, datetime

    from fakes.world import make_market

    calls = []
    event_rows = [event(1), event(2)]
    open_markets = [make_market("1", "one"), make_market("3", "three")]
    closed_markets = [
        make_market("2", "two", closed=True),
        make_market("4", "four", closed=True),
    ]
    event_highwater_calls = 0
    interrupt_page_once = True

    class Response:
        status = 200
        received_at = datetime.now(UTC)

        def __init__(self, body):
            self.body = json.dumps(body).encode()
            self.json = body

    class BootstrapClient:
        def get(self, path, params):
            nonlocal event_highwater_calls, interrupt_page_once
            calls.append((path, dict(params)))
            if path == "/events":
                if params.get("ascending") is True:
                    return Response([event_rows[-1]])
                if path == "/events" and params.get("order") == "id":
                    event_highwater_calls += 1
                    if event_highwater_calls > 1:
                        raise RequestBudgetExceeded("stop after bootstrap")
                    return Response([event_rows[-1]])
            if path == "/events/keyset":
                if interrupt_page_once:
                    interrupt_page_once = False
                    raise CaptureStopped("interrupt bootstrap page")
                return Response({"events": event_rows})
            if path == "/markets":
                closed = params.get("closed") is True
                return Response([closed_markets[-1] if closed else open_markets[-1]])
            if path == "/markets/keyset":
                rows = closed_markets if params["closed"] else open_markets
                return Response({"markets": rows})
            raise AssertionError((path, params))

    current_sync.discover(
        settings,
        conn,
        BootstrapClient(),
        deadline=time.monotonic() + 10,
    )
    assert current_store.get_state(conn, "keyset_pending") is not None
    result = current_sync.discover(
        settings,
        conn,
        BootstrapClient(),
        deadline=time.monotonic() + 10,
    )
    assert current_store.get_state(conn, "keyset_pending") is None
    assert current_store.get_state(conn, "keyset_bootstrap_complete") is True
    assert result["frontiers"]["events"]["frontier"] == 2
    assert result["frontiers"]["markets"]["frontier"] == 4
    assert current_store.get_state(conn, "keyset_discovery")["lanes"]["markets"]["passes"]["open"][
        "done"
    ]
    assert current_store.get_state(conn, "keyset_discovery")["lanes"]["markets"]["passes"][
        "closed"
    ]["done"]
    assert conn.execute("SELECT count(*) FROM _reconcile_attempts").fetchone()[0] == 0


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
