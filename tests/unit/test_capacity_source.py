import hashlib
import json
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

import duckdb
import pytest

from fakes.capacity_source import MAX_AUDIT_BYTES, CapacitySource
from fakes.world import make_event, make_market
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_store import apply, initialize


def test_keyset_accepts_bounded_filtered_control_ids_and_rejects_invalid_filters():
    source = CapacitySource.__new__(CapacitySource)
    params = [
        ("limit", "2"),
        ("order", "id"),
        ("ascending", "true"),
        ("id", "2"),
        ("id", "4"),
        ("after_cursor", "Mg"),
    ]
    request = "/events/keyset?" + urlencode(params)
    kind, ids, closed, native, keyset, cursor_args = source._request(request)
    assert (kind, ids, closed, native, keyset) == ("events", ["2", "4"], None, False, True)
    assert cursor_args == ("Mg", 2, ["2", "4"])

    invalid_filters = [
        [*params, ("id", "4")],
        [("limit", "1"), *params[1:]],
        [
            ("limit", "101"),
            ("order", "id"),
            ("ascending", "true"),
            *[("id", str(value)) for value in range(1, 102)],
        ],
        [("limit", "1"), ("order", "id"), ("ascending", "true"), ("id", "01")],
    ]
    for invalid in invalid_filters:
        with pytest.raises(ValueError):
            source._request("/events/keyset?" + urlencode(invalid))


@pytest.fixture
def seed(tmp_path):
    root = tmp_path / "seed"
    root.mkdir()
    conn = duckdb.connect(str(root / "catalogue.duckdb"))
    initialize(conn)
    market = make_market("10", "Open", version="v1")
    market["positionIds"] = [str(10**75 + 1), str(10**75 + 2)]
    null = make_market("12", "Unknown")
    null["closed"] = None
    unresolved = make_market(
        "13", "Home or away", version="v1", outcomes=("Home Team", "Away Team")
    )
    unresolved.pop("clobTokenIds")
    unresolved.pop("positionIds")
    empty = make_market("14", "Unresolved outcome", version="v1")
    empty.update(outcomes="[]", outcomePrices="[]", lastTradePrice=None, bestBid=None, bestAsk=None)
    empty.pop("clobTokenIds")
    empty.pop("positionIds")
    events = [
        make_event("1", "Open", markets=[market, null]),
        make_event("2", "Closed", closed=True, markets=[make_market("11", "Closed", closed=True)]),
        make_event("3", "Empty"),
        make_event("4", "Shared", markets=[market]),
        make_event("5", "Unusable", markets=[unresolved, empty]),
    ]
    apply(
        conn,
        project_response(events, "events", received_at="2026-01-01T00:00:00Z", response_id="seed"),
    )
    for raw_market, response_id, received_at in (
        (unresolved, "0000000000000000000000000000000d", "2026-01-01T00:00:13Z"),
        (empty, "0000000000000000000000000000000e", "2026-01-01T00:00:14Z"),
    ):
        body = json.dumps(raw_market, separators=(",", ":")).encode()
        write_page(
            root / "raw" / response_id,
            1,
            body,
            {
                "request": {"endpoint": f"/markets/{raw_market['id']}", "params": {}},
                "received_at": received_at,
                "status": 200,
            },
        )
        apply(
            conn,
            project_response(
                [raw_market], "markets", received_at=received_at, response_id=response_id
            ),
        )
    conn.close()
    return root


def get(source, path):
    try:
        with urlopen(source.url + path, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def test_frozen_full_shapes_and_controls(seed, tmp_path):
    before = hashlib.sha256((seed / "catalogue.duckdb").read_bytes()).hexdigest()
    with CapacitySource(seed, tmp_path / "audit", delay_s=0) as source:
        assert source.controls["closed_market_id"] == "11"
        status, event = get(source, "/events/1")
        assert status == 200
        assert {m["id"] for m in event["markets"]} == {"10", "12"}
        direct = get(source, "/markets/10")[1]
        assert direct == next(m for m in event["markets"] if m["id"] == "10")
        assert {p["id"] for p in direct["events"]} == {"1", "4"}
        assert direct["positionIds"] == [str(10**75 + 1), str(10**75 + 2)]
        assert get(source, "/events/3")[1]["markets"] == []
        assert get(source, "/markets?id=12&limit=1&closed=false")[1][0]["closed"] is None
        assert get(source, "/markets?id=10&id=11&limit=2&closed=true")[1][0]["id"] == "11"
        assert get(source, "/events?limit=1&order=id&ascending=false")[1][0]["id"] == "5"
        market_13 = get(source, "/markets/13")[1]
        assert json.loads(market_13["outcomes"]) == ["Home Team", "Away Team"]
        assert "clobTokenIds" not in market_13
        assert "positionIds" not in market_13
        assert json.loads(get(source, "/markets/14")[1]["outcomes"]) == []
        assert get(source, "/events/99")[0] == 404
        assert get(source, "/events?id=99&limit=1")[1] == []
        status, mixed = get(
            source, "/markets?id=10&id=11&id=99&limit=3&closed=any&include_tag=true"
        )
        assert status == 200
        assert [market["id"] for market in mixed] == ["10", "11"]
        assert get(source, "/markets?limit=1&closed=any")[0] == 400
        status, page = get(source, "/events/keyset?limit=2&order=id&ascending=true")
        assert status == 200
        assert [item["id"] for item in page["events"]] == ["1", "2"]
        assert page["next_cursor"]
        status, filtered = get(
            source,
            "/events/keyset?limit=2&order=id&ascending=true&id=2&id=4",
        )
        assert status == 200
        assert [item["id"] for item in filtered["events"]] == ["2", "4"]
        assert filtered["next_cursor"]
        filtered_cursor = filtered["next_cursor"]
        status, filtered_tail = get(
            source,
            "/events/keyset?limit=2&order=id&ascending=true&id=2&id=4"
            f"&after_cursor={filtered_cursor}",
        )
        assert status == 200
        assert filtered_tail["events"] == []
        assert "next_cursor" not in filtered_tail
        cursor = page["next_cursor"]
        status, next_page = get(
            source, f"/events/keyset?limit=2&order=id&ascending=true&after_cursor={cursor}"
        )
        assert status == 200
        assert [item["id"] for item in next_page["events"]] == ["3", "4"]
        status, market_page = get(
            source,
            "/markets/keyset?limit=2&order=id&ascending=true&closed=true&include_tag=true",
        )
        assert status == 200
        assert all(item["closed"] is True for item in market_page["markets"])
        assert (
            get(source, "/markets/keyset?limit=2&order=id&ascending=true&after_cursor=invalid")[0]
            == 400
        )
        for path in (
            "/other",
            "/events/01",
            "/events?id=1&limit=1&secret=x",
            "/events?" + "&".join(["id=1"] * 101) + "&limit=101",
        ):
            assert get(source, path)[0] == 400
        assert source.stats["audit_bytes"] > 0
    assert hashlib.sha256((seed / "catalogue.duckdb").read_bytes()).hexdigest() == before
    audit = [json.loads(line) for line in (tmp_path / "audit").read_text().splitlines()]
    assert set(audit[0]["complete_memberships"]["1"]) == {"10", "12"}
    assert "question" not in (tmp_path / "audit").read_text()


def test_bounds_and_audit_path(seed, tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="invalid source limits"):
        CapacitySource(seed, tmp_path / "oversized-audit", max_audit_bytes=MAX_AUDIT_BYTES + 1)
    with pytest.raises(ValueError):
        CapacitySource(seed, seed / "audit", delay_s=0)
    with CapacitySource(seed, tmp_path / "small-audit", delay_s=0, max_audit_bytes=1) as source:
        assert get(source, "/events/1")[0] == 507
        assert source.failure == "audit limit exceeded"
        assert source.stats["audit_bytes"] == 0
    monkeypatch.setattr("fakes.capacity_source.MAX_RESPONSE_BYTES", 100)
    with CapacitySource(seed, tmp_path / "response", delay_s=0) as source:
        assert get(source, "/events/1")[0] == 413
    assert (tmp_path / "response").stat().st_mode & 0o777 == 0o600


def test_100_id_lookup_cost(seed, tmp_path):
    import time

    with CapacitySource(seed, tmp_path / "timing", delay_s=0) as source:
        started = time.perf_counter()
        rows = source.records("markets", list(map(str, range(1, 101))))
        elapsed = time.perf_counter() - started
        assert {r["id"] for r in rows} == {"10", "11", "12", "13", "14"}
        print(f"100-ID tiny-seed lookup: {elapsed:.6f}s")


def test_query_count_is_constant_per_batch(seed, tmp_path):
    with CapacitySource(seed, tmp_path / "queries", delay_s=0) as source:
        batches = (["1"], list(map(str, range(1, 101))))
        for kind, expected in (("markets", (3, 4)), ("events", (5, 6))):
            for ids, query_count in zip(batches, expected, strict=True):
                before = source.stats.get("sql_queries", 0)
                source.records(kind, ids)
                assert source.stats["sql_queries"] - before == query_count


def test_supplied_symlink_and_changed_seed_rejected(seed, tmp_path):
    alias = tmp_path / "alias"
    alias.symlink_to(seed, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        CapacitySource(alias, tmp_path / "symlink-audit", delay_s=0)
    with CapacitySource(seed, tmp_path / "identity", delay_s=0) as source:
        import os

        db = seed / "catalogue.duckdb"
        original = db.stat()
        os.utime(db, ns=(original.st_atime_ns, original.st_mtime_ns + 1_000_000))
        with pytest.raises(RuntimeError, match="identity changed"):
            source.records("markets", ["10"])
        os.utime(db, ns=(original.st_atime_ns, original.st_mtime_ns))


def test_market_list_default_and_probe_policy(seed, tmp_path):
    from oddsfox_catalogue.config import GammaSettings
    from oddsfox_catalogue.current_config import CurrentSettings, validate_root
    from oddsfox_catalogue.current_sync import probe_policy
    from oddsfox_catalogue.gamma.http import GammaClient

    with CapacitySource(seed, tmp_path / "two-pass", delay_s=0) as source:
        query = "/markets?id=10&id=11&id=12&limit=3"
        assert {r["id"] for r in get(source, query)[1]} == {"10", "12"}
        assert get(source, "/markets/11")[1]["closed"] is True
        opened = get(source, query + "&closed=false")[1]
        closed = get(source, query + "&closed=true")[1]
        assert {r["id"] for r in opened + closed} == {"10", "11", "12"}
        assert get(source, "/markets?limit=1&order=id&ascending=false")[1][0]["id"] == "14"
        settings = CurrentSettings(
            tmp_path / "probe", gamma=GammaSettings(base_url=source.url, allow_loopback=True)
        )
        validate_root(settings.root, initialize=True)

        def controls(kind):
            return {
                status: [source.controls[f"{status}_{kind}_id"]]
                for status in ("open", "closed", "absent")
            }

        with GammaClient(settings.gamma) as client:
            policy = probe_policy(
                settings, client, event_ids=controls("event"), market_ids=controls("market")
            )
        assert policy["lanes"]["markets"]["supported"] is True
        assert policy["lanes"]["markets"]["passes"] == [False, True]
        assert policy["lanes"]["events"]["passes"] == [None]
