import hashlib
import json
from urllib.error import HTTPError
from urllib.request import urlopen

import duckdb
import pytest

from fakes.capacity_source import CapacitySource
from fakes.world import make_event, make_market
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_store import apply, initialize


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
    events = [
        make_event("1", "Open", markets=[market, null]),
        make_event("2", "Closed", closed=True, markets=[make_market("11", "Closed", closed=True)]),
        make_event("3", "Empty"),
        make_event("4", "Shared", markets=[market]),
    ]
    apply(
        conn,
        project_response(events, "events", received_at="2026-01-01T00:00:00Z", response_id="seed"),
    )
    conn.execute(
        "INSERT INTO markets (venue,market_id,source_market_version,protocol,usable,identity_error) VALUES ('polymarket','13','v1','ctf',false,'CTF market lacks clobTokenIds'),('polymarket','14','v1','ctf',false,'outcomes must contain nonempty labels')"
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
        assert get(source, "/events?limit=1&order=id&ascending=false")[1][0]["id"] == "4"
        assert get(source, "/markets/13")[1]["outcomes"] == ["Yes", "No"]
        assert "clobTokenIds" not in get(source, "/markets/13")[1]
        assert get(source, "/markets/14")[1]["outcomes"] == []
        assert get(source, "/events/99")[0] == 404
        assert get(source, "/events?id=99&limit=1")[1] == []
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
        for kind, expected in (("markets", 3), ("events", 5)):
            for ids in (["1"], list(map(str, range(1, 101)))):
                before = source.stats.get("sql_queries", 0)
                source.records(kind, ids)
                assert source.stats["sql_queries"] - before == expected


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
