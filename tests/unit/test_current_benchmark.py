"""Small independent tests; never copy an operator seed or access live Gamma."""

import hashlib
import importlib.util
from pathlib import Path

import httpx
import pytest

from fakes.current_gamma import CurrentGammaServer

SPEC = importlib.util.spec_from_file_location(
    "benchmark_current", Path(__file__).parents[2] / "scripts/benchmark-current.py"
)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_required_arguments_and_defaults():
    with pytest.raises(SystemExit):
        benchmark.parser().parse_args([])
    args = benchmark.parser().parse_args(["--seed-root", "seed", "--output", "output"])
    assert (args.runs, args.delta_markets, args.delay_s) == (3, 1000, 0.1)
    assert args.max_retained_bytes == 256 * 1024**3


def test_independent_exact_assets_nulls_and_frozen_source():
    market = dict(
        id="11",
        question=None,
        slug="m",
        conditionId="0x" + "1" * 64,
        version="v2",
        active=None,
        closed=None,
        archived=False,
        outcomes=["Yes", "No"],
        clobTokenIds=None,
        positionIds=[str(10**75), str(10**75 + 1)],
        events=[{"id": "1"}],
    )
    events = {
        "1": dict(
            id="1", title=None, slug="e", active=None, closed=None, archived=False, markets=[market]
        )
    }
    markets = {"11": market}
    expected = benchmark.expected_delta(events, markets, ["1"], ["11"])
    assert expected["outcomes"][0]["position_id"] == str(10**75)
    assert expected["outcomes"][0]["clob_token_id"] is None
    assert expected["markets"][0]["closed"] is None
    source = CurrentGammaServer(events, markets)
    with httpx.Client(
        transport=httpx.MockTransport(source.handle), base_url="http://fixture"
    ) as client:
        assert client.get("/markets/11").json() == client.get("/markets?id=11").json()[0]
        market["question"] = "mutated"
        assert client.get("/markets/11").json()["question"] is None


def test_failed_envelope_does_not_raise_limits(tmp_path, monkeypatch):
    monkeypatch.setattr(benchmark, "directory_bytes", lambda _: 100)
    with pytest.raises(ValueError, match="envelope"):
        benchmark.capacity(tmp_path / "seed", tmp_path / "output", 199)


@pytest.mark.parametrize("count", [12, 1000])
def test_delta_preserves_ten_old_memberships_and_uses_seed_frontiers(tmp_path, count):
    import duckdb

    from fakes.world import make_event, make_market
    from oddsfox_catalogue.current_rows import project_response
    from oddsfox_catalogue.current_store import apply, initialize

    with duckdb.connect(":memory:") as conn:
        initialize(conn)
        originals = [
            make_event(str(i), f"event {i}", markets=[make_market(str(i), "old")])
            for i in range(1, 11)
        ]
        apply(
            conn,
            project_response(
                originals, "events", received_at="2026-10-14T00:00:00Z", response_id="seed"
            ),
        )
        reservations = [str(2**256 - 1)] + [
            str(
                int.from_bytes(
                    hashlib.sha256(f"benchmark-current-v1:{mid}:0:0".encode()).digest(), "big"
                )
            )
            for mid in ("11", "12")
        ]
        for index, asset in enumerate(reservations):
            conn.execute(
                "INSERT INTO _identity_claims VALUES (?, ?, ?, ?, ?)",
                [
                    "polymarket",
                    "suppressed",
                    "candidate" if index == 1 else "accepted",
                    "poly_v2_position",
                    asset,
                ],
            )
        source = benchmark.build_source(conn, {"events": 10, "markets": 10}, count)
        assert source == benchmark.build_source(conn, {"events": 10, "markets": 10}, count)
        events, markets, controls, new_events, new_markets = source
        assets = [
            asset
            for mid in new_markets
            for asset in (markets[mid]["positionIds"] or markets[mid]["clobTokenIds"])
        ]
        assert len(assets) == len(set(assets))
        assert all(0 < int(asset) < 2**256 for asset in assets)
        assert set(assets).isdisjoint(reservations)
        assert min(map(int, assets)) < 2**254
        assert max(map(int, assets)) > 3 * 2**254
        assert [len(markets[mid]["outcomes"]) for mid in new_markets[:3]] == [8, 16, 32]
        assert markets["11"]["positionIds"] is not None
        assert assets[0] == str(
            int.from_bytes(hashlib.sha256(b"benchmark-current-v1:11:0:1").digest(), "big")
        )
        if count >= 1000:
            assert len(events[new_events[0]]["markets"]) > 100
        assert len(new_events) == benchmark.math.ceil(count / 4)
        assert new_markets == [str(i) for i in range(11, 11 + count)]
        assert all(len(events[str(i)]["markets"]) == 2 for i in range(1, 11))
        assert all(any(m["id"] == str(i) for m in events[str(i)]["markets"]) for i in range(1, 11))
        assert max(map(int, events)) == 10 + len(new_events)
        assert max(map(int, markets)) == 10 + count
        assert controls["absent-event"] not in events
        fixture = CurrentGammaServer(events, markets)
        assert fixture.stats["http_attempts"] == 0
        for table in benchmark.SCHEMAS:
            conn.execute(f"COPY {table} TO '{tmp_path / (table + '.parquet')}' (FORMAT PARQUET)")
        delta = benchmark.expected_delta(events, markets, new_events, new_markets)
        projected = project_response(
            list(events.values()), "events", received_at="2026-10-14T00:00:00Z", response_id="delta"
        )
        for table in ("markets", "outcomes"):
            actual = [row for row in getattr(projected, table) if row["market_id"] in new_markets]
            assert sorted(actual, key=str) == sorted(delta[table], key=str)
        assert all(
            sorted(projected.memberships[eid], key=int)
            == sorted([m["id"] for m in events[eid]["markets"]], key=int)
            for eid in events
        )
        for table, rows in delta.items():
            names = benchmark.columns(table)
            if rows:
                conn.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' for _ in names)})",
                    [[r[n] for n in names] for r in rows],
                )
        assert benchmark.verify_expected(conn, tmp_path, delta)["event_markets"] == 10 + count
        conn.execute("DROP TABLE expected_events")
        conn.execute("UPDATE events SET title='wrong' WHERE event_id='11'")
        with pytest.raises(ValueError, match="exact seed-plus-delta mismatch"):
            benchmark.verify_expected(conn, tmp_path, delta)


@pytest.mark.parametrize(
    ("retained_event", "retained_market", "frontier"),
    [(20, 30, 10), (12, 12, 25), (2**63 + 1, 2**64 + 1, 10)],
)
def test_source_allocates_after_retained_nested_entities_without_advancing_frontier(
    retained_event, retained_market, frontier
):
    import duckdb

    from fakes.world import make_event, make_market
    from oddsfox_catalogue.current_rows import project_response
    from oddsfox_catalogue.current_store import apply, initialize

    with duckdb.connect(":memory:") as conn:
        initialize(conn)
        originals = [
            make_event(str(i), "old event", markets=[make_market(str(i), "old market")])
            for i in range(1, 10)
        ] + [
            make_event(
                str(retained_event), "nested", markets=[make_market(str(retained_market), "nested")]
            )
        ]
        apply(
            conn,
            project_response(
                originals, "events", received_at="2026-10-14T00:00:00Z", response_id="seed"
            ),
        )
        frontiers = {"events": frontier, "markets": frontier}
        events, markets, controls, new_events, new_markets = benchmark.build_source(
            conn, frontiers, 12
        )
        assert frontiers == {"events": frontier, "markets": frontier}
        assert benchmark.retained_maxima(conn) == {
            "events": retained_event,
            "markets": retained_market,
        }
        assert int(new_events[0]) == max(retained_event, frontier) + 1
        assert int(new_markets[0]) == max(retained_market, frontier) + 1
        assert events[str(retained_event)]["title"] == "nested"
        assert markets[str(retained_market)]["question"] == "nested"
        assert str(retained_market) not in new_markets
        assert str(retained_event) not in new_events
        for lane, source in (("event", events), ("market", markets)):
            assert int(controls[f"absent-{lane}"]) > max(map(int, source))
            assert controls[f"open-{lane}"] in source
            assert controls[f"closed-{lane}"] in source


@pytest.mark.parametrize("frontier", [-1, True, "10", 10**20, 10**20 - 1])
def test_source_rejects_invalid_or_exhausted_frontiers(frontier):
    import duckdb

    from oddsfox_catalogue.current_store import initialize

    with duckdb.connect(":memory:") as conn:
        initialize(conn)
        with pytest.raises(ValueError, match="canonical"):
            benchmark.build_source(conn, {"events": frontier, "markets": frontier}, 12)


def test_cli_pins_canonical_limits_and_preserves_failure_output(tmp_path, monkeypatch):
    from types import SimpleNamespace

    captured = {}

    def invoke(command, **kwargs):
        captured.update(command=command, **kwargs)
        return SimpleNamespace(returncode=1, stdout='{"status":"blocked"}', stderr="capacity")

    monkeypatch.setattr(benchmark.subprocess, "run", invoke)
    monkeypatch.setenv("CATALOGUE_CATALOGUE_MAX_DURATION_S", "999")
    result = benchmark.cli(tmp_path, "http://127.0.0.1:123", None, "refresh")
    assert result["returncode"] == 1
    assert result["stderr"] == "capacity"
    assert captured["env"]["CATALOGUE_CATALOGUE_MAX_DURATION_S"] == "300.0"
    assert captured["env"]["CATALOGUE_CATALOGUE_FREE_RESERVE_BYTES"] == str(32 * 1024**3)
    assert captured["env"]["CATALOGUE_GAMMA_REQUESTS_PER_SECOND"] == "10.0"
    with pytest.raises(ValueError, match="capacity"):
        benchmark.successful(result)


def test_accounting_deduplicates_ancestor_overlap_and_counts_legacy(tmp_path, monkeypatch):
    walks = []
    sizes = {tmp_path / "legacy": 70, tmp_path / "seed": 20, tmp_path / "output": 10}

    def size(path):
        walks.append(path)
        return sizes[path]

    monkeypatch.setattr(benchmark, "directory_bytes", size)
    monkeypatch.setattr(
        benchmark.shutil, "disk_usage", lambda _: type("Usage", (), {"free": 128 * 1024**3})()
    )
    legacy = tmp_path / "legacy"
    roots = [legacy / "raw", legacy, legacy / "raw", tmp_path / "seed" / "published"]
    result = benchmark.capacity(tmp_path / "seed", tmp_path / "output", 100, account_roots=roots)
    assert result["combined_bytes"] == 100
    assert set(walks) == set(sizes)
    assert len(walks) == 3
    with pytest.raises(ValueError, match="envelope"):
        benchmark.capacity(tmp_path / "seed", tmp_path / "output", 99, account_roots=roots)


def test_repeatable_account_root_argument():
    args = benchmark.parser().parse_args(
        [
            "--seed-root",
            "seed",
            "--output",
            "output",
            "--account-root",
            "legacy",
            "--account-root",
            "backups",
        ]
    )
    assert args.account_root == [Path("legacy"), Path("backups")]


def test_realistic_market_default_omission_and_two_pass_native_parity():
    source = CurrentGammaServer(market_default_open=True)
    with httpx.Client(
        transport=httpx.MockTransport(source.handle), base_url="http://fixture"
    ) as client:
        assert client.get("/markets/21").json()["closed"] is True
        assert client.get("/markets?id=21").json() == []
        merged = []
        for closed in ("false", "true"):
            response = client.get(f"/markets?id=11&id=21&closed={closed}")
            assert response.status_code == 200
            merged.extend(response.json())
        assert {row["id"]: row for row in merged} == {
            key: client.get(f"/markets/{key}").json() for key in ("11", "21")
        }
        assert client.get("/events?id=2").json() == [client.get("/events/2").json()]
        assert client.get("/markets?limit=1&order=id&ascending=false").json()[0]["id"] == "12"
        assert (
            client.get("/markets?limit=1&order=id&ascending=false&closed=true").json()[0]["id"]
            == "21"
        )
        assert client.get("/markets?id=21&closed=True").status_code == 400


def test_unchanged_highwater_requests_follow_certificate_passes():
    certificate = {"lanes": {"events": {"passes": [None]}, "markets": {"passes": [False, True]}}}
    heads = [
        {"path": f"/{lane}?limit=1&order=id&ascending=false{suffix}", "status": 200}
        for lane, suffix in (
            ("events", ""),
            ("markets", "&closed=false"),
            ("markets", "&closed=true"),
        )
    ]
    benchmark.verify_highwater_only(heads, certificate)
    with pytest.raises(ValueError, match="passes differ"):
        benchmark.verify_highwater_only(heads[:2], certificate)
    with pytest.raises(ValueError, match="certified highwater"):
        benchmark.verify_highwater_only(
            [*heads, {"path": "/markets?id=11", "status": 200}], certificate
        )


def test_gap_discovery_counts_actual_passes_retries_and_includes_whole_refresh_time():
    responses = [
        {"path": path}
        for path in (
            "/markets?id=11&id=12&closed=false",
            "/markets?id=11&id=12&closed=true",
            "/markets?id=11&id=12&closed=true",
            "/markets?id=31&closed=false",
            "/events?id=11&id=21",
            "/events?limit=1&order=id&ascending=false",
        )
    ]
    report = benchmark.gap_discovery(
        responses, {"events": 10, "markets": 10}, {"events": 20, "markets": 30}, 2.5
    )
    assert report["window_http_attempts"] == {"events": 1, "markets": 3}
    assert report["included_in_refresh_elapsed_wall_s"] == 2.5
    assert "not isolated" in report["timing_scope"]


@pytest.mark.parametrize("temporary_full", [False, True])
def test_preflight_uses_bounded_readonly_connection_before_validation(
    tmp_path, monkeypatch, temporary_full
):
    import duckdb

    from oddsfox_catalogue import current_config

    seed = current_config.validate_root(tmp_path / "seed", initialize=True)
    settings = current_config.CurrentSettings(seed)
    with duckdb.connect(str(settings.store_path)) as conn:
        conn.execute("CREATE TABLE evidence(value INTEGER)")
        conn.execute("INSERT INTO evidence VALUES (42)")
    before = benchmark.digest(settings.store_path)
    visited = []

    def validate(conn):
        visited.append(True)
        assert conn.execute("SELECT * FROM evidence").fetchall() == [(42,)]
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0] == 2
        assert conn.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "1.8 GiB"
        assert conn.execute("SELECT current_setting('temp_directory')").fetchone()[0] == str(
            seed / "tmp"
        )
        assert (
            conn.execute("SELECT current_setting('max_temp_directory_size')").fetchone()[0]
            == "8.0 GiB"
        )
        with pytest.raises(duckdb.InvalidInputException, match="read-only"):
            conn.execute("INSERT INTO evidence VALUES (43)")
        raise ValueError("preflight resource probe complete")

    monkeypatch.setattr(benchmark, "validate_store", validate)
    if temporary_full:
        original_size = current_config.directory_bytes
        monkeypatch.setattr(
            current_config,
            "directory_bytes",
            lambda path: (
                settings.limits.max_temp_bytes + 1 if path == seed / "tmp" else original_size(path)
            ),
        )
    output = tmp_path / "output"
    args = benchmark.parser().parse_args(["--seed-root", str(seed), "--output", str(output)])
    assert benchmark.run(args) == 1
    report = benchmark.json.loads((output / "report.json").read_text())
    if temporary_full:
        assert "temporary storage allowance exhausted" in report["error"]
        assert visited == []
    else:
        assert report["error"] == "ValueError: preflight resource probe complete"
        assert visited == [True]
        assert (seed / "tmp").is_dir()
    assert benchmark.digest(settings.store_path) == before
    assert not (seed / "catalogue.duckdb.wal").exists()
