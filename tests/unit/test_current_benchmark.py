"""Small independent tests; never copy an operator seed or access live Gamma."""

import hashlib
import importlib.util
import math
from pathlib import Path

import httpx
import pytest

from fakes.current_gamma import CurrentGammaServer
from oddsfox_catalogue.current_publish import publish as publish_release

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
        benchmark,
        "aggregate_usage",
        lambda path: {
            "logical_owned_bytes": sizes[path],
            "symlink_count": 0,
            "policy": "fixture aggregate",
        },
    )
    monkeypatch.setattr(
        benchmark.shutil, "disk_usage", lambda _: type("Usage", (), {"free": 128 * 1024**3})()
    )
    legacy = tmp_path / "legacy"
    roots = [legacy / "raw", legacy, legacy / "raw", tmp_path / "seed" / "published"]
    result = benchmark.capacity(tmp_path / "seed", tmp_path / "output", 100, account_roots=roots)
    assert result["combined_bytes"] == 100
    assert set(walks) == {tmp_path / "seed", tmp_path / "output"}
    assert len(walks) == 2
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

    def verify(given_settings, *, connection):
        assert given_settings == settings
        conn = connection
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

    monkeypatch.setattr(benchmark, "verify_current", verify)
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


@pytest.mark.parametrize("seed_state", ["valid", "publication-behind", "private-corrupt"])
def test_real_seed_preflight_validates_private_store_once_before_build_or_copy(
    tmp_path, monkeypatch, seed_state
):
    from fakes.world import make_event, make_market
    from oddsfox_catalogue import current_publish, current_store
    from oddsfox_catalogue.current_rows import project_response

    root = benchmark.validate_root(tmp_path / "seed", initialize=True)
    settings = benchmark.CurrentSettings(root)
    with benchmark.connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.apply(
            conn,
            project_response(
                [make_event("1", "retained", markets=[make_market("2", "native")])],
                "events",
                received_at="2026-10-14T00:00:00Z",
                response_id="seed",
                native_market_ids=["2"],
            ),
        )
        current_store.set_state(
            conn,
            "discovery",
            {
                "version": 2,
                "next_lane": 0,
                "pending": None,
                "lanes": {
                    lane: {"frontier": 2, "highwater": None, "phase": "tail"}
                    for lane in ("events", "markets")
                },
            },
        )
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 2
        publish_release(settings, conn)
        if seed_state == "publication-behind":
            current_store.set_state(conn, "generation", 2)
        elif seed_state == "private-corrupt":
            conn.execute("DELETE FROM _identity_claims WHERE role='accepted'")
    source_hash = benchmark.digest(settings.store_path)
    pointer = (settings.published_dir / "current.json").read_bytes()
    original_validate = current_store._validate_store
    validated, built = [], []

    def validate(connection, **kwargs):
        validated.append(connection)
        return original_validate(connection, **kwargs)

    def build(connection, frontiers, count):
        assert len(validated) == 1
        assert connection is validated[0]
        built.append(True)
        raise ValueError("stopped after verified preflight")

    def forbidden_copy(*args, **kwargs):
        pytest.fail("preflight reached seed copying")

    monkeypatch.setattr(current_store, "_validate_store", validate)
    monkeypatch.setattr(current_publish, "_validate_store", validate)
    monkeypatch.setattr(benchmark, "build_source", build)
    monkeypatch.setattr(benchmark.shutil, "copytree", forbidden_copy)
    monkeypatch.setattr(benchmark.shutil, "copy2", forbidden_copy)
    output = tmp_path / "output"
    args = benchmark.parser().parse_args(["--seed-root", str(root), "--output", str(output)])
    assert benchmark.run(args) == 1
    report = benchmark.json.loads((output / "report.json").read_text())
    assert len(validated) == (0 if seed_state == "publication-behind" else 1)
    assert built == ([True] if seed_state == "valid" else [])
    expected = {
        "valid": "stopped after verified preflight",
        "publication-behind": "seed publication is behind its store",
        "private-corrupt": "current store integrity violation",
    }[seed_state]
    assert expected in report["error"]
    assert benchmark.digest(settings.store_path) == source_hash
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    assert not list(output.glob("run-*"))


@pytest.mark.parametrize(
    ("version", "unexpected_fault"), [("v1", False), ("v2", False), ("v2", True)]
)
def test_representative_resumed_repair_real_http_and_exact_oracle(
    tmp_path, monkeypatch, version, unexpected_fault
):
    from fakes.world import make_event, make_market
    from oddsfox_catalogue import current_store
    from oddsfox_catalogue.current_rows import project_response

    root = benchmark.validate_root(tmp_path / "seed", initialize=True)
    settings = benchmark.CurrentSettings(root)
    with benchmark.connect_store(settings) as conn:
        current_store.initialize(conn)
        originals = [
            make_event(
                str(i), f"Event {i}", markets=[make_market(str(i), "original", version=version)]
            )
            for i in range(1, 11)
        ]
        current_store.apply(
            conn,
            project_response(
                originals, "events", received_at="2000-01-01T00:00:00Z", response_id="seed"
            ),
        )
        current_store.set_state(
            conn,
            "discovery",
            {
                "version": 2,
                "next_lane": 0,
                "pending": None,
                "lanes": {
                    lane: {"frontier": 10, "highwater": None, "phase": "tail"}
                    for lane in ("events", "markets")
                },
            },
        )
        seed_release = publish_release(settings, conn)
        baseline = tmp_path / "baseline"
        baseline.mkdir()
        for table in benchmark.SCHEMAS:
            benchmark.shutil.copy2(Path(seed_release["path"]) / f"{table}.parquet", baseline)
        events, markets, controls, new_events, new_markets = benchmark.build_source(
            conn, {"events": 10, "markets": 10}, 12
        )
        fixture = benchmark.repair_fixture(conn, markets, new_markets)
        assert fixture["market_id"] == "1"
        delta = benchmark.expected_delta(events, markets, new_events, new_markets)
    with CurrentGammaServer(events, markets, market_default_open=True) as source:
        options = [part for key, value in controls.items() for part in (f"--{key}-id", value)]
        certificate = benchmark.successful(benchmark.cli(root, source.url, None, "probe", *options))
        original_fetch = benchmark.fetch_apply
        if unexpected_fault:

            def fail_native(*args, **kwargs):
                if kwargs.get("native"):
                    raise RuntimeError("unrelated preparation fault")
                return original_fetch(*args, **kwargs)

            monkeypatch.setattr(benchmark, "fetch_apply", fail_native)
        with (
            benchmark.FileLock(str(settings.run_lock_path)),
            benchmark.connect_store(settings) as conn,
        ):
            if unexpected_fault:
                with pytest.raises(RuntimeError, match="unrelated preparation fault"):
                    benchmark.prepare_identity_repair(
                        settings, conn, source, events, markets, fixture
                    )
                return
            prepared = benchmark.prepare_identity_repair(
                settings, conn, source, events, markets, fixture
            )
            assert (
                prepared["source_sha256"]
                == hashlib.sha256(
                    benchmark.json.dumps([events, markets], sort_keys=True).encode()
                ).hexdigest()
            )
            assert prepared["fetch_pending"]["identity"]["native"] is True
            assert all(
                benchmark.urlsplit(row["path"]).path != "/markets/1" for row in prepared["http"]
            )
            assert conn.execute("SELECT usable FROM markets WHERE market_id='1'").fetchone() == (
                False,
            )
            publication = benchmark.verify_current(settings, connection=conn)
            assert conn.execute(
                "SELECT count(*) FROM read_parquet(?) WHERE market_id='1'",
                [str(Path(publication["path"]) / "outcomes.parquet")],
            ).fetchone() == (0,)
        before = source.stats["http_attempts"]
        refreshed = benchmark.successful(benchmark.cli(root, source.url, None, "refresh"))
        requests = source.stats["responses"][before:]
        assert (
            benchmark.verify_repair_requests(requests, fixture["market_id"])["native_http_attempts"]
            == 1
        )
        assert refreshed["http_attempts"] == len(requests)
        with benchmark.connect_store(settings) as conn:
            assert benchmark.get_state(conn, "fetch_pending") is None
            assert (
                benchmark.records(
                    conn, "outcomes", "market_id=? ORDER BY outcome_index", [fixture["market_id"]]
                )
                == fixture["mapping"]
            )
            benchmark.verify_expected(conn, baseline, delta)
        before = source.stats["http_attempts"]
        unchanged = benchmark.successful(benchmark.cli(root, source.url, None, "refresh"))
        assert unchanged["publication"]["noop"]
        assert unchanged["publication"]["release_id"] == refreshed["publication"]["release_id"]
        benchmark.verify_highwater_only(source.stats["responses"][before:], certificate)


def test_repair_fixture_requires_existing_usable_reconstructable_market():
    import duckdb

    from oddsfox_catalogue.current_store import initialize

    with duckdb.connect(":memory:") as conn:
        initialize(conn)
        with pytest.raises(ValueError, match="existing usable reconstructable"):
            benchmark.repair_fixture(conn, {}, [])


@pytest.mark.parametrize(
    "paths",
    [
        [],
        ["/events", "/markets/1"],
        ["/markets/2"],
        ["/markets/1", "/markets/1"],
        ["/markets/1", "/events/1"],
    ],
)
def test_repair_request_assertions_reject_wrong_order_identity_and_extra_native(paths):
    with pytest.raises(ValueError, match="native repair"):
        benchmark.verify_repair_requests([{"path": path} for path in paths], "1")


def test_aggregate_usage_counts_link_text_without_opening_targets(tmp_path, monkeypatch):
    account = tmp_path / "account"
    account.mkdir()
    (account / "owned").write_bytes(b"abc")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"x" * 10000)
    links = [account / name for name in ("file", "directory", "cycle", "dangling")]
    links[0].symlink_to(outside / "secret")
    links[1].symlink_to(outside, target_is_directory=True)
    links[2].symlink_to(".", target_is_directory=True)
    links[3].symlink_to(outside / "missing")
    expected = 3 + sum(link.lstat().st_size for link in links)
    original_open = benchmark.os.open

    def checked_open(path, flags, *args, **kwargs):
        assert str(path) not in {"outside", "secret", str(outside), str(outside / "secret")}
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(benchmark.os, "open", checked_open)
    usage = benchmark.aggregate_usage(account)
    assert usage["logical_owned_bytes"] == expected
    assert usage["symlink_count"] == 4
    assert "never followed or read" in usage["policy"]


def test_aggregate_usage_rejects_special_files(tmp_path):
    benchmark.os.mkfifo(tmp_path / "fifo")
    with pytest.raises(ValueError, match="special file"):
        benchmark.aggregate_usage(tmp_path)


@pytest.mark.parametrize("entry_type", ["file", "directory"])
def test_aggregate_usage_rejects_unreadable_entries(tmp_path, entry_type):
    entry = tmp_path / "unreadable"
    entry.mkdir() if entry_type == "directory" else entry.write_bytes(b"data")
    entry.chmod(0)
    try:
        with pytest.raises((ValueError, PermissionError), match="unreadable|Permission"):
            benchmark.aggregate_usage(tmp_path)
    finally:
        entry.chmod(0o700 if entry_type == "directory" else 0o600)


def test_aggregate_usage_does_not_skip_directory_listing_failures(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    inode = blocked.stat().st_ino
    original_listdir = benchmark.os.listdir

    def fail_listing(descriptor):
        if isinstance(descriptor, int) and benchmark.os.fstat(descriptor).st_ino == inode:
            raise PermissionError("injected unreadable directory")
        return original_listdir(descriptor)

    monkeypatch.setattr(benchmark.os, "listdir", fail_listing)
    with pytest.raises(PermissionError, match="injected unreadable directory"):
        benchmark.aggregate_usage(tmp_path)


def test_aggregate_usage_rejects_directory_swapped_to_symlink_before_descent(tmp_path, monkeypatch):
    account = tmp_path / "account"
    account.mkdir()
    child = account / "child"
    child.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "unowned").write_bytes(b"must not follow")
    original_open = benchmark.os.open
    swapped = []

    def swap_then_open(path, flags, *args, **kwargs):
        if path == "child" and kwargs.get("dir_fd") is not None and not swapped:
            swapped.append(True)
            child.rename(account / "moved")
            child.symlink_to(outside, target_is_directory=True)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(benchmark.os, "open", swap_then_open)
    with pytest.raises(OSError):
        benchmark.aggregate_usage(account)
    assert swapped == [True]


@pytest.mark.parametrize("strict_name", ["seed", "output"])
def test_capacity_keeps_nested_canonical_roots_strict_under_aggregate_ancestor(
    tmp_path, strict_name
):
    from oddsfox_catalogue.gamma.http import RequestBudgetExceeded

    seed, output = tmp_path / "seed", tmp_path / "output"
    seed.mkdir()
    output.mkdir()
    (tmp_path / strict_name / "forbidden").symlink_to(tmp_path / "missing")
    with pytest.raises(RequestBudgetExceeded, match="symlink"):
        benchmark.capacity(seed, output, 1024, account_roots=[tmp_path])


def test_capacity_deduplicates_aggregate_ancestor_but_validates_strict_children(
    tmp_path, monkeypatch
):
    seed, output, legacy = (tmp_path / name for name in ("seed", "output", "legacy"))
    for path in (seed, output, legacy):
        path.mkdir()
    (seed / "owned").write_bytes(b"abc")
    (output / "owned").write_bytes(b"defgh")
    link = legacy / "python"
    link.symlink_to("/outside/not-read/python")
    expected = 8 + link.lstat().st_size
    monkeypatch.setattr(
        benchmark.shutil, "disk_usage", lambda _: type("Usage", (), {"free": 128 * 1024**3})()
    )
    result = benchmark.capacity(
        seed, output, expected, account_roots=[legacy, tmp_path, seed, legacy]
    )
    assert result["combined_bytes"] == expected
    assert result["accounted_roots"] == {str(tmp_path): expected}
    assert result["symlink_count"] == 1
    assert result["strict_root_bytes"] == {str(seed): 3, str(output): 5}
    assert result["accounted_root_usage"][str(tmp_path)]["symlink_count"] == 1
    with pytest.raises(ValueError, match="envelope"):
        benchmark.capacity(seed, output, expected - 1, account_roots=[tmp_path])


def test_aggregate_usage_rejects_symlink_root_and_symlink_ancestors(tmp_path):
    from oddsfox_catalogue.current_contract import CurrentError

    actual = tmp_path / "actual"
    actual.mkdir()
    (actual / "child").mkdir()
    link = tmp_path / "linked"
    link.symlink_to(actual, target_is_directory=True)
    for root in (link, link / "child"):
        with pytest.raises(CurrentError, match="symlink"):
            benchmark.aggregate_usage(root)


@pytest.mark.parametrize(
    ("publication_seconds", "fail_publication"),
    [(40, False), (200, False), (299, False), (400, False), (40, True)],
)
def test_preparation_calibrates_changed_publication_without_relaxing_refresh_budget(
    tmp_path, monkeypatch, publication_seconds, fail_publication
):
    from types import SimpleNamespace

    from fakes.world import make_event, make_market
    from oddsfox_catalogue import current_publish, current_runtime, current_store, current_sync
    from oddsfox_catalogue.current_rows import project_response

    root = benchmark.validate_root(tmp_path / "seed", initialize=True)
    settings = benchmark.CurrentSettings(root)
    with benchmark.connect_store(settings) as conn:
        current_store.initialize(conn)
        original = make_event("1", "retained", markets=[make_market("1", "retained")])
        current_store.apply(
            conn,
            project_response(
                [original], "events", received_at="2000-01-01T00:00:00Z", response_id="seed"
            ),
        )
        publish_release(settings, conn)
        current_store.set_state(conn, "previous_publish_seconds", 915)
        market = benchmark.market_payload(conn, benchmark.records(conn, "markets")[0])
        events = {"1": {**original, "markets": [market]}}
        markets = {"1": market}
        fixture = benchmark.repair_fixture(conn, markets, [])
    with CurrentGammaServer(events, markets) as source:
        # Integral anchors keep simulated durations exact across machine uptimes.
        clock = [float(math.ceil(benchmark.time.monotonic()))]
        started = clock[0]
        monkeypatch.setattr(benchmark, "time", SimpleNamespace(monotonic=lambda: clock[0]))
        monkeypatch.setattr(current_runtime, "time", SimpleNamespace(monotonic=lambda: clock[0]))
        original_publish = current_publish.publish
        original_verify = benchmark.verify_current
        original_client = benchmark.GammaClient
        client_calls = []

        def client(gamma, **kwargs):
            client_calls.append(kwargs)
            assert kwargs["max_duration_s"] == 300
            assert kwargs["deadline"] == started + 300
            return original_client(gamma, **kwargs)

        def publish(given_settings, given_conn, *, deadline):
            assert given_settings.limits == settings.limits
            assert given_settings.limits.max_duration_s == 300
            assert deadline == started + 14400
            if fail_publication:
                clock[0] += publication_seconds
                raise RuntimeError("preparation publication failed")
            result = original_publish(given_settings, given_conn, deadline=deadline)
            assert not result["noop"]
            clock[0] += publication_seconds
            return result

        def verify(*args, **kwargs):
            result = original_verify(*args, **kwargs)
            clock[0] += 7
            return result

        monkeypatch.setattr(benchmark, "GammaClient", client)
        monkeypatch.setattr(current_publish, "publish", publish)
        monkeypatch.setattr(benchmark, "verify_current", verify)
        with benchmark.connect_store(settings) as conn:
            current_store.set_state(
                conn,
                "bulk_policy",
                {
                    **current_sync.POLICY,
                    "origin": source.url,
                    "lanes": {
                        lane: {"supported": True, "passes": [None]}
                        for lane in ("events", "markets")
                    },
                },
            )
            if fail_publication:
                with pytest.raises(RuntimeError, match="preparation publication failed"):
                    benchmark.prepare_identity_repair(
                        settings, conn, source, events, markets, fixture
                    )
                assert current_store.get_state(conn, "previous_publish_seconds") == 915
                assert current_store.get_state(conn, "fetch_pending") is None
                assert source.stats["http_attempts"] == 1
                assert all(
                    benchmark.urlsplit(row["path"]).path == "/markets"
                    for row in source.stats["responses"]
                )
                return
            prepared = benchmark.prepare_identity_repair(
                settings, conn, source, events, markets, fixture
            )
            assert prepared["copied_previous_publish_seconds"] == 915
            assert prepared["resulting_previous_publish_seconds"] == publication_seconds
            assert prepared["invalidation_publication_s"] == publication_seconds
            assert prepared["invalidation_extra_verify_s"] == 7
            assert prepared["elapsed_s"] == publication_seconds + 7
            assert prepared["preparation_publication_deadline_monotonic"] == started + 14400
            assert current_store.get_state(conn, "previous_publish_seconds") == publication_seconds
            assert current_store.get_state(conn, "fetch_pending") == prepared["fetch_pending"]
            assert source.stats["http_attempts"] == 1
            assert all(
                benchmark.urlsplit(row["path"]).path == "/markets"
                for row in source.stats["responses"]
            )
            assert len(client_calls) == 1
        if publication_seconds >= 200:
            monkeypatch.setattr(current_publish, "publish", original_publish)
            normal_deadline = clock[0] + 300
            reserve = max(settings.limits.publication_reserve_s, 1.5 * publication_seconds)
            assert reserve >= 300
            if publication_seconds == 299:
                assert reserve == 448.5
            normal_requests = []

            def normal_client(gamma, **kwargs):
                assert kwargs["max_duration_s"] == 300
                assert kwargs["deadline"] == normal_deadline - reserve
                assert kwargs["deadline"] <= clock[0]
                return original_client(
                    gamma,
                    **kwargs,
                    clock=lambda: clock[0],
                    transport=httpx.MockTransport(
                        lambda request: (
                            normal_requests.append(request) or pytest.fail("no acquisition budget")
                        )
                    ),
                )

            monkeypatch.setattr(current_runtime, "GammaClient", normal_client)
            local = benchmark.replace(
                settings,
                gamma=benchmark.replace(settings.gamma, base_url=source.url, allow_loopback=True),
            )
            result = current_runtime.execute(local, "refresh", {}, normal_deadline)
            assert result["budget_exhausted"] and result["pending"]
            assert result["http_attempts"] == 0
            assert normal_requests == []
            with benchmark.connect_store(settings) as conn:
                assert (
                    current_store.get_state(conn, "previous_publish_seconds") == publication_seconds
                )
                assert current_store.get_state(conn, "fetch_pending") == prepared["fetch_pending"]
