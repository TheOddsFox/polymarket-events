"""Real HTTP/CLI proofs for current catalogue publication and offline recovery."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import duckdb
import httpx
import pytest
from filelock import FileLock, Timeout

from fakes.current_gamma import CurrentGammaServer
from oddsfox_catalogue import current_publish, current_runtime, current_store, current_sync
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.runlock import run_lock

pytestmark = pytest.mark.timeout(90)


def cli(root, *args, source="http://127.0.0.1:9", code=0):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CATALOGUE_")}
    env.update(
        CATALOGUE_GAMMA_BASE_URL=source,
        CATALOGUE_GAMMA_ALLOW_LOOPBACK="1",
        CATALOGUE_GAMMA_MAX_RETRIES="0",
        CATALOGUE_CATALOGUE_MAX_DURATION_S="120",
    )
    result = subprocess.run(
        [sys.executable, "-m", "oddsfox_catalogue", "--root", str(root), *map(str, args)],
        cwd=root.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == code, result.stdout + result.stderr
    return json.loads(result.stdout)


def probe(root, source):
    return cli(
        root,
        "probe",
        "--open-event-id",
        "1",
        "--closed-event-id",
        "2",
        "--absent-event-id",
        "999",
        "--open-market-id",
        "11",
        "--closed-market-id",
        "21",
        "--absent-market-id",
        "999",
        source=source,
    )


def assert_assets(path, source):
    with duckdb.connect(":memory:") as conn:
        actual = conn.execute(
            "SELECT market_id,asset_kind,asset_id,clob_token_id,position_id "
            "FROM read_parquet(?) ORDER BY market_id,outcome_index",
            [str(Path(path) / "outcomes.parquet")],
        ).fetchall()
    expected = []
    for key, market in sorted(source.markets.items()):
        if market["version"] == "v1":
            expected.extend(
                (key, "ctf_token", asset, asset, None)
                for asset in json.loads(market["clobTokenIds"])
            )
        else:
            expected.extend(
                (key, "poly_v2_position", asset, None, asset) for asset in market["positionIds"]
            )
    assert actual == expected


def test_fixture_native_bulk_highwater_freezing_and_accounting():
    fixture = CurrentGammaServer()
    with httpx.Client(
        transport=httpx.MockTransport(fixture.handle), base_url="http://fixture"
    ) as client:
        native = client.get("/events/1")
        assert native.status_code == 200
        assert [m["id"] for m in native.json()["markets"]] == ["11", "12"]
        assert client.get("/events/999").status_code == 404
        bulk = client.get(
            "/markets", params=[("id", "11"), ("id", "21"), ("id", "999"), ("include_tag", "true")]
        )
        assert [(m["id"], m["closed"]) for m in bulk.json()] == [("11", False), ("21", True)]
        assert (
            client.get("/markets", params={"limit": 1, "order": "id", "ascending": False}).json()[
                0
            ]["id"]
            == "21"
        )
        assert client.get("/markets", params={"closed": False}).status_code == 400
        external = fixture.events
        external["1"]["title"] = "not part of this capture"
        assert client.get("/events/1").json()["title"] == "Open event"
    stats = fixture.stats
    assert stats["http_attempts"] == 6
    assert stats["downloaded_bytes"] == sum(row["bytes"] for row in stats["responses"])
    assert stats["downloaded_bytes"] > len(native.content)


def test_fixture_failure_plan_and_whole_capture_replacement():
    fixture = CurrentGammaServer()
    fixture.failure_plan.extend([503, 200])
    request = httpx.Request("GET", "http://fixture/markets/11")
    assert fixture.handle(request).status_code == 503
    assert fixture.handle(request).json()["id"] == "11"
    events, markets = fixture.events, fixture.markets
    markets["11"]["question"] = "Changed native question?"
    with pytest.raises(ValueError, match="one frozen capture"):
        fixture.replace(events=events, markets=markets)
    assert fixture.handle(request).json()["question"] == "Market 11?"
    events["1"]["markets"][0] = markets["11"]
    fixture.replace(events=events, markets=markets)
    markets["11"]["question"] = "not retained"
    assert fixture.handle(request).json()["question"] == "Changed native question?"


def test_pending_native_receipt_resumes_frozen_capture_without_http(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path.resolve() / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1),
    )
    validate_root(settings.root, initialize=True)
    fixture = CurrentGammaServer()
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        with (
            GammaClient(settings.gamma, transport=httpx.MockTransport(fixture.handle)) as client,
            monkeypatch.context() as patch,
        ):

            def fail_apply(*_args, **_kwargs):
                raise RuntimeError("crash after durable receipt")

            patch.setattr(current_store, "apply", fail_apply)
            with pytest.raises(RuntimeError, match="crash after durable receipt"):
                current_sync.fetch_apply(settings, conn, client, "events", [1], native=True)
        assert fixture.stats["http_attempts"] == 1
        assert current_store.get_state(conn, "fetch_pending") is not None
        assert conn.execute("SELECT count(*) FROM events").fetchone() == (0,)
    events, markets = fixture.events, fixture.markets
    events["1"]["title"] = "Changed after capture"
    fixture.replace(events=events, markets=markets)

    def no_network(_request):
        pytest.fail("durable pending receipt attempted a new HTTP request")

    with (
        connect_store(settings) as conn,
        GammaClient(settings.gamma, transport=httpx.MockTransport(no_network)) as client,
    ):
        assert current_sync.resume_pending(settings, conn, client) == {
            "responses": 1,
            "records": 1,
            "requests": 0,
        }
        assert current_store.get_state(conn, "fetch_pending") is None
        assert conn.execute("SELECT title FROM events WHERE event_id='1'").fetchone() == (
            "Open event",
        )
        assert conn.execute("SELECT count(*) FROM event_markets").fetchone() == (2,)
        assert current_sync.resume_pending(settings, conn, client) == {"responses": 0}
        assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (4,)
        assert client.stats.requests == 0


def test_cli_current_metadata_and_full_pass_offline(tmp_path):
    root = tmp_path.resolve() / "catalogue"
    assert cli(root, "init")["status"] == "initialized"
    initial = cli(root, "status")
    assert "last_backup" not in initial
    assert initial["full_pass"]["started_at"] is None
    assert initial["full_pass"]["completed_at"] is None
    with CurrentGammaServer() as source:
        certificate = probe(root, source.url)
        assert all(lane["supported"] for lane in certificate["lanes"].values())
        before = source.stats
        refreshed = cli(root, "refresh", source=source.url)
        measured = source.stats
        assert refreshed["http_attempts"] == measured["http_attempts"] - before["http_attempts"]
        assert (
            refreshed["downloaded_bytes"]
            == measured["downloaded_bytes"] - before["downloaded_bytes"]
        )
        assert refreshed["publication"]["counts"] == {
            "events": 2,
            "markets": 3,
            "event_markets": 3,
            "outcomes": 6,
        }
        assert refreshed["source_complete"] is False
        verified = cli(root, "verify")
        assert verified["valid"] is True
        assert verified["coverage"]["complete"] is False
        assert_assets(verified["path"], source)
        producer, bundle = tmp_path / "producer", tmp_path / "metadata"
        exported = cli(
            producer,
            "metadata",
            "refresh",
            "--market-id",
            "11",
            "--market-id",
            "12",
            "--catalogue-root",
            str(root),
            "--output",
            bundle,
            source=source.url,
        )
        assert (exported["found"], exported["absent"], exported["failed"]) == (2, 0, 0)
        assert (
            json.loads((bundle / "manifest.json").read_text())["contract"]
            == "oddsfox.polymarket.metadata.v1"
        )
        reconciled = cli(root, "reconcile", source=source.url)
        assert reconciled["status"] == "published"
        assert reconciled["http_attempts"] >= 2
        assert reconciled["source_complete"] is False
        full = cli(root, "reconcile", "--full-pass", source=source.url)
        assert full["full_pass"]["started_at"] is not None
        assert full["source_complete"] is False
        remaining = full["full_pass"]["remaining"]
        assert isinstance(remaining, int) and remaining >= 0
        assert (full["full_pass"]["completed_at"] is not None) == (remaining == 0)
        requests = source.stats["http_attempts"]
        status = cli(root, "status")
        assert status["full_pass"]["started_at"] == full["full_pass"]["started_at"]
        assert source.stats["http_attempts"] == requests
    assert cli(root, "verify")["valid"] is True


def test_minimal_v1_root_refresh_and_full_pass_without_import(tmp_path):
    root = tmp_path.resolve() / "catalogue"
    assert cli(root, "init")["status"] == "initialized"
    help_text = subprocess.run(
        [sys.executable, "-m", "oddsfox_catalogue", "--help"],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout
    assert "import-release" not in help_text
    assert "backup" not in help_text
    with CurrentGammaServer() as source:
        probe(root, source.url)
        refreshed = cli(root, "refresh", source=source.url)
        assert refreshed["status"] == "published"
        full = cli(root, "reconcile", "--full-pass", source=source.url)
        assert full["full_pass"]["remaining"] == 0
        assert full["full_pass"]["completed_at"] is not None
        assert full["source_complete"] is False
    verified = cli(root, "verify")
    assert verified["valid"] is True
    assert verified["coverage"]["complete"] is False


def test_source_change_failed_publication_retains_verified_previous_pointer(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "catalogue"
    cli(root, "init")
    with CurrentGammaServer() as source:
        probe(root, source.url)
        first = cli(root, "refresh", source=source.url)["publication"]
        pointer = root / "published/current.json"
        original_pointer = pointer.read_bytes()
        events, markets = source.events, source.markets
        events["1"]["title"] = "Changed source title"
        source.replace(events=events, markets=markets)
        settings = CurrentSettings(
            root,
            gamma=replace(CurrentSettings(root).gamma, base_url=source.url, allow_loopback=True),
            limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
        )
        from oddsfox_catalogue.current_sync import fetch_apply

        with connect_store(settings) as conn, GammaClient(settings.gamma) as client:
            fetch_apply(settings, conn, client, "events", [1])
            assert conn.execute("SELECT title FROM events WHERE event_id='1'").fetchone() == (
                "Changed source title",
            )
            with monkeypatch.context() as patch:

                def fail_export(*_args):
                    raise OSError("injected publication failure")

                patch.setattr(current_publish, "_export", fail_export)
                with pytest.raises(OSError, match="injected publication failure"):
                    current_publish.publish(settings, conn)
            assert pointer.read_bytes() == original_pointer
            previous = current_publish.verify_current(settings, connection=conn)
            assert previous["release_id"] == first["release_id"]
            assert previous["store_generation"] > previous["published_generation"]
            next_release = current_publish.publish(settings, conn)
            assert next_release["release_id"] != first["release_id"]
            assert json.loads(pointer.read_text())["previous_release_id"] == first["release_id"]
    assert cli(root, "verify")["release_id"] == next_release["release_id"]


def _stall_child(_pipe, _settings, _operation, _options, _deadline):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(60)


def test_supervisor_stall_stops_child_before_writer_lock_release(tmp_path, monkeypatch):
    settings = CurrentSettings(tmp_path.resolve() / "catalogue")
    assert settings.limits.max_duration_s == 1200
    context = multiprocessing.get_context("spawn")
    processes = []

    class TrackedContext:
        Pipe = staticmethod(context.Pipe)

        @staticmethod
        def Process(*args, **kwargs):
            process = context.Process(*args, **kwargs)
            processes.append(process)
            return process

    monkeypatch.setattr(current_runtime, "_child", _stall_child)
    monkeypatch.setattr(current_runtime.multiprocessing, "get_context", lambda _: TrackedContext())
    competing = FileLock(str(settings.run_lock_path))
    started = time.monotonic()
    with run_lock(settings.run_lock_path):
        with pytest.raises(CurrentError, match="deadline exhausted"):
            current_runtime.supervise(settings, "refresh", {}, duration=0.5)
        assert len(processes) == 1
        assert processes[0].exitcode is not None
        assert not processes[0].is_alive()
        with pytest.raises(Timeout):
            competing.acquire(timeout=0)
    with competing.acquire(timeout=0):
        assert not processes[0].is_alive()
    assert time.monotonic() - started < 6
