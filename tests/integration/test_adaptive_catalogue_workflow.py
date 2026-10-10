"""Bounded native leaves must survive the complete certified warehouse workflow."""

from __future__ import annotations

import json
from datetime import timedelta

import duckdb
import httpx
import pytest

from fakes.fake_gamma import FakeGamma
from fakes.harness import FIXED_NOW, build_runtime
from fakes.world import World, make_event, make_market
from oddsfox_catalogue.capture.reader import iter_scan_pages, scan_dir_for
from oddsfox_catalogue.capture.runner import run_capture
from oddsfox_catalogue.capture.writer import read_body
from oddsfox_catalogue.certification import assert_build_valid
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.ids import iso_utc, sha256_bytes
from oddsfox_catalogue.pipeline import dbt_stage, load_stage, publish_stage
from oddsfox_catalogue.publish import PROJECTION_TABLE_QUERIES, current_release, verify_release
from oddsfox_catalogue.rebuild import rebuild_and_verify
from oddsfox_catalogue.runlock import run_lock
from oddsfox_catalogue.semantics import OPERATIONAL_RELATIONS, SEMANTIC_RELATIONS

pytestmark = pytest.mark.timeout(240)
RESPONSE_CAP = 5 * 1024


class OrderedNativeGamma(FakeGamma):
    """Keep native whitespace and nested row order visible in committed evidence."""

    def __init__(self, world):
        super().__init__(world)
        self.event_bodies = {}

    def handle(self, request):
        response = super().handle(request)
        if request.url.path != "/events/keyset" or "id" not in request.url.params:
            return response
        body = response.json()
        body["events"].reverse()
        for event in body["events"]:
            event["markets"].reverse()
        body["source_note"] = "retained native response"
        raw = json.dumps(body, indent=1).encode()
        requested = tuple(request.url.params.get_list("id"))
        self.event_bodies[requested] = raw
        return httpx.Response(response.status_code, content=raw)


def test_adaptive_leaves_certify_publish_and_rebuild_all_semantics_offline(tmp_path, monkeypatch):
    world = World()
    for event_id in range(1, 4):
        title = f"Event {event_id}"
        markets = [
            make_market(
                str(10 * event_id + index),
                f"Market {10 * event_id + index}?",
                closed=True,
                version="v1",
                event_stub={"id": str(event_id), "title": title},
            )
            for index in (1, 2)
        ]
        world.add_event(make_event(str(event_id), title, markets=markets, closed=True))
    fake = OrderedNativeGamma(world)
    runtime, clock = build_runtime(
        tmp_path / "catalogue",
        fake,
        env={
            "CATALOGUE_CAPTURE_WORKERS": "1",
            "CATALOGUE_CAPTURE_MAX_RESPONSE_BYTES": str(RESPONSE_CAP),
        },
    )
    receipts = iter(FIXED_NOW + timedelta(seconds=index) for index in range(1000))
    settings = runtime.settings
    runtime.client.close()
    runtime.client = GammaClient(
        settings.gamma,
        transport=fake.transport(),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: next(receipts),
        max_body_bytes=RESPONSE_CAP,
        max_requests=settings.capture.max_requests,
        max_download_bytes=settings.capture.max_download_bytes,
        max_duration_s=settings.capture.max_duration_s,
    )
    try:
        captured = run_capture(runtime, "bootstrap")
        assert captured.status == "captured"
        batch = runtime.ledger.get_batch(captured.batch_id)
        scan = next(
            row
            for row in runtime.ledger.list_scans(captured.batch_id)
            if row["scan_name"] == "events_ids_0001"
        )
        assert json.loads(scan["params_json"])["hi"] == 3
        pages = list(iter_scan_pages(settings, batch, scan))
        assert [(manifest["offset_start"], manifest["offset_end"]) for manifest, _ in pages] == [
            (1, 1),
            (2, 2),
            (3, 3),
        ]
        assert all(manifest["page_unit_revision"] == 2 for manifest, _ in pages)
        assert len(fake.event_bodies[("1", "2", "3")]) > RESPONSE_CAP
        assert len(fake.event_bodies[("2", "3")]) > RESPONSE_CAP
        assert all(manifest["body_bytes"] <= RESPONSE_CAP for manifest, _ in pages)
        directory = scan_dir_for(
            settings, batch["observation_date"], batch["batch_id"], scan["scan_id"]
        )
        for manifest, records in pages:
            target = str(manifest["offset_start"])
            assert (
                read_body(directory, manifest, trusted_root=settings.raw_dir)
                == (fake.event_bodies[(target,)])
            )
            assert manifest["params"] == {"limit": 1, "id": [target]}
            assert [market["id"] for market in records[0]["markets"]] == [
                str(10 * int(target) + 2),
                str(10 * int(target) + 1),
            ]
        observed = [manifest["observed_at"] for manifest, _ in pages]
        assert observed == sorted(set(observed))
        expected_observations = [
            (str(manifest["offset_start"]), manifest["observed_at"]) for manifest, _ in pages
        ]
        raw_hashes = {
            path: sha256_bytes(path.read_bytes())
            for path in settings.raw_dir.rglob("*")
            if path.is_file()
        }
    finally:
        runtime.client.close()
        runtime.ledger.close()

    loaded = load_stage(settings, now=lambda: FIXED_NOW)
    assert loaded.pages_loaded == captured.pages_written
    assert loaded.batches_registered == [captured.batch_id]
    assert loaded.event_rows == 3 and loaded.market_rows == 12 and loaded.quarantined == 0
    built = dbt_stage(settings, ["build"])
    assert built.returncode == 0, built.stdout[-5000:] + built.stderr[-5000:]
    receipt = assert_build_valid(settings)
    assert set(receipt["binding"]["warehouse"]["relations"]) == set(SEMANTIC_RELATIONS)
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        actual_observations = connection.execute(
            "SELECT event_id, observed_at FROM history.event_history ORDER BY event_id"
        ).fetchall()
        assert [(event_id, iso_utc(received)) for event_id, received in actual_observations] == (
            expected_observations
        )
        assert connection.execute(
            "SELECT entity_id, json_pointer FROM bronze.market_observations "
            "WHERE source_kind='event_embedded' ORDER BY page_id, json_pointer"
        ).fetchall() == [
            (str(10 * event_id + index), f"/events/0/markets/{ordinal}")
            for event_id in range(1, 4)
            for ordinal, index in enumerate((2, 1))
        ]
        assert connection.execute("SELECT count(*) FROM core.markets_current").fetchone() == (6,)
        assert connection.execute("SELECT count(*) FROM core.outcomes_current").fetchone() == (12,)

    release = publish_stage(settings, now=lambda: FIXED_NOW)
    assert release.tables["events"]["rows"] == 3
    assert release.tables["markets"]["rows"] == 6
    assert release.tables["outcomes"]["rows"] == 12
    assert release.tables["quarantine"]["rows"] == 0
    verify_release(settings, release.path)
    pointer_before = (settings.published_dir / "current.json").read_bytes()
    requests_before = list(fake.requests)

    def unexpected_acquisition(*args, **kwargs):
        pytest.fail("offline semantic rebuild attempted a Gamma request")

    monkeypatch.setattr(GammaClient, "get", unexpected_acquisition)
    with run_lock(settings.run_lock_path):
        report = rebuild_and_verify(settings, scratch=tmp_path / "rebuilt")
    assert report.matched and report.mismatches == []
    assert set(report.tables) == (
        set(SEMANTIC_RELATIONS)
        | {"schema:" + name for name in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)}
        | {"published:" + name for name in PROJECTION_TABLE_QUERIES}
        | {"coverage", "capture_inventory"}
    )
    assert all(pair["live"] == pair["rebuilt"] for pair in report.tables.values())
    assert fake.requests == requests_before
    assert {path: sha256_bytes(path.read_bytes()) for path in raw_hashes} == raw_hashes
    assert (settings.published_dir / "current.json").read_bytes() == pointer_before
    assert current_release(settings)["release_id"] == release.release_id
    verify_release(settings, release.path)
