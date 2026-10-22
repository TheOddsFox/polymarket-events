"""V6-only fake-provider behavior for canonical body-limit splitting."""

from __future__ import annotations

import json
from dataclasses import replace
from urllib.error import HTTPError
from urllib.request import urlopen

import duckdb
import pytest

from fakes import capacity_source
from fakes.capacity_source import CapacitySource
from fakes.world import make_event, make_market
from oddsfox_catalogue import current_store, current_sync
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import GammaClient, ResponseTooLarge


def _event(entity_id):
    return {
        "id": str(entity_id),
        "active": True,
        "closed": False,
        "archived": False,
        "markets": [],
    }


def _get_bytes(url):
    try:
        with urlopen(url, timeout=10) as response:
            return response.status, response.read()
    except HTTPError as response:
        return response.code, response.read()


@pytest.fixture
def seed(tmp_path):
    root = tmp_path / "seed"
    root.mkdir()
    conn = duckdb.connect(str(root / "catalogue.duckdb"))
    current_store.initialize(conn)
    current_store.apply(
        conn,
        project_response(
            [
                make_event("1", "Open", markets=[make_market("10", "Open")]),
                make_event(
                    "2",
                    "Closed",
                    closed=True,
                    markets=[make_market("11", "Closed", closed=True)],
                ),
            ],
            "events",
            received_at="2026-01-01T00:00:00Z",
            response_id="seed",
        ),
    )
    conn.close()
    return root


def _client_settings(root, url):
    settings = CurrentSettings(
        root,
        gamma=replace(
            GammaSettings(),
            base_url=url,
            allow_loopback=True,
            max_retries=0,
            backoff_cap_s=0,
        ),
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(root, initialize=True)
    conn = duckdb.connect(":memory:")
    current_store.initialize(conn)
    current_store.set_state(
        conn,
        "bulk_policy",
        {
            **current_sync.POLICY,
            "origin": url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        },
    )
    return settings, conn


def test_v6_overflow_marker_triggers_canonical_batch_split_and_is_audited(
    seed, tmp_path, monkeypatch
):
    monkeypatch.setattr(capacity_source, "MAX_RESPONSE_BYTES", 128)
    audit = tmp_path / "v6-audit.jsonl"
    with CapacitySource(seed, audit, delay_s=0, allow_oversized_200=True) as source:
        # Two individual event objects fit, while their combined response exceeds the
        # lowered fixture limit and returns only the bounded oversize marker.
        source.records = lambda kind, ids, closed=None: [_event(key) for key in ids]
        marker_status, marker_body = _get_bytes(source.url + "/events?id=1&id=2&limit=2")
        assert marker_status == 200
        assert len(marker_body) == 129
        assert marker_body == b" " * 129

        settings, conn = _client_settings(tmp_path / "target", source.url)
        try:
            client = GammaClient(
                settings.gamma,
                max_body_bytes=128,
                max_requests=10,
                max_download_bytes=1024,
                sleep=lambda _seconds: None,
            )
            try:
                before_attempts = source.stats["http_attempts"]
                before_bytes = source.stats["downloaded_bytes"]
                result, omitted = current_sync.fetch_unit(
                    settings, conn, client, "events", ["1", "2"]
                )
                assert result["responses"] == 2
                assert omitted == []
                assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 2
                assert result["requests"] == 2  # The oversized first attempt is not settled.
                assert client.stats.requests == 3  # It still counts as a real HTTP attempt.
                assert source.stats["http_attempts"] - before_attempts == client.stats.requests
                assert (
                    source.stats["downloaded_bytes"] - before_bytes == client.stats.downloaded_bytes
                )
            finally:
                client.close()
        finally:
            conn.close()

    audit_rows = [json.loads(line) for line in audit.read_text().splitlines()]
    markers = [row for row in audit_rows if row.get("overflow_marker") is True]
    assert len(markers) == 2  # Direct proof request plus canonical initial batch.
    for marker in markers:
        assert marker["status"] == 200
        assert marker["body_bytes"] == 129
        assert marker["emitted_ids"] == []
        assert marker["complete_memberships"] == {}


def test_v6_single_oversized_id_remains_visible_and_default_still_returns_413(
    seed, tmp_path, monkeypatch
):
    monkeypatch.setattr(capacity_source, "MAX_RESPONSE_BYTES", 128)
    with CapacitySource(seed, tmp_path / "legacy-audit", delay_s=0) as legacy:
        legacy.records = lambda _kind, ids, _closed=None: [
            {**_event(key), "title": "large" * 40} for key in ids
        ]
        status, _body = _get_bytes(legacy.url + "/events?id=1&limit=1")
        assert status == 413

    with CapacitySource(
        seed, tmp_path / "v6-single-audit", delay_s=0, allow_oversized_200=True
    ) as source:
        source.records = lambda _kind, ids, _closed=None: [
            {**_event(key), "title": "large" * 40} for key in ids
        ]
        settings, conn = _client_settings(tmp_path / "single-target", source.url)
        try:
            client = GammaClient(
                settings.gamma,
                max_body_bytes=128,
                max_requests=10,
                max_download_bytes=1024,
                sleep=lambda _seconds: None,
            )
            try:
                with pytest.raises(ResponseTooLarge):
                    current_sync.fetch_unit(settings, conn, client, "events", ["1"])
                assert current_store.get_state(conn, "fetch_pending") is not None
            finally:
                client.close()
        finally:
            conn.close()
        row = json.loads((tmp_path / "v6-single-audit").read_text().splitlines()[0])
        assert row["overflow_marker"] is True
        assert row["emitted_ids"] == []
