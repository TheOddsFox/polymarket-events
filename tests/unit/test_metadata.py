import gzip
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from oddsfox_catalogue.backup import create_backup, verify_backup
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.cli import main
from oddsfox_catalogue.config import GammaSettings, Settings
from oddsfox_catalogue.gamma.http import GammaClient, MalformedResponse, RequestBudgetExceeded
from oddsfox_catalogue.metadata import (
    CONTRACT,
    MetadataError,
    Observation,
    export_metadata,
    project,
    refresh_metadata,
    validate_market_ids,
)

CONDITION = "0x" + "ab" * 32
NOW = datetime(2026, 10, 10, tzinfo=UTC)


def market(market_id: str = "123", **extra):
    return {
        "id": market_id,
        "version": "v1",
        "conditionId": CONDITION,
        "outcomes": '["Above", "Below"]',
        "clobTokenIds": '["100", "200"]',
        **extra,
    }


def observation(raw: dict, *, kind="market_direct", receipt="2026-10-10T00:00:00.000Z"):
    return Observation(raw, {"observation_id": "obs", "received_at": receipt, "source_kind": kind})


def relations(path: Path):
    return {
        name: json.loads((path / f"{name}.json").read_text())
        for name in ("markets", "outcomes", "memberships", "identity_history", "coverage")
    }


def raw_page(
    settings: Settings, payload, *, key="markets", stamp="2026-10-10T00:00:00.000Z", name="test"
):
    write_page(
        settings.raw_dir / name,
        1,
        json.dumps(payload).encode(),
        {
            "page_id": name,
            "batch_id": name,
            "observed_at": stamp,
            "record_key": key,
            "endpoint": "/" + key,
            "http_status": 200,
        },
    )


def mock_client(handler, **kwargs):
    return GammaClient(
        GammaSettings(max_retries=0, requests_per_second=100000),
        transport=httpx.MockTransport(handler),
        now=lambda: NOW,
        **kwargs,
    )


def test_identity_is_independent_of_prices_and_preserves_exact_native_values():
    raw = market(
        outcomePrices="broken",
        orderPriceMinTickSize=Decimal("0.000000000000000001"),
        active=None,
        closed=False,
        startDate="2026-01-01T00:00:00Z",
    )
    projected, outcomes = project(observation(raw))
    assert projected["usable"]
    assert projected["active"] is None and projected["closed"] is False
    assert projected["tick_size"] == "0.000000000000000001"
    assert projected["start_at"] == "2026-01-01T00:00:00.000Z"
    assert [row["outcome_index"] for row in outcomes] == [1, 2]
    assert [row["asset_id"] for row in outcomes] == ["100", "200"]
    assert all(row["chain_index_set"] is None for row in outcomes)


def test_multi_outcome_and_position_only_identities():
    raw = market(
        version="v2",
        clobTokenIds=None,
        positionIds=["100", "200", "300"],
        outcomes=["A", "B", "C"],
        conditionId=None,
    )
    projected, outcomes = project(observation(raw))
    assert projected["usable"]
    assert len(outcomes) == 3
    assert all(
        row["asset_kind"] == "poly_v2_position" and row["clob_token_id"] is None for row in outcomes
    )
    assert outcomes[0]["position_id"] == "100"


@pytest.mark.parametrize(
    "extra",
    [
        {"clobTokenIds": '["100", "100"]'},
        {"clobTokenIds": '["100"]'},
        {"clobTokenIds": '["01", "200"]'},
        {"clobTokenIds": [str(2**256), "200"]},
        {"version": "v2"},
        {"version": None},
        {"version": "v3"},
        {"version": 1},
        {"version": True},
        {"version": "2"},
        {"conditionId": "invalid"},
    ],
)
def test_bad_or_ambiguous_identity_is_quarantined(extra):
    projected, outcomes = project(observation(market(**extra)))
    assert not projected["usable"] and projected["identity_error"]
    assert outcomes == []


@pytest.mark.parametrize("ids", [[], ["../123"], ["01"], ["0"], ["1"] * 101])
def test_selection_is_explicit_and_bounded(ids):
    with pytest.raises(MetadataError):
        validate_market_ids(ids)


def test_offline_direct_authority_nulls_history_and_enclosing_membership(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market(active=None)], name="direct", stamp="2026-10-01T00:00:00Z")
    raw_page(
        settings,
        [{"id": "55", "markets": [market(active=True)]}],
        key="events",
        name="nested",
        stamp="2026-10-02T00:00:00Z",
    )
    output = tmp_path / "bundle"
    manifest = export_metadata(settings, ["123"], output)
    rows = relations(output)
    assert manifest["contract"] == CONTRACT
    assert rows["markets"][0]["active"] is None
    assert rows["markets"][0]["source_kind"] == "market_direct"
    assert len(rows["identity_history"]) == 2
    assert rows["memberships"][0]["event_id"] == "55"
    assert rows["coverage"][0]["status"] == "found"
    assert not settings.ledger_path.exists()
    assert not (settings.published_dir / "current.json").exists()
    with pytest.raises(MetadataError, match="immutable"):
        export_metadata(settings, ["123"], output)


def test_explicit_empty_membership_does_not_refill_old_enclosing_event(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market(events=[])], name="direct")
    raw_page(settings, [{"id": "55", "markets": [market()]}], key="events", name="nested")
    output = tmp_path / "bundle"
    export_metadata(settings, ["123"], output)
    assert relations(output)["memberships"] == []


def test_same_numeric_id_across_asset_kinds_does_not_collide(tmp_path):
    settings = Settings(tmp_path)
    raw_page(
        settings,
        [market(), market("124", version="v2", clobTokenIds=None, positionIds=["100", "200"])],
    )
    output = tmp_path / "bundle"
    export_metadata(settings, ["123", "124"], output)
    assert all(row["usable"] for row in relations(output)["markets"])


def test_native_identity_with_multiple_owners_is_unusable(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market(), market("124")])
    output = tmp_path / "bundle"
    export_metadata(settings, ["123", "124"], output)
    assert all(not row["usable"] for row in relations(output)["markets"])
    assert relations(output)["outcomes"] == []


def test_refresh_only_selected_ids_accounts_failures_and_does_not_load_or_publish(tmp_path):
    settings = Settings(tmp_path)
    called = []

    def handle(request):
        called.append(request.url.path)
        if request.url.path == "/markets/123":
            return httpx.Response(200, json=market())
        if request.url.path == "/markets/124":
            return httpx.Response(404, json={"error": "gone"})
        return httpx.Response(200, json=market("999"))

    output = tmp_path / "bundle"
    with mock_client(handle) as client:
        refresh_metadata(settings, ["125", "123", "124"], output, client=client)
    assert called == ["/markets/123", "/markets/124", "/markets/125"]
    assert [row["status"] for row in relations(output)["coverage"]] == ["found", "absent", "failed"]
    assert [row["market_id"] for row in relations(output)["markets"]] == ["123"]
    with Ledger(settings.ledger_path) as ledger:
        assert ledger.list_batches() == []
        assert len(ledger._all("SELECT * FROM metadata_requests")) == 3
    assert not settings.warehouse_path.exists()
    assert not (settings.published_dir / "current.json").exists()
    offline = tmp_path / "offline"
    export_metadata(settings, ["123", "124", "125"], offline)
    assert [row["status"] for row in relations(offline)["coverage"]] == [
        "found",
        "absent",
        "failed",
    ]


def test_absent_or_failed_refresh_never_refills_stale_market(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market()], stamp="2026-09-01T00:00:00Z")
    with mock_client(lambda _: httpx.Response(404, json={})) as client:
        refresh_metadata(settings, ["123"], tmp_path / "refresh", client=client)
    export_metadata(settings, ["123"], tmp_path / "offline")
    for name in ("refresh", "offline"):
        assert relations(tmp_path / name)["markets"] == []
        assert relations(tmp_path / name)["coverage"][0]["status"] == "absent"


def test_backup_retains_targeted_capture_evidence(tmp_path):
    settings = Settings(tmp_path)
    with mock_client(lambda _: httpx.Response(200, json=market())) as client:
        refresh_metadata(
            settings, ["123"], settings.data_dir / "metadata" / "bundles" / "run", client=client
        )
    backup = create_backup(settings, now=NOW)
    assert list((backup / "metadata" / "raw").glob("**/*.json.gz"))
    assert (backup / "metadata" / "bundles" / "run" / "manifest.json").exists()
    assert verify_backup(backup) == []


def test_newer_direct_catalogue_observation_supersedes_old_targeted_absence(tmp_path):
    settings = Settings(tmp_path)
    with mock_client(lambda _: httpx.Response(404, json={})) as client:
        refresh_metadata(settings, ["123"], tmp_path / "absent", client=client)
    raw_page(settings, [market()], stamp="2026-10-11T00:00:00.000Z", name="newer")
    export_metadata(settings, ["123"], tmp_path / "newer")
    assert relations(tmp_path / "newer")["coverage"][0]["status"] == "found"
    assert len(relations(tmp_path / "newer")["markets"]) == 1


def test_offline_missing_observation_is_not_source_absence_and_cli_reports_failure(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    output = tmp_path / "bundle"
    assert main(["metadata", "export", "--market-id", "123", "--output", str(output)]) == 2
    assert relations(output)["coverage"][0]["error"] == "no_observation"


def test_raw_checksum_corruption_blocks_export(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market()])
    next(settings.raw_dir.glob("**/*.json.gz")).write_bytes(b"bad")
    with pytest.raises(MetadataError, match="checksum"):
        export_metadata(settings, ["123"], tmp_path / "bundle")


def test_targeted_transport_bounds_bodies_and_rejects_compression(tmp_path):
    body = json.dumps(market()).encode()
    with (
        mock_client(
            lambda _: httpx.Response(
                200, content=gzip.compress(body), headers={"Content-Encoding": "gzip"}
            ),
            max_body_bytes=len(body),
            trust_env=False,
        ) as client,
        pytest.raises(MalformedResponse, match="compressed response"),
    ):
        client.get("/markets/123")
    with (
        mock_client(lambda _: httpx.Response(200, content=body), max_body_bytes=2) as client,
        pytest.raises(MalformedResponse, match="byte limit"),
    ):
        client.get("/markets/123")


def test_targeted_transport_rejects_redirect_and_non_gamma_host(tmp_path):
    settings = Settings(tmp_path)
    with mock_client(
        lambda _: httpx.Response(302, headers={"Location": "https://evil.invalid"})
    ) as client:
        refresh_metadata(settings, ["123"], tmp_path / "redirect", client=client)
    assert relations(tmp_path / "redirect")["coverage"][0]["status"] == "failed"
    bad = Settings(tmp_path, gamma=GammaSettings(base_url="https://evil.invalid"))
    with pytest.raises(MetadataError, match="fixed Gamma"):
        refresh_metadata(bad, ["123"], tmp_path / "bad")


def test_shared_request_cap_blocks_second_selected_market_before_http(tmp_path):
    settings = Settings(tmp_path)
    called = []

    def handle(request):
        called.append(request.url.path)
        return httpx.Response(200, json=market(request.url.path.rsplit("/", 1)[1]))

    with mock_client(handle, max_requests=1, max_download_bytes=10_000) as client:
        result = refresh_metadata(settings, ["123", "124"], tmp_path / "bundle", client=client)
    assert called == ["/markets/123"]
    assert result["http_attempts"] == 1
    assert result["downloaded_bytes"] > 0
    assert [row["status"] for row in relations(tmp_path / "bundle")["coverage"]] == [
        "found",
        "failed",
    ]
    stored = json.loads((tmp_path / "bundle" / "manifest.json").read_text())
    assert "http_attempts" not in stored and "downloaded_bytes" not in stored


def test_download_accounting_includes_partial_response_and_retry_bytes():
    class PartialBody(httpx.SyncByteStream):
        def __iter__(self):
            yield b"abc"
            raise httpx.ReadError("synthetic mid-stream failure")

    attempts = []

    def handle(_):
        attempts.append(1)
        if len(attempts) == 1:
            return httpx.Response(200, stream=PartialBody())
        return httpx.Response(200, content=b"[]")

    with mock_client(handle, max_requests=2, max_download_bytes=5, max_body_bytes=100) as client:
        client._sleep = lambda _: None
        client._limiter._sleep = lambda _: None
        client._delay = lambda *_: 0
        assert client.get("/markets/123", max_retries=1).body == b"[]"
        assert client.stats.downloaded_bytes == 5
        assert client.stats.requests == 2
        with pytest.raises(RequestBudgetExceeded):
            client.get("/markets/124")
    assert len(attempts) == 2


def test_http_error_body_counts_toward_cumulative_retry_budget():
    attempts = []

    def handle(_):
        attempts.append(1)
        return httpx.Response(500, content=b"abcd")

    with mock_client(handle, max_requests=5, max_download_bytes=5, max_body_bytes=100) as client:
        client._delay = lambda *_: 0
        with pytest.raises(RequestBudgetExceeded):
            client.get("/markets/123", max_retries=4)
        assert client.stats.requests == 2
        assert client.stats.downloaded_bytes == 8
    assert len(attempts) == 2


def test_spawned_gamma_clients_share_allowance():
    called = []
    with mock_client(
        lambda _: called.append(1) or httpx.Response(200, content=b"[]"),
        max_requests=1,
        max_download_bytes=100,
    ) as client:
        with client.spawn() as sibling:
            sibling.get("/markets/123")
        with pytest.raises(RequestBudgetExceeded):
            client.get("/markets/124")
        assert client.stats.requests == 1 and client.stats.downloaded_bytes == 2
    assert called == [1]


def test_refresh_cli_returns_attempt_metrics_with_failed_coverage(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))

    def make_client(settings, **kwargs):
        return GammaClient(
            settings,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=market(request.url.path.rsplit("/", 1)[1]))
            ),
            **kwargs,
        )

    monkeypatch.setattr("oddsfox_catalogue.metadata.GammaClient", make_client)
    assert (
        main(
            [
                "metadata",
                "refresh",
                "--market-id",
                "123",
                "--market-id",
                "124",
                "--max-requests",
                "1",
                "--max-download-bytes",
                "10000",
                "--output",
                str(tmp_path / "bundle"),
            ]
        )
        == 2
    )
    report = json.loads(capsys.readouterr().out)
    assert report["http_attempts"] == 1 and report["downloaded_bytes"] > 0
    assert report["found"] == 1 and report["failed"] == 1


def test_zero_delegated_cap_makes_no_http_attempt(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))

    def make_client(settings, **kwargs):
        return GammaClient(
            settings,
            transport=httpx.MockTransport(lambda _: pytest.fail("no HTTP allowance")),
            **kwargs,
        )

    monkeypatch.setattr("oddsfox_catalogue.metadata.GammaClient", make_client)
    assert (
        main(
            [
                "metadata",
                "refresh",
                "--market-id",
                "123",
                "--max-requests",
                "0",
                "--max-download-bytes",
                "0",
                "--output",
                str(tmp_path / "bundle"),
            ]
        )
        == 2
    )
    report = json.loads(capsys.readouterr().out)
    assert report["http_attempts"] == report["downloaded_bytes"] == 0


def test_targeted_compressed_stream_is_not_read():
    class NeverRead(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError("compressed response must be rejected before decoding")
            yield b""

    def handler(request):
        assert request.headers["Accept-Encoding"] == "identity"
        return httpx.Response(500, headers={"Content-Encoding": "gzip"}, stream=NeverRead())

    with (
        mock_client(handler, max_body_bytes=2) as client,
        pytest.raises(MalformedResponse, match="compressed response"),
    ):
        client.get("/markets/123")


def test_export_output_budget_does_not_commit_partial_bundle(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market()])
    with pytest.raises(MetadataError, match="output exceeds byte limit"):
        export_metadata(settings, ["123"], tmp_path / "bundle", max_output_bytes=20)
    assert not (tmp_path / "bundle").exists()
    assert not list(tmp_path.glob(".metadata-*"))


def test_source_timestamp_precision_matches_shared_warehouse_projection():
    from oddsfox_catalogue.normalization import normalize_market

    raw = market(
        updatedAt="2026-10-10T01:02:03.139795+02:00", endDate="2026-10-11T01:02:03.001234Z"
    )
    observed = observation(raw)
    projected, _ = project(observed)
    normalized = normalize_market(raw, observed.provenance)
    assert (
        projected["source_updated_at"]
        == normalized["source_updated_at"]
        == "2026-10-09T23:02:03.139795Z"
    )
    assert projected["end_at"] == normalized["end_at"] == "2026-10-11T01:02:03.001234Z"


def test_absurd_decimal_exponent_is_rejected_before_expansion():
    from oddsfox_catalogue.normalization import NormalizationError, decimal_string

    with pytest.raises(NormalizationError, match="expansion exceeds"):
        decimal_string(Decimal("1e100000000"))
    with pytest.raises(NormalizationError, match="expansion exceeds"):
        decimal_string(Decimal("1e-100000000"))


def test_receipt_authority_compares_instants_across_timezone_offsets(tmp_path):
    settings = Settings(tmp_path)
    raw_page(settings, [market(question="Later")], name="later", stamp="2026-10-10T00:00:00.001Z")
    raw_page(
        settings,
        [market(question="Earlier")],
        name="earlier",
        stamp="2026-10-10T02:00:00.000+02:00",
    )
    export_metadata(settings, ["123"], tmp_path / "bundle")
    assert relations(tmp_path / "bundle")["markets"][0]["question"] == "Later"
