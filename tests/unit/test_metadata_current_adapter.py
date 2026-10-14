"""The bounded metadata adapter retains raw evidence without a historical ledger."""

import json
import subprocess
import sys
from datetime import UTC, datetime

import httpx
import pytest

from oddsfox_catalogue.config import GammaSettings, Settings
from oddsfox_catalogue.gamma.http import GammaClient
from oddsfox_catalogue.metadata import CONTRACT, RELATIONS, export_metadata, refresh_metadata


def test_import_does_not_require_historical_ledger():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.modules['oddsfox_catalogue.capture.ledger'] = None; "
            "import oddsfox_catalogue.metadata",
        ],
        check=True,
    )


@pytest.fixture
def refreshed_bundle(tmp_path):
    settings = Settings(tmp_path)
    bodies = {
        "123": {
            "id": "123",
            "version": "v1",
            "conditionId": "0x" + "ab" * 32,
            "outcomes": '["Yes", "No"]',
            "clobTokenIds": '["100", "200"]',
        },
        "124": {
            "id": "124",
            "version": "v2",
            "conditionId": "0x" + "cd" * 32,
            "outcomes": ["Yes", "No"],
            "positionIds": ["300", "400"],
        },
        "126": {"id": "999"},
    }
    downloaded = []

    def handle(request):
        market_id = request.url.path.rsplit("/", 1)[1]
        response = httpx.Response(
            200 if market_id in bodies else 404, json=bodies.get(market_id, {})
        )
        downloaded.append(len(response.content))
        return response

    output = tmp_path / "bundle"
    with GammaClient(
        GammaSettings(requests_per_second=100000),
        transport=httpx.MockTransport(handle),
        now=lambda: datetime(2026, 10, 10, tzinfo=UTC),
    ) as client:
        result = refresh_metadata(settings, ["126", "125", "124", "123"], output, client=client)
    return settings, output, result, sum(downloaded)


def test_refresh_retains_contract_accounting_and_offline_evidence_without_ledger(refreshed_bundle):
    settings, output, result, downloaded = refreshed_bundle
    assert not settings.ledger_path.exists()
    assert not settings.state_dir.exists()
    assert result["contract"] == CONTRACT
    assert set(result["files"]) == {f"{name}.json" for name in RELATIONS}
    assert set(path.name for path in output.iterdir()) == set(result["files"]) | {"manifest.json"}
    assert result["http_attempts"] == 4
    assert result["downloaded_bytes"] == downloaded
    manifest = json.loads((output / "manifest.json").read_text())
    assert "http_attempts" not in manifest and "downloaded_bytes" not in manifest
    statuses = ["found", "found", "absent", "failed"]
    assert [row["status"] for row in json.loads((output / "coverage.json").read_text())] == statuses
    markers = sorted((settings.data_dir / "metadata" / "raw").glob("**/_request.json"))
    assert [json.loads(path.read_text())["status"] for path in markers] == statuses
    offline = output.parent / "offline"
    export_metadata(settings, ["123", "124", "125", "126"], offline)
    for name in ("markets", "outcomes", "memberships", "identity_history"):
        assert (offline / f"{name}.json").read_bytes() == (output / f"{name}.json").read_bytes()
    assert [
        row["status"] for row in json.loads((offline / "coverage.json").read_text())
    ] == statuses
    outcomes = json.loads((output / "outcomes.json").read_text())
    assert [(row["asset_kind"], row["asset_id"]) for row in outcomes] == [
        ("ctf_token", "100"),
        ("ctf_token", "200"),
        ("poly_v2_position", "300"),
        ("poly_v2_position", "400"),
    ]
    assert [row["clob_token_id"] for row in outcomes[:2]] == ["100", "200"]
    assert [row["position_id"] for row in outcomes[2:]] == ["300", "400"]
    assert not settings.ledger_path.exists()


def test_installed_market_data_consumer_accepts_unchanged_bundle(refreshed_bundle):
    consumer = pytest.importorskip("polymarket_market_data.metadata")
    _, output, _, _ = refreshed_bundle
    bundle = consumer.load_metadata(output, consumer.manifest_digest(output))
    assets = consumer.select_assets(bundle, ["123", "124"])
    assert [(row["asset_kind"], row["asset_id"]) for row in assets] == [
        ("ctf_token", "100"),
        ("ctf_token", "200"),
        ("poly_v2_position", "300"),
        ("poly_v2_position", "400"),
    ]
