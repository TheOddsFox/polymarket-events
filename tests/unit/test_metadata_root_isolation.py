"""Metadata handoffs must preserve catalogue roots before acquiring writer resources."""

import json
import os

import httpx
import pytest

from oddsfox_catalogue import cli, metadata
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.current_config import validate_root
from oddsfox_catalogue.gamma.http import GammaClient


@pytest.fixture(autouse=True)
def offline_settings(monkeypatch):
    for name in os.environ:
        if name.startswith("CATALOGUE_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("CATALOGUE_GAMMA_ALLOW_LOOPBACK", "1")


@pytest.fixture
def current_root(tmp_path):
    return validate_root(tmp_path / "v3", initialize=True)


def market():
    return {
        "id": "123",
        "version": "v1",
        "conditionId": "0x" + "ab" * 32,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["100", "200"],
    }


def raw_evidence(root):
    settings = Settings(root)
    write_page(
        settings.raw_dir / "tiny",
        1,
        json.dumps([market()]).encode(),
        {
            "page_id": "tiny",
            "batch_id": "tiny",
            "observed_at": "2026-10-10T00:00:00Z",
            "record_key": "markets",
            "endpoint": "/markets",
            "http_status": 200,
        },
    )
    return settings


def tree(root):
    return {
        str(path.relative_to(root)): (
            ("symlink", str(path.readlink()))
            if path.is_symlink()
            else ("directory", None)
            if path.is_dir()
            else ("file", path.read_bytes())
        )
        for path in root.rglob("*")
    }


def command(operation, root, output):
    args = [
        "--root",
        str(root),
        "metadata",
        operation,
        "--market-id",
        "123",
        "--output",
        str(output),
    ]
    if operation == "refresh":
        args += ["--max-requests", "0"]
    return args


@pytest.fixture
def forbidden_activity(monkeypatch):
    calls = []
    original_lock = cli.run_lock

    def lock(*args, **kwargs):
        calls.append("lock")
        return original_lock(*args, **kwargs)

    def client(*args, **kwargs):
        calls.append("client")
        raise AssertionError("rejected metadata must not construct an HTTP client")

    monkeypatch.setattr(cli, "run_lock", lock)
    monkeypatch.setattr(metadata, "GammaClient", client)
    return calls


def assert_isolated_rejection(args, tmp_path, current_root, calls, capsys):
    before = tree(tmp_path)
    assert cli.main(args) == 3
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "blocked"
    assert "isolat" in result["message"]
    assert result["http_attempts"] == result["downloaded_bytes"] == 0
    assert calls == []
    assert tree(tmp_path) == before
    assert validate_root(current_root) == current_root


@pytest.mark.parametrize("operation", ["export", "refresh"])
@pytest.mark.parametrize("suffix", [".", "metadata", ".state/deeper/metadata"])
def test_producer_cannot_be_a_catalogue_root_or_descendant(
    tmp_path, current_root, operation, suffix, forbidden_activity, capsys
):
    assert_isolated_rejection(
        command(operation, current_root / suffix, tmp_path / "bundle"),
        tmp_path,
        current_root,
        forbidden_activity,
        capsys,
    )


@pytest.mark.parametrize("operation", ["export", "refresh"])
@pytest.mark.parametrize("suffix", [".", "metadata", ".state/deeper/metadata"])
def test_output_cannot_be_a_catalogue_root_or_descendant(
    tmp_path, current_root, operation, suffix, forbidden_activity, capsys
):
    producer = raw_evidence(tmp_path / "producer")
    assert_isolated_rejection(
        command(operation, producer.root, current_root / suffix),
        tmp_path,
        current_root,
        forbidden_activity,
        capsys,
    )


@pytest.mark.parametrize("operation", ["export", "refresh"])
def test_output_symlink_alias_cannot_hide_a_catalogue_ancestor(
    tmp_path, current_root, operation, forbidden_activity, capsys
):
    producer = raw_evidence(tmp_path / "producer")
    alias = tmp_path / "alias"
    alias.symlink_to(current_root, target_is_directory=True)
    assert_isolated_rejection(
        command(operation, producer.root, alias / ".state" / "metadata"),
        tmp_path,
        current_root,
        forbidden_activity,
        capsys,
    )


def test_cyclic_output_path_is_blocked_before_refresh_activity(
    tmp_path, current_root, forbidden_activity, capsys
):
    alias = tmp_path / "loop"
    alias.symlink_to(alias, target_is_directory=True)
    assert_isolated_rejection(
        command("refresh", tmp_path / "producer", alias / "bundle"),
        tmp_path,
        current_root,
        forbidden_activity,
        capsys,
    )


@pytest.mark.parametrize("setting", ["data_dir", "state_dir"])
@pytest.mark.parametrize("spelling", ["absolute", "relative", "symlink"])
@pytest.mark.parametrize("suffix", [".", ".state/deeper"])
def test_refresh_checks_configured_evidence_and_lock_paths_before_activity(
    tmp_path, current_root, setting, spelling, suffix, monkeypatch, forbidden_activity, capsys
):
    producer = tmp_path / "producer"
    target = current_root / suffix
    if spelling == "relative":
        target = "../v3/" + suffix
    elif spelling == "symlink":
        alias = tmp_path / "alias"
        alias.symlink_to(current_root, target_is_directory=True)
        target = alias / suffix
    config = tmp_path / "metadata.toml"
    config.write_text(f"[paths]\n{setting} = {json.dumps(str(target))}\n")
    monkeypatch.setenv("CATALOGUE_CONFIG", str(config))
    assert_isolated_rejection(
        command("refresh", producer, tmp_path / "bundle"),
        tmp_path,
        current_root,
        forbidden_activity,
        capsys,
    )


@pytest.mark.parametrize("operation", ["export", "refresh"])
def test_sibling_producer_and_output_remain_supported(
    tmp_path, current_root, operation, monkeypatch, capsys
):
    producer = raw_evidence(tmp_path / "v3-metadata")
    output = tmp_path / "v3-metadata-output"
    before = tree(current_root)
    requests = []

    def respond(request):
        requests.append(request.url.path)
        return httpx.Response(200, json=market())

    def client(settings, **kwargs):
        return GammaClient(settings, transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(metadata, "GammaClient", client)
    args = command(operation, producer.root, output)
    if operation == "refresh":
        args[-1] = "1"
    assert cli.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["found"] == 1 and result["failed"] == 0
    assert result["http_attempts"] == (operation == "refresh")
    assert requests == (["/markets/123"] if operation == "refresh" else [])
    assert json.loads((output / "markets.json").read_text())[0]["market_id"] == "123"
    assert tree(current_root) == before
    assert validate_root(current_root) == current_root


def test_offline_export_from_legacy_raw_preserves_legacy_state(
    tmp_path, current_root, forbidden_activity, capsys
):
    legacy = raw_evidence(tmp_path / "legacy")
    legacy.state_dir.mkdir()
    legacy.ledger_path.write_bytes(b"preserved legacy ledger")
    legacy.warehouse_path.parent.mkdir()
    legacy.warehouse_path.write_bytes(b"preserved legacy warehouse")
    before = tree(legacy.root)
    output = tmp_path / "legacy-export"
    assert cli.main(command("export", legacy.root, output)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["found"] == 1 and result["http_attempts"] == 0
    assert json.loads((output / "outcomes.json").read_text())[0]["asset_id"] == "100"
    assert forbidden_activity == []
    assert tree(legacy.root) == before
    assert validate_root(current_root) == current_root
