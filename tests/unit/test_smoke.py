import hashlib
import json
import subprocess

import pytest

from oddsfox_catalogue import smoke


def test_failed_command_keeps_stdout_accounting(monkeypatch, tmp_path):
    result = subprocess.CompletedProcess(
        [], 2, '{"http_attempts": 2, "downloaded_bytes": 17}', "warning"
    )
    monkeypatch.setattr(smoke.subprocess, "run", lambda *a, **k: result)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "lookup", ["unused"])
    assert smoke.finish(tmp_path, report, smoke.SmokeFailure("lookup failed")) == 1
    saved = json.loads((tmp_path / "report.json").read_text())
    assert saved["http_attempts"] == 2
    assert saved["downloaded_bytes"] == 17
    assert saved["accounting_complete"] is False


def test_last_stderr_json_and_invalid_diagnostics(monkeypatch):
    result = subprocess.CompletedProcess([], 1, "", 'started\n{"message":"rejected"}\n')
    monkeypatch.setattr(smoke.subprocess, "run", lambda *a, **k: result)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="rejected"):
        smoke.command(report, "scan", ["unused"])
    result.stdout, result.stderr, result.returncode = "[]", "", 0
    with pytest.raises(smoke.SmokeFailure, match="invalid JSON"):
        smoke.command(report, "scan", ["unused"])


def test_timeout_does_not_claim_complete_accounting(monkeypatch, tmp_path):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("unused", 180)

    monkeypatch.setattr(smoke.subprocess, "run", timeout)
    report = {"steps": []}
    with pytest.raises(subprocess.TimeoutExpired) as error:
        smoke.command(report, "scan", ["unused"])
    smoke.finish(tmp_path, report, error.value)
    assert report["accounting_complete"] is False
    assert report["error"] == "TimeoutExpired"


def test_output_never_reuses_existing_or_symlink_root(tmp_path):
    with pytest.raises(smoke.SmokeFailure, match="fresh"):
        smoke.new_output(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(smoke.SmokeFailure, match="symlink"):
        smoke.new_output(link / "new")
    assert smoke.new_output(tmp_path / "new") == tmp_path / "new"


def test_explicit_loopback_survives_but_ambient_catalogue_settings_do_not(monkeypatch, tmp_path):
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "http://127.0.0.1:1234")
    monkeypatch.setenv("CATALOGUE_GAMMA_ALLOW_LOOPBACK", "true")
    monkeypatch.setenv("CATALOGUE_FAULT", "mid_publish")
    monkeypatch.setenv("CATALOGUE_CAPTURE_MAX_REQUESTS", "999999")
    env = smoke.catalogue_environment(tmp_path)
    assert env["CATALOGUE_GAMMA_BASE_URL"] == "http://127.0.0.1:1234"
    assert env["CATALOGUE_GAMMA_ALLOW_LOOPBACK"] == "true"
    assert env["CATALOGUE_ROOT"] == str(tmp_path / "catalogue")
    assert "CATALOGUE_FAULT" not in env
    assert "CATALOGUE_CAPTURE_MAX_REQUESTS" not in env


@pytest.mark.parametrize(
    "market_ids", [[], ["1"] * 2, [str(x) for x in range(1, 7)], ["0"], ["1.0"], ["１２"]]
)
def test_invalid_catalogue_selection_never_launches(monkeypatch, tmp_path, market_ids):
    monkeypatch.setattr(smoke.subprocess, "Popen", lambda *a, **k: pytest.fail("unexpected launch"))
    with pytest.raises(smoke.SmokeFailure):
        smoke.run_catalogue(tmp_path, market_ids, {"steps": []})


def test_catalogue_smoke_requires_an_explicit_selection(tmp_path):
    output = tmp_path / "smoke"
    assert smoke.main(["--workflow", "catalogue", "--output", str(output)]) == 1
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "failed"
    assert report["http_attempts"] == 0
    assert report["accounting_complete"] is False
    assert not (output / "catalogue").exists()


def test_catalogue_replay_compares_history_and_stops_before_backup(monkeypatch, tmp_path):
    from copy import deepcopy

    initial = {
        "capture": {"batch_ids": ["first"]},
        "warehouse": {
            "relations": {
                "core.markets_current": {"rows": 1},
                "bronze.market_observations": {"rows": 1},
                "history.market_history": {"rows": 1},
            }
        },
    }
    second = deepcopy(initial)
    second["capture"]["batch_ids"].append("second")
    second["warehouse"]["relations"]["history.market_history"]["rows"] = 2
    changed = deepcopy(second)
    changed["warehouse"]["relations"]["history.market_history"]["semantic_sha256"] = "changed"
    states = iter([initial, second, changed])
    monkeypatch.setattr(smoke, "_catalogue_state", lambda *a: (next(states), {}, {}))
    monkeypatch.setattr(smoke, "_metadata_fields", lambda *a: None)
    launched = []

    def fake_command(report, name, argv, env=None, **kwargs):
        launched.append(name)
        if name.startswith("capture_"):
            return {"status": "captured", "records": 1, "batch_id": name}
        if name.startswith("load_"):
            return {"pages_loaded": 1}
        if name == "offline_export":
            return {"http_attempts": 0, "found": 1, "failed": 0}
        if name.startswith("verify_"):
            return {"verified": True}
        return {}

    monkeypatch.setattr(smoke, "command", fake_command)
    with pytest.raises(smoke.SmokeFailure, match="offline semantic replay drift"):
        smoke.run_catalogue(tmp_path, ["1"], {"steps": []})
    assert launched[-1] == "verify_replay"
    assert "raw_rebuild" not in launched
    assert "backup_create" not in launched


def metadata_bundle(output, markets, outcomes):
    bundle = output / "bundle"
    bundle.mkdir()
    inventory = {}
    for name, rows in (("markets.json", markets), ("outcomes.json", outcomes)):
        raw = json.dumps(rows).encode()
        (bundle / name).write_bytes(raw)
        inventory[name] = {"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    raw = json.dumps({"contract": "oddsfox.polymarket.metadata.v1", "files": inventory}).encode()
    (bundle / "manifest.json").write_bytes(raw)
    return {"manifest_sha256": hashlib.sha256(raw).hexdigest()}


def usable_market(market_id="1"):
    return {"market_id": market_id, "usable": True, "identity_error": None, "protocol": "ctf"}


def unusable_market(market_id="1"):
    return {
        "market_id": market_id,
        "usable": False,
        "identity_error": "missing source market version",
        "protocol": None,
    }


def nominated_outcomes(market_id="1"):
    return [
        {
            "market_id": market_id,
            "asset_kind": "ctf_token",
            "asset_id": str(index + 10),
            "outcome_index": index,
        }
        for index in (1, 2)
    ]


def test_catalogue_accepts_accounted_unusable_identities_without_nominated_assets(tmp_path):
    markets = [unusable_market("1"), unusable_market("2")]
    result = metadata_bundle(tmp_path, markets, [])
    report = {}
    smoke._metadata_fields(tmp_path, ["1", "2"], result, report)
    assert report["native_assets"] == []
    assert report["market_id"] == "1"
    assert report["metadata_identities"] == [
        {key: market[key] for key in ("market_id", "usable", "identity_error")}
        for market in markets
    ]


def test_catalogue_accepts_mixed_identity_status_and_retains_first_native_assets(tmp_path):
    markets = [usable_market("1"), unusable_market("2")]
    result = metadata_bundle(tmp_path, markets, nominated_outcomes())
    report = {}
    smoke._metadata_fields(tmp_path, ["1", "2"], result, report)
    assert len(report["native_assets"]) == 2
    assert report["metadata_identities"][1]["usable"] is False


def test_numeric_asset_ids_across_native_kinds_do_not_collide(tmp_path):
    markets = [usable_market("1"), usable_market("2")]
    markets[1]["protocol"] = "polymarket_v2"
    outcomes = nominated_outcomes("1") + nominated_outcomes("2")
    for outcome in outcomes[2:]:
        outcome["asset_kind"] = "poly_v2_position"
    result = metadata_bundle(tmp_path, markets, outcomes)
    report = {}
    smoke._metadata_fields(tmp_path, ["1", "2"], result, report)
    assert all(identity["usable"] for identity in report["metadata_identities"])


def test_integer_assets_cannot_hide_duplicate_ownership(tmp_path):
    markets = [usable_market("1"), usable_market("2")]
    outcomes = nominated_outcomes("1") + nominated_outcomes("2")
    for outcome in outcomes[2:]:
        outcome["asset_id"] = int(outcome["asset_id"])
    result = metadata_bundle(tmp_path, markets, outcomes)
    with pytest.raises(smoke.SmokeFailure):
        smoke._metadata_fields(tmp_path, ["1", "2"], result, {})


@pytest.mark.parametrize(
    "failure",
    [
        "no_reason",
        "bogus_assets",
        "no_assets",
        "invalid_id",
        "invalid_kind",
        "invalid_ordinal",
        "non_boolean",
    ],
)
def test_catalogue_rejects_contradictory_identity_evidence(tmp_path, failure):
    market, outcomes = usable_market(), nominated_outcomes()
    if failure == "no_reason":
        market, outcomes = unusable_market(), []
        market["identity_error"] = None
    elif failure == "bogus_assets":
        market = unusable_market()
    elif failure == "no_assets":
        outcomes = []
    elif failure == "invalid_id":
        outcomes[0]["asset_id"] = "0"
    elif failure == "invalid_kind":
        outcomes[0]["asset_kind"] = "unknown"
    elif failure == "invalid_ordinal":
        outcomes[0]["outcome_index"] = True
    elif failure == "non_boolean":
        market["usable"] = 1
    result = metadata_bundle(tmp_path, [market], outcomes)
    with pytest.raises(ValueError):
        smoke._metadata_fields(tmp_path, ["1"], result, {})


def test_default_metadata_workflow_keeps_strict_usable_identity_gate(monkeypatch, tmp_path):
    result = metadata_bundle(tmp_path, [unusable_market()], [])
    monkeypatch.setattr(smoke, "command", lambda *a, **k: {**result, "found": 1, "failed": 0})
    with pytest.raises(smoke.SmokeFailure, match="quarantined or incomplete"):
        smoke.run(tmp_path, "1", {"steps": []})
