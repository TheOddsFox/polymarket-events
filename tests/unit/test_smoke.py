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
