import json
from pathlib import Path

import pytest

from oddsfox_catalogue.cli import main
from oddsfox_catalogue.runlock import RunBusy, run_lock


def test_version_prints_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == "0.1.0"


def test_config_show_reads_root_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    monkeypatch.setenv("CATALOGUE_GAMMA_PAGE_LIMIT", "25")
    assert main(["config", "show"]) == 0
    captured = capsys.readouterr()
    shown = json.loads(captured.out)
    assert " INFO " not in captured.out
    assert shown["gamma"]["page_limit"] == 25
    assert shown["root"] == str(tmp_path.resolve())


def test_status_without_ledger_is_harmless(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    assert main(["status"]) == 0
    assert "no ledger yet" in capsys.readouterr().out


def test_run_lock_is_exclusive(tmp_path: Path) -> None:
    lock_path = tmp_path / ".state" / "catalogue.lock"
    with run_lock(lock_path), pytest.raises(RunBusy), run_lock(lock_path):
        pass
    with run_lock(lock_path):
        pass


def test_capture_rejects_unknown_mode() -> None:
    with pytest.raises(SystemExit):
        main(["capture", "--mode", "hourly"])
