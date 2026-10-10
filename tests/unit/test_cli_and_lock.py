import json
import logging
import os
import signal
from pathlib import Path

import pytest

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.cli import configure_logging, main
from oddsfox_catalogue.config import load_settings
from oddsfox_catalogue.runlock import RunBusy, run_lock


def test_http_client_does_not_log_request_urls() -> None:
    configure_logging()
    assert logging.getLogger("httpx").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() == logging.WARNING


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


@pytest.mark.parametrize(("signum", "exit_code"), [(signal.SIGTERM, 143), (signal.SIGHUP, 129)])
def test_a_stop_signal_during_capture_records_failed_stage_and_exits_128_plus_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, signum: int, exit_code: int
) -> None:
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "http://127.0.0.1:9")
    # Four workers: the signal has to drain the pool, not only a serial scan.
    monkeypatch.setenv("CATALOGUE_CAPTURE_WORKERS", "4")

    def explode(*_args: object, **_kwargs: object) -> None:
        os.kill(os.getpid(), signum)

    # Planning reads the high-water mark before any scan. Keep that off the network.
    monkeypatch.setattr("oddsfox_catalogue.gamma.scans._high_water", lambda *_a, **_k: 1)
    monkeypatch.setattr("oddsfox_catalogue.capture.runner._run_scan", explode)

    assert main(["refresh", "--mode", "bootstrap"]) == exit_code

    settings = load_settings()
    ledger = Ledger(settings.ledger_path)
    try:
        runs = ledger.stage_runs()
    finally:
        ledger.close()
    assert len(runs) == 1
    assert runs[0]["stage"] == "capture:bootstrap"
    assert runs[0]["status"] == "failed"
    assert runs[0]["error"] == f"Terminated: {signal.Signals(signum).name}"


def test_sigterm_during_argument_parsing_exits_143(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        os.kill(os.getpid(), signal.SIGTERM)
        raise AssertionError("SIGTERM was not turned into Terminated")

    monkeypatch.setattr("oddsfox_catalogue.cli.argparse.ArgumentParser.parse_args", boom)
    assert main(["status"]) == 143
