import json
import logging
import os
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from oddsfox_catalogue.cli import configure_logging, main
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


def test_status_without_current_store_does_not_create_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    assert main(["status"]) == 3
    assert "not initialized" in capsys.readouterr().out
    assert list(tmp_path.iterdir()) == []


def test_run_lock_is_exclusive(tmp_path: Path) -> None:
    lock_path = tmp_path / ".state" / "catalogue.lock"

    def competing_writer():
        with pytest.raises(RunBusy), run_lock(lock_path):
            pass

    with run_lock(lock_path), ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(competing_writer).result(timeout=5)
    with run_lock(lock_path):
        pass


def test_nested_owner_lock_preserves_process_exclusion_after_inner_error(tmp_path):
    path = tmp_path / "owner.lock"
    code = (
        "from filelock import FileLock, Timeout\n"
        "import sys\n"
        "try:\n FileLock(sys.argv[1]).acquire(timeout=0)\n"
        "except Timeout:\n sys.exit(3)\n"
    )
    with run_lock(path):
        with (
            pytest.raises(RuntimeError, match="inner"),
            run_lock(tmp_path / "nested" / ".." / "owner.lock", timeout=0.1),
        ):
            raise RuntimeError("inner")
        result = subprocess.run([sys.executable, "-c", code, str(path)], timeout=5)
        assert result.returncode == 3
    assert subprocess.run([sys.executable, "-c", code, str(path)], timeout=5).returncode == 0


def test_capture_rejects_unknown_mode() -> None:
    with pytest.raises(SystemExit):
        main(["capture", "--mode", "hourly"])


def test_sigterm_during_argument_parsing_exits_143(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        os.kill(os.getpid(), signal.SIGTERM)
        raise AssertionError("SIGTERM was not turned into Terminated")

    monkeypatch.setattr("oddsfox_catalogue.cli.argparse.ArgumentParser.parse_args", boom)
    assert main(["status"]) == 143
