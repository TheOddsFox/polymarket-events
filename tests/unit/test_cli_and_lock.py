import json
import logging
import os
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import oddsfox_catalogue.runlock as runlock_module
from oddsfox_catalogue.cli import configure_logging, main
from oddsfox_catalogue.current_config import CurrentSettings
from oddsfox_catalogue.metadata import _catalogue_identity
from oddsfox_catalogue.runlock import RunBusy, run_lock


def test_http_client_does_not_log_request_urls() -> None:
    configure_logging()
    assert logging.getLogger("httpx").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() == logging.WARNING


def test_version_prints_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == "1.0.0"


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


def test_run_lock_preserves_lock_file_and_rejects_unsafe_paths(tmp_path: Path) -> None:
    lock_path = tmp_path / ".catalogue.lock"
    lock_path.write_bytes(b"existing lock metadata")
    with run_lock(lock_path):
        assert lock_path.read_bytes() == b"existing lock metadata"

    source = tmp_path / "config.toml"
    source.write_bytes(b"catalogue configuration")
    lock_path.unlink()
    lock_path.hardlink_to(source)
    with pytest.raises(ValueError, match="single-link regular file"), run_lock(lock_path):
        pass
    assert source.read_bytes() == b"catalogue configuration"

    lock_path.unlink()
    target = tmp_path / "external"
    target.write_bytes(b"external data")
    lock_path.symlink_to(target)
    with pytest.raises(OSError), run_lock(lock_path):
        pass
    assert target.read_bytes() == b"external data"

    lock_path.unlink()
    os.mkfifo(lock_path)
    with pytest.raises(ValueError, match="single-link regular file"), run_lock(lock_path):
        pass
    assert lock_path.is_fifo()


def test_metadata_identity_rejects_hardlinked_store_without_mutating_it(tmp_path: Path) -> None:
    root = tmp_path / "catalogue"
    root.mkdir()
    store = root / "catalogue.duckdb"
    store.write_bytes(b"database sentinel")
    (root / ".catalogue.lock").hardlink_to(store)
    before = store.read_bytes()

    with pytest.raises(ValueError, match="single-link regular file"):
        _catalogue_identity(CurrentSettings(root), ["1"])

    assert store.read_bytes() == before


def test_run_lock_detects_path_replacement_and_releases_original_descriptor(tmp_path: Path) -> None:
    lock_path = tmp_path / ".catalogue.lock"
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement lock")

    with (
        pytest.raises(ValueError, match="changed during the locked operation"),
        run_lock(lock_path),
    ):
        lock_path.unlink()
        replacement.rename(lock_path)

    assert lock_path.read_bytes() == b"replacement lock"
    with run_lock(lock_path):
        pass


def test_run_lock_rechecks_inode_after_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_path = tmp_path / ".catalogue.lock"
    source = tmp_path / "catalogue.duckdb"
    source.write_bytes(b"database sentinel")
    original_lstat = os.lstat
    checks = 0

    def replace_before_second_check(path, *args, **kwargs):
        nonlocal checks
        if Path(path) == lock_path:
            checks += 1
            if checks == 2:
                lock_path.unlink()
                lock_path.hardlink_to(source)
        return original_lstat(path, *args, **kwargs)

    monkeypatch.setattr(runlock_module.os, "lstat", replace_before_second_check)
    with pytest.raises(ValueError, match="changed while acquiring"), run_lock(lock_path):
        pass

    assert source.read_bytes() == b"database sentinel"


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
