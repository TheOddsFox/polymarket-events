from dataclasses import replace
from pathlib import Path

import pytest

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.dbt_runner import dbt_environment
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.limits import (
    directory_bytes,
    enforce_storage_limits,
    remaining_temp_bytes,
    retained_bytes,
    temporary_bytes,
)


def test_retained_limit_counts_external_warehouse_and_pending_bytes(tmp_path: Path) -> None:
    settings = Settings(tmp_path / "operator")
    external = tmp_path / "external.duckdb"
    external.write_bytes(b"12345")
    settings = replace(
        settings,
        paths=replace(settings.paths, warehouse_file=str(external)),
        capture=replace(settings.capture, max_retained_bytes=7),
    )
    enforce_storage_limits(settings, additional_bytes=2)
    with pytest.raises(RequestBudgetExceeded, match="retained"):
        enforce_storage_limits(settings, additional_bytes=3)


def test_nested_data_state_are_not_double_counted_and_temp_is_bounded(tmp_path: Path) -> None:
    settings = Settings(tmp_path)
    settings = replace(
        settings,
        paths=replace(settings.paths, state_dir="data/state"),
        capture=replace(settings.capture, max_retained_bytes=4, max_temp_bytes=3),
    )
    settings.temporary_dir.mkdir(parents=True)
    (settings.temporary_dir / "spill").write_bytes(b"123")
    enforce_storage_limits(settings)
    (settings.temporary_dir / "spill").write_bytes(b"1234")
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        enforce_storage_limits(settings)


def test_storage_scan_rejects_symlinks_instead_of_following_them(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    (root / "outside").symlink_to(tmp_path)
    with pytest.raises(RequestBudgetExceeded, match="symlink"):
        directory_bytes(root)


@pytest.mark.parametrize("removed", ["file", "directory"])
def test_storage_scan_tolerates_jobs_removed_during_traversal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, removed: str
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    (root / "retained").write_bytes(b"12345")
    job = root / "job"
    if removed == "file":
        job.write_bytes(b"temporary")
        original = Path.lstat

        def lstat(path, *args, **kwargs):
            if path == job:
                job.unlink()
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "lstat", lstat)
    else:
        job.mkdir()
        original = Path.iterdir

        def iterdir(path):
            if path == job:
                job.rmdir()
            return original(path)

        monkeypatch.setattr(Path, "iterdir", iterdir)
    assert directory_bytes(root) == 5


def test_storage_scan_does_not_hide_permission_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    original = Path.iterdir

    def iterdir(path):
        if path == root:
            raise PermissionError("unreadable storage")
        return original(path)

    monkeypatch.setattr(Path, "iterdir", iterdir)
    with pytest.raises(PermissionError, match="unreadable storage"):
        directory_bytes(root)


def test_dlt_load_packages_count_toward_temporary_allowance(tmp_path: Path) -> None:
    settings = Settings(tmp_path)
    settings = replace(settings, capture=replace(settings.capture, max_temp_bytes=4))
    packages = settings.dlt_pipelines_dir / "pipeline" / "load"
    packages.mkdir(parents=True)
    (packages / "package.jsonl").write_bytes(b"1234")
    enforce_storage_limits(settings)
    (packages / "package.jsonl").write_bytes(b"12345")
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        enforce_storage_limits(settings)


def test_nested_dlt_working_path_is_counted_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(tmp_path)
    working = settings.temporary_dir / "dlt"
    monkeypatch.setattr(Settings, "dlt_pipelines_dir", property(lambda _: working))
    working.mkdir(parents=True)
    (working / "package.jsonl").write_bytes(b"123")
    (settings.temporary_dir / "spill").write_bytes(b"12")
    assert temporary_bytes(settings) == 5
    assert retained_bytes(settings) == 5


def test_external_dlt_working_path_counts_in_both_allowances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(tmp_path / "operator")
    working = tmp_path / "external-dlt"
    monkeypatch.setattr(Settings, "dlt_pipelines_dir", property(lambda _: working))
    working.mkdir()
    (working / "extract.jsonl").write_bytes(b"123")
    settings = replace(settings, capture=replace(settings.capture, max_retained_bytes=2))
    assert temporary_bytes(settings) == retained_bytes(settings) == 3
    with pytest.raises(RequestBudgetExceeded, match="retained"):
        enforce_storage_limits(settings)


def test_dbt_spill_allowance_subtracts_existing_working_files(tmp_path: Path) -> None:
    settings = Settings(tmp_path)
    settings = replace(settings, capture=replace(settings.capture, max_temp_bytes=10))
    settings.dlt_pipelines_dir.mkdir(parents=True)
    package = settings.dlt_pipelines_dir / "package.jsonl"
    package.write_bytes(b"12345678")
    assert remaining_temp_bytes(settings) == 2
    assert dbt_environment(settings)["CATALOGUE_DUCKDB_MAX_TEMP_BYTES"] == "2"
    package.write_bytes(b"1234567890")
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        dbt_environment(settings)
