"""Combined accounting shares traversal, never snapshots across calls implicitly."""

import os
from dataclasses import FrozenInstanceError, replace

import pytest

from oddsfox_catalogue import limits
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


@pytest.fixture(params=["dirfd", "path"], autouse=True)
def scanner(request, monkeypatch):
    if request.param == "path":
        monkeypatch.setattr(os, "supports_dir_fd", set())
    elif (
        not hasattr(os, "fwalk")
        or os.stat not in os.supports_dir_fd
        or os.listdir not in os.supports_fd
    ):
        pytest.skip("descriptor-relative accounting unavailable")


def test_combined_usage_counts_overlapping_external_and_hard_link_paths(tmp_path, monkeypatch):
    settings = Settings(tmp_path / "operator")
    data, state, temporary = [tmp_path / name for name in ("data", "state", "temp")]
    warehouse = tmp_path / "warehouse.duckdb"
    settings = replace(
        settings,
        paths=replace(
            settings.paths, data_dir=str(data), state_dir=str(state), warehouse_file=str(warehouse)
        ),
    )
    monkeypatch.setattr(Settings, "temporary_dir", property(lambda _: temporary))
    monkeypatch.setattr(Settings, "dlt_pipelines_dir", property(lambda _: temporary / "dlt"))
    for directory in (settings.root, data, state, temporary / "dlt"):
        directory.mkdir(parents=True)
    (settings.root / "notes").write_bytes(b"12")
    (data / "capture.partial").write_bytes(b"123")
    os.link(data / "capture.partial", data / "other-name")
    (state / "checkpoint").write_bytes(b"1234")
    warehouse.write_bytes(b"12345")
    warehouse.with_name(warehouse.name + ".wal").write_bytes(b"123456")
    (temporary / "spill.partial").write_bytes(b"1234567")
    (temporary / "dlt" / "job").write_bytes(b"12345678")
    usage = limits.measure_storage(settings)
    assert usage == limits.StorageUsage(31, 15)
    assert usage.retained_bytes == limits.retained_bytes(settings)
    assert usage.temporary_bytes == limits.temporary_bytes(settings)


def test_overlapping_duplicate_roots_are_scanned_once(tmp_path, monkeypatch):
    settings = Settings(tmp_path)
    settings = replace(settings, paths=replace(settings.paths, state_dir="data"))
    settings.temporary_dir.mkdir(parents=True)
    (settings.temporary_dir / "spill").write_bytes(b"123")
    settings.dlt_pipelines_dir.mkdir(parents=True)
    (settings.dlt_pipelines_dir / "job").write_bytes(b"12")
    original = limits._scan_directory
    scanned = []

    def scan(root, account=None):
        scanned.append(root)
        return original(root, account)

    monkeypatch.setattr(limits, "_scan_directory", scan)
    assert limits.measure_storage(settings) == limits.StorageUsage(5, 5)
    assert scanned == [tmp_path]


def test_measurement_and_default_limit_calls_are_fresh(tmp_path):
    settings = Settings(tmp_path)
    settings = replace(
        settings, capture=replace(settings.capture, max_retained_bytes=5, max_temp_bytes=4)
    )
    settings.temporary_dir.mkdir(parents=True)
    payload = settings.temporary_dir / "spill"
    payload.write_bytes(b"12")
    usage = limits.measure_storage(settings)
    assert usage == limits.StorageUsage(2, 2)
    payload.write_bytes(b"12345")
    assert limits.measure_storage(settings) == limits.StorageUsage(5, 5)
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        limits.enforce_storage_limits(settings)
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        limits.remaining_temp_bytes(settings)
    payload.unlink()
    assert limits.measure_storage(settings) == limits.StorageUsage(0, 0)
    limits.enforce_storage_limits(settings)
    assert limits.remaining_temp_bytes(settings) == 4
    with pytest.raises(FrozenInstanceError):
        usage.retained_bytes = 0


def test_explicit_usage_reuses_snapshot_without_scanning(tmp_path, monkeypatch):
    settings = Settings(tmp_path)
    settings = replace(
        settings, capture=replace(settings.capture, max_retained_bytes=5, max_temp_bytes=4)
    )
    usage = limits.StorageUsage(3, 2)

    def unexpected_scan(_):
        pytest.fail("explicit usage must not scan storage")

    monkeypatch.setattr(limits, "measure_storage", unexpected_scan)
    assert limits.remaining_temp_bytes(settings, usage=usage) == 2
    limits.enforce_storage_limits(settings, additional_bytes=2, usage=usage)
    with pytest.raises(RequestBudgetExceeded, match="retained"):
        limits.enforce_storage_limits(settings, additional_bytes=3, usage=usage)
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        limits.enforce_storage_limits(settings, usage=limits.StorageUsage(0, 5))
    with pytest.raises(RequestBudgetExceeded, match="temporary"):
        limits.remaining_temp_bytes(settings, usage=limits.StorageUsage(0, 4))


@pytest.mark.parametrize("alias_parent", [False, True])
def test_redundant_input_symlink_is_validated_before_root_deduplication(tmp_path, alias_parent):
    settings = Settings(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    (tmp_path / "alias").symlink_to(target, target_is_directory=True)
    configured = "alias/../data" if alias_parent else "alias"
    settings = replace(settings, paths=replace(settings.paths, data_dir=configured))
    with pytest.raises(RequestBudgetExceeded, match="symlink"):
        limits.measure_storage(settings)


def test_external_regular_file_can_be_a_temporary_root(tmp_path, monkeypatch):
    settings = Settings(tmp_path / "missing")
    temporary = tmp_path / "external.partial"
    temporary.write_bytes(b"123")
    monkeypatch.setattr(Settings, "temporary_dir", property(lambda _: temporary))
    assert limits.measure_storage(settings) == limits.StorageUsage(0, 3)


def test_combined_measurement_visits_overlapping_directories_once(tmp_path, monkeypatch):
    if not hasattr(os, "fwalk") or os.stat not in os.supports_dir_fd:
        pytest.skip("descriptor traversal counter requires fwalk")
    settings = Settings(tmp_path)
    settings.temporary_dir.mkdir(parents=True)
    settings.dlt_pipelines_dir.mkdir(parents=True)
    (settings.temporary_dir / "spill").write_bytes(b"123")
    (settings.dlt_pipelines_dir / "job").write_bytes(b"12")
    original = os.fwalk
    visits = []

    def counted_walk(*args, **kwargs):
        for point in original(*args, **kwargs):
            visits.append(point[0])
            yield point

    monkeypatch.setattr(os, "fwalk", counted_walk)
    assert limits.measure_storage(settings) == limits.StorageUsage(5, 5)
    combined_visits = len(visits)
    assert len(set(visits)) == combined_visits
    visits.clear()
    assert limits.retained_bytes(settings) == limits.temporary_bytes(settings) == 5
    assert len(visits) == combined_visits + 2


def test_similar_path_prefixes_do_not_expand_allowances(tmp_path, monkeypatch):
    settings = Settings(tmp_path / "operator")
    temporary = settings.root / "tmp"
    sibling = settings.root / "tmp-other"
    monkeypatch.setattr(Settings, "temporary_dir", property(lambda _: temporary))
    for directory in (temporary, sibling):
        directory.mkdir(parents=True)
    (temporary / "job").write_bytes(b"123")
    (sibling / "retained").write_bytes(b"12345")
    assert limits.measure_storage(settings) == limits.StorageUsage(8, 3)


def test_external_file_prefix_does_not_include_a_sibling(tmp_path, monkeypatch):
    settings = Settings(tmp_path / "operator")
    settings.root.mkdir()
    temporary = settings.root / "spill"
    temporary.write_bytes(b"123")
    temporary.with_name("spill-extra").write_bytes(b"12345")
    monkeypatch.setattr(Settings, "temporary_dir", property(lambda _: temporary))
    assert limits.measure_storage(settings) == limits.StorageUsage(8, 3)
