import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from oddsfox_catalogue.current_config import (
    CurrentSettings,
    admit,
    connect_store,
    load_current_settings,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError


def test_unknown_and_legacy_roots_are_untouched(tmp_path):
    root = tmp_path / "legacy"
    root.mkdir()
    (root / "raw").mkdir()
    before = list(root.iterdir())
    with pytest.raises(CurrentError, match="unstamped"):
        validate_root(root, initialize=True)
    assert list(root.iterdir()) == before
    assert not (root / ".catalogue.lock").exists()


def test_stamp_and_bounded_connection(tmp_path):
    root = validate_root(tmp_path / "new", initialize=True)
    settings = CurrentSettings(root)
    with connect_store(settings) as con:
        assert con.execute("select 42").fetchone() == (42,)
    assert json.loads((root / "catalogue.json").read_text())["contract"].endswith("store.v4")
    assert admit(settings)["retained_bytes"] > 0


def test_paths_and_nested_roots_rejected(tmp_path):
    root = validate_root(tmp_path / "root", initialize=True)
    with pytest.raises(CurrentError, match="nested"):
        validate_root(root / "child", initialize=True)
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(CurrentError, match="symlink"):
        validate_root(link)
    with pytest.raises(CurrentError, match="traversal"):
        validate_root(root / ".." / "other")


def test_limits_and_legacy_config(tmp_path):
    root = tmp_path / "new"
    settings = load_current_settings(root, env={})
    assert settings.gamma.requests_per_second == 10
    with pytest.raises(CurrentError, match="hard maximum"):
        load_current_settings(root, env={"CATALOGUE_CATALOGUE_MAX_REQUESTS": "25001"})
    with pytest.raises(CurrentError, match="at most"):
        load_current_settings(root, env={"CATALOGUE_GAMMA_REQUESTS_PER_SECOND": "11"})
    root.mkdir()
    (root / "config.toml").write_text("[paths]\nroot='data'\n")
    with pytest.raises(CurrentError, match="legacy"):
        load_current_settings(root, env={})


def test_zero_one_config_with_retired_keys_loads_and_current_limits_apply(tmp_path):
    root = tmp_path / "v0"
    root.mkdir()
    (root / "config.toml").write_text(
        "[gamma]\nrequests_per_second = 10.0\nmax_retries = 4\nbackoff_cap_s = 30.0\n\n"
        "[catalogue]\nmax_retained_bytes = 137438953472\nmax_temp_bytes = 8589934592\n"
        "free_reserve_bytes = 34359738368\nmax_raw_bytes = 8589934592\n"
        "raw_max_age_s = 604800\nactive_recheck_s = 72000\ncold_recheck_s = 5011200\n\n"
        '[load]\ndataset_name = "bronze"\nmax_pages_per_run = 200\n'
        'duckdb_memory_limit = "2GB"\nduckdb_threads = 2\n'
    )
    limits = load_current_settings(root, env={}).limits
    assert limits.max_retained_bytes == 128 * 1024**3
    assert limits.max_temp_bytes == 8 * 1024**3
    assert limits.free_reserve_bytes == 32 * 1024**3
    assert limits.cold_recheck_s == 58 * 86400
    retired = {"max_raw_bytes", "raw_max_age_s"}
    assert retired.isdisjoint(asdict(limits))


def test_unknown_future_catalogue_key_still_fails(tmp_path):
    root = tmp_path / "future"
    root.mkdir()
    (root / "config.toml").write_text("[catalogue]\nmax_future_bytes = 1\n")
    with pytest.raises(ValueError, match=r"unknown keys in \[catalogue\]"):
        load_current_settings(root, env={})


def test_approved_defaults_and_sample_config_match(tmp_path):
    settings = load_current_settings(tmp_path / "root", env={})
    assert asdict(settings.limits) == {
        "max_duration_s": 1200.0,
        "max_requests": 25_000,
        "max_download_bytes": 4 * 1024**3,
        "max_response_bytes": 16 * 1024**2,
        "max_retained_bytes": 160 * 1024**3,
        "max_temp_bytes": 16 * 1024**3,
        "free_reserve_bytes": 8 * 1024**3,
        "active_recheck_s": 20 * 3600,
        "cold_recheck_s": 58 * 86400,
        "publication_reserve_s": 60.0,
    }
    sample = load_current_settings(
        tmp_path / "root",
        env={
            "CATALOGUE_CONFIG": str(Path(__file__).resolve().parents[2] / "config/catalogue.toml")
        },
    )
    assert sample == settings


@pytest.mark.parametrize("duration,retained_gib", [(1200, 160), (300, 512)])
def test_root_config_accepts_values_up_to_hard_maximums(tmp_path, duration, retained_gib):
    settings = load_current_settings(
        tmp_path / "root",
        env={
            "CATALOGUE_CATALOGUE_MAX_DURATION_S": str(duration),
            "CATALOGUE_CATALOGUE_MAX_RETAINED_BYTES": str(retained_gib * 1024**3),
        },
    )
    assert settings.limits.max_duration_s == duration
    assert settings.limits.max_retained_bytes == retained_gib * 1024**3


@pytest.mark.parametrize(
    "name,value",
    [
        ("max_duration_s", 1200.001),
        ("max_retained_bytes", 512 * 1024**3 + 1),
        ("max_requests", 25_001),
        ("max_download_bytes", 4 * 1024**3 + 1),
        ("max_response_bytes", 16 * 1024**2 + 1),
        ("max_temp_bytes", 64 * 1024**3 + 1),
        ("free_reserve_bytes", 256 * 1024**3 + 1),
        ("publication_reserve_s", 59.999),
        ("publication_reserve_s", 1200),
    ],
)
def test_resource_limits_reject_values_outside_approved_policy(tmp_path, name, value):
    with pytest.raises(CurrentError):
        load_current_settings(
            tmp_path / "root", env={f"CATALOGUE_CATALOGUE_{name.upper()}": str(value)}
        )


@pytest.mark.parametrize("name", ["max_duration_s", "max_retained_bytes"])
@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_resource_limit_expansion_preserves_positive_finite_validation(tmp_path, name, value):
    with pytest.raises((CurrentError, ValueError)):
        load_current_settings(tmp_path / "root", env={f"CATALOGUE_CATALOGUE_{name.upper()}": value})


def test_admission_checks_reserve_and_retained(tmp_path):
    root = validate_root(tmp_path / "root", initialize=True)
    settings = CurrentSettings(root)
    with pytest.raises(CurrentError, match="reserve"):
        admit(replace(settings, limits=replace(settings.limits, free_reserve_bytes=2**63)))
    with pytest.raises(CurrentError, match="retained"):
        admit(replace(settings, limits=replace(settings.limits, max_retained_bytes=1)))
