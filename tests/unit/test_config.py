from pathlib import Path

import pytest

from oddsfox_catalogue.config import load_settings, settings_as_dict


def test_defaults_match_committed_file() -> None:
    root = Path(__file__).resolve().parents[2]
    settings = load_settings(root=root, env={})
    assert settings.gamma.base_url == "https://gamma-api.polymarket.com"
    assert settings.gamma.requests_per_second == 2.0
    assert settings.gamma.read_timeout_s == 60.0
    assert settings.gamma.max_retries == 12
    assert settings.gamma.backoff_base_s == 5.0
    assert settings.gamma.backoff_cap_s == 300.0
    assert settings.gamma.include_chat is False
    assert settings.capture.id_partition_size == 50_000
    assert settings.capture.max_id_override == 0
    assert settings.capture.workers == 1
    assert settings.load.max_pages_per_run == 200
    assert settings.warehouse_path == root / "data" / "warehouse" / "catalogue.duckdb"
    assert settings.ledger_path == root / ".state" / "ledger.sqlite"


def test_env_overrides_apply_with_correct_types(tmp_path: Path) -> None:
    env = {
        "CATALOGUE_GAMMA_REQUESTS_PER_SECOND": "1.5",
        "CATALOGUE_GAMMA_INCLUDE_CHAT": "true",
        "CATALOGUE_LOAD_MAX_PAGES_PER_RUN": "7",
    }
    settings = load_settings(root=tmp_path, env=env)
    assert settings.gamma.requests_per_second == 1.5
    assert settings.gamma.include_chat is True
    assert settings.load.max_pages_per_run == 7


def test_workers_below_one_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="workers"):
        load_settings(root=tmp_path, env={"CATALOGUE_CAPTURE_WORKERS": "0"})


def test_bad_boolean_override_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_settings(root=tmp_path, env={"CATALOGUE_GAMMA_INCLUDE_CHAT": "maybe"})


def test_unknown_key_in_file_is_rejected(tmp_path: Path) -> None:
    config = tmp_path / "catalogue.toml"
    config.write_text("[gamma]\nnot_a_setting = 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="not_a_setting"):
        load_settings(root=tmp_path, env={"CATALOGUE_CONFIG": str(config)})


def test_relative_paths_resolve_under_root(tmp_path: Path) -> None:
    settings = load_settings(root=tmp_path, env={})
    assert settings.raw_dir == tmp_path / "data" / "raw"
    assert settings.run_lock_path == tmp_path / ".state" / "catalogue.lock"


def test_settings_serialise() -> None:
    settings = load_settings(root=Path(__file__).resolve().parents[2], env={})
    data = settings_as_dict(settings)
    assert data["gamma"]["page_limit"] == 100


def test_finite_invocation_defaults_and_installed_resources(tmp_path: Path) -> None:
    settings = load_settings(root=tmp_path, env={})
    assert settings.capture.max_requests == 25_000
    assert settings.capture.max_download_bytes == 4 * 1024**3
    assert settings.capture.max_duration_s == 14_400
    assert settings.capture.max_response_bytes == 16 * 1024**2
    assert settings.capture.max_retained_bytes == 64 * 1024**3
    assert settings.capture.max_temp_bytes == 8 * 1024**3
    assert settings.dbt_project_dir.joinpath("dbt_project.yml").is_file()
    assert settings.dbt_profiles_dir == settings.dbt_project_dir
    assert not settings.dbt_project_dir.is_relative_to(tmp_path)
    assert settings.temporary_dir == tmp_path / ".state" / "tmp"


@pytest.mark.parametrize(
    "key",
    [
        "max_requests",
        "max_download_bytes",
        "max_response_bytes",
        "max_retained_bytes",
        "max_temp_bytes",
    ],
)
def test_nonpositive_allowances_are_rejected(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError, match=key):
        load_settings(root=tmp_path, env={f"CATALOGUE_CAPTURE_{key.upper()}": "0"})


@pytest.mark.parametrize("duration", ["0", "-1", "inf", "nan"])
def test_unbounded_or_invalid_duration_is_rejected(tmp_path: Path, duration: str) -> None:
    with pytest.raises(ValueError, match="max_duration_s"):
        load_settings(root=tmp_path, env={"CATALOGUE_CAPTURE_MAX_DURATION_S": duration})


def test_response_allowance_cannot_bypass_hard_cap(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="16 MiB"):
        load_settings(
            root=tmp_path, env={"CATALOGUE_CAPTURE_MAX_RESPONSE_BYTES": str(16 * 1024**2 + 1)}
        )


@pytest.mark.parametrize("memory", ["unlimited", "0GB", "-1GB", "infGB"])
def test_duckdb_memory_is_finite_and_positive(tmp_path: Path, memory: str) -> None:
    with pytest.raises(ValueError, match="duckdb_memory_limit"):
        load_settings(root=tmp_path, env={"CATALOGUE_LOAD_DUCKDB_MEMORY_LIMIT": memory})


def test_toml_boolean_cannot_supply_a_numeric_allowance(tmp_path: Path) -> None:
    config = tmp_path / "catalogue.toml"
    config.write_text("[capture]\nmax_requests=true\n")
    with pytest.raises(ValueError, match="max_requests"):
        load_settings(root=tmp_path, env={"CATALOGUE_CONFIG": str(config)})
