from pathlib import Path

import pytest

from oddsfox_catalogue.config import load_settings, settings_as_dict


def test_defaults_match_committed_file() -> None:
    root = Path(__file__).resolve().parents[2]
    settings = load_settings(root=root, env={})
    assert settings.gamma.base_url == "https://gamma-api.polymarket.com"
    assert settings.gamma.requests_per_second == 5.0
    assert settings.gamma.read_timeout_s == 60.0
    assert settings.gamma.max_retries == 12
    assert settings.gamma.backoff_base_s == 5.0
    assert settings.gamma.backoff_cap_s == 300.0
    assert settings.gamma.include_chat is False
    assert settings.capture.id_partition_size == 50_000
    assert settings.capture.max_id_override == 0
    assert settings.capture.workers == 4
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
