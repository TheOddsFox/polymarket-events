import json
from dataclasses import replace

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
    assert json.loads((root / "catalogue.json").read_text())["contract"].endswith("store.v2")
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
    with pytest.raises(CurrentError, match="ceiling"):
        load_current_settings(root, env={"CATALOGUE_CATALOGUE_MAX_REQUESTS": "25001"})
    with pytest.raises(CurrentError, match="at most"):
        load_current_settings(root, env={"CATALOGUE_GAMMA_REQUESTS_PER_SECOND": "11"})
    root.mkdir()
    (root / "config.toml").write_text("[load]\nmax_pages_per_run=1\n")
    with pytest.raises(CurrentError, match="legacy"):
        load_current_settings(root, env={})


def test_admission_checks_reserve_and_retained(tmp_path):
    root = validate_root(tmp_path / "root", initialize=True)
    settings = CurrentSettings(root)
    with pytest.raises(CurrentError, match="reserve"):
        admit(replace(settings, limits=replace(settings.limits, free_reserve_bytes=2**63)))
    with pytest.raises(CurrentError, match="retained"):
        admit(replace(settings, limits=replace(settings.limits, max_retained_bytes=1)))
