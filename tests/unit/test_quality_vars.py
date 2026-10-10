"""Quality configuration no longer supplies legacy SQL publication baselines."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from oddsfox_catalogue.config import load_settings
from oddsfox_catalogue.dbt_runner import quality_dbt_vars, with_quality_vars


def _settings(tmp_path: Path, **env: str):
    return load_settings(root=tmp_path, env=env)


def test_quality_limits_do_not_become_legacy_sql_vars(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT="25",
    )
    assert quality_dbt_vars(settings) == {}
    assert settings.quality.open_events_drop_error_pct == 25


def test_with_quality_vars_appends_when_no_vars_given(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    args = with_quality_vars(settings, ["build"])
    assert args[:1] == ["build"]
    assert args[1] == "--vars"
    assert json.loads(args[2]) == quality_dbt_vars(settings)


def test_with_quality_vars_preserves_explicit_caller_vars(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    args = with_quality_vars(settings, ["build", "--vars", '{"projection_version": "v2"}'])
    merged = json.loads(args[args.index("--vars") + 1])
    assert merged == {"projection_version": "v2"}
    assert args.count("--vars") == 1


def test_with_quality_vars_rejects_a_non_object_vars(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    with pytest.raises(ValueError, match="JSON object"):
        with_quality_vars(settings, ["build", "--vars", "[1, 2]"])


def test_vars_equals_is_canonical_and_duplicates_are_rejected(tmp_path):
    settings = _settings(tmp_path)
    assert with_quality_vars(settings, ["build", '--vars={"projection_version":"v2"}']) == [
        "build",
        "--vars",
        '{"projection_version": "v2"}',
    ]
    with pytest.raises(ValueError, match="duplicate"):
        with_quality_vars(settings, ["build", "--vars={}", "--vars", "{}"])


def test_warn_above_error_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not exceed"):
        _settings(
            tmp_path,
            CATALOGUE_QUALITY_OPEN_EVENTS_DROP_WARN_PCT="40",
            CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT="30",
        )


@pytest.mark.parametrize(
    "env",
    [
        {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "1.5"},
        {"CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "-0.1"},
        {"CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT": "150"},
    ],
)
def test_out_of_range_limits_are_rejected(tmp_path: Path, env: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="must be between"):
        _settings(tmp_path, **env)
