"""Quality limits: validation of the [quality] section and their translation into dbt vars."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from oddsfox_catalogue.config import load_settings
from oddsfox_catalogue.dbt_runner import quality_dbt_vars, with_quality_vars


def _settings(tmp_path: Path, **env: str):
    return load_settings(root=tmp_path, env=env)


def test_quality_vars_come_from_the_configured_limits(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT="25",
        CATALOGUE_QUALITY_UNRESOLVED_REFERENCE_MAX_RATIO="0.05",
    )
    assert quality_dbt_vars(settings) == {
        "max_open_events_drop_pct": 0.25,
        "max_unresolved_reference_ratio": 0.05,
    }


def test_with_quality_vars_appends_when_no_vars_given(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    args = with_quality_vars(settings, ["build"])
    assert args[:1] == ["build"]
    assert args[1] == "--vars"
    assert json.loads(args[2]) == quality_dbt_vars(settings)


def test_with_quality_vars_lets_the_caller_override_a_key(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    args = with_quality_vars(settings, ["build", "--vars", '{"max_open_events_drop_pct": 0.9}'])
    merged = json.loads(args[args.index("--vars") + 1])
    assert merged["max_open_events_drop_pct"] == 0.9
    assert (
        merged["max_unresolved_reference_ratio"]
        == quality_dbt_vars(settings)["max_unresolved_reference_ratio"]
    )
    assert args.count("--vars") == 1


def test_with_quality_vars_rejects_a_non_object_vars(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    with pytest.raises(ValueError, match="JSON object"):
        with_quality_vars(settings, ["build", "--vars", "[1, 2]"])


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
        {"CATALOGUE_QUALITY_UNRESOLVED_REFERENCE_MAX_RATIO": "2"},
        {"CATALOGUE_QUALITY_OPEN_EVENTS_DROP_ERROR_PCT": "150"},
    ],
)
def test_out_of_range_limits_are_rejected(tmp_path: Path, env: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="must be between"):
        _settings(tmp_path, **env)
