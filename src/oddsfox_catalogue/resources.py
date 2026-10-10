"""Immutable resources installed with the catalogue package."""

from importlib.resources import files
from pathlib import Path


def dbt_project_dir() -> Path:
    return Path(str(files("oddsfox_catalogue").joinpath("dbt")))
