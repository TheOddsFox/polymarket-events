"""Shared pytest setup."""

from __future__ import annotations

import os

# Importing the Dagster definitions module builds them from the project root. Tests build
# definitions explicitly with a temporary root instead, so keep the module import lazy.
os.environ.setdefault("CATALOGUE_DAGSTER_LAZY", "1")
