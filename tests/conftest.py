"""Tests use explicit fixtures and never contact live Gamma."""

import os
import tempfile

from oddsfox_catalogue.current_config import data_path, repository_data_dir

repo_data_dir = repository_data_dir()
if repo_data_dir is not None:
    pytest_temp_dir = data_path(repo_data_dir / "tmp")
    pytest_temp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = str(pytest_temp_dir)
    tempfile.tempdir = str(pytest_temp_dir)
