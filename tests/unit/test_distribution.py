import importlib.util
import zipfile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "distribution_audit", Path(__file__).resolve().parents[2] / "scripts/verify-distribution.py"
)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def wheel(tmp_path: Path, extra: str | None = None) -> Path:
    path = tmp_path / "test.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name in ("__init__.py", "cli.py", "dbt/dbt_project.yml", "dbt/profiles.yml"):
            archive.writestr("oddsfox_catalogue/" + name, "synthetic")
        if extra:
            archive.writestr(extra, "synthetic")
    return path


def test_distribution_audit_accepts_minimal_installed_resources(tmp_path: Path) -> None:
    assert module.audit(wheel(tmp_path))["members"] == 4


@pytest.mark.parametrize(
    "extra",
    [
        "oddsfox_catalogue/dbt/target/generated.py",
        "oddsfox_catalogue/credentials.json",
        "oddsfox_catalogue/data/raw/page.sql",
        "../outside.py",
    ],
)
def test_distribution_audit_rejects_generated_secret_and_escape_members(
    tmp_path: Path, extra: str
) -> None:
    with pytest.raises(ValueError):
        module.audit(wheel(tmp_path, extra))
