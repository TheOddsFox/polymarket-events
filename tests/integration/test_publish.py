"""Publication: release layout, fingerprints, pointer swap, retention, and blocking."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse
from fakes.harness import FIXED_NOW, make_settings
from oddsfox_catalogue.cli import main
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.publish import (
    CURRENT_POINTER,
    PROJECTION_TABLE_QUERIES,
    PublishBlocked,
    current_release,
    fingerprint_parquet,
    prune_releases,
    publish_release,
)
from oddsfox_catalogue.warehouse import BaselineMissing

TESTS_DIR = Path(__file__).resolve().parents[1]
PUBLISH_CHILD = TESTS_DIR / "fakes" / "publish_child.py"


@pytest.fixture(scope="module")
def built_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("publish_root")
    build_warehouse(root)
    return root


def _copy_of_built(built_root: Path, dest: Path) -> Settings:
    """A fresh project root holding a copy of the built warehouse, so tests never share state."""
    settings = make_settings(dest)
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    source = make_settings(built_root).warehouse_path
    settings.warehouse_path.write_bytes(source.read_bytes())
    return settings


def _count(path: Path) -> int:
    con = duckdb.connect()
    try:
        return int(con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
    finally:
        con.close()


def test_release_has_every_table_fingerprint_and_pointer(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW, git_sha="testsha")

    assert info.path == settings.published_dir / "releases" / info.release_id
    assert set(info.tables) == set(PROJECTION_TABLE_QUERIES)
    for name, table in info.tables.items():
        parquet = info.path / f"{name}.parquet"
        assert parquet.exists()
        assert table["rows"] == _count(parquet), f"{name} fingerprint row count disagrees"
        assert table["rows"] > 0 or name == "event_series"  # demo world has no series

    pointer = current_release(settings)
    assert pointer is not None and pointer["release_id"] == info.release_id
    manifest = json.loads((info.path / "release.json").read_text(encoding="utf-8"))
    assert manifest["git_sha"] == "testsha"
    assert manifest["batch_ids"], "release must record which batches it was built from"
    import hashlib

    assert (
        pointer["manifest_sha256"]
        == hashlib.sha256((info.path / "release.json").read_bytes()).hexdigest()
    )
    assert not list((settings.published_dir / "releases").glob(".staging-*"))


def test_events_release_matches_the_mart(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW, git_sha=None)
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        mart = con.execute("SELECT count(*) FROM marts.mart_event_catalogue").fetchone()[0]
    finally:
        con.close()
    assert info.tables["events"]["rows"] == mart


def test_fingerprint_does_not_depend_on_row_order(tmp_path) -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE a AS SELECT * FROM (VALUES (1,'x'),(2,'y'),(3,'z')) t(n, s)")
        con.execute("CREATE TABLE b AS SELECT * FROM (VALUES (3,'z'),(1,'x'),(2,'y')) t(n, s)")
        con.execute("CREATE TABLE c AS SELECT * FROM (VALUES (1,'x'),(2,'y'),(4,'z')) t(n, s)")
        for name in ("a", "b", "c"):
            con.execute(f"COPY {name} TO '{tmp_path / name}.parquet' (FORMAT PARQUET)")
        a = fingerprint_parquet(con, tmp_path / "a.parquet")
        b = fingerprint_parquet(con, tmp_path / "b.parquet")
        c = fingerprint_parquet(con, tmp_path / "c.parquet")
    finally:
        con.close()
    assert a == b, "same rows in another order must fingerprint identically"
    assert a != c, "a changed row must change the fingerprint"


def test_second_release_repoints_and_retention_keeps_current(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW, git_sha=None)
    second = publish_release(settings, now=FIXED_NOW + timedelta(days=1), git_sha=None)
    assert first.release_id != second.release_id
    assert current_release(settings)["release_id"] == second.release_id

    removed = prune_releases(settings, keep=1)
    assert removed == [first.release_id]
    remaining = sorted(p.name for p in (settings.published_dir / "releases").iterdir())
    assert remaining == [second.release_id]


def test_release_ids_never_collide(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    ids = {publish_release(settings, now=FIXED_NOW, git_sha=None).release_id for _ in range(3)}
    assert len(ids) == 3


def test_publish_without_a_warehouse_is_blocked_and_leaves_nothing(tmp_path) -> None:
    settings = make_settings(tmp_path)
    with pytest.raises(BaselineMissing):
        publish_release(settings, now=FIXED_NOW, git_sha=None)
    assert current_release(settings) is None
    assert not settings.published_dir.exists(), "a blocked publish must create no directories"


def test_empty_warehouse_is_blocked(tmp_path) -> None:
    settings = make_settings(tmp_path)
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.connect(str(settings.warehouse_path)).close()
    with pytest.raises(BaselineMissing):
        publish_release(settings, now=FIXED_NOW, git_sha=None)
    assert current_release(settings) is None
    releases = settings.published_dir / "releases"
    assert not releases.exists() or not any(releases.iterdir())


def test_misaligned_outcomes_block_publication(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    con = duckdb.connect(str(settings.warehouse_path))
    try:
        # The model is a view in dbt, so replace it with one row that represents a misalignment.
        con.execute("CREATE OR REPLACE VIEW core.quarantine_market_outcomes AS SELECT 1 AS marker")
    finally:
        con.close()
    with pytest.raises(PublishBlocked, match="misaligned"):
        publish_release(settings, now=FIXED_NOW, git_sha=None)
    assert current_release(settings) is None


def test_crash_mid_publish_keeps_the_previous_release_current(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW, git_sha=None)

    env = {**os.environ, "CATALOGUE_FAULT": "mid_publish", "PYTHONPATH": str(TESTS_DIR)}
    crashed = subprocess.run(
        [sys.executable, str(PUBLISH_CHILD), str(tmp_path)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr

    pointer = current_release(settings)
    assert pointer is not None and pointer["release_id"] == first.release_id, (
        "a crash before the pointer swap must leave the previous release current"
    )
    # The renamed-but-unreferenced release is an orphan. It must not be served.
    releases = sorted(p.name for p in (settings.published_dir / "releases").iterdir())
    assert len(releases) == 2

    # The next publish succeeds and moves the pointer; retention then removes the orphan.
    recovered = publish_release(settings, now=FIXED_NOW + timedelta(days=1), git_sha=None)
    assert current_release(settings)["release_id"] == recovered.release_id
    assert (settings.published_dir / CURRENT_POINTER).exists()
    prune_releases(settings, keep=1)
    assert sorted(p.name for p in (settings.published_dir / "releases").iterdir()) == [
        recovered.release_id
    ]


def test_cli_current_reports_no_release_yet(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    assert main(["current"]) == 0
    assert "no published release yet" in capsys.readouterr().out


def test_cli_publish_reports_a_blocked_release_with_exit_code_3(
    tmp_path, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    assert main(["publish"]) == 3
    assert "error:" in capsys.readouterr().err


def test_replay_publishes_from_raw_pages_without_calling_gamma(
    tmp_path, monkeypatch, capsys
) -> None:
    """Replay loads what is already captured, builds, and publishes. Gamma is unreachable here,
    so any network call would fail the command."""
    from fakes.built_warehouse import capture_and_load
    from oddsfox_catalogue.dbt_runner import DBT_DIR

    capture_and_load(tmp_path)
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    monkeypatch.setenv("CATALOGUE_PATHS_DBT_PROJECT_DIR", str(DBT_DIR))
    monkeypatch.setenv("CATALOGUE_PATHS_DBT_PROFILES_DIR", str(DBT_DIR))
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "http://127.0.0.1:9")
    assert main(["replay"]) == 0
    out = capsys.readouterr().out
    assert '"release_id"' in out
    settings = make_settings(tmp_path)
    assert current_release(settings) is not None
