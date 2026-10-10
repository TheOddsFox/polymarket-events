"""Publication: release layout, fingerprints, pointer swap, retention, and blocking."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, capture_and_load, copy_built
from fakes.harness import FIXED_NOW, make_settings
from fakes.world import demo_world
from oddsfox_catalogue.cli import main
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import CRASH_EXIT_CODE
from oddsfox_catalogue.publish import (
    CURRENT_POINTER,
    PROJECTION_TABLE_QUERIES,
    PublishBlocked,
    current_release,
    fingerprint_parquet,
    publish_release,
    verify_release,
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
    return copy_built(built_root, dest)


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
        assert table["rows"] > 0 or name in {"event_series", "quarantine"}
        assert table["schema"] and len(table["semantic_sha256"]) == 64

    pointer = current_release(settings)
    assert pointer is not None and pointer["release_id"] == info.release_id
    manifest = json.loads((info.path / "release.json").read_text(encoding="utf-8"))
    assert manifest["git_sha"] == "testsha"
    assert manifest["contract"] == "oddsfox.polymarket.catalogue.v2"
    assert manifest["normalization_revision"] == "2"
    assert (info.path / "coverage.json").is_file()
    assert manifest["capture_inventory"]
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


@pytest.mark.parametrize("removed", ["page", "scan", "gzip_header"])
def test_missing_registered_capture_inventory_blocks_and_retains_pointer(
    built_root, tmp_path, removed
):
    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW)
    manifest = next(settings.raw_dir.glob("*/*/*/p*.manifest.json"))
    if removed == "gzip_header":
        payload = manifest.parent / json.loads(manifest.read_bytes())["file"]
        changed = bytearray(payload.read_bytes())
        changed[4] ^= 1  # Header mtime changes the object hash while retaining the decoded body.
        payload.write_bytes(changed)
    elif removed == "scan":
        shutil.rmtree(manifest.parent)
    else:
        payload = json.loads(manifest.read_bytes())["file"]
        (manifest.parent / payload).unlink()
        manifest.unlink()
    with pytest.raises(PublishBlocked, match="certification"):
        publish_release(settings, now=FIXED_NOW + timedelta(days=1))
    assert current_release(settings)["release_id"] == first.release_id


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


def test_second_release_repoints_and_retains_prior_release(built_root, tmp_path) -> None:
    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW, git_sha=None)
    second = publish_release(settings, now=FIXED_NOW + timedelta(days=1), git_sha=None)
    assert first.release_id != second.release_id
    assert current_release(settings)["release_id"] == second.release_id

    remaining = sorted(p.name for p in (settings.published_dir / "releases").iterdir())
    assert remaining == [first.release_id, second.release_id]
    verify_release(settings, first.path)


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
    with pytest.raises(PublishBlocked):
        publish_release(settings, now=FIXED_NOW, git_sha=None)
    assert current_release(settings) is None
    releases = settings.published_dir / "releases"
    assert not releases.exists() or not any(releases.iterdir())


def test_unavailable_identities_publish_with_quarantine_evidence(tmp_path) -> None:
    world = demo_world()
    market_id = next(iter(world.markets))
    world.markets[market_id]["version"] = "unknown"
    for event in world.events.values():
        for market in event["markets"]:
            if market["id"] == market_id:
                market["version"] = "unknown"
    settings = capture_and_load(tmp_path, world)
    from oddsfox_catalogue.pipeline import dbt_stage

    result = dbt_stage(settings, ["build"])
    assert result.returncode == 0, result.stdout[-4000:]
    info = publish_release(settings, now=FIXED_NOW)
    assert info.tables["quarantine"]["rows"] > 0
    con = duckdb.connect(str(settings.warehouse_path))
    try:
        assert con.execute(
            "SELECT usable FROM core.markets_current WHERE market_id = ?", [market_id]
        ).fetchone() == (False,)
        assert con.execute(
            "SELECT count(*) FROM core.outcomes_current WHERE market_id = ?", [market_id]
        ).fetchone() == (0,)
    finally:
        con.close()


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

    # The next publish succeeds and preserves all prior immutable evidence.
    recovered = publish_release(settings, now=FIXED_NOW + timedelta(days=1), git_sha=None)
    assert current_release(settings)["release_id"] == recovered.release_id
    assert (settings.published_dir / CURRENT_POINTER).exists()
    assert len(list((settings.published_dir / "releases").iterdir())) == 3
    assert first.path.is_dir()


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


def _rewrite_release(info, name, value):
    """Model a corrupted writer that recomputes physical hashes but not semantic content."""
    import hashlib

    from oddsfox_catalogue.semantics import canonical_json_chunks

    target = info.path / name
    target.write_bytes(b"".join(canonical_json_chunks(value)))
    manifest = json.loads((info.path / "release.json").read_bytes())
    key = "coverage" if name == "coverage.json" else "capture_inventory"
    manifest[key]["bytes"] = target.stat().st_size
    manifest[key]["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
    (info.path / "release.json").write_text(json.dumps(manifest))


@pytest.mark.parametrize(
    "damage",
    [
        "extra",
        "missing",
        "bytes",
        "schema",
        "contract",
        "revision",
        "digest",
        "rows_bool",
        "symlink",
        "directory",
    ],
)
def test_immutable_verification_rejects_output_and_contract_drift(built_root, tmp_path, damage):
    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW)
    target = info.path / "events.parquet"
    manifest_path = info.path / "release.json"
    manifest = json.loads(manifest_path.read_bytes())
    if damage == "extra":
        (info.path / "unexpected.txt").write_text("unexpected")
    elif damage == "missing":
        target.unlink()
    elif damage == "bytes":
        target.write_bytes(target.read_bytes() + b"changed")
    elif damage == "symlink":
        moved = tmp_path / "elsewhere.parquet"
        target.rename(moved)
        target.symlink_to(moved)
    elif damage == "directory":
        target.unlink()
        target.mkdir()
    else:
        if damage == "schema":
            manifest["tables"]["events"]["schema"][0]["name"] = "renamed"
        elif damage == "contract":
            manifest["contract"] = "oddsfox.polymarket.catalogue.v99"
        elif damage == "revision":
            manifest["normalization_revision"] = "99"
        elif damage == "digest":
            manifest["tables"]["events"]["semantic_sha256"] = "0" * 64
        else:
            manifest["tables"]["events"]["rows"] = True
        manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(PublishBlocked, match="verification"):
        verify_release(settings, info.path)


@pytest.mark.parametrize(
    "relative", ["../outside", "/tmp/outside", "a/../b", "a//b", "a\\b", "./a"]
)
def test_rechecksummed_inventory_cannot_introduce_unsafe_paths(built_root, tmp_path, relative):
    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW)
    inventory = json.loads((info.path / "capture-inventory.json").read_bytes())
    inventory[0]["path"] = relative
    _rewrite_release(info, "capture-inventory.json", inventory)
    with pytest.raises(PublishBlocked):
        verify_release(settings, info.path)


@pytest.mark.parametrize(
    "damage",
    [
        "unsealed",
        "missing_unit",
        "missing_batch",
        "empty_means_nonempty",
        "float_revision",
        "bad_received",
    ],
)
def test_rechecksummed_coverage_must_account_for_its_inventory(built_root, tmp_path, damage):
    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW)
    coverage = json.loads((info.path / "coverage.json").read_bytes())
    if damage == "unsealed":
        coverage["batches"][0]["scope"]["sealed"] = False
    elif damage == "missing_unit":
        coverage["units"].pop()
    elif damage == "missing_batch":
        coverage["batches"].pop()
    elif damage == "float_revision":
        coverage["batches"][0]["scope"]["revision"] = 2.0
    elif damage == "bad_received":
        coverage["units"][0]["received_at"] = "not-a-timestamp"
    else:
        coverage["units"][0]["records"] = 1
        coverage["units"][0]["status"] = "success_empty"
    _rewrite_release(info, "coverage.json", coverage)
    with pytest.raises(PublishBlocked):
        verify_release(settings, info.path)


@pytest.mark.parametrize(
    "damage",
    ["traversal", "absolute", "wrong_hash", "no_hash", "identity", "symlink", "duplicate_key"],
)
def test_current_pointer_is_confined_and_binds_the_verified_manifest(built_root, tmp_path, damage):
    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW)
    pointer_path = settings.published_dir / CURRENT_POINTER
    pointer = json.loads(pointer_path.read_bytes())
    if damage == "traversal":
        pointer["path"] = "../releases/" + info.release_id
    elif damage == "absolute":
        pointer["path"] = str(info.path)
    elif damage == "wrong_hash":
        pointer["manifest_sha256"] = "0" * 64
    elif damage == "no_hash":
        pointer.pop("manifest_sha256")
    elif damage == "identity":
        pointer["release_id"] = "20261001T000000Z"
    elif damage == "symlink":
        outside = tmp_path / "pointer.json"
        pointer_path.rename(outside)
        pointer_path.symlink_to(outside)
    else:
        pointer_path.write_text('{"release_id":"x","release_id":"y"}')
    if damage not in {"symlink", "duplicate_key"}:
        pointer_path.write_text(json.dumps(pointer))
    with pytest.raises(PublishBlocked):
        current_release(settings)


def test_immutable_verification_does_not_depend_on_dirty_or_missing_warehouse(built_root, tmp_path):
    from oddsfox_catalogue.certification import mark_dirty

    settings = _copy_of_built(built_root, tmp_path)
    info = publish_release(settings, now=FIXED_NOW)
    mark_dirty(settings, "injected failed operation")
    assert verify_release(settings, info.path)["release_id"] == info.release_id
    settings.warehouse_path.unlink()
    assert current_release(settings)["release_id"] == info.release_id
    assert verify_release(settings, info.path)["tables"] == info.tables


def test_corrupt_candidate_leaves_previous_pointer_and_evidence(built_root, tmp_path, monkeypatch):
    import oddsfox_catalogue.publish as module

    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW)
    previous = (settings.published_dir / CURRENT_POINTER).read_bytes()
    original = module._export_release

    def corrupt(*args, **kwargs):
        manifest = original(*args, **kwargs)
        (args[1] / "events.parquet").write_bytes(b"broken")
        return manifest

    monkeypatch.setattr(module, "_export_release", corrupt)
    with pytest.raises(PublishBlocked, match="verification"):
        publish_release(settings, now=FIXED_NOW + timedelta(days=1))
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == previous
    assert current_release(settings)["release_id"] == first.release_id
    assert list((settings.published_dir / "releases").glob(".staging-*"))


def test_pointer_write_failure_preserves_previous_verified_release(
    built_root, tmp_path, monkeypatch
):
    import oddsfox_catalogue.publish as module

    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW)
    previous = (settings.published_dir / CURRENT_POINTER).read_bytes()
    original = module.os.replace

    def fail_pointer(source, destination):
        if Path(destination).name == CURRENT_POINTER:
            raise OSError("injected pointer failure")
        return original(source, destination)

    monkeypatch.setattr(module.os, "replace", fail_pointer)
    with pytest.raises(OSError, match="injected"):
        publish_release(settings, now=FIXED_NOW + timedelta(days=1))
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == previous
    assert current_release(settings)["release_id"] == first.release_id
    releases = settings.published_dir / "releases"
    assert len(list(releases.iterdir())) == 2
    assert all(verify_release(settings, p) for p in releases.iterdir())


@pytest.mark.parametrize("direct_only", [False, True])
def test_certified_empty_and_direct_only_catalogues_can_publish(tmp_path, direct_only):
    from fakes.world import World, make_market

    world = World()
    if direct_only:
        world.add_direct_market(make_market("201", "A standalone market?"))
    settings = build_warehouse_with_world(tmp_path, world)
    info = publish_release(settings, now=FIXED_NOW)
    assert info.tables["events"]["rows"] == 0
    assert info.tables["markets"]["rows"] == int(direct_only)
    assert verify_release(settings, info.path)["release_id"] == info.release_id
    coverage = json.loads((info.path / "coverage.json").read_bytes())
    assert coverage["declared_scans_complete"] is True
    assert coverage["source_catalogue_complete"] is False
    assert any(unit["status"] == "success_empty" for unit in coverage["units"])


def build_warehouse_with_world(root, world):
    from oddsfox_catalogue.pipeline import dbt_stage

    settings = capture_and_load(root, world)
    result = dbt_stage(settings, ["build"])
    assert result.returncode == 0, result.stdout[-4000:]
    return settings


def test_retained_quota_preflight_preserves_pointer_before_parquet_write(built_root, tmp_path):
    from dataclasses import replace

    from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
    from oddsfox_catalogue.limits import retained_bytes

    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW)
    previous = (settings.published_dir / CURRENT_POINTER).read_bytes()
    constrained = replace(
        settings,
        capture=replace(settings.capture, max_retained_bytes=retained_bytes(settings) + 1024**2),
    )
    with pytest.raises(RequestBudgetExceeded, match="retained"):
        publish_release(constrained, now=FIXED_NOW + timedelta(days=1))
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == previous
    assert current_release(constrained)["release_id"] == first.release_id
    candidates = list((settings.published_dir / "releases").glob(".staging-*"))
    assert len(candidates) == 1
    assert not list(candidates[0].glob("*.parquet"))


def test_valid_but_wrong_export_cannot_promote_a_certified_warehouse(
    built_root, tmp_path, monkeypatch
):
    import oddsfox_catalogue.publish as module

    settings = _copy_of_built(built_root, tmp_path)
    first = publish_release(settings, now=FIXED_NOW)
    previous = (settings.published_dir / CURRENT_POINTER).read_bytes()
    queries = dict(module.PROJECTION_TABLE_QUERIES)
    queries["events"] = f"SELECT * FROM ({queries['events']}) r WHERE false"
    monkeypatch.setattr(module, "PROJECTION_TABLE_QUERIES", queries)
    with pytest.raises(PublishBlocked, match="certified projection"):
        publish_release(settings, now=FIXED_NOW + timedelta(days=1))
    assert (settings.published_dir / CURRENT_POINTER).read_bytes() == previous
    assert current_release(settings)["release_id"] == first.release_id
