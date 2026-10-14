"""A corrupt current release must not discard the last verified rollback release."""

import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.fingerprints import semantic_fingerprint


def advance(conn, day):
    store.apply(
        conn,
        project_response(
            [{"id": "1", "title": str(day), "markets": [{"id": "2"}]}],
            "events",
            received_at=f"2026-10-{day:02d}T00:00:00Z",
            response_id=str(day),
        ),
    )


@pytest.fixture
def history(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(str(settings.store_path))
    store.initialize(conn)
    advance(conn, 1)
    first = publisher.publish(settings, conn)
    advance(conn, 2)
    second = publisher.publish(settings, conn)
    advance(conn, 3)
    yield settings, conn, first, second
    conn.close()


def pointer_path(settings):
    return settings.published_dir / "current.json"


def fingerprints(path):
    return {
        file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in Path(path).iterdir()
    }


def corrupt(release):
    with (Path(release["path"]) / "events.parquet").open("ab") as handle:
        handle.write(b"corrupt")


def write_manifest(release, manifest):
    (Path(release["path"]) / "manifest.json").write_text(json.dumps(manifest))


def reseal_invalid_membership(conn, release):
    path = Path(release["path"])
    file = path / "event_markets.parquet"
    temporary = path / "replacement.parquet"
    conn.execute(
        f"COPY (SELECT venue,'999' AS event_id,market_id FROM {publisher._parquet(file)}) "
        "TO ? (FORMAT PARQUET)",
        [str(temporary)],
    )
    temporary.replace(file)
    manifest = json.loads((path / "manifest.json").read_text())
    manifest["relations"]["event_markets"] = {
        **semantic_fingerprint(conn, "SELECT * FROM read_parquet(?)", [str(file)]),
        "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
        "bytes": file.stat().st_size,
    }
    write_manifest(release, manifest)


MALFORMED_METADATA = [
    "manifest-list",
    "manifest-null",
    "relations-list",
    "relations-null",
    "counts-list",
    "counts-null",
    "descriptor-list",
    "descriptor-null",
]


def malformed_metadata(release, damage):
    manifest = json.loads((Path(release["path"]) / "manifest.json").read_text())
    field, kind = damage.split("-")
    value = [] if kind == "list" else None
    if field == "manifest":
        manifest = value
    elif field == "descriptor":
        manifest["relations"]["events"] = value
    else:
        manifest[field] = value
    write_manifest(release, manifest)


@pytest.mark.parametrize(
    "damage", ["physical", "manifest", "resealed-membership", *MALFORMED_METADATA]
)
def test_corrupt_current_retains_verified_previous_after_promotion(history, damage):
    settings, conn, first, second = history
    before = fingerprints(first["path"])
    if damage == "physical":
        corrupt(second)
    elif damage == "manifest":
        with (Path(second["path"]) / "manifest.json").open("ab") as handle:
            handle.write(b" ")
    else:
        if damage == "resealed-membership":
            reseal_invalid_membership(conn, second)
        else:
            malformed_metadata(second, damage)
        pointer = json.loads(pointer_path(settings).read_text())
        pointer["manifest_sha256"] = hashlib.sha256(
            (Path(second["path"]) / "manifest.json").read_bytes()
        ).hexdigest()
        pointer_path(settings).write_text(json.dumps(pointer))

    with pytest.raises(CurrentError):
        publisher.verify_current(settings, connection=conn)

    third = publisher.publish(settings, conn)

    pointer = json.loads(pointer_path(settings).read_text())
    assert pointer["release_id"] == third["release_id"]
    assert pointer["previous_release_id"] == first["release_id"]
    assert fingerprints(first["path"]) == before
    assert publisher.verify_release(first["path"], settings=settings)["valid"]
    assert publisher.verify_current(settings, connection=conn)["generation"] == 3
    assert {path.name for path in (settings.published_dir / "releases").iterdir()} == {
        first["release_id"],
        third["release_id"],
    }


@pytest.mark.parametrize("failure", ["export", "rename", "pointer", "deadline"])
def test_failed_fallback_publication_preserves_both_pinned_releases(history, monkeypatch, failure):
    settings, conn, first, second = history
    corrupt(second)
    pointer = pointer_path(settings)
    original_pointer = pointer.read_bytes()
    originals = {item["path"]: fingerprints(item["path"]) for item in (first, second)}
    original_replace = publisher.os.replace
    clock = [0]

    def check_pins():
        assert pointer.read_bytes() == original_pointer
        assert {path: fingerprints(path) for path in originals} == originals

    def fail_export(*args):
        check_pins()
        raise OSError("injected export failure")

    def replace(source, destination):
        is_release = Path(source).is_dir()
        if (failure == "rename" and is_release) or (
            failure == "pointer" and destination == pointer
        ):
            check_pins()
            raise OSError("injected promotion failure")
        original_replace(source, destination)
        if failure == "deadline" and is_release:
            check_pins()
            clock[0] = 1

    monkeypatch.setattr(publisher.os, "replace", replace)
    if failure == "export":
        monkeypatch.setattr(publisher, "_export", fail_export)
    if failure == "deadline":
        monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    error = CurrentError if failure == "deadline" else OSError
    with pytest.raises(error, match="deadline|injected"):
        publisher.publish(settings, conn, deadline=1 if failure == "deadline" else None)
    check_pins()
    assert store.generation(conn) == 3
    assert publisher.verify_release(first["path"], settings=settings)["valid"]


@pytest.mark.parametrize(
    "invalid_id",
    ["../escape", "A" * 32, "a" * 31, "a" * 33, "0" * 32, "self", "", 3, [], {}],
)
def test_invalid_previous_pointer_fails_closed(history, invalid_id):
    settings, conn, first, second = history
    corrupt(second)
    pointer = json.loads(pointer_path(settings).read_text())
    pointer["previous_release_id"] = second["release_id"] if invalid_id == "self" else invalid_id
    pointer_path(settings).write_text(json.dumps(pointer))
    assert_fails_closed(settings, conn, first, second)


def assert_fails_closed(settings, conn, first, second):
    pointer = pointer_path(settings).read_bytes()
    originals = {item["path"]: fingerprints(item["path"]) for item in (first, second)}
    inventory = {path.name for path in settings.published_dir.iterdir()}
    releases = {path.name for path in (settings.published_dir / "releases").iterdir()}
    with pytest.raises(CurrentError, match="(?i)previous|fallback|rollback") as error:
        publisher.publish(settings, conn)
    assert any(word in str(error.value).lower() for word in ("restore", "recover", "repair"))
    assert pointer_path(settings).read_bytes() == pointer
    assert {path: fingerprints(path) for path in originals} == originals
    assert {path.name for path in settings.published_dir.iterdir()} == inventory
    assert {path.name for path in (settings.published_dir / "releases").iterdir()} == releases
    assert store.generation(conn) == 3


@pytest.mark.parametrize(
    "damage",
    [
        "identity",
        "future",
        "negative",
        "bool",
        "float",
        "string",
        "null",
        "json",
        "missing",
        "physical",
        "resealed-membership",
        *MALFORMED_METADATA,
    ],
)
def test_invalid_previous_release_fails_closed(history, damage):
    settings, conn, first, second = history
    corrupt(second)
    manifest_path = Path(first["path"]) / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if damage == "identity":
        manifest["release_id"] = second["release_id"]
        write_manifest(first, manifest)
    elif damage in {"future", "negative", "bool", "float", "string", "null"}:
        manifest["generation"] = {
            "future": 4,
            "negative": -1,
            "bool": True,
            "float": 1.0,
            "string": "1",
            "null": None,
        }[damage]
        write_manifest(first, manifest)
    elif damage == "json":
        manifest_path.write_text("{")
    elif damage == "missing":
        manifest_path.unlink()
    elif damage == "physical":
        corrupt(first)
    elif damage == "resealed-membership":
        reseal_invalid_membership(conn, first)
    else:
        malformed_metadata(first, damage)
    assert_fails_closed(settings, conn, first, second)


def test_missing_previous_directory_fails_closed(history):
    settings, conn, first, second = history
    corrupt(second)
    shutil.rmtree(first["path"])
    before = pointer_path(settings).read_bytes()
    second_before = fingerprints(second["path"])
    with pytest.raises(CurrentError, match="(?i)previous|fallback|rollback"):
        publisher.publish(settings, conn)
    assert pointer_path(settings).read_bytes() == before
    assert fingerprints(second["path"]) == second_before
    assert not Path(first["path"]).exists()


def test_symlink_previous_directory_fails_closed_without_touching_target(history):
    settings, conn, first, second = history
    corrupt(second)
    previous = Path(first["path"])
    target = settings.root.parent / "previous-sentinel"
    previous.rename(target)
    previous.symlink_to(target, target_is_directory=True)
    before = fingerprints(target)
    assert_fails_closed(settings, conn, first, second)
    assert previous.is_symlink()
    assert fingerprints(target) == before


@pytest.mark.parametrize("generation", [0, 3])
def test_previous_generation_bounds_are_inclusive(history, generation):
    settings, conn, first, second = history
    corrupt(second)
    manifest = json.loads((Path(first["path"]) / "manifest.json").read_text())
    manifest["generation"] = generation
    write_manifest(first, manifest)
    publisher.publish(settings, conn)
    assert (
        json.loads(pointer_path(settings).read_text())["previous_release_id"] == first["release_id"]
    )


@pytest.mark.parametrize("previous", ["null", "absent"])
def test_corrupt_current_without_previous_can_be_repaired(history, previous):
    settings, conn, _, second = history
    corrupt(second)
    pointer = json.loads(pointer_path(settings).read_text())
    if previous == "null":
        pointer["previous_release_id"] = None
    else:
        del pointer["previous_release_id"]
    pointer_path(settings).write_text(json.dumps(pointer))
    third = publisher.publish(settings, conn)
    assert json.loads(pointer_path(settings).read_text())["previous_release_id"] is None
    assert publisher.verify_current(settings)["release_id"] == third["release_id"]


def test_healthy_current_takes_precedence_over_corrupt_older_release(history):
    settings, conn, first, second = history
    corrupt(first)
    third = publisher.publish(settings, conn)
    assert (
        json.loads(pointer_path(settings).read_text())["previous_release_id"]
        == second["release_id"]
    )
    assert publisher.verify_current(settings, connection=conn)["release_id"] == third["release_id"]
    assert not Path(first["path"]).exists()


@pytest.mark.parametrize("operation", ["verify-current", "fallback-publication"])
def test_older_release_verification_reuses_supplied_connection(history, monkeypatch, operation):
    settings, conn, first, second = history

    def forbidden(*args, **kwargs):
        pytest.fail("verification opened a second DuckDB memory engine")

    monkeypatch.setattr(publisher, "memory_connection", forbidden)
    if operation == "verify-current":
        result = publisher.verify_current(settings, connection=conn)
        assert result["published_generation"] == 2
        assert result["store_generation"] == 3
    else:
        corrupt(second)
        publisher.publish(settings, conn)
        assert (
            json.loads(pointer_path(settings).read_text())["previous_release_id"]
            == first["release_id"]
        )


def test_noop_rechecks_corruption_and_rebuilds_with_verified_fallback(history):
    settings, conn, _, second = history
    third = publisher.publish(settings, conn)
    before = pointer_path(settings).read_bytes()
    assert publisher.publish(settings, conn)["noop"] is True
    assert pointer_path(settings).read_bytes() == before
    corrupt(third)
    rebuilt = publisher.publish(settings, conn)
    assert rebuilt["noop"] is False
    assert rebuilt["release_id"] != third["release_id"]
    assert rebuilt["generation"] == 3
    assert (
        json.loads(pointer_path(settings).read_text())["previous_release_id"]
        == second["release_id"]
    )
    assert publisher.verify_current(settings, connection=conn)["valid"]
