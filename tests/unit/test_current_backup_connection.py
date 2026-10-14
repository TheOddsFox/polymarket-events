"""Backup verification must share its caller's bounded database engine."""

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import SCHEMAS, CurrentError
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
def catalogue(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    releases = []
    with connect_store(settings) as conn:
        store.initialize(conn)
        for day in (1, 2):
            advance(conn, day)
            releases.append(publisher.publish(settings, conn))
    return settings, releases


def forbid_memory_connection(*args, **kwargs):
    pytest.fail("backup opened a second DuckDB engine beside its source connection")


def spy_verification(monkeypatch):
    original = publisher._verify_release_on_connection
    calls = []

    def measured(path, conn, *, bind_store):
        calls.append((Path(path).name, conn, bind_store))
        return original(path, conn, bind_store=bind_store)

    monkeypatch.setattr(publisher, "_verify_release_on_connection", measured)
    monkeypatch.setattr(backup, "_verify_release_on_connection", measured)
    monkeypatch.setattr(publisher, "memory_connection", forbid_memory_connection)
    return calls


@pytest.mark.parametrize("ahead", [False, True], ids=["same-generation", "store-ahead"])
def test_source_inventory_reuses_supplied_connection(catalogue, monkeypatch, ahead):
    settings, releases = catalogue
    calls = spy_verification(monkeypatch)
    with connect_store(settings) as conn:
        if ahead:
            advance(conn, 3)
        files = backup._files(settings, conn)
    expected = {"catalogue.json", "catalogue.duckdb", "published/current.json"}
    for release in releases:
        prefix = "published/releases/" + release["release_id"] + "/"
        expected.update(
            prefix + name for name in ("manifest.json", *(f"{t}.parquet" for t in SCHEMAS))
        )
    assert set(files) == expected
    assert len(calls) == 3
    assert all(connection is conn for _, connection, _ in calls)
    assert calls[0] == (releases[-1]["release_id"], conn, not ahead)
    assert {(identifier, bound) for identifier, _, bound in calls[1:]} == {
        (release["release_id"], False) for release in releases
    }


@pytest.mark.parametrize("damage", ["physical", "resealed-membership"])
def test_previous_corruption_rejected_with_borrowed_connection(catalogue, monkeypatch, damage):
    settings, releases = catalogue
    previous = Path(releases[0]["path"])
    original_pointer = (settings.published_dir / "current.json").read_bytes()
    with connect_store(settings) as conn:
        advance(conn, 3)
        if damage == "physical":
            with (previous / "events.parquet").open("ab") as handle:
                handle.write(b"corrupt")
        else:
            file = previous / "event_markets.parquet"
            replacement = previous / "replacement.parquet"
            conn.execute(
                f"COPY (SELECT venue,'999' AS event_id,market_id FROM {publisher._parquet(file)}) "
                "TO ? (FORMAT PARQUET)",
                [str(replacement)],
            )
            replacement.replace(file)
            manifest = json.loads((previous / "manifest.json").read_text())
            manifest["relations"]["event_markets"] = {
                **semantic_fingerprint(conn, "SELECT * FROM read_parquet(?)", [str(file)]),
                "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
                "bytes": file.stat().st_size,
            }
            (previous / "manifest.json").write_text(json.dumps(manifest))
        calls = spy_verification(monkeypatch)
        with pytest.raises(CurrentError, match="corrupt|integrity"):
            backup._files(settings, conn)
        assert all(connection is conn for _, connection, _ in calls)
        assert (releases[0]["release_id"], conn, False) in calls
        assert (settings.published_dir / "current.json").read_bytes() == original_pointer
        assert store.validate_store(conn)["valid"]


@pytest.mark.parametrize("ahead", [False, True], ids=["same-generation", "store-ahead"])
def test_create_backup_verifies_source_and_private_copy_on_separate_connections(
    catalogue, monkeypatch, ahead
):
    settings, releases = catalogue
    if ahead:
        with connect_store(settings) as conn:
            advance(conn, 3)
    temporary = settings.root.parent / "verification"
    temporary.mkdir()
    monkeypatch.setattr(backup.tempfile, "gettempdir", lambda: str(temporary))
    calls = spy_verification(monkeypatch)
    original_connect = backup.connect_store
    active, opened = [], []

    @contextmanager
    def tracked_connect(given_settings, **kwargs):
        assert not active, "source and private verification engines overlapped"
        with original_connect(given_settings, **kwargs) as conn:
            active.append(conn)
            opened.append((given_settings.root, conn))
            try:
                yield conn
            finally:
                active.pop()

    monkeypatch.setattr(backup, "connect_store", tracked_connect)
    result = backup.create_backup(settings)
    assert result["valid"]
    assert not active
    assert len(opened) == 3
    source, private, state_write = opened
    assert source[0] == state_write[0] == settings.root
    assert private[0].is_relative_to(temporary)
    assert source[1] is not private[1]
    assert len(calls) == 5
    assert all(connection is source[1] for _, connection, _ in calls[:3])
    assert all(connection is private[1] for _, connection, _ in calls[3:])
    assert calls[0] == (releases[-1]["release_id"], source[1], not ahead)
    assert {(identifier, bound) for identifier, _, bound in calls[1:3]} == {
        (release["release_id"], False) for release in releases
    }
    assert {(identifier, bound) for identifier, _, bound in calls[3:]} == {
        (releases[0]["release_id"], False),
        (releases[1]["release_id"], not ahead),
    }
    with connect_store(settings, read_only=True) as conn:
        assert store.get_state(conn, "last_backup") == result
