import json
from dataclasses import replace

import duckdb
import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError, ResponseRows
from oddsfox_catalogue.current_store import apply, initialize


@pytest.fixture
def store(tmp_path):
    settings = CurrentSettings(
        tmp_path / "root", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(str(settings.store_path))
    initialize(conn)
    apply(
        conn,
        ResponseRows(
            "2026-01-01T00:00:00Z",
            "1",
            events=[dict(venue="polymarket", event_id="1", title="hello")],
        ),
    )
    yield settings, conn
    conn.close()


def test_roundtrip_noop_and_independent_current(store):
    settings, conn = store
    result = publisher.publish(settings, conn)
    assert result["counts"]["events"] == 1
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    assert publisher.publish(settings, conn)["noop"]
    assert pointer.read_bytes() == before
    apply(
        conn,
        ResponseRows(
            "2026-01-02T00:00:00Z",
            "2",
            events=[dict(venue="polymarket", event_id="1", title="changed")],
        ),
    )
    assert publisher.verify_current(settings)["generation"] == 1
    assert publisher.verify_current(settings, connection=conn)["store_generation"] == 2
    next_result = publisher.publish(settings, conn)
    assert next_result["generation"] == 2
    assert json.loads(pointer.read_text())["previous_release_id"] == result["release_id"]


def test_each_publication_validates_source_once_and_external_verify_remains_fresh(
    store, monkeypatch
):
    settings, conn = store
    original = publisher.validate_store
    calls = []

    def checked(connection):
        calls.append(connection)
        return original(connection)

    monkeypatch.setattr(publisher, "validate_store", checked)
    result = publisher.publish(settings, conn)
    assert calls == [conn]
    calls.clear()
    assert publisher.publish(settings, conn)["noop"]
    assert calls == [conn]
    calls.clear()
    assert publisher.verify_release(publisher.Path(result["path"]), connection=conn)["valid"]
    assert calls == [conn]


def test_physical_and_manifest_corruption(store):
    settings, conn = store
    release = publisher.publish(settings, conn)
    path = publisher.Path(release["path"])
    manifest = path / "manifest.json"
    original = manifest.read_bytes()
    manifest.write_bytes(original + b" ")
    with pytest.raises(CurrentError, match="manifest hash"):
        publisher.verify_current(settings)
    manifest.write_bytes(original)
    with (path / "events.parquet").open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(CurrentError, match="corrupt"):
        publisher.verify_current(settings)


def test_internally_consistent_wrong_export_rejected(store, monkeypatch):
    settings, conn = store
    original = publisher._export

    def wrong(connection, table, destination):
        if table == "events":
            connection.execute(
                "COPY (SELECT venue,event_id,'wrong' AS title,slug,active,closed,archived FROM events) TO ? (FORMAT PARQUET)",
                [str(destination)],
            )
        else:
            original(connection, table, destination)

    monkeypatch.setattr(publisher, "_export", wrong)
    with pytest.raises(CurrentError, match="differs from source"):
        publisher.publish(settings, conn)
    assert not (settings.published_dir / "current.json").exists()


@pytest.mark.parametrize("failure", ["export", "rename", "pointer"])
def test_failure_preserves_previous_pointer(store, monkeypatch, failure):
    settings, conn = store
    publisher.publish(settings, conn)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    apply(
        conn,
        ResponseRows(
            "2026-01-02T00:00:00Z",
            "2",
            events=[dict(venue="polymarket", event_id="1", title="changed")],
        ),
    )
    original_replace = publisher.os.replace

    def fail(*args, **kwargs):
        raise OSError("injected")

    if failure == "export":
        monkeypatch.setattr(publisher, "_export", fail)
    elif failure == "rename":

        def fail_rename(source, destination):
            if publisher.Path(source).is_dir():
                fail()
            original_replace(source, destination)

        monkeypatch.setattr(publisher.os, "replace", fail_rename)
    else:

        def fail_pointer(source, destination):
            if destination == pointer:
                fail()
            original_replace(source, destination)

        monkeypatch.setattr(publisher.os, "replace", fail_pointer)
    with pytest.raises(OSError, match="injected"):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert conn.execute("SELECT title FROM events").fetchone()[0] == "changed"
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("mutation", ["null", "duplicate", "schema"])
def test_invalid_export_rejected(store, monkeypatch, mutation):
    settings, conn = store
    original = publisher._export

    def wrong(connection, table, destination):
        if table != "events":
            return original(connection, table, destination)
        query = {
            "null": "SELECT venue,NULL::VARCHAR AS event_id,title,slug,active,closed,archived FROM events",
            "duplicate": "SELECT * FROM events UNION ALL SELECT * FROM events",
            "schema": "SELECT event_id,venue,title,slug,active,closed,archived FROM events",
        }[mutation]
        connection.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(destination)])

    monkeypatch.setattr(publisher, "_export", wrong)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
