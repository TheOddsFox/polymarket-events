"""Raw responses are kept by reference only: store state, pending work and pins."""

import json
from dataclasses import replace
from itertools import chain, islice, repeat

import duckdb
import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import apply, initialize, set_state

RECEIPT, MEMBERSHIP, LATEST, INVALIDATED, PENDING, NESTED, UNREFERENCED = (
    format(index, "032x") for index in range(1, 8)
)
RAW_BATCH_SIZE = 8192
RAW_REFERENCE_QUERIES = (
    "SELECT response FROM _receipts",
    "SELECT response FROM _membership",
    "SELECT latest_response FROM _membership",
    "SELECT invalidated_response FROM _identity",
)


class GeneratedRawCursor:
    """A large cursor that makes unbounded materialization visible in the test."""

    def __init__(self, rows, count, *, fail_after_batches=None):
        self.rows = iter(rows)
        self.remaining = count
        self.fail_after_batches = fail_after_batches
        self.batches = 0
        self.fetchmany_sizes = []

    def fetchall(self):
        if self.remaining > RAW_BATCH_SIZE:
            raise AssertionError("large raw reference result fetched without a bound")
        return self.fetchmany(self.remaining)

    def fetchmany(self, size):
        if size > RAW_BATCH_SIZE:
            raise AssertionError("raw reference fetch exceeded the bounded batch size")
        if self.fail_after_batches is not None and self.batches >= self.fail_after_batches:
            raise RuntimeError("injected raw reference cursor failure")
        self.fetchmany_sizes.append(size)
        self.batches += 1
        batch = list(islice(self.rows, size))
        self.remaining -= len(batch)
        return batch


class GeneratedRawConnection:
    def __init__(self, rows_by_query, sync_values=()):
        self.rows_by_query = rows_by_query
        self.sync_values = sync_values
        self.executed = []
        self.cursors = []

    def execute(self, query):
        self.executed.append(query)
        if query == "SELECT value FROM sync_state":
            rows = [(value,) for value in self.sync_values]
            cursor = GeneratedRawCursor(rows, len(rows))
        else:
            rows, count = self.rows_by_query[query]
            cursor = GeneratedRawCursor(rows, count)
        self.cursors.append(cursor)
        return cursor


def raw_rows_with_none_first_batch(*tail):
    return chain(repeat((None,), RAW_BATCH_SIZE), ((value,) for value in tail))


class FailingRawConnection:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = GeneratedRawCursor(
            chain(
                ((RECEIPT,),),
                repeat((None,), RAW_BATCH_SIZE - 1),
                ((LATEST,),),
            ),
            RAW_BATCH_SIZE + 1,
            fail_after_batches=1,
        )

    def execute(self, query, *args, **kwargs):
        if query == RAW_REFERENCE_QUERIES[0]:
            return self.cursor
        return self.connection.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.connection, name)


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
            RECEIPT,
            events=[dict(venue="polymarket", event_id="1", title="hello")],
        ),
    )
    yield settings, conn
    conn.close()


def make_raw(settings, identifier):
    directory = settings.raw_dir / identifier
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "p000001.json.gz").write_bytes(b"1234")
    return directory


def test_referenced_raw_streams_large_results_in_bounded_batches():
    rows_by_query = {
        query: (raw_rows_with_none_first_batch(*tail), RAW_BATCH_SIZE + len(tail))
        for query, tail in zip(
            RAW_REFERENCE_QUERIES,
            (
                (RECEIPT, RECEIPT, None),
                (MEMBERSHIP, MEMBERSHIP),
                (LATEST, RECEIPT),
                (INVALIDATED,),
            ),
            strict=True,
        )
    }
    connection = GeneratedRawConnection(
        rows_by_query,
        [
            json.dumps({"raw_id": PENDING}),
            json.dumps({"lanes": [{"nested": {"raw_id": NESTED}}]}),
        ],
    )

    assert publisher._referenced_raw(connection) == {
        RECEIPT,
        MEMBERSHIP,
        LATEST,
        INVALIDATED,
        PENDING,
        NESTED,
    }
    assert connection.executed == ["SELECT value FROM sync_state", *RAW_REFERENCE_QUERIES]
    assert all(
        cursor.fetchmany_sizes and max(cursor.fetchmany_sizes) <= RAW_BATCH_SIZE
        for cursor in connection.cursors[1:]
    )


def test_reference_scan_failure_aborts_publication_before_raw_prune(store):
    settings, conn = store
    garbage = make_raw(settings, UNREFERENCED)
    unprocessed_reference = make_raw(settings, LATEST)
    failing = FailingRawConnection(conn)

    with pytest.raises(RuntimeError, match="injected raw reference cursor failure"):
        publisher.publish(settings, failing)

    assert failing.cursor.fetchmany_sizes == [RAW_BATCH_SIZE]
    assert garbage.exists()
    assert unprocessed_reference.exists()


def advance(conn, title, index):
    apply(
        conn,
        ResponseRows(
            "2026-01-02T00:00:00Z",
            format(0x100 + index, "032x"),
            events=[dict(venue="polymarket", event_id="1", title=title)],
        ),
    )


def test_referenced_set_covers_receipts_membership_identity_and_pins(store):
    _settings, conn = store
    conn.execute(
        "INSERT INTO _membership VALUES ('polymarket','1',current_timestamp,?,false,"
        "current_timestamp,?)",
        [MEMBERSHIP, LATEST],
    )
    conn.execute(
        "INSERT INTO _identity VALUES ('polymarket','11',NULL,NULL,current_timestamp,?)",
        [INVALIDATED],
    )
    set_state(conn, "fetch_pending", {"raw_id": PENDING, "remaining": []})
    set_state(conn, "progress", {"lanes": [{"nested": {"raw_id": NESTED}}]})
    referenced = publisher._referenced_raw(conn)
    assert {RECEIPT, MEMBERSHIP, LATEST, INVALIDATED, PENDING, NESTED} <= referenced
    assert UNREFERENCED not in referenced


def test_publication_deletes_only_unreferenced_raw_and_keeps_every_reference(store):
    settings, conn = store
    conn.execute(
        "INSERT INTO _membership VALUES ('polymarket','1',current_timestamp,?,false,"
        "current_timestamp,?)",
        [MEMBERSHIP, LATEST],
    )
    conn.execute(
        "INSERT INTO _identity VALUES ('polymarket','11',NULL,NULL,current_timestamp,?)",
        [INVALIDATED],
    )
    set_state(conn, "fetch_pending", {"raw_id": PENDING, "remaining": []})
    set_state(conn, "progress", {"lanes": [{"nested": {"raw_id": NESTED}}]})
    kept = {
        name: make_raw(settings, name)
        for name in (RECEIPT, MEMBERSHIP, LATEST, INVALIDATED, PENDING, NESTED)
    }
    garbage = make_raw(settings, UNREFERENCED)
    operator_data = settings.raw_dir / "operator-data"
    operator_data.mkdir()
    result = publisher.publish(settings, conn)
    assert result["noop"] is False
    assert result["raw_retention"] == {
        "deleted_count": 1,
        "deleted_bytes": 4,
    }
    assert not garbage.exists()
    assert all(path.exists() for path in kept.values())
    assert operator_data.exists()


def test_unchanged_publication_deletes_unreferenced_raw_and_keeps_references(store):
    settings, conn = store
    referenced = make_raw(settings, RECEIPT)
    assert publisher.publish(settings, conn)["noop"] is False
    garbage = make_raw(settings, UNREFERENCED)
    pending = make_raw(settings, PENDING)
    set_state(conn, "fetch_pending", {"raw_id": PENDING, "remaining": []})
    result = publisher.publish(settings, conn)
    assert result["noop"] is True
    assert result["raw_retention"] == {"deleted_count": 1, "deleted_bytes": 4}
    assert not garbage.exists()
    assert referenced.exists()
    assert pending.exists()


def test_unchanged_publication_reports_failed_raw_deletion_without_failing(store, monkeypatch):
    settings, conn = store
    assert publisher.publish(settings, conn)["noop"] is False
    make_raw(settings, UNREFERENCED)

    def fail(*_args):
        raise OSError("injected delete failure")

    monkeypatch.setattr(publisher, "_prune_unreferenced_raw", fail)
    result = publisher.publish(settings, conn)
    assert result["noop"] is True
    assert "injected delete failure" in result["retention_warning"]


def test_pending_raw_is_never_deleted_by_later_publications(store):
    settings, conn = store
    pending = make_raw(settings, PENDING)
    set_state(conn, "fetch_pending", {"raw_id": PENDING, "remaining": []})
    publisher.publish(settings, conn)
    advance(conn, "changed once", 1)
    publisher.publish(settings, conn)
    assert pending.exists()
    conn.execute("DELETE FROM sync_state WHERE key='fetch_pending'")
    advance(conn, "changed twice", 2)
    publisher.publish(settings, conn)
    assert not pending.exists()


def test_manifest_records_referenced_raw_counts_and_full_pass(store):
    settings, conn = store
    make_raw(settings, RECEIPT)
    make_raw(settings, UNREFERENCED)
    result = publisher.publish(settings, conn)
    raw = result["raw"]
    assert raw["referenced_count"] == 1
    assert raw["referenced_bytes"] == 4
    assert set(result["full_pass"]) == {
        "started_at",
        "completed_at",
        "remaining",
        "overdue",
        "unresolved",
        "identity_repair_count",
        "parent_event_repair_count",
    }
    assert result["full_pass"]["started_at"] is None


def test_retention_failure_is_reported_without_undoing_the_release(store, monkeypatch):
    settings, conn = store
    make_raw(settings, UNREFERENCED)

    def fail(*_args):
        raise OSError("injected delete failure")

    monkeypatch.setattr(publisher, "_prune_unreferenced_raw", fail)
    result = publisher.publish(settings, conn)
    assert "injected delete failure" in result["retention_warning"]
    assert publisher.verify_current(settings, connection=conn)["release_id"] == result["release_id"]
