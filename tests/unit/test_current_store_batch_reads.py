import duckdb
import pytest

from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import (
    _market_snapshots,
    _snapshot,
    apply,
    initialize,
)


class CountingConnection:
    def __init__(self, connection):
        self.connection = connection
        self.reads = []

    def execute(self, query, parameters=None):
        if (
            query.startswith("SELECT")
            and "JOIN (VALUES" in query
            and "_identity_claims" not in query
        ):
            self.reads.append((query, parameters))
        return (
            self.connection.execute(query, parameters)
            if parameters is not None
            else self.connection.execute(query)
        )

    def executemany(self, query, parameters):
        return self.connection.executemany(query, parameters)


@pytest.fixture
def db():
    with duckdb.connect(":memory:") as connection:
        initialize(connection)
        yield connection


@pytest.mark.parametrize("size", [0, 1, 1000, 1001])
@pytest.mark.parametrize("table", ["markets", "outcomes", "_identity"])
def test_market_snapshots_bound_deduplicate_and_preserve_missing_keys(db, size, table):
    keys = [("venue", str(i)) for i in range(size)]
    counted = CountingConnection(db)
    assert _market_snapshots(counted, table, keys + keys) == {key: [] for key in keys}
    assert len(counted.reads) == (size + 999) // 1000
    assert all(len(parameters) <= 2000 for _, parameters in counted.reads)


def test_snapshots_keep_full_sorted_rows_and_venue_identity(db):
    for venue in ["a", "b"]:
        db.execute(
            "INSERT INTO markets (venue,market_id,question) VALUES (?, '1', ?)", [venue, venue]
        )
        for index in [2, 1]:
            db.execute(
                "INSERT INTO outcomes (venue,market_id,outcome_index,outcome_label) VALUES (?, '1', ?, ?)",
                [venue, index, f"{venue}-{index}"],
            )
        db.execute("INSERT INTO _identity VALUES (?, '1', '[]', NULL, NULL, 'receipt')", [venue])
    keys = [("a", "1"), ("b", "1"), ("a", "missing")]
    for table in ["markets", "outcomes"]:
        actual = _market_snapshots(db, table, keys + keys)
        for key in keys:
            assert actual[key] == _snapshot(db, table, "venue=? AND market_id=?", key)
    identities = _market_snapshots(db, "_identity", keys)
    assert identities[("a", "1")] == [("a", "1", "[]", None, None, "receipt")]
    assert identities[("b", "1")] == [("b", "1", "[]", None, None, "receipt")]
    assert identities[("a", "missing")] == []


@pytest.mark.parametrize("size", [2, 1001])
def test_apply_snapshot_reads_scale_with_chunks_not_markets(db, size):
    response = ResponseRows(
        received_at="2026-01-01T00:00:00Z",
        response_id="first",
        markets=[dict(venue="venue", market_id=str(i), usable=False) for i in range(size)],
    )
    counted = CountingConnection(db)
    assert apply(counted, response)["changed"]
    assert len(counted.reads) == 6 * ((size + 999) // 1000)
    assert all(len(parameters) <= 2000 for _, parameters in counted.reads)
    counted.reads.clear()
    assert not apply(counted, response)["changed"]
    assert len(counted.reads) == 6 * ((size + 999) // 1000)
