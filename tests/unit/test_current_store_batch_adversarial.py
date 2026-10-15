"""Failures in batched post-write reads must leave the response unapplied."""

import duckdb
import pytest
from test_current_store import db as db
from test_current_store import packet
from test_current_store_noop import snapshot

from oddsfox_catalogue import current_store as store


def test_batched_identity_read_oom_after_mutations_rolls_back_and_retries_cleanly(db):
    initial = packet()
    store.apply(db, initial, {"cursor": "old"})
    db.execute("INSERT INTO _parent_repairs VALUES ('1')")
    before = snapshot(db)
    changed = packet(2, assets=("103", "104"), native=True)
    changed.events[0]["title"] = "Changed event"
    changed.markets[0]["question"] = None
    changed.memberships = {"1": []}

    class FailingRead:
        failed_after_mutations = False

        def execute(self, query, parameters=None):
            if "FROM _identity JOIN (VALUES" in query:
                receipt = db.execute(
                    "SELECT response FROM _receipts "
                    "WHERE kind='market' AND venue='polymarket' AND id='2'"
                ).fetchone()
                if receipt == ("2",):
                    assert db.execute("SELECT * FROM event_markets").fetchall() == []
                    assert db.execute("SELECT count(*) FROM _parent_repairs").fetchone() == (0,)
                    self.failed_after_mutations = True
                    raise duckdb.OutOfMemoryException("batched identity read exhausted memory")
            return db.execute(query, parameters) if parameters is not None else db.execute(query)

        def executemany(self, query, parameters):
            return db.executemany(query, parameters)

    failing = FailingRead()
    with pytest.raises(duckdb.OutOfMemoryException, match="batched identity read exhausted"):
        store.apply(failing, changed, {"cursor": "new"})
    assert failing.failed_after_mutations
    assert snapshot(db) == before
    assert store.generation(db) == 1
    actual = store.apply(db, changed, {"cursor": "new"})
    assert actual["generation"] == 2
    assert store.get_state(db, "cursor") == "new"
    with duckdb.connect(":memory:") as clean:
        store.initialize(clean)
        store.apply(clean, initial, {"cursor": "old"})
        clean.execute("INSERT INTO _parent_repairs VALUES ('1')")
        assert store.apply(clean, changed, {"cursor": "new"}) == actual
        assert snapshot(db) == snapshot(clean)
    assert store.validate_store(db)["valid"]
