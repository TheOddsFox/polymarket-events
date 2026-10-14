import duckdb
import pytest

from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.current_store import (
    apply,
    generation,
    get_state,
    initialize,
    summary,
    validate_store,
)


def packet(day=1, market="2", assets=("101", "102"), native=False, usable=True):
    return ResponseRows(
        received_at=f"2026-01-{day:02d}T00:00:00Z",
        response_id=str(day),
        events=[dict(venue="polymarket", event_id="1", title="Event")],
        markets=[
            dict(
                venue="polymarket",
                market_id=market,
                question="Q",
                usable=usable,
                source_market_version="v1",
                protocol="ctf",
                condition_id="0x" + "1" * 64,
            )
        ],
        outcomes=[
            dict(
                venue="polymarket",
                market_id=market,
                condition_id="0x" + "1" * 64,
                outcome_index=i,
                asset_kind="ctf_token",
                asset_id=a,
                clob_token_id=a,
                outcome_label=str(i),
            )
            for i, a in enumerate(assets, start=1)
        ]
        if usable
        else [],
        memberships={"1": [market]},
        native_market_ids={market} if native else set(),
    )


@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    initialize(conn)
    yield conn
    conn.close()


def test_noop_late_and_empty_membership(db):
    apply(db, packet(), {"cursor": "one"})
    assert generation(db) == 1
    apply(db, packet(2))
    assert generation(db) == 1
    apply(db, packet())
    assert get_state(db, "cursor") == "one"
    empty = packet(3)
    empty.memberships = {"1": []}
    apply(db, empty)
    assert db.execute("SELECT count(*) FROM event_markets").fetchone()[0] == 0
    unknown = packet(4)
    unknown.memberships = {"1": None}
    apply(db, unknown)
    assert db.execute("SELECT count(*) FROM event_markets").fetchone()[0] == 0
    assert validate_store(db)["valid"]


def test_embedded_change_requires_new_native(db):
    apply(db, packet())
    apply(db, packet(3, assets=("103", "104")))
    assert db.execute("SELECT usable FROM markets").fetchone() == (False,)
    apply(db, packet(2, assets=("101", "102"), native=True))
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    apply(db, packet(4, assets=("103", "104")))
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    apply(db, packet(5, assets=("103", "104"), native=True))
    assert db.execute("SELECT usable FROM markets").fetchone() == (True,)
    assert validate_store(db)["valid"]


def test_invalid_incumbent_keeps_ownership(db):
    apply(db, packet())
    apply(db, packet(2, usable=False))
    apply(db, packet(3, market="3"))
    assert db.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    apply(db, packet(4, market="3", assets=("103", "104"), native=True))
    assert db.execute("SELECT usable FROM markets WHERE market_id='2'").fetchone() == (False,)
    assert db.execute("SELECT usable FROM markets WHERE market_id='3'").fetchone() == (True,)


def test_progress_failure_rolls_back_domain_and_generation(db):
    apply(db, packet(), {"cursor": 1})
    with pytest.raises(TypeError):
        apply(db, packet(2, assets=("103", "104"), native=True), {"cursor": object()})
    assert generation(db) == 1
    assert get_state(db, "cursor") == 1
    assert db.execute("SELECT asset_id FROM outcomes ORDER BY asset_id").fetchall() == [
        ("101",),
        ("102",),
    ]


def test_response_tie_and_labels_are_public_changes(db):
    first = packet()
    first.response_id = "a"
    apply(db, first)
    later = packet()
    later.response_id = "b"
    later.outcomes[0]["outcome_label"] = "Yes"
    apply(db, later)
    assert generation(db) == 2
    assert db.execute("SELECT usable FROM markets").fetchone() == (True,)
    apply(db, first)
    assert generation(db) == 2


def test_conflicting_candidates_all_require_new_native(db):
    apply(db, packet())
    apply(db, packet(2, market="3"))
    assert db.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    apply(db, packet(3, market="3", assets=("103", "104"), native=True))
    assert db.execute("SELECT usable FROM markets WHERE market_id='2'").fetchone() == (False,)
    apply(db, packet(4, native=True))
    assert db.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (2,)
    assert validate_store(db)["valid"]


def test_unknown_membership_does_not_hide_late_complete(db):
    unknown = packet(3)
    unknown.memberships = {"1": None}
    apply(db, unknown)
    apply(db, packet(2))
    assert db.execute("SELECT count(*) FROM event_markets").fetchone() == (1,)


def test_unknown_after_complete_and_old_unknown(db):
    apply(db, packet())
    unknown = packet(3)
    unknown.memberships = {"1": None}
    apply(db, unknown)
    assert summary(db)["unresolved_memberships"] == 1
    complete = packet(4)
    apply(db, complete)
    assert summary(db)["unresolved_memberships"] == 0
    apply(db, unknown)
    assert summary(db)["unresolved_memberships"] == 0


def test_unknown_existing_database_is_never_stamped():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE unrelated(id INTEGER)")
    with pytest.raises(ValueError, match="unrecognized"):
        initialize(conn)
    assert conn.execute("SHOW TABLES").fetchall() == [("unrelated",)]
    conn.close()


def test_bad_input_before_write_rolls_back(db):
    bad = packet()
    del bad.events[0]["event_id"]
    with pytest.raises(KeyError):
        apply(db, bad, {"cursor": 1})
    assert generation(db) == 0
    assert get_state(db, "cursor") is None


def test_apply_and_reopen_do_not_run_full_store_scans(db, monkeypatch):
    import oddsfox_catalogue.current_store as store

    def forbidden(*args, **kwargs):
        raise AssertionError("full scan on response path")

    monkeypatch.setattr(store, "summary", forbidden)
    monkeypatch.setattr(store, "validate_store", forbidden)
    assert apply(db, packet()) == {"generation": 1, "changed": True, "events": 1, "markets": 1}
    initialize(db)


def test_summary_limits_repairs_and_reports_totals(db):
    for i in range(4):
        response = packet(i + 1, market=str(i + 1), usable=False)
        response.parent_event_ids = {str(100 + i)}
        apply(db, response)
    result = summary(db, repair_limit=2)
    assert result["identity_repair_count"] == 4
    assert result["parent_event_repair_count"] == 4
    assert len(result["identity_repairs"]) == 2
    assert len(result["parent_event_repairs"]) == 2
    assert summary(db, repair_limit=0)["identity_repairs"] == []


def test_claim_scans_are_batched_per_response(db):
    class CountingConnection:
        def __init__(self, connection):
            self.connection = connection
            self.claim_queries = []

        def execute(self, query, parameters=None):
            if "_identity_claims" in query:
                self.claim_queries.append(query)
            return (
                self.connection.execute(query, parameters)
                if parameters is not None
                else self.connection.execute(query)
            )

        def executemany(self, query, parameters):
            return self.connection.executemany(query, parameters)

    response = packet()
    response.events = []
    response.memberships = {}
    response.markets = []
    response.outcomes = []
    for i in range(10):
        item = packet(market=str(i + 1), assets=(str(1000 + 2 * i), str(1001 + 2 * i)))
        response.markets.extend(item.markets)
        response.outcomes.extend(item.outcomes)
    counted = CountingConnection(db)
    apply(counted, response)
    assert len(counted.claim_queries) == 2
    assert counted.claim_queries[0].startswith("SELECT")
    assert "JOIN (VALUES" in counted.claim_queries[0]
    assert counted.claim_queries[1].startswith("DELETE")
