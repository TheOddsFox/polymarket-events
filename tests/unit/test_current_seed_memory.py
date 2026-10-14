"""Many membership pages must fit one import transaction with bounded SQL memory."""

from datetime import UTC, datetime, timedelta

import duckdb

from oddsfox_catalogue import current_import as importer
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_contract import ResponseRows
from oddsfox_catalogue.fingerprints import semantic_fingerprint


def test_many_membership_pages_fit_one_256mb_transaction_without_changing_entities(tmp_path):
    pages, page_size = 200, 100
    start = datetime(2026, 10, 14, tzinfo=UTC)
    with duckdb.connect(":memory:") as conn:
        conn.execute("SET memory_limit='256MB'")
        conn.execute("SET threads=2")
        conn.execute("SET temp_directory=?", [str(tmp_path / "spill")])
        conn.execute("SET max_temp_directory_size='256MB'")
        store.initialize(conn)
        conn.execute(
            """INSERT INTO events (venue,event_id,title,active)
            SELECT 'polymarket',id::VARCHAR,'original:'||id,id%2=0
            FROM range(1,?) ids(id)""",
            [pages * page_size + 1],
        )
        before = semantic_fingerprint(conn, "SELECT * FROM events")
        state = conn.execute("SELECT * FROM sync_state ORDER BY key").fetchall()
        conn.execute("BEGIN")
        try:
            conn.execute("""CREATE TEMP TABLE seed_evidence (
                event_id VARCHAR,receipt TIMESTAMPTZ,response VARCHAR,members JSON)""")
            for page in range(pages):
                first = page * page_size + 1
                importer._membership_evidence(
                    conn,
                    ResponseRows(
                        received_at=(start + timedelta(seconds=page)).isoformat(),
                        response_id=f"page:{page}",
                        memberships={str(key): [] for key in range(first, first + page_size)},
                    ),
                )
            importer._finish_memberships(conn)
            assert conn.execute("SELECT count(*) FROM _membership").fetchone() == (
                pages * page_size,
            )
            assert conn.execute(
                """SELECT count(*) FROM _membership WHERE venue<>'polymarket'
                OR unresolved IS DISTINCT FROM false
                OR receipt IS DISTINCT FROM latest_receipt
                OR response IS DISTINCT FROM latest_response
                OR response IS DISTINCT FROM 'page:'||((event_id::BIGINT-1)//?)::VARCHAR
                OR receipt IS DISTINCT FROM ?::TIMESTAMPTZ
                    + ((event_id::BIGINT-1)//?)*INTERVAL '1 second'""",
                [page_size, start, page_size],
            ).fetchone() == (0,)
            assert semantic_fingerprint(conn, "SELECT * FROM events") == before
            for table in ("markets", "outcomes", "event_markets"):
                assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
            assert conn.execute("SELECT * FROM sync_state ORDER BY key").fetchall() == state
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        assert store.validate_store(conn)["valid"]
        assert store.generation(conn) == 0
