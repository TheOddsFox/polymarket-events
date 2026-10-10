import duckdb

from oddsfox_catalogue.fingerprints import semantic_fingerprint


def test_semantic_digest_preserves_nulls_precision_duplicates_and_schema():
    con = duckdb.connect()
    try:
        query = "SELECT * FROM (VALUES (NULL::VARCHAR), ('0.123456789012345678901')) AS t(v)"
        original = semantic_fingerprint(con, query)
        assert original == semantic_fingerprint(con, query + " ORDER BY v DESC")
        assert original != semantic_fingerprint(con, query.replace("NULL::VARCHAR", "''"))
        assert original != semantic_fingerprint(con, query + " UNION ALL SELECT NULL")
        assert original != semantic_fingerprint(con, query.replace("t(v)", "t(other)"))
        assert original["rows"] == 2
    finally:
        con.close()


def test_timestamp_digest_uses_utc_independently_of_session_timezone():
    con = duckdb.connect()
    try:
        query = "SELECT TIMESTAMPTZ '2026-10-10T14:30:00+02:00' AS observed_at"
        con.execute("SET TimeZone = 'Europe/Zurich'")
        first = semantic_fingerprint(con, query)
        con.execute("SET TimeZone = 'America/New_York'")
        assert semantic_fingerprint(con, query) == first
    finally:
        con.close()
