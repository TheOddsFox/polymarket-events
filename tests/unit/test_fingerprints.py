import hashlib
import json

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


def test_binary_order_reproduces_lowercase_hex_contract_including_leading_zeroes():
    json_rows = [f'{{"v":{i}}}' for i in range(256)]
    hashes = sorted(hashlib.sha256(row.encode()).hexdigest() for row in json_rows)
    assert any(value.startswith("0") for value in hashes)
    assert any(value.startswith("f") for value in hashes)
    schema = [{"name": "v", "type": "BIGINT"}]
    expected = hashlib.sha256(json.dumps(schema, sort_keys=True).encode() + b"\n")
    expected.update(b"".join(value.encode() + b"\n" for value in hashes))
    with duckdb.connect(":memory:") as connection:
        actual = semantic_fingerprint(connection, "SELECT i AS v FROM range(?) t(i)", [256])
    assert actual == {
        "rows": 256,
        "schema": schema,
        "semantic_sha256": expected.hexdigest(),
        "semantic_digest_revision": "sha256-row-multiset-v1",
    }
