"""Fingerprint all semantic rows without retaining the source during the hash sort."""

import hashlib
import json
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

from oddsfox_catalogue.fingerprints import semantic_fingerprint

REVISION = "sha256-row-multiset-v1"
SETTINGS_QUERY = (
    "SELECT current_setting('memory_limit'), current_setting('threads'), "
    "current_setting('temp_directory'), current_setting('max_temp_directory_size'), "
    "current_setting('preserve_insertion_order')"
)
SMALL_QUERY = """
SELECT * FROM (VALUES
    (1::INTEGER, NULL::VARCHAR, 0.123456789012345678::DECIMAL(38,18),
     TIMESTAMPTZ '2026-10-10T14:30:00+02:00', '雪☃'::VARCHAR,
     '{"a":1,"b":["雪",null]}'::JSON, [1,NULL,2]::INTEGER[], '0.123456789012345678'::VARCHAR),
    (1::INTEGER, NULL::VARCHAR, 0.123456789012345678::DECIMAL(38,18),
     TIMESTAMPTZ '2026-10-10T14:30:00+02:00', '雪☃'::VARCHAR,
     '{"a":1,"b":["雪",null]}'::JSON, [1,NULL,2]::INTEGER[], '0.123456789012345678'::VARCHAR),
    (2::INTEGER, ''::VARCHAR, -0.000000000000000001::DECIMAL(38,18),
     TIMESTAMPTZ '2026-10-10T12:30:00Z', 'café'::VARCHAR,
     '{}'::JSON, []::INTEGER[], '-0.000000000000000001'::VARCHAR)
) t(id, note, amount, observed_at, label, evidence, indexes, decimal_text)
"""
SMALL_SCHEMA = [
    {"name": "id", "type": "INTEGER"},
    {"name": "note", "type": "VARCHAR"},
    {"name": "amount", "type": "DECIMAL(38,18)"},
    {"name": "observed_at", "type": "TIMESTAMP WITH TIME ZONE"},
    {"name": "label", "type": "VARCHAR"},
    {"name": "evidence", "type": "JSON"},
    {"name": "indexes", "type": "INTEGER[]"},
    {"name": "decimal_text", "type": "VARCHAR"},
]
FIRST_ROW = (
    '{"id":1,"note":null,"amount":0.123456789012345678,'
    '"observed_at":"2026-10-10 12:30:00+00","label":"雪☃",'
    '"evidence":{"a":1,"b":["雪",null]},"indexes":[1,null,2],'
    '"decimal_text":"0.123456789012345678"}'
)
SECOND_ROW = (
    '{"id":2,"note":"","amount":-0.000000000000000001,'
    '"observed_at":"2026-10-10 12:30:00+00","label":"café",'
    '"evidence":{},"indexes":[],"decimal_text":"-0.000000000000000001"}'
)


def _golden(schema, json_rows):
    """Compute the contract from independently specified JSON, never from DuckDB output."""
    digest = hashlib.sha256(json.dumps(schema, sort_keys=True).encode() + b"\n")
    row_hashes = sorted(hashlib.sha256(row.encode()).hexdigest() for row in json_rows)
    for row_hash in row_hashes:
        digest.update(row_hash.encode() + b"\n")
    return {
        "rows": len(row_hashes),
        "schema": schema,
        "semantic_sha256": digest.hexdigest(),
        "semantic_digest_revision": REVISION,
    }


def _temporary_tables(connection):
    return set(
        connection.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE temporary"
        ).fetchall()
    )


@pytest.mark.parametrize("preserve_order", [True, False])
def test_source_join_and_hash_sort_fit_separately_with_exact_digest(tmp_path, preserve_order):
    # At 32MiB and two SQL threads the previous single query raises OOM. Each phase fits
    # when source execution is released before sorting these 250,000 narrow row hashes.
    query = (
        "SELECT l.i AS id, l.payload, r.payload AS other "
        "FROM (SELECT i, md5(i::VARCHAR) || repeat('x', 256) AS payload "
        "FROM range(250000) t(i)) l "
        "JOIN (SELECT i, md5((i+1)::VARCHAR) || repeat('y', 256) AS payload "
        "FROM range(250000) t(i)) r USING(i)"
    )
    # Independently generated in Python from compact JSON of the specified source rows,
    # including every duplicate hash if present; the reference does not query DuckDB.
    expected = {
        "rows": 250000,
        "schema": [
            {"name": "id", "type": "BIGINT"},
            {"name": "payload", "type": "VARCHAR"},
            {"name": "other", "type": "VARCHAR"},
        ],
        "semantic_sha256": "68a8a3c2b57395f00d3c90a19dc2f89e2be3c0feb5d80febd403e6f66c954ec9",
        "semantic_digest_revision": REVISION,
    }
    with duckdb.connect(
        ":memory:",
        config={
            "memory_limit": "32MiB",
            "threads": 2,
            "temp_directory": str(tmp_path),
            "max_temp_directory_size": "64MiB",
            "preserve_insertion_order": preserve_order,
        },
    ) as connection:
        before = connection.execute(SETTINGS_QUERY).fetchone()
        assert semantic_fingerprint(connection, query) == expected
        assert connection.execute(SETTINGS_QUERY).fetchone() == before
        assert not _temporary_tables(connection)
        assert (
            sum(path.stat().st_size for path in tmp_path.rglob("*") if path.is_file())
            <= 64 * 1024**2
        )


@pytest.mark.parametrize("timezone", ["Europe/Zurich", "America/New_York"])
@pytest.mark.parametrize("order", ["ASC", "DESC"])
def test_golden_duplicate_null_decimal_timestamp_and_nested_rows(timezone, order):
    with duckdb.connect(":memory:", config={"threads": 2, "memory_limit": "64MiB"}) as connection:
        connection.execute("SET TimeZone = ?", [timezone])
        assert semantic_fingerprint(
            connection, f"SELECT * FROM ({SMALL_QUERY}) r WHERE id > ? ORDER BY id {order}", [0]
        ) == _golden(SMALL_SCHEMA, [FIRST_ROW, FIRST_ROW, SECOND_ROW])
        assert connection.execute("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)


def test_empty_and_filtered_parameterized_rows_keep_the_exact_schema():
    with duckdb.connect(":memory:") as connection:
        query = f"SELECT * FROM ({SMALL_QUERY}) r WHERE id > ?"
        assert semantic_fingerprint(connection, query, [2]) == _golden(SMALL_SCHEMA, [])
        assert semantic_fingerprint(connection, query, [1]) == _golden(SMALL_SCHEMA, [SECOND_ROW])
        renamed = [{"name": "new_id", "type": "INTEGER"}, *SMALL_SCHEMA[1:]]
        assert semantic_fingerprint(
            connection, f"SELECT id AS new_id, * EXCLUDE(id) FROM ({SMALL_QUERY}) r WHERE false"
        ) == _golden(renamed, [])


class FaultConnection:
    """Inject errors at actual CTAS/read/fetch boundaries while retaining a real connection."""

    def __init__(self, connection, failure):
        self.connection = connection
        self.failure = failure
        self.created_name = None
        self.ordered_read = False
        self.fetched = 0

    @property
    def description(self):
        return self.connection.description

    def execute(self, sql, parameters=None):
        statement = sql.strip().upper()
        if statement.startswith("CREATE TEMP TABLE "):
            self.created_name = sql.split()[3]
        self.ordered_read = (
            self.created_name is not None
            and statement.startswith("SELECT ")
            and "ORDER BY" in statement
        )
        if self.ordered_read and self.failure == "ordered-execute":
            raise RuntimeError("injected ordered execute failure")
        self.connection.execute(sql, parameters or [])
        return self

    def fetchone(self):
        if self.ordered_read and self.failure in {"fetch", "interrupt"}:
            self.fetched += 1
            if self.fetched == 2:
                failure = KeyboardInterrupt if self.failure == "interrupt" else RuntimeError
                raise failure("injected fetch failure")
        return self.connection.fetchone()

    def fetchmany(self, size):
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows


@pytest.mark.parametrize(
    "failure", ["source-bind", "source-execute", "ordered-execute", "fetch", "interrupt"]
)
def test_failed_fingerprint_cleans_only_its_temporary_hashes_and_preserves_settings(
    tmp_path, failure
):
    with duckdb.connect(
        ":memory:",
        config={
            "memory_limit": "64MiB",
            "threads": 2,
            "temp_directory": str(tmp_path),
            "max_temp_directory_size": "64MiB",
            "preserve_insertion_order": True,
        },
    ) as connection:
        connection.execute("CREATE TEMP TABLE retained_marker AS SELECT 'keep' AS value")
        before_tables = _temporary_tables(connection)
        before_settings = connection.execute(SETTINGS_QUERY).fetchone()
        query = "SELECT i AS id FROM range(5) t(i)"
        if failure == "source-bind":
            query = "SELECT absent_column FROM range(5) t(i)"
        elif failure == "source-execute":
            query = "SELECT CASE WHEN i=4 THEN error('injected source failure') ELSE i END AS id FROM range(5) t(i)"
        expected_error, message = {
            "source-bind": (duckdb.BinderException, "absent_column"),
            "source-execute": (duckdb.InvalidInputException, "injected source failure"),
            "ordered-execute": (RuntimeError, "injected ordered execute failure"),
            "fetch": (RuntimeError, "injected fetch failure"),
            "interrupt": (KeyboardInterrupt, "injected fetch failure"),
        }[failure]
        proxy = FaultConnection(connection, failure)
        with pytest.raises(expected_error, match=message):
            semantic_fingerprint(proxy, query)
        if failure != "source-bind":
            assert proxy.created_name is not None
        if failure in {"fetch", "interrupt"}:
            assert proxy.fetched == 2
        assert _temporary_tables(connection) == before_tables
        assert connection.execute("SELECT * FROM retained_marker").fetchall() == [("keep",)]
        assert connection.execute(SETTINGS_QUERY).fetchone() == before_settings
        assert semantic_fingerprint(connection, "SELECT 1 AS id")["rows"] == 1


def test_existing_temporary_name_collision_is_not_removed(monkeypatch):
    identifier = UUID(int=1)
    monkeypatch.setattr("oddsfox_catalogue.fingerprints.uuid4", lambda: identifier)
    name = f'"temp"."main"."_oddsfox_fingerprint_{identifier.hex}"'
    with duckdb.connect(":memory:") as connection:
        connection.execute(f"CREATE TEMP TABLE {name} (sentinel VARCHAR)")
        connection.execute(f"INSERT INTO {name} VALUES ('keep collision')")
        before = _temporary_tables(connection)
        with pytest.raises(duckdb.CatalogException, match="already exists"):
            semantic_fingerprint(connection, "SELECT 1 AS id")
        assert connection.execute(f"SELECT * FROM {name}").fetchall() == [("keep collision",)]
        assert _temporary_tables(connection) == before


def test_borrowed_nonmain_cursor_does_not_touch_a_same_named_persistent_table(monkeypatch):
    identifier = UUID(int=2)
    monkeypatch.setattr("oddsfox_catalogue.fingerprints.uuid4", lambda: identifier)
    name = f'"_oddsfox_fingerprint_{identifier.hex}"'
    with duckdb.connect(":memory:", config={"memory_limit": "64MiB", "threads": 2}) as owner:
        owner.execute("CREATE SCHEMA borrowed")
        owner.execute(f"CREATE TABLE borrowed.{name} AS SELECT 'persistent marker' AS value")
        owner.execute("CREATE TABLE borrowed.source_rows AS SELECT 7 AS id")
        with owner.cursor() as cursor:
            cursor.execute("SET schema='borrowed'")
            before = cursor.execute(SETTINGS_QUERY).fetchone()
            assert semantic_fingerprint(cursor, "SELECT * FROM source_rows") == _golden(
                [{"name": "id", "type": "INTEGER"}], ['{"id":7}']
            )
            assert cursor.execute("SELECT current_schema()").fetchone() == ("borrowed",)
            assert cursor.execute(SETTINGS_QUERY).fetchone() == before
            assert not _temporary_tables(cursor)
            assert cursor.execute(f"SELECT * FROM {name}").fetchall() == [("persistent marker",)]
        assert owner.execute(f"SELECT * FROM borrowed.{name}").fetchall() == [
            ("persistent marker",)
        ]


def test_fingerprinting_does_not_commit_the_callers_transaction():
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE TABLE source_rows (id INTEGER)")
        connection.execute("BEGIN TRANSACTION")
        connection.execute("INSERT INTO source_rows VALUES (7)")
        for _ in range(4):
            assert semantic_fingerprint(connection, "SELECT * FROM source_rows") == _golden(
                [{"name": "id", "type": "INTEGER"}], ['{"id":7}']
            )
            assert not _temporary_tables(connection)
        connection.execute("ROLLBACK")
        assert connection.execute("SELECT * FROM source_rows").fetchall() == []


def test_readonly_file_fingerprint_keeps_source_order_and_database_bytes(tmp_path):
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE source_rows (id INTEGER, label VARCHAR)")
        connection.execute(
            "INSERT INTO source_rows VALUES (3, 'third'), (1, 'first'), (2, 'second')"
        )
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with duckdb.connect(
        str(path),
        read_only=True,
        config={
            "memory_limit": "64MiB",
            "threads": 2,
            "temp_directory": str(tmp_path / "spill"),
            "max_temp_directory_size": "64MiB",
            "preserve_insertion_order": True,
        },
    ) as connection:
        before = connection.execute(SETTINGS_QUERY).fetchone()
        expected = _golden(
            [
                {"name": "id", "type": "INTEGER"},
                {"name": "label", "type": "VARCHAR"},
            ],
            ['{"id":3,"label":"third"}', '{"id":1,"label":"first"}', '{"id":2,"label":"second"}'],
        )
        assert semantic_fingerprint(connection, "SELECT * FROM source_rows") == expected
        assert connection.execute("SELECT id FROM source_rows").fetchall() == [(3,), (1,), (2,)]
        assert connection.execute(SETTINGS_QUERY).fetchone() == before
        assert not _temporary_tables(connection)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original
    assert not Path(str(path) + ".wal").exists()


def test_hash_materialization_uses_32_byte_blobs_without_changing_golden_digest():
    class ObservedConnection(FaultConnection):
        def __init__(self, connection):
            super().__init__(connection, None)
            self.hash_storage = None

        def execute(self, sql, parameters=None):
            result = super().execute(sql, parameters)
            if sql.strip().upper().startswith("CREATE TEMP TABLE "):
                self.hash_storage = self.connection.execute(
                    f"SELECT typeof(row), octet_length(row) FROM {self.created_name}"
                ).fetchall()
            return result

    with duckdb.connect(":memory:") as connection:
        observed = ObservedConnection(connection)
        assert semantic_fingerprint(observed, SMALL_QUERY) == _golden(
            SMALL_SCHEMA, [FIRST_ROW, FIRST_ROW, SECOND_ROW]
        )
        assert observed.hash_storage == [("BLOB", 32)] * 3
        assert not _temporary_tables(connection)
