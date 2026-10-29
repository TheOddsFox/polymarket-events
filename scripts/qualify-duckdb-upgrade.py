#!/usr/bin/env python3
"""Inventory and copy-proof a quiescent, stamped catalogue_store.v4 root."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import resource
import shutil
import stat
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from oddsfox_catalogue import current_publish  # noqa: E402
from oddsfox_catalogue.current_config import (  # noqa: E402
    GIB,
    data_path,
    load_current_settings,
    safe_path,
    validate_root,
)
from oddsfox_catalogue.current_contract import STORE_CONTRACT, CurrentError  # noqa: E402
from oddsfox_catalogue.current_store import initialize, validate_store  # noqa: E402
from oddsfox_catalogue.fingerprints import semantic_fingerprint  # noqa: E402
from oddsfox_catalogue.runlock import RunBusy  # noqa: E402

MEMORY_BYTES = 2 * GIB
THREADS = 2


def require(condition, message):
    if not condition:
        raise ValueError(message)


@contextmanager
def _catalogue_lock(path):
    path = data_path(path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        opened = os.fstat(fd)
        require(stat.S_ISREG(opened.st_mode), "catalogue lock is not a regular file")
        require(opened.st_nlink == 1, "catalogue lock has multiple hard links")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RunBusy(f"another catalogue run holds {path}") from exc
        current = os.stat(path, follow_symlinks=False)
        require(
            stat.S_ISREG(current.st_mode)
            and current.st_nlink == 1
            and (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino),
            "catalogue lock path changed while acquiring its lock",
        )
        try:
            yield
        finally:
            current = os.stat(path, follow_symlinks=False)
            require(
                stat.S_ISREG(current.st_mode)
                and current.st_nlink == 1
                and (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino),
                "catalogue lock path changed during qualification",
            )
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def _open_regular(path: Path, flags=os.O_RDONLY):
    fd = os.open(path, flags | getattr(os, "O_NOFOLLOW", 0))
    mode = os.fstat(fd).st_mode
    if not stat.S_ISREG(mode):
        os.close(fd)
        raise ValueError("catalogue root contains a special file")
    return os.fdopen(fd, "rb" if flags == os.O_RDONLY else "wb")


def _file_digest(path: Path):
    digest = hashlib.sha256()
    size = 0
    with _open_regular(path) as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
            size += len(block)
    return size, digest.hexdigest()


def _root_paths(root: Path):
    """Walk without following links and return relative files/directories."""
    root = safe_path(root)
    require(root.is_dir(), "catalogue root is not a regular directory")
    directories, files = [], []

    def visit(directory: Path, relative: Path):
        with os.scandir(directory) as entries:
            for entry in sorted(entries, key=lambda item: item.name):
                path = Path(entry.path)
                rel = relative / entry.name
                mode = entry.stat(follow_symlinks=False).st_mode
                require(not stat.S_ISLNK(mode), "catalogue root contains a symlink")
                require(
                    rel == Path("catalogue.json") or rel.name != "catalogue.json",
                    "catalogue root contains a nested catalogue root",
                )
                if stat.S_ISDIR(mode):
                    directories.append(rel)
                    visit(path, rel)
                elif stat.S_ISREG(mode):
                    files.append(rel)
                else:
                    raise ValueError("catalogue root contains a special file")

    visit(root, Path())
    tmp = root / "tmp"
    if tmp.exists():
        require(
            tmp.is_dir() and not any(tmp.iterdir()), "catalogue root tmp directory is not empty"
        )
    return directories, files


def _source_layout(root: Path):
    root = validate_root(root)
    directories, files = _root_paths(root)
    require(Path("catalogue.duckdb") in files, "catalogue database is missing")
    require(
        Path("catalogue.duckdb.wal") not in files,
        "catalogue database has a WAL; close writers and checkpoint before qualification",
    )
    db_path = root / "catalogue.duckdb"
    sidefiles, side_bytes = [], 0
    side_directories = []
    for rel in directories:
        if rel != Path("tmp"):
            side_directories.append(rel)
    for rel in files:
        if rel == Path(".catalogue.lock") or rel == Path("catalogue.duckdb"):
            continue
        size = (root / rel).stat(follow_symlinks=False).st_size
        sidefiles.append((rel, size))
        side_bytes += size
    return {
        "root": root,
        "directories": directories,
        "files": files,
        "side_directories": side_directories,
        "sidefiles": sidefiles,
        "side_bytes": side_bytes,
        "database": db_path,
    }


def _tree_fingerprint(layout):
    root = layout["root"]
    records = []
    for rel in layout["directories"]:
        if rel != Path("tmp"):
            records.append({"kind": "directory", "path": rel.as_posix()})
    side_bytes = 0
    for rel, _ in layout["sidefiles"]:
        size, digest = _file_digest(root / rel)
        side_bytes += size
        records.append({"kind": "file", "path": rel.as_posix(), "bytes": size, "sha256": digest})
    records.sort(key=lambda row: (row["kind"], row["path"]))
    db_bytes, db_sha = _file_digest(layout["database"])
    return {
        "sidefiles": {
            "file_count": len(layout["sidefiles"]),
            "directory_count": len(layout["side_directories"]),
            "bytes": side_bytes,
            "sha256": _sha256_bytes(_json_bytes(records)),
        },
        "database": {"bytes": db_bytes, "sha256": db_sha},
    }


def _storage_tag(connection):
    rows = connection.execute(
        "SELECT tags['storage_version'] FROM duckdb_databases() "
        "WHERE database_name=current_database()"
    ).fetchall()
    require(len(rows) == 1 and rows[0][0], "cannot determine database storage version")
    return rows[0][0]


def _engine_version_pair():
    parts = duckdb.__version__.split(".")
    pair = tuple(map(int, parts[:2]))
    require(pair in {(1, 5), (2, 0)}, "qualification supports DuckDB 1.5.x or 2.0.x")
    return pair


def _canonical_sql(value):
    """Collapse insignificant whitespace while retaining quoted SQL text."""
    if value is None:
        return None
    result, quote, whitespace = [], None, False
    for char in value.strip().rstrip(";").strip():
        if quote:
            result.append(char)
            if char == quote:
                quote = None
        elif char in "'\"`":
            if whitespace and result:
                result.append(" ")
            whitespace = False
            quote = char
            result.append(char)
        elif char.isspace():
            whitespace = True
        else:
            if whitespace and result:
                result.append(" ")
            whitespace = False
            result.append(char)
    return "".join(result)


def _qualified(schema, name):
    def quote(part):
        return '"' + str(part).replace('"', '""') + '"'

    return f"{quote(schema)}.{quote(name)}"


def _table_semantics(connection, database):
    tables = connection.execute(
        "SELECT schema_name,table_name FROM duckdb_tables() "
        "WHERE database_name=? AND NOT internal AND NOT temporary "
        "ORDER BY schema_name,table_name",
        [database],
    ).fetchall()
    column_catalog_fields = {
        row[0] for row in connection.execute("SELECT * FROM duckdb_columns() LIMIT 0").description
    }
    column_tags = "tags" if "tags" in column_catalog_fields else "'{}'::MAP(VARCHAR,VARCHAR)"
    table_schemas, table_data, total_rows = [], [], 0
    for schema, table in tables:
        relation = _qualified(schema, table)
        description = connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        column_metadata = {
            row[0]: {"comment": row[1], "tags": row[2]}
            for row in connection.execute(
                f"SELECT column_name,comment,{column_tags} AS tags FROM duckdb_columns() "
                "WHERE database_name=? AND schema_name=? AND table_name=? "
                "AND NOT internal ORDER BY column_index",
                [database, schema, table],
            ).fetchall()
        }
        table_metadata = connection.execute(
            "SELECT comment,tags,sql FROM duckdb_tables() "
            "WHERE database_name=? AND schema_name=? AND table_name=?",
            [database, schema, table],
        ).fetchone()
        require(table_metadata is not None, "table catalog metadata is missing")
        table_schemas.append(
            {
                "schema": schema,
                "name": table,
                "comment": table_metadata[0],
                "tags": table_metadata[1],
                "sql": _canonical_sql(table_metadata[2]),
                "columns": [
                    {
                        "name": row[0],
                        "type": str(row[1]),
                        "null": row[2],
                        "key": row[3],
                        "default": _canonical_sql(row[4]),
                        "extra": _canonical_sql(row[5]),
                        **column_metadata[row[0]],
                    }
                    for row in description
                ],
            }
        )
        # Hash JSON rows in DuckDB so row values stay typed and the 5M-row table is streamed.
        projections = []
        for column in description:
            name = '"' + column[0].replace('"', '""') + '"'
            if re.search(r"\bJSON\b", str(column[1]), re.IGNORECASE):
                projections.append(f"CAST({name} AS VARCHAR) AS {name}")
            else:
                projections.append(name)
        fingerprint = semantic_fingerprint(
            connection,
            f"SELECT to_json(r) AS _row FROM (SELECT {','.join(projections)} FROM {relation}) AS r",
        )
        total_rows += fingerprint["rows"]
        table_data.append(
            {
                "schema": schema,
                "name": table,
                "rows": fingerprint["rows"],
                "sha256": fingerprint["semantic_sha256"],
            }
        )
    return table_schemas, table_data, total_rows


def _object_semantics(connection, database):
    schemas = [
        [row[0], row[1], row[2], _canonical_sql(row[3])]
        for row in connection.execute(
            "SELECT schema_name,comment,tags,sql FROM duckdb_schemas() WHERE database_name=? "
            "AND NOT internal ORDER BY schema_name",
            [database],
        ).fetchall()
    ]
    views = [
        [row[0], row[1], row[2], row[3], _canonical_sql(row[4])]
        for row in connection.execute(
            "SELECT schema_name,view_name,comment,tags,sql FROM duckdb_views() "
            "WHERE database_name=? AND NOT internal AND NOT temporary "
            "ORDER BY schema_name,view_name",
            [database],
        ).fetchall()
    ]
    macros = [
        [
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            _canonical_sql(row[11]),
        ]
        for row in connection.execute(
            "SELECT schema_name,function_name,function_type,parameters,parameter_types,"
            "varargs,macro_definition,return_type,description,comment,tags,alias_of "
            "FROM duckdb_functions() WHERE database_name=? "
            "AND NOT internal AND function_type IN ('macro','table_macro') "
            "ORDER BY schema_name,function_name,function_type,parameters",
            [database],
        ).fetchall()
    ]
    unsupported_functions = connection.execute(
        "SELECT count(*) FROM duckdb_functions() WHERE database_name=? AND NOT internal "
        "AND function_type NOT IN ('macro','table_macro')",
        [database],
    ).fetchone()[0]
    sequences = [
        [row[0], row[1], row[2], row[3], *row[4:]]
        for row in connection.execute(
            "SELECT schema_name,sequence_name,comment,tags,start_value,min_value,max_value,increment_by,"
            "cycle,last_value,sql FROM duckdb_sequences() WHERE database_name=? AND NOT temporary "
            "ORDER BY schema_name,sequence_name",
            [database],
        ).fetchall()
    ]
    indexes = [
        [
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            _canonical_sql(row[7]),
            _canonical_sql(row[8]),
        ]
        for row in connection.execute(
            "SELECT schema_name,index_name,table_name,comment,tags,is_unique,is_primary,expressions,sql "
            "FROM duckdb_indexes() WHERE database_name=? "
            "ORDER BY schema_name,index_name,table_name",
            [database],
        ).fetchall()
    ]
    constraints = [
        [
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            _canonical_sql(row[5]),
            row[6],
            row[7],
            row[8],
        ]
        for row in connection.execute(
            "SELECT schema_name,table_name,constraint_name,constraint_type,"
            "constraint_column_names,expression,referenced_table,referenced_column_names,"
            "constraint_text FROM duckdb_constraints() WHERE database_name=? "
            "ORDER BY schema_name,table_name,constraint_index",
            [database],
        ).fetchall()
    ]
    types = [
        [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]]
        for row in connection.execute(
            "SELECT schema_name,type_name,type_size,logical_type,type_category,labels,comment,tags "
            "FROM duckdb_types() WHERE database_name=? AND NOT internal "
            "ORDER BY schema_name,type_name",
            [database],
        ).fetchall()
    ]
    triggers = []
    trigger_catalog = {
        row[0]
        for row in connection.execute("SELECT function_name FROM duckdb_functions()").fetchall()
    }
    if "duckdb_triggers" in trigger_catalog:
        triggers = [
            list(row)
            for row in connection.execute(
                "SELECT schema_name,trigger_name,table_name,action_timing,event_manipulation,"
                "columns,for_each,sql FROM duckdb_triggers() WHERE database_name=? "
                "ORDER BY schema_name,trigger_name",
                [database],
            ).fetchall()
        ]
    object_data = {
        "schemas": schemas,
        "views": views,
        "macros": macros,
        "sequences": sequences,
        "indexes": indexes,
        "constraints": constraints,
        "types": types,
        "triggers": triggers,
        "unsupported_functions": unsupported_functions,
    }
    counts = {
        "schemas": len(schemas),
        "views": len(views),
        "macros": len(macros),
        "sequences": len(sequences),
        "indexes": len(indexes),
        "constraints": len(constraints),
        "types": len(types),
        "triggers": len(triggers),
        "unsupported_functions": unsupported_functions,
    }
    return object_data, counts


def _connection_config(settings):
    pair = _engine_version_pair()
    config = {
        "memory_limit": f"{min(MEMORY_BYTES, settings.limits.duckdb_memory_bytes)}B",
        "threads": str(min(THREADS, int(settings.limits.duckdb_threads))),
        "max_temp_directory_size": f"{settings.limits.max_temp_bytes}B",
        "preserve_insertion_order": "false",
    }
    if pair >= (2, 0):
        config.update(async_threads=config["threads"], read_ahead_depth="-1")
    return config


def _database_inventory(root, settings, temporary):
    with duckdb.connect(
        str(root / "catalogue.duckdb"), read_only=True, config=_connection_config(settings)
    ) as connection:
        connection.execute("SET temp_directory=?", [str(temporary)])
        connection.execute("SET TimeZone='UTC'")
        initialize(connection)
        verification = current_publish.verify_current(settings, connection=connection)
        require(verification.get("valid") is True, "current release is not valid")
        if verification.get("store_generation") != verification.get("published_generation"):
            validate_store(connection)
        pointer = current_publish._pointer(settings)
        previous = pointer.get("previous_release_id")
        previous_valid = previous is None
        if previous is not None:
            path = settings.published_dir / "releases" / previous
            release = current_publish._verify_release_on_connection(
                path, connection, bind_store=False
            )
            require(release.get("release_id") == previous, "previous release identity mismatch")
            previous_valid = True
        database = connection.execute("SELECT current_database()").fetchone()[0]
        schemas, data, row_count = _table_semantics(connection, database)
        objects, object_counts = _object_semantics(connection, database)
        return {
            "engine_version": duckdb.__version__,
            "storage_version": _storage_tag(connection),
            "table_count": len(schemas),
            "row_count": row_count,
            "schema_sha256": _sha256_bytes(_json_bytes(schemas)),
            "data_sha256": _sha256_bytes(_json_bytes(data)),
            "schema_objects_sha256": _sha256_bytes(_json_bytes(objects)),
            "schema_object_counts": object_counts,
            "release_valid": True,
            "previous_release_present": previous is not None,
            "previous_release_valid": previous_valid,
            "quiescent": True,
        }


def _prepare_source_locked(root, settings, temporary):
    root = safe_path(Path(root))
    require(root.is_dir(), "catalogue root is not a regular directory")
    validate_root(root)
    layout = _source_layout(root)
    before = _tree_fingerprint(layout)
    database = _database_inventory(root, settings, temporary)
    after_layout = _source_layout(root)
    after = _tree_fingerprint(after_layout)
    require(before == after, "catalogue source changed during qualification")
    return settings, layout, before, database


def _prepare_source(root, temporary, settings=None):
    root = safe_path(Path(root))
    validate_root(root)
    settings = settings or load_current_settings(root, env={})
    with _catalogue_lock(settings.run_lock_path):
        return _prepare_source_locked(root, settings, temporary)


def _safe_evidence_output(path: Path, *roots: Path):
    path = data_path(path)
    require(not path.exists(), "qualification report output must be fresh")
    require(path.parent.is_dir(), "qualification report parent must exist")
    for root in roots:
        resolved = root.resolve()
        require(
            path != resolved and resolved not in path.parents,
            "report output must be outside catalogue roots",
        )
    return path


def _temp_directory(parent: Path, *roots: Path):
    parent = safe_path(parent)
    data_path(parent / "duckdb-qualification-probe")
    path = Path(tempfile.mkdtemp(prefix="duckdb-qualification-", dir=parent))
    path = data_path(path)
    for root in roots:
        resolved = root.resolve()
        require(
            path != resolved and resolved not in path.parents,
            "temporary evidence path overlaps a catalogue root",
        )
    return path


def _write_report(path: Path, report):
    path = data_path(path)
    encoded = (json.dumps(report, sort_keys=True, indent=2) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def inventory(root: Path, output: Path):
    root = data_path(root)
    output = _safe_evidence_output(output, root)
    temporary = _temp_directory(output.parent, root)
    try:
        settings = load_current_settings(root, env={})
        require(
            shutil.disk_usage(temporary).free
            >= settings.limits.max_temp_bytes + settings.limits.free_reserve_bytes,
            "evidence temporary volume cannot reserve the configured temp allowance",
        )
        settings, _, physical, database = _prepare_source(root, temporary, settings)
        report = {
            "format": "oddsfox.duckdb-upgrade-inventory.v1",
            "root_contract": STORE_CONTRACT,
            "engine_version": database["engine_version"],
            "source": {
                "non_database": physical["sidefiles"],
                "database_bytes": physical["database"]["bytes"],
                "database_allocated_bytes": (root / "catalogue.duckdb").stat().st_blocks * 512,
                "database_sha256": physical["database"]["sha256"],
                **{key: value for key, value in database.items() if key != "engine_version"},
            },
        }
        _write_report(output, report)
        return report
    finally:
        shutil.rmtree(temporary)


def _validate_destination(source: Path, destination: Path, output: Path):
    source, destination = safe_path(source), data_path(destination)
    require(not destination.exists(), "conversion destination must not exist")
    require(destination.parent.is_dir(), "conversion destination parent must exist")
    source, destination = source.resolve(), destination.resolve()
    require(
        source != destination
        and source not in destination.parents
        and destination not in source.parents,
        "conversion source and destination must be physically disjoint",
    )
    _safe_evidence_output(output, source, destination)
    for ancestor in destination.parents:
        if (ancestor / "catalogue.json").exists():
            raise ValueError(
                "conversion destination cannot be nested inside another catalogue root"
            )
        if (
            any((ancestor / name).exists() for name in ("ledger.sqlite", "warehouse"))
            or (ancestor / ".state" / "ledger.sqlite").exists()
        ):
            raise ValueError("conversion destination cannot be nested inside a legacy root")
    return source, destination


def _copy_sidefiles(layout, destination):
    destination = data_path(destination)
    destination.mkdir(mode=0o700)
    for relative in sorted(
        layout["directories"], key=lambda item: (len(item.parts), item.as_posix())
    ):
        (destination / relative).mkdir(mode=0o700, parents=True, exist_ok=True)
    for relative, _ in layout["sidefiles"]:
        source_path, target_path = layout["root"] / relative, destination / relative
        target_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with _open_regular(source_path) as source_stream:
            fd = os.open(
                target_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with os.fdopen(fd, "wb") as target_stream:
                shutil.copyfileobj(source_stream, target_stream, 1024 * 1024)


def _copy_database(source, destination, storage_version, settings, temporary):
    destination, temporary = data_path(destination), data_path(temporary)
    require(
        not (destination / "catalogue.duckdb").exists(),
        "conversion database destination must be absent",
    )
    pair = _engine_version_pair()
    require(
        storage_version != "latest" or pair >= (2, 0),
        "latest storage conversion requires DuckDB 2.x",
    )
    previous_umask = os.umask(0o077)
    try:
        memory = duckdb.connect(":memory:", config=_connection_config(settings))
        try:
            memory.execute("SET temp_directory=?", [str(temporary)])
            source_sql = "'" + str(source / "catalogue.duckdb").replace("'", "''") + "'"
            target_sql = "'" + str(destination / "catalogue.duckdb").replace("'", "''") + "'"
            memory.execute(f"ATTACH {source_sql} AS source_catalogue (READ_ONLY)")
            memory.execute(
                f"ATTACH {target_sql} AS target_catalogue (STORAGE_VERSION '{storage_version}')"
            )
            memory.execute("COPY FROM DATABASE source_catalogue TO target_catalogue")
        finally:
            memory.close()
    finally:
        os.umask(previous_umask)


def _allocated_bytes(path):
    return path.stat().st_blocks * 512


def convert(root: Path, destination: Path, storage_version: str, output: Path):
    root = data_path(root)
    root, destination = _validate_destination(root, destination, output)
    output = _safe_evidence_output(output, root, destination)
    validate_root(root)
    settings = load_current_settings(root, env={})
    limits = settings.limits
    require(
        limits.max_retained_bytes + limits.max_temp_bytes + limits.free_reserve_bytes
        <= shutil.disk_usage(destination.parent).free,
        "conversion cannot reserve the configured root, temp allowance and free-space reserve",
    )
    temporary = _temp_directory(destination.parent, root, destination)
    try:
        with _catalogue_lock(settings.run_lock_path):
            settings, layout, source_physical_before, source_inventory = _prepare_source_locked(
                root, settings, temporary
            )
            source_total = (
                source_physical_before["sidefiles"]["bytes"]
                + source_physical_before["database"]["bytes"]
            )
            require(
                source_total <= limits.max_retained_bytes,
                "catalogue source exceeds retained-size limit",
            )
            unsupported = source_inventory["schema_object_counts"]["unsupported_functions"]
            require(unsupported == 0, "catalogue contains unsupported database functions")
            require(
                source_inventory["schema_object_counts"]["triggers"] == 0,
                "COPY conversion does not qualify database triggers",
            )
            started = time.monotonic()
            _copy_sidefiles(layout, destination)
            _copy_database(root, destination, storage_version, settings, temporary)
            target_settings = load_current_settings(destination, env={})
            target_layout = _source_layout(destination)
            target_physical = _tree_fingerprint(target_layout)
            target_inventory = _database_inventory(destination, target_settings, temporary)
            source_after = _tree_fingerprint(_source_layout(root))
            require(
                source_after == source_physical_before, "catalogue source changed during conversion"
            )
            require(
                source_inventory["schema_sha256"] == target_inventory["schema_sha256"]
                and source_inventory["data_sha256"] == target_inventory["data_sha256"]
                and source_inventory["schema_objects_sha256"]
                == target_inventory["schema_objects_sha256"],
                "converted database inventory differs from source",
            )
            require(
                source_physical_before["sidefiles"] == target_physical["sidefiles"],
                "converted root sidefiles differ from source",
            )
            target_total = (
                target_physical["sidefiles"]["bytes"] + target_physical["database"]["bytes"]
            )
            require(target_total > 0, "converted root has no physical data")
            require(
                target_total <= limits.max_retained_bytes,
                "converted root exceeds retained-size limit",
            )
            require(
                shutil.disk_usage(destination.parent).free >= limits.free_reserve_bytes,
                "conversion consumed the configured free-space reserve",
            )
            report = {
                "format": "oddsfox.duckdb-upgrade-conversion.v1",
                "root_contract": STORE_CONTRACT,
                "engine_version": duckdb.__version__,
                "storage_version_requested": storage_version,
                "conversion_elapsed_s": time.monotonic() - started,
                "peak_rss_bytes": _peak_rss_bytes(),
                "peak_rss_scope": "qualifier process lifetime",
                "destination_total_bytes": target_total,
                "source": {
                    "database_bytes": source_physical_before["database"]["bytes"],
                    "database_allocated_bytes": _allocated_bytes(root / "catalogue.duckdb"),
                    "database_sha256": source_physical_before["database"]["sha256"],
                    "storage_version": source_inventory["storage_version"],
                    "schema_sha256": source_inventory["schema_sha256"],
                    "data_sha256": source_inventory["data_sha256"],
                    "schema_objects_sha256": source_inventory["schema_objects_sha256"],
                    "non_database_sha256": source_physical_before["sidefiles"]["sha256"],
                },
                "target": {
                    "database_bytes": target_physical["database"]["bytes"],
                    "database_allocated_bytes": _allocated_bytes(destination / "catalogue.duckdb"),
                    "database_sha256": target_physical["database"]["sha256"],
                    "storage_version": target_inventory["storage_version"],
                    "schema_sha256": target_inventory["schema_sha256"],
                    "data_sha256": target_inventory["data_sha256"],
                    "schema_objects_sha256": target_inventory["schema_objects_sha256"],
                    "non_database_sha256": target_physical["sidefiles"]["sha256"],
                },
                "equal": {
                    "source_unchanged": True,
                    "sidefiles": True,
                    "tables_and_rows": True,
                    "schema_objects": True,
                    "current_release_valid": target_inventory["release_valid"],
                    "previous_release_valid": target_inventory["previous_release_valid"],
                },
            }
            _write_report(output, report)
            return report
    finally:
        shutil.rmtree(temporary)


def _peak_rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    inventory_parser = subparsers.add_parser("inventory")
    inventory_parser.add_argument("--root", type=Path, required=True)
    inventory_parser.add_argument("--output", type=Path, required=True)
    convert_parser = subparsers.add_parser("convert")
    convert_parser.add_argument("--root", type=Path, required=True)
    convert_parser.add_argument("--destination", type=Path, required=True)
    convert_parser.add_argument("--storage-version", choices=("latest", "v1.5.0"), required=True)
    convert_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "inventory":
            inventory(args.root, args.output)
        else:
            convert(args.root, args.destination, args.storage_version, args.output)
    except (CurrentError, RunBusy, OSError, ValueError, duckdb.Error) as exc:
        parser.exit(2, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
