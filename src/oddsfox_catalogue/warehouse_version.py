"""Refuse legacy warehouses before any product mutation; roots are never migrated."""

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import CONTRACT, NORMALIZATION_REVISION


class WarehouseVersionError(ValueError):
    pass


def ensure_warehouse_contract(settings: Settings, *, create: bool = False) -> None:
    path = settings.warehouse_path
    if any(member.is_symlink() for member in (path, *path.parents)):
        raise WarehouseVersionError("warehouse path contains a symlink")
    if not path.exists() and not create:
        return
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            from oddsfox_catalogue.certification import mark_dirty

            mark_dirty(settings, "warehouse initialization")
    try:
        connection = duckdb.connect(
            str(path),
            read_only=not create,
            config={
                "memory_limit": settings.load.duckdb_memory_limit,
                "threads": settings.load.duckdb_threads,
                "max_temp_directory_size": "0B",
            },
        )
    except duckdb.Error as exc:
        raise WarehouseVersionError(
            "unreadable warehouse; retain this root and use a fresh root"
        ) from exc
    try:
        stamped = connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'catalogue_meta' AND table_name = 'contract'"
        ).fetchone()[0]
        if stamped:
            rows = connection.execute(
                "SELECT contract, normalization_revision FROM catalogue_meta.contract"
            ).fetchall()
            if rows == [(CONTRACT, NORMALIZATION_REVISION)]:
                return
        elif (
            create
            and connection.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN "
                "('information_schema', 'pg_catalog')"
            ).fetchone()[0]
            == 0
        ):
            from oddsfox_catalogue.certification import mark_dirty

            mark_dirty(settings, "warehouse contract initialization")
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute("CREATE SCHEMA catalogue_meta")
                connection.execute(
                    "CREATE TABLE catalogue_meta.contract "
                    "(contract VARCHAR NOT NULL, normalization_revision VARCHAR NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO catalogue_meta.contract VALUES (?, ?)",
                    [CONTRACT, NORMALIZATION_REVISION],
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            return
        raise WarehouseVersionError(
            "incompatible warehouse contract; retain this root and use a fresh CATALOGUE_ROOT"
        )
    except duckdb.Error as exc:
        raise WarehouseVersionError(
            "incompatible warehouse schema; use a fresh CATALOGUE_ROOT"
        ) from exc
    finally:
        connection.close()
