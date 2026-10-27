from datetime import date, datetime
from decimal import Decimal

import duckdb
import pytest

from oddsfox_catalogue import current_config
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, memory_connection


@pytest.mark.parametrize(
    "factory",
    [connect_store, memory_connection],
    ids=["persistent", "memory"],
)
def test_connections_start_with_bounded_duckdb_2_runtime_settings(tmp_path, monkeypatch, factory):
    root = current_config.validate_root(tmp_path / "catalogue", initialize=True)
    settings = CurrentSettings(root)
    real_connect = duckdb.connect
    connect_options = []

    def observe_connect(*args, **kwargs):
        connect_options.append(kwargs.get("config", {}).copy())
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(current_config.duckdb, "connect", observe_connect)

    with factory(settings) as connection:
        assert connection.execute(
            """SELECT current_setting('async_threads'), current_setting('read_ahead_depth'),
                      current_setting('threads'), current_setting('memory_limit')"""
        ).fetchone() == (2, -1, 2, "2.0 GiB")

    assert len(connect_options) == 1
    assert connect_options[0]["async_threads"] == 2
    assert connect_options[0]["read_ahead_depth"] == -1


def test_dbapi_scalar_types_and_repr_remain_stable():
    with duckdb.connect(":memory:") as connection:
        row = connection.execute(
            """SELECT 7::BIGINT,
                      0.123456789012345678::DECIMAL(38,18),
                      DATE '2026-10-27',
                      TIMESTAMP '2026-10-27 08:30:00',
                      '{\"x\":1}'::JSON,
                      [1,NULL,2]::INTEGER[],
                      NULL::VARCHAR"""
        ).fetchone()

    assert tuple(map(type, row)) == (int, Decimal, date, datetime, str, list, type(None))
    assert repr(row) == (
        "(7, Decimal('0.123456789012345678'), datetime.date(2026, 10, 27), "
        "datetime.datetime(2026, 10, 27, 8, 30), '{\"x\":1}', [1, None, 2], None)"
    )
