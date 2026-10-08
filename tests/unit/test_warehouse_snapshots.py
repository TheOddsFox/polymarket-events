"""last_two_open_event_counts: which failures stay silent and which must be logged."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue.warehouse import last_two_open_event_counts

LOGGER = "oddsfox_catalogue.warehouse"


def _snapshot_db(path: Path, column: str = "open_events") -> None:
    connection = duckdb.connect(str(path))
    connection.execute("CREATE SCHEMA marts")
    connection.execute(
        f"CREATE TABLE marts.catalogue_snapshots (captured_at TIMESTAMP, {column} INTEGER)"
    )
    connection.execute(
        f"INSERT INTO marts.catalogue_snapshots (captured_at, {column}) VALUES "
        "(now() - INTERVAL 1 HOUR, 100), (now(), 80)"
    )
    connection.close()


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage() for r in caplog.records if r.name == LOGGER and r.levelno >= logging.WARNING
    ]


def test_skip_warning_is_reported_through_the_supplied_callback(tmp_path: Path) -> None:
    warehouse = tmp_path / "catalogue.duckdb"
    warehouse.write_bytes(b"not a duckdb file" * 4096)
    reported: list[str] = []

    result = last_two_open_event_counts(
        warehouse, warn=lambda message, *args: reported.append(message % args)
    )

    assert result is None
    assert len(reported) == 1
    assert "cannot open" in reported[0]


def test_returns_previous_and_latest_counts(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    warehouse = tmp_path / "catalogue.duckdb"
    _snapshot_db(warehouse)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert last_two_open_event_counts(warehouse) == (100, 80)
    assert _warnings(caplog) == []


def test_missing_file_returns_none_without_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert last_two_open_event_counts(tmp_path / "absent.duckdb") is None
    assert _warnings(caplog) == []


def test_missing_snapshot_table_returns_none_without_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    warehouse = tmp_path / "catalogue.duckdb"
    duckdb.connect(str(warehouse)).close()

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert last_two_open_event_counts(warehouse) is None
    assert _warnings(caplog) == []


def test_snapshot_column_drift_logs_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    warehouse = tmp_path / "catalogue.duckdb"
    _snapshot_db(warehouse, column="open_count")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert last_two_open_event_counts(warehouse) is None
    messages = _warnings(caplog)
    assert len(messages) == 1
    assert "snapshot query failed" in messages[0]


def test_corrupt_file_logs_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    warehouse = tmp_path / "catalogue.duckdb"
    warehouse.write_bytes(b"not a duckdb file" * 4096)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert last_two_open_event_counts(warehouse) is None
    messages = _warnings(caplog)
    assert len(messages) == 1
    assert "cannot open" in messages[0]


def test_file_held_by_read_write_connection_logs_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    warehouse = tmp_path / "catalogue.duckdb"
    _snapshot_db(warehouse)
    writer = duckdb.connect(str(warehouse))
    try:
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            assert last_two_open_event_counts(warehouse) is None
    finally:
        writer.close()
    messages = _warnings(caplog)
    assert len(messages) == 1
    assert "cannot open" in messages[0]
