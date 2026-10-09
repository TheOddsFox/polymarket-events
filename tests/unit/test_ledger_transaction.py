"""Ledger transactions: a signal at BEGIN or COMMIT leaves no open transaction and masks nothing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from oddsfox_catalogue.capture.ledger import Ledger


class _Signal(BaseException):
    """Stands in for SIGTERM, delivered right after one statement runs."""


class _SignalAfter:
    """Connection proxy that raises ``_Signal`` once, right after the named statement runs."""

    def __init__(self, conn: Any, statement: str) -> None:
        self._conn = conn
        self._statement = statement
        self._armed = True

    def execute(self, sql: str, *args: Any) -> Any:
        result = self._conn.execute(sql, *args)
        if self._armed and sql == self._statement:
            self._armed = False
            raise _Signal(sql)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@pytest.mark.parametrize("statement", ["BEGIN IMMEDIATE", "COMMIT"])
def test_a_signal_at_begin_or_commit_leaves_no_open_transaction(
    tmp_path: Path, statement: str
) -> None:
    ledger = Ledger(tmp_path / "ledger.sqlite")
    real = ledger._conn
    try:
        ledger._conn = _SignalAfter(real, statement)
        with pytest.raises(_Signal), ledger.transaction() as conn:
            conn.execute("SELECT 1")
        assert not real.in_transaction

        ledger._conn = real
        with ledger.transaction() as conn:
            conn.execute("SELECT 1")
    finally:
        ledger._conn = real
        ledger.close()
