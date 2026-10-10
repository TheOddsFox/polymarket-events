"""Operator stop signals. SIGHUP and SIGTERM raise ``Terminated``, except during a drain.

Outside a hold, a signal raises ``Terminated`` where the process is. Inside a hold, the
first signal is recorded and nothing is raised, so a worker pool can finish draining and
release its clients. When the hold ends, the caller raises the recorded signal.

Signal handlers and the drain both run on the main thread, so the state needs no lock.
A lock would deadlock if a signal arrived while the hold held it.
"""

from __future__ import annotations

import signal
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass


class Terminated(BaseException):
    """The operator stopped the process with SIGHUP or SIGTERM.

    A ``BaseException`` so the capture recorder still writes its ``stage_runs`` row,
    and so ``except Exception`` does not treat an intentional stop as a crash worth retrying.
    """

    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(signal.Signals(signum).name)


@dataclass
class HeldSignal:
    """The first stop signal received during a hold, or ``None`` if none arrived."""

    signum: int | None = None


class SignalHold:
    """Defers stop signals while a drain runs. The first signal received is kept."""

    def __init__(self) -> None:
        self._depth = 0
        self._first: int | None = None

    def receive(self, signum: int) -> None:
        """Handle one stop signal. Raises ``Terminated`` unless a hold is active."""
        if self._depth == 0:
            raise Terminated(signum)
        if self._first is None:
            self._first = signum

    @contextmanager
    def hold(self) -> Iterator[HeldSignal]:
        """Defer signals for the body. ``held.signum`` is set when the hold ends."""
        held = HeldSignal()
        self._depth += 1
        try:
            yield held
        finally:
            self._depth -= 1
            if self._depth == 0:
                held.signum, self._first = self._first, None


SIGNALS = SignalHold()
