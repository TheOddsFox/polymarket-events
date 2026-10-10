"""Stop-signal hold: a signal raises outside a drain, and is held (first one kept) inside one."""

from __future__ import annotations

import signal

import pytest

from oddsfox_catalogue import cli, signals
from oddsfox_catalogue.signals import SignalHold, Terminated


def test_a_signal_outside_a_hold_raises_terminated() -> None:
    hold = SignalHold()
    with pytest.raises(Terminated) as info:
        hold.receive(signal.SIGTERM)
    assert info.value.signum == signal.SIGTERM
    assert str(info.value) == "SIGTERM"


def test_a_signal_inside_a_hold_is_recorded_and_raised_when_the_hold_ends() -> None:
    hold = SignalHold()
    with hold.hold() as held:
        hold.receive(signal.SIGHUP)  # recorded, not raised
        assert held.signum is None  # set only once the hold ends
    assert held.signum == signal.SIGHUP


def test_the_first_signal_in_a_hold_is_the_one_kept() -> None:
    hold = SignalHold()
    with hold.hold() as held:
        hold.receive(signal.SIGHUP)
        hold.receive(signal.SIGTERM)
    assert held.signum == signal.SIGHUP


def test_nested_holds_release_only_at_the_outermost_exit() -> None:
    hold = SignalHold()
    with hold.hold() as outer:
        with hold.hold() as inner:
            hold.receive(signal.SIGTERM)
        assert inner.signum is None
        hold.receive(signal.SIGHUP)  # the outer hold still holds, so this is recorded
    assert outer.signum == signal.SIGTERM


def test_the_hold_ends_even_when_its_body_raises() -> None:
    hold = SignalHold()
    with pytest.raises(RuntimeError, match="drain failed"), hold.hold() as held:
        hold.receive(signal.SIGHUP)
        raise RuntimeError("drain failed")
    assert held.signum == signal.SIGHUP
    with pytest.raises(Terminated):
        hold.receive(signal.SIGTERM)


def test_a_new_hold_does_not_inherit_an_earlier_signal() -> None:
    hold = SignalHold()
    with hold.hold():
        hold.receive(signal.SIGHUP)
    with hold.hold() as held:
        pass
    assert held.signum is None


def test_the_cli_and_the_hold_share_one_terminated_class() -> None:
    assert cli.Terminated is signals.Terminated
