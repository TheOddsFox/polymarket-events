"""Measurements remain monotonic, payload-free, and visible on failures."""

import logging

import pytest

from oddsfox_catalogue import timing


@pytest.mark.parametrize("fails", [False, True])
def test_phase_logs_elapsed_even_on_failure(monkeypatch, caplog, fails):
    ticks = iter([100.0, 100.125])
    monkeypatch.setattr(timing, "perf_counter", lambda: next(ticks))
    caplog.set_level(logging.INFO, logger=timing.__name__)

    def run():
        with timing.phase_timing("test.phase"):
            if fails:
                raise ValueError("private payload")

    if fails:
        with pytest.raises(ValueError, match="private payload"):
            run()
    else:
        run()
    assert caplog.messages == ["pipeline_phase name=test.phase elapsed_s=0.125000"]


def test_counter_logs_only_explicit_measurement(caplog):
    caplog.set_level(logging.INFO, logger=timing.__name__)
    timing.record_counter("test.rows", 3)
    assert caplog.messages == ["pipeline_counter name=test.rows value=3"]
