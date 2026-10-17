"""Independent boundaries for the verified retry and publication defects."""

import sys

import duckdb
import pytest

from oddsfox_catalogue import current_reconcile, current_store
from oddsfox_catalogue.current_config import CurrentSettings
from oddsfox_catalogue.gamma.http import CaptureStopped, backoff_delay


class CeilingRng:
    def uniform(self, low, high):
        assert low == 0.0
        return high


@pytest.mark.parametrize(
    ("attempt", "base", "cap", "expected"),
    [
        (0, 5e-324, 1.0, 5e-324),
        (1024, 5e-324, 1.0, float.fromhex("0x1p-50")),
        (1073, 5e-324, 1.0, 0.5),
        (1074, 5e-324, 2.0, 1.0),
        (0, sys.float_info.max, sys.float_info.max, sys.float_info.max),
        (1, sys.float_info.max, 0.25, 0.25),
        (10**100, 1.0, 0.25, 0.25),
    ],
)
def test_retry_ceiling_preserves_representable_scaling(attempt, base, cap, expected):
    assert backoff_delay(attempt, base, cap, CeilingRng()) == expected


@pytest.mark.parametrize(("now", "expected_resumes"), [(849.999, 1), (850.0, 0), (850.001, 0)])
def test_scheduler_stops_at_measured_publication_reserve(
    tmp_path, monkeypatch, now, expected_resumes
):
    settings = CurrentSettings(tmp_path / "catalogue")
    resumed = []

    def resume_pending(*_args):
        resumed.append(True)
        raise CaptureStopped("synthetic acquisition stop")

    monkeypatch.setattr(current_reconcile.time, "monotonic", lambda: now)
    monkeypatch.setattr(current_reconcile, "resume_pending", resume_pending)
    with duckdb.connect(":memory:") as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "previous_publish_seconds", 100)
        result = current_reconcile._reconcile_units(settings, conn, object(), deadline=1000)

    assert len(resumed) == expected_resumes
    assert result == {
        "responses": 0,
        "active_batches": 0,
        "cold_batches": 0,
        "source_complete": False,
    }
