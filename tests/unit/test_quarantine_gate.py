"""The quarantine gate: a share exactly at the cap passes, anything above it blocks."""

from __future__ import annotations

import pytest

from oddsfox_catalogue.load.runner import quarantine_over_limit


def test_share_exactly_at_the_cap_passes() -> None:
    # 1 of 100 observed is exactly 0.01, the default cap.
    assert (
        quarantine_over_limit(event_rows=60, market_rows=39, quarantine_rows=1, max_ratio=0.01)
        is False
    )


def test_share_just_above_the_cap_blocks() -> None:
    # 2 of 100 observed is 0.02, above 0.01.
    assert (
        quarantine_over_limit(event_rows=60, market_rows=38, quarantine_rows=2, max_ratio=0.01)
        is True
    )


def test_a_batch_with_no_records_passes() -> None:
    assert quarantine_over_limit(0, 0, 0, max_ratio=0.0) is False


@pytest.mark.parametrize(
    ("quarantined", "max_ratio", "blocked"),
    [
        (0, 0.0, False),  # nothing quarantined never blocks, even at a zero cap
        (1, 0.0, True),  # any quarantine blocks at a zero cap
        (1, 1.0, False),  # a cap of 1.0 never blocks
    ],
)
def test_zero_and_full_caps(quarantined: int, max_ratio: float, blocked: bool) -> None:
    assert (
        quarantine_over_limit(
            event_rows=9, market_rows=0, quarantine_rows=quarantined, max_ratio=max_ratio
        )
        is blocked
    )
