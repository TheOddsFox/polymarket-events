"""Plan guards: which closed-market windows wait, and which market batches may be resumed."""

from __future__ import annotations

from typing import Any

import pytest

from oddsfox_catalogue.capture.runner import _held_closed_windows, _unsafe_to_resume

OPEN = "markets_keyset_open"
CLOSED = "markets_closed_ids_0001"
EVENTS = "events_ids_0001"


def _row(
    name: str,
    *,
    plan_order: int,
    status: str = "running",
    attempt: int = 1,
    scan_id: str | None = None,
) -> dict[str, Any]:
    return {
        "scan_id": scan_id or f"{name}#{attempt}",
        "scan_name": name,
        "attempt": attempt,
        "plan_order": plan_order,
        "status": status,
    }


def test_closed_windows_wait_while_the_open_crawl_runs() -> None:
    scans = [_row(OPEN, plan_order=1), _row(CLOSED, plan_order=2)]
    assert _held_closed_windows(scans) == {f"{CLOSED}#1"}


def test_a_completed_open_crawl_releases_the_closed_windows() -> None:
    scans = [_row(OPEN, plan_order=1, status="complete"), _row(CLOSED, plan_order=2)]
    assert _held_closed_windows(scans) == set()


def test_only_the_latest_open_attempt_decides_the_hold() -> None:
    scans = [
        _row(OPEN, plan_order=1, status="abandoned", attempt=1, scan_id="open-1"),
        _row(OPEN, plan_order=1, status="complete", attempt=2, scan_id="open-2"),
        _row(CLOSED, plan_order=2),
    ]
    assert _held_closed_windows(scans) == set()


def test_a_batch_with_no_open_crawl_holds_its_closed_windows() -> None:
    assert _held_closed_windows([_row(CLOSED, plan_order=1)]) == {f"{CLOSED}#1"}


def test_the_open_crawl_first_in_plan_order_is_safe_to_resume() -> None:
    scans = [_row(OPEN, plan_order=1), _row(EVENTS, plan_order=2), _row(CLOSED, plan_order=3)]
    assert not _unsafe_to_resume(scans)


def test_closed_windows_planned_before_the_open_crawl_are_unsafe_to_resume() -> None:
    scans = [_row(EVENTS, plan_order=1), _row(CLOSED, plan_order=2), _row(OPEN, plan_order=3)]
    assert _unsafe_to_resume(scans)


def test_a_batch_without_an_open_crawl_or_any_scans_is_unsafe() -> None:
    assert _unsafe_to_resume([_row(EVENTS, plan_order=1)])
    assert _unsafe_to_resume([])


@pytest.mark.parametrize(
    "name",
    ["events_keyset_all", "events_archived_offset", "markets_keyset_closed", "events_keyset_open"],
)
def test_a_deep_keyset_or_offset_scan_name_is_unsafe_even_with_the_open_crawl_first(
    name: str,
) -> None:
    scans = [_row(OPEN, plan_order=1), _row(name, plan_order=2)]
    assert _unsafe_to_resume(scans)


@pytest.mark.parametrize(
    "name",
    [
        "events_ids_0001",
        "events_ids_tail",
        "markets_closed_ids_0001",
        "markets_closed_ids_tail",
        "events_by_id_0001",
        "events_by_id_single_0001",
        # Past 9999 windows the index has five digits. The name must still count as id-range.
        "events_ids_10000",
        "markets_closed_ids_10000",
        "events_by_id_10000",
    ],
)
def test_every_scan_name_the_id_range_plan_produces_is_safe(name: str) -> None:
    scans = [_row(OPEN, plan_order=1), _row(name, plan_order=2)]
    assert not _unsafe_to_resume(scans)
