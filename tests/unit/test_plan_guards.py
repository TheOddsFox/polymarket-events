"""Plan guards: which closed-market windows wait, and which market batches may be resumed."""

from __future__ import annotations

from typing import Any

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


def test_a_batch_without_an_open_crawl_is_unsafe_but_an_empty_one_is_not() -> None:
    assert _unsafe_to_resume([_row(EVENTS, plan_order=1)])
    assert not _unsafe_to_resume([])
