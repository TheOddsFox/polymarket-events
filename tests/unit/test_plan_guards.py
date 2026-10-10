"""Plan guards: which closed-market windows wait, and which market batches may be resumed."""

from __future__ import annotations

from typing import Any

import pytest

from oddsfox_catalogue.capture.runner import _held_closed_windows, _scope
from oddsfox_catalogue.config import CaptureSettings, GammaSettings
from oddsfox_catalogue.gamma.scans import sealed_plan

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


def test_legacy_unsealed_scope_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported capture scope"):
        _scope({"scope_json": "{}"})


def test_current_catalogue_scale_produces_a_small_finite_plan() -> None:
    specs = sealed_plan(
        "bootstrap", GammaSettings(), CaptureSettings(), {"events": 200_000, "markets": 5_234_660}
    )
    assert len(specs) == 110
    assert not any(spec.param_dict.get("tail") for spec in specs)
    assert specs[-1].param_dict["hi"] == 5_234_660


def test_huge_high_water_with_tiny_partition_fails_before_materializing_plan() -> None:
    with pytest.raises(ValueError, match="excessive finite scan plan"):
        sealed_plan(
            "bootstrap",
            GammaSettings(),
            CaptureSettings(id_partition_size=1),
            {"events": 1, "markets": 10**19},
        )
