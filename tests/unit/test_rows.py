"""Pure page-to-envelope transform: no dlt, no DuckDB."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from fakes.world import event_stub, make_event, make_market
from oddsfox_catalogue.ids import observation_id
from oddsfox_catalogue.load.rows import (
    DuplicateObservation,
    PageContext,
    assert_unique,
    parse_timestamp,
    rows_for_page,
)

OBSERVED = datetime(2026, 10, 8, 6, 0, 0, tzinfo=UTC)


def _ctx(record_key: str = "events") -> PageContext:
    return PageContext(
        page_id="page-1",
        batch_id="batch-1",
        endpoint="/events/keyset",
        observed_at=OBSERVED,
        record_key=record_key,
    )


def test_event_rows_carry_envelope_and_deterministic_ids() -> None:
    event = make_event("101", "Alpha")
    first = rows_for_page(_ctx(), [event])
    second = rows_for_page(_ctx(), [event])

    assert len(first.events) == 1
    row = first.events[0]
    assert row["observation_id"] == observation_id("page-1", "/events/0")
    assert row["entity_id"] == "101"
    assert row["venue"] == "polymarket"
    assert row["observed_at"] == OBSERVED
    assert row == second.events[0], "replaying the same page must produce identical rows"


def test_nested_event_stubs_are_references_not_observations() -> None:
    event = make_event("101", "Alpha", markets=[make_market("8001", "Q?")])
    rows = rows_for_page(_ctx(), [event])

    assert [r["entity_id"] for r in rows.events] == ["101"]
    assert len(rows.markets) == 1
    assert rows.markets[0]["source_kind"] == "event_embedded"
    assert rows.markets[0]["json_pointer"] == "/events/0/markets/0"


def test_direct_market_pages_use_market_pointer() -> None:
    market = make_market("8001", "Q?", event_stub=event_stub(make_event("101", "Alpha")))
    rows = rows_for_page(_ctx("markets"), [market])

    assert rows.events == []
    assert rows.markets[0]["source_kind"] == "market_direct"
    assert rows.markets[0]["json_pointer"] == "/markets/0"


def test_record_without_id_is_quarantined_and_rest_of_page_loads() -> None:
    good = make_event("101", "Alpha")
    bad = {"title": "no id"}
    rows = rows_for_page(_ctx(), [bad, good])

    assert [r["entity_id"] for r in rows.events] == ["101"]
    assert len(rows.quarantine) == 1
    assert rows.quarantine[0]["entity"] == "event"
    assert rows.quarantine[0]["json_pointer"] == "/events/0"


def test_non_object_record_is_quarantined() -> None:
    rows = rows_for_page(_ctx(), ["not-an-object"])

    assert rows.events == []
    assert len(rows.quarantine) == 1


def test_non_list_nested_markets_is_quarantined_not_fatal() -> None:
    event = make_event("101", "Alpha")
    event["markets"] = {"oops": True}
    rows = rows_for_page(_ctx(), [event])

    assert [r["entity_id"] for r in rows.events] == ["101"]
    assert rows.markets == []
    assert rows.quarantine[0]["entity"] == "market"


def test_duplicate_observation_ids_raise() -> None:
    rows = rows_for_page(_ctx(), [make_event("101", "A"), make_event("102", "B")])
    assert_unique(rows.events)
    rows.events.append(dict(rows.events[0]))
    with pytest.raises(DuplicateObservation):
        assert_unique(rows.events)


def test_fetch_failed_is_quarantined_with_that_reason() -> None:
    rows = rows_for_page(
        _ctx(),
        [{"id": "202", "title": "ok"}],
        [{"id": "101", "reason": "fetch_failed"}],
    )
    assert [row["entity_id"] for row in rows.events] == ["202"]
    assert len(rows.quarantine) == 1
    assert rows.quarantine[0]["reason"] == "fetch_failed"
    assert rows.quarantine[0]["entity"] == "event"
    assert rows.quarantine[0]["payload"]["id"] == "101"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-08T06:00:00Z", datetime(2026, 10, 8, 6, 0, tzinfo=UTC)),
        ("2026-10-08T06:00:00+00:00", datetime(2026, 10, 8, 6, 0, tzinfo=UTC)),
        (None, None),
        ("", None),
        ("not-a-date", None),
        (123, None),
    ],
)
def test_parse_timestamp(raw: object, expected: datetime | None) -> None:
    assert parse_timestamp(raw) == expected
