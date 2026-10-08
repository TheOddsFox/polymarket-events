from datetime import UTC, datetime, timedelta, timezone

import pytest

from oddsfox_catalogue.ids import (
    canonical_json,
    ids_hash,
    iso_utc,
    make_batch_id,
    make_page_id,
    make_scan_id,
    observation_date,
    observation_id,
    parse_batch_id,
    payload_hash,
)

NOW = datetime(2026, 10, 8, 6, 30, 5, tzinfo=UTC)


def test_batch_id_round_trips() -> None:
    batch_id = make_batch_id("daily", NOW)
    assert batch_id == "20261008T063005Z-daily"
    started, mode = parse_batch_id(batch_id)
    assert started == NOW and mode == "daily"


def test_batch_id_normalises_non_utc_times() -> None:
    local = NOW.astimezone(timezone(timedelta(hours=2)))
    assert make_batch_id("bootstrap", local) == "20261008T063005Z-bootstrap"


def test_naive_datetimes_are_rejected() -> None:
    with pytest.raises(ValueError):
        iso_utc(datetime(2026, 1, 1))


def test_malformed_batch_ids_are_rejected() -> None:
    with pytest.raises(ValueError):
        parse_batch_id("2026-10-08-daily")
    with pytest.raises(ValueError):
        make_batch_id("hourly", NOW)


def test_scan_and_page_ids_embed_their_position() -> None:
    batch_id = make_batch_id("bootstrap", NOW)
    scan_id = make_scan_id(batch_id, "events_keyset_all", 2)
    assert scan_id == f"{batch_id}.events_keyset_all.a2"
    assert make_page_id(scan_id, 7) == f"{scan_id}.p000007"


def test_observation_id_depends_on_page_and_pointer() -> None:
    page = "page-a"
    first = observation_id(page, "/events/0")
    assert first == observation_id(page, "/events/0")
    assert first != observation_id(page, "/events/1")
    assert first != observation_id("page-b", "/events/0")
    # The NUL separator prevents concatenation collisions between page and pointer.
    assert observation_id("a", "/bc") != observation_id("ab", "/c")


def test_observation_pointer_must_be_absolute() -> None:
    with pytest.raises(ValueError):
        observation_id("page", "events/0")


def test_payload_hash_ignores_key_order_but_not_values() -> None:
    assert payload_hash({"a": 1, "b": [2, 3]}) == payload_hash({"b": [2, 3], "a": 1})
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})


def test_canonical_json_rejects_nan() -> None:
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_ids_hash_is_order_independent() -> None:
    assert ids_hash(["3", "1", "2"]) == ids_hash(["1", "2", "3"])
    assert ids_hash(["1"]) != ids_hash(["1", "2"])


def test_observation_date_is_utc_calendar_day() -> None:
    late = datetime(2026, 10, 8, 23, 30, tzinfo=timezone(timedelta(hours=-5)))
    assert observation_date(late) == "2026-10-09"
