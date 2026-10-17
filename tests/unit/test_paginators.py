import pytest

from oddsfox_catalogue.gamma.http import MalformedResponse
from oddsfox_catalogue.gamma.paginators import _window_params, unpack


def test_unpack_accepts_each_documented_shape() -> None:
    keyset, cursor = unpack({"events": [{"id": "1"}], "next_cursor": "c"}, "events")
    assert keyset == [{"id": "1"}] and cursor == "c"
    bare, cursor = unpack([{"id": "2"}], "events")
    assert bare == [{"id": "2"}] and cursor is None
    single, cursor = unpack({"id": "3", "title": "x"}, "events")
    assert single == [{"id": "3", "title": "x"}] and cursor is None


@pytest.mark.parametrize(
    "body",
    [
        "text",
        {"events": "not-a-list"},
        {"events": [], "next_cursor": 5},
    ],
)
def test_unpack_rejects_wrong_envelopes(body) -> None:
    with pytest.raises(MalformedResponse):
        unpack(body, "events")


def test_record_level_problems_are_left_for_quarantine() -> None:
    """Invalid records must not fail a page: the loader quarantines them instead."""
    records, _ = unpack({"events": [{"no_id": 1}, 7]}, "events")
    assert records == [{"no_id": 1}, 7]


def test_window_params_bound_ids_and_add_market_tags() -> None:
    assert _window_params([1, 2], "events", None) == {"limit": 2, "id": [1, 2]}
    assert _window_params([3], "markets", False, {"order": "id"}) == {
        "limit": 1,
        "id": [3],
        "closed": False,
        "include_tag": True,
        "order": "id",
    }
