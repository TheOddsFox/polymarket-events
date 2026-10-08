import base64
import json
from datetime import UTC, datetime

import httpx
import pytest

from fakes.harness import FakeClock
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.gamma.http import GammaClient, MalformedResponse, ScanFailed
from oddsfox_catalogue.gamma.paginators import (
    PageState,
    keyset_pages,
    offset_pages,
    single_event_page,
    unpack,
)

NOW = datetime(2026, 10, 8, tzinfo=UTC)


def _client(handler) -> GammaClient:
    settings = GammaSettings(base_url="https://gamma.fake.test", requests_per_second=1000.0)
    clock = FakeClock()
    return GammaClient(
        settings,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: NOW,
    )


def _cursor(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode()


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


def test_keyset_follows_cursors_until_absent() -> None:
    # Keyed by the decoded cursor value; None is the first request.
    pages = {
        None: {"events": [{"id": "1"}], "next_cursor": _cursor("a")},
        "a": {"events": [{"id": "2"}], "next_cursor": _cursor("b")},
        "b": {"events": [{"id": "3"}]},
    }
    seen_cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("after_cursor")
        seen_cursors.append(cursor)
        decoded = None if cursor is None else base64.urlsafe_b64decode(cursor).decode()
        return httpx.Response(200, json=pages[decoded])

    client = _client(handler)
    results = list(keyset_pages(client, "/events/keyset", {"limit": 1}, "events"))
    assert [r.seq for r in results] == [1, 2, 3]
    assert [r.terminal for r in results] == [False, False, True]
    assert seen_cursors == [None, _cursor("a"), _cursor("b")]
    assert results[0].output_cursor == _cursor("a")
    assert results[-1].output_cursor is None


def test_keyset_keeps_filters_on_every_request() -> None:
    requests: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        return httpx.Response(200, json={"events": []})

    list(keyset_pages(_client(handler), "/events/keyset", {"closed": True, "limit": 5}, "events"))
    assert requests == [{"closed": "true", "limit": "5"}]


def test_repeated_cursor_fails_the_scan() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # Every page points back to the same cursor.
        return httpx.Response(200, json={"events": [{"id": "1"}], "next_cursor": _cursor("loop")})

    with pytest.raises(ScanFailed):
        list(keyset_pages(_client(handler), "/events/keyset", {}, "events"))


def test_empty_page_with_cursor_fails_the_scan() -> None:
    client = _client(
        lambda r: httpx.Response(200, json={"events": [], "next_cursor": _cursor("x")})
    )
    with pytest.raises(ScanFailed):
        list(keyset_pages(client, "/events/keyset", {}, "events"))


def test_keyset_resume_starts_from_saved_cursor_and_seq() -> None:
    sent: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request.url.params.get("after_cursor"))
        return httpx.Response(200, json={"events": [{"id": "9"}]})

    start = PageState(seq=4, cursor=_cursor("saved"), seen_cursors=frozenset({_cursor("saved")}))
    results = list(keyset_pages(_client(handler), "/events/keyset", {}, "events", start))
    assert sent == [_cursor("saved")]
    assert results[0].seq == 5


def test_offset_advances_by_records_and_stops_at_empty_page() -> None:
    offsets: list[int] = []
    pages = {0: [{"id": "1"}, {"id": "2"}], 2: [{"id": "3"}], 3: []}

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        return httpx.Response(200, json=pages[offset])

    results = list(offset_pages(_client(handler), "/events", {"limit": 2}, "events"))
    assert offsets == [0, 2, 3]
    assert [r.record_count for r in results] == [2, 1, 0]
    assert results[-1].terminal is True
    assert results[0].output_offset == 2


def test_offset_repeating_a_page_fails() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"id": "1"}, {"id": "2"}])

    with pytest.raises(ScanFailed):
        list(offset_pages(_client(handler), "/events", {"limit": 2}, "events"))


def test_offset_resume_uses_saved_offset() -> None:
    offsets: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        offsets.append(int(request.url.params["offset"]))
        return httpx.Response(200, json=[])

    list(offset_pages(_client(handler), "/events", {}, "events", PageState(seq=2, offset=200)))
    assert offsets == [200]


def test_single_event_404_is_an_empty_terminal_page() -> None:
    client = _client(lambda r: httpx.Response(404, json={"error": "nope"}))
    page = single_event_page(client, "77", seq=1)
    assert page.record_count == 0 and page.terminal and page.http_status == 404


def test_single_event_must_describe_the_requested_id() -> None:
    client = _client(lambda r: httpx.Response(200, json={"id": "78", "title": "wrong"}))
    with pytest.raises(MalformedResponse):
        single_event_page(client, "77", seq=1)


def test_single_event_ids_must_be_numeric() -> None:
    client = _client(lambda r: httpx.Response(200, json={}))
    with pytest.raises(ValueError):
        single_event_page(client, "abc", seq=1)


def test_json_helper_round_trip() -> None:
    body = json.dumps({"events": [{"id": "1"}], "next_cursor": "z"})
    assert unpack(json.loads(body), "events")[1] == "z"
