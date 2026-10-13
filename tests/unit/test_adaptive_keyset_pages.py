"""Adaptive keyset pages preserve durable cursors, native evidence and finite budgets."""

import json
from copy import deepcopy
from datetime import timedelta

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, FakeClock, build_runtime
from fakes.world import World, make_event
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import (
    DurabilityError,
    iter_scan_pages,
    read_page_records,
    scan_dir_for,
    validate_page_identity,
)
from oddsfox_catalogue.capture.writer import read_body, read_manifest, write_page
from oddsfox_catalogue.config import GammaSettings
from oddsfox_catalogue.gamma.http import (
    GammaClient,
    MalformedResponse,
    RequestBudgetExceeded,
    RetriesExhausted,
    ScanFailed,
)
from oddsfox_catalogue.gamma.paginators import PageState, keyset_pages
from oddsfox_catalogue.gamma.scans import ScanSpec
from oddsfox_catalogue.ids import ids_hash, iso_utc, make_page_id, sha256_bytes

BODY_CAP = 256


def _client(handler, **limits):
    clock = FakeClock()
    receipts = iter(FIXED_NOW + timedelta(seconds=index) for index in range(100))
    return GammaClient(
        GammaSettings(base_url="https://gamma.fake.test", requests_per_second=1000, max_retries=0),
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: next(receipts),
        max_body_bytes=BODY_CAP,
        **limits,
    )


def _body(ids, next_cursor=None):
    return json.dumps(
        {"events": [{"id": str(value)} for value in ids], "next_cursor": next_cursor},
        indent=1,
    ).encode()


def test_oversized_keyset_retries_same_cursor_and_retains_lower_limit():
    calls, bodies = [], []
    native = [_body([2, 3], "next"), _body([4])]

    def handler(request):
        limit = int(request.url.params["limit"])
        cursor = request.url.params.get("after_cursor")
        calls.append((cursor, limit))
        body = b"x" * (BODY_CAP + 1) if limit > 25 else native[0 if cursor == "previous" else 1]
        bodies.append(body)
        return httpx.Response(200, content=body)

    client = _client(handler)
    try:
        pages = list(
            keyset_pages(
                client,
                "/events/keyset",
                {"limit": 100, "closed": False, "_high_water": 1000},
                "events",
                PageState(seq=1, cursor="previous"),
            )
        )
        assert calls == [("previous", 100), ("previous", 50), ("previous", 25), ("next", 25)]
        assert [page.seq for page in pages] == [2, 3]
        assert [page.input_cursor for page in pages] == ["previous", "next"]
        assert [page.output_cursor for page in pages] == ["next", None]
        assert [page.terminal for page in pages] == [False, True]
        assert [page.page_unit_revision for page in pages] == [3, 3]
        assert [page.response.body for page in pages] == native
        assert [[row["id"] for row in page.records] for page in pages] == [["2", "3"], ["4"]]
        assert pages[0].response.received_at < pages[1].response.received_at
        assert all(page.response.endpoint == page.endpoint for page in pages)
        assert all(page.response.params["limit"] == page.params["limit"] == 25 for page in pages)
        assert client.stats.requests == 4
        assert client.stats.downloaded_bytes == sum(map(len, bodies))
    finally:
        client.close()


def test_oversized_stream_counts_partial_failed_bytes_and_stops_reading_before_retry():
    calls, emitted, closed = [], [], []
    native = _body([1])

    class BoundedStream(httpx.SyncByteStream):
        def __iter__(self):
            for chunk in (b"a" * 200, b"b" * 100, b"never-read" * 100):
                emitted.append(len(chunk))
                yield chunk

        def close(self):
            closed.append(True)

    def handler(request):
        limit = int(request.url.params["limit"])
        calls.append(limit)
        return (
            httpx.Response(200, stream=BoundedStream())
            if limit == 100
            else httpx.Response(200, content=native)
        )

    client = _client(handler)
    try:
        [page] = keyset_pages(client, "/events/keyset", {"limit": 100}, "events")
        assert calls == [100, 50]
        assert emitted == [200, 100] and closed == [True]
        assert client.stats.requests == 2
        assert client.stats.downloaded_bytes == 300 + len(native)
        assert page.response.body == native and page.record_count == 1
    finally:
        client.close()


@pytest.mark.parametrize("limit", [{"max_requests": 1}, {"max_download_bytes": BODY_CAP}])
def test_failed_oversized_attempt_cannot_reset_request_or_download_budget(limit):
    calls = []

    def handler(request):
        calls.append(int(request.url.params["limit"]))
        return httpx.Response(200, content=b"x" * (BODY_CAP + 1))

    client = _client(handler, **limit)
    try:
        with pytest.raises(RequestBudgetExceeded):
            next(keyset_pages(client, "/events/keyset", {"limit": 100}, "events"))
        assert calls == [100]
        assert client.stats.requests == 1
        assert client.stats.downloaded_bytes == BODY_CAP + 1
    finally:
        client.close()


def test_irreducible_singleton_oversize_fails_without_advancing_or_looping():
    from oddsfox_catalogue.gamma.http import ResponseTooLarge

    calls = []

    def handler(request):
        calls.append((request.url.params.get("after_cursor"), int(request.url.params["limit"])))
        return httpx.Response(200, content=b"x" * (BODY_CAP + 1))

    client = _client(handler)
    try:
        with pytest.raises(ResponseTooLarge):
            next(
                keyset_pages(
                    client,
                    "/events/keyset",
                    {"limit": 3},
                    "events",
                    PageState(seq=7, cursor="saved"),
                )
            )
        assert calls == [("saved", 3), ("saved", 1)]
        assert client.stats.requests == 2 and client.stats.downloaded_bytes == 2 * (BODY_CAP + 1)
    finally:
        client.close()


@pytest.mark.parametrize(
    "failure", ["json", "unicode", "envelope", "status", "large-error", "encoding", "transport"]
)
def test_non_size_failures_never_reduce_keyset_limit(failure):
    from oddsfox_catalogue.gamma.http import ResponseTooLarge

    calls = []

    def handler(request):
        calls.append(int(request.url.params["limit"]))
        if failure == "transport":
            raise httpx.ConnectError("offline transport failed", request=request)
        if failure == "encoding":
            return httpx.Response(
                200,
                stream=httpx.ByteStream(b"invalid compressed bytes"),
                headers={"Content-Encoding": "gzip"},
            )
        if failure == "status":
            return httpx.Response(413, content=b"must not split on status")
        if failure == "large-error":
            return httpx.Response(503, content=b"x" * (BODY_CAP + 1))
        bodies = {"json": b"not JSON", "unicode": b"\xff", "envelope": b'{"markets":[]}'}
        return httpx.Response(200, content=bodies[failure])

    client = _client(handler)
    try:
        with pytest.raises(
            RetriesExhausted if failure == "transport" else MalformedResponse
        ) as caught:
            next(keyset_pages(client, "/events/keyset", {"limit": 100}, "events"))
        assert not isinstance(caught.value, ResponseTooLarge)
        assert calls == [100] and client.stats.requests == 1
    finally:
        client.close()


@pytest.mark.parametrize("failure", ["cursor", "order", "records", "empty-cursor", "count"])
def test_smaller_retry_preserves_sealed_pagination_constraints(failure):
    calls = []
    records = [2, 1] if failure == "order" else [1]
    records = [] if failure == "empty-cursor" else records
    records = list(range(1, 4)) if failure == "count" else records
    body = _body(records, "saved" if failure == "cursor" else "next")

    def handler(request):
        limit = int(request.url.params["limit"])
        calls.append((request.url.params.get("after_cursor"), limit))
        return httpx.Response(200, content=b"x" * (BODY_CAP + 1) if limit == 4 else body)

    client = _client(handler)
    try:
        start = PageState(
            seq=1, cursor="saved", last_ids_hash=ids_hash(["1"]) if failure == "records" else None
        )
        with pytest.raises(MalformedResponse if failure in {"order", "count"} else ScanFailed):
            next(
                keyset_pages(
                    client, "/events/keyset", {"limit": 4, "_high_water": 10}, "events", start
                )
            )
        assert calls == [("saved", 4), ("saved", 2)]
    finally:
        client.close()


def test_high_water_termination_preserves_observed_cursor_after_shrinking():
    calls = []

    def handler(request):
        limit = int(request.url.params["limit"])
        calls.append(limit)
        return httpx.Response(
            200, content=b"x" * (BODY_CAP + 1) if limit == 100 else _body([9, 10], "beyond")
        )

    client = _client(handler)
    try:
        [page] = keyset_pages(client, "/events/keyset", {"limit": 100, "_high_water": 10}, "events")
        assert calls == [100, 50]
        assert page.terminal and page.output_cursor == "beyond"
        assert [row["id"] for row in page.records] == ["9", "10"]
    finally:
        client.close()


def test_legacy_keyset_without_explicit_limit_remains_revision_one():
    client = _client(lambda request: httpx.Response(200, content=_body([1])))
    try:
        [page] = keyset_pages(client, "/events/keyset", {}, "events")
        assert page.page_unit_revision == 1 and "limit" not in page.params
    finally:
        client.close()


def _context():
    batch = {"batch_id": "20261008T060000Z-daily", "observation_date": "2026-10-08"}
    spec = ScanSpec(
        "events_keyset_open",
        "keyset",
        "/events/keyset",
        "events",
        tuple(
            sorted(
                {"limit": 100, "closed": False, "include_template": True, "_high_water": 10}.items()
            )
        ),
    )
    scan = runner._scan_row(
        batch["batch_id"], spec, attempt=1, plan_order=1, started_at=iso_utc(FIXED_NOW)
    )
    return batch, scan


def _manifest(batch, scan, *, seq=1, limit=2, input_cursor=None, output_cursor="next", revision=3):
    params = {"limit": limit, "closed": False, "include_template": True}
    if input_cursor is not None:
        params["after_cursor"] = input_cursor
    return {
        "seq": seq,
        "page_unit_revision": revision,
        "page_id": make_page_id(scan["scan_id"], seq),
        **batch,
        **{
            key: scan[key]
            for key in ("scan_id", "scan_name", "attempt", "plan_order", "record_key", "endpoint")
        },
        "params": params,
        "input_cursor": input_cursor,
        "output_cursor": output_cursor,
        "offset_start": None,
        "offset_end": None,
        "terminal": output_cursor is None,
        "http_status": 200,
        "record_count": 2,
        "ids_hash": ids_hash(["1", "2"]),
        "observed_at": iso_utc(FIXED_NOW),
        "retries": 0,
        "latency_s": 0.0,
    }


@pytest.mark.parametrize(
    "tamper",
    [
        "bool",
        "float",
        "string",
        "zero",
        "above-plan",
        "params-type",
        "kind",
        "flag",
        "cursor",
        "endpoint",
        "unknown",
        "revision-type",
    ],
)
def test_revision_three_identity_rejects_tampered_limit_or_sealed_request(tamper):
    batch, scan = _context()
    manifest = _manifest(batch, scan)
    if tamper in {"bool", "float", "string", "zero", "above-plan"}:
        manifest["params"]["limit"] = {
            "bool": True,
            "float": 2.0,
            "string": "2",
            "zero": 0,
            "above-plan": 101,
        }[tamper]
    elif tamper == "params-type":
        manifest["params"] = []
    elif tamper == "kind":
        scan["kind"] = "offset"
    elif tamper == "flag":
        manifest["params"]["include_template"] = False
    elif tamper == "cursor":
        manifest["input_cursor"] = "not-the-saved-cursor"
        manifest["params"]["after_cursor"] = "not-the-saved-cursor"
    elif tamper == "endpoint":
        manifest["endpoint"] = "/markets/keyset"
    else:
        manifest["page_unit_revision"] = 4 if tamper == "unknown" else 3.0
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, manifest, seq=1)


def test_legacy_to_revision_three_is_readable_and_downgrade_is_rejected(tmp_path):
    from oddsfox_catalogue.config import Settings

    settings = Settings(tmp_path)
    batch, scan = _context()
    first = _manifest(batch, scan, limit=100, revision=1)
    first.pop("page_unit_revision")
    validate_page_identity(batch, scan, first, seq=1)
    second = _manifest(batch, scan, seq=2, limit=25, input_cursor="next", output_cursor=None)
    second.update(record_count=1, ids_hash=ids_hash(["3"]))
    validate_page_identity(batch, scan, second, seq=2, previous=first)
    directory = settings.raw_dir / "synthetic"
    written = write_page(directory, 2, _body([3]), second)
    retained = read_manifest(written.manifest_path, trusted_root=settings.raw_dir)
    assert read_page_records(
        settings, directory, retained, "events", scan=scan, previous=first
    ) == [{"id": "3"}]
    third = _manifest(batch, scan, seq=3, limit=100, input_cursor="again", revision=1)
    previous = deepcopy(second)
    previous.update(terminal=False, output_cursor="again")
    with pytest.raises(DurabilityError, match="downgraded"):
        validate_page_identity(batch, scan, third, seq=3, previous=previous)


def test_valid_checksums_cannot_claim_more_records_than_the_actual_limit(tmp_path):
    from oddsfox_catalogue.config import Settings

    settings = Settings(tmp_path)
    batch, scan = _context()
    manifest = _manifest(batch, scan, limit=1, output_cursor=None)
    validate_page_identity(batch, scan, manifest, seq=1)
    directory = settings.raw_dir / "synthetic"
    written = write_page(directory, 1, _body([1, 2]), manifest)
    retained = read_manifest(written.manifest_path, trusted_root=settings.raw_dir)
    with pytest.raises(DurabilityError, match="committed source unit"):
        read_page_records(settings, directory, retained, "events", scan=scan)


def test_real_daily_capture_resumes_same_native_cursor_and_rebuilds_raw_ledger(tmp_path):
    from oddsfox_catalogue.gamma.http import ResponseTooLarge

    world = World()
    for value in range(1, 5):
        world.add_event(make_event(str(value), f"Event {value}"))
    fake = FakeGamma(world)
    broken = {"active": True}
    calls, native = [], {}

    def keyset_response(request):
        cursor, limit = request.url.params.get("after_cursor"), int(request.url.params["limit"])
        calls.append((cursor, limit))
        if (cursor is None and limit > 50) or (
            cursor == "after-1" and (broken["active"] or limit > 25)
        ):
            return httpx.Response(200, content=b"x" * (BODY_CAP + 1))
        records, following = (
            ([1], "after-1")
            if cursor is None
            else ([2], "after-2")
            if cursor == "after-1"
            else ([3, 4], None)
        )
        body = _body(records, following)
        native[cursor] = body
        return httpx.Response(200, content=body)

    fake.rules.extend(
        [
            Rule(
                lambda request: request.url.path == "/events/keyset", keyset_response, remaining=100
            ),
            Rule(
                lambda request: request.url.path == "/events",
                lambda request: httpx.Response(200, content=_body([4])),
                remaining=100,
            ),
        ]
    )
    runtime, _ = build_runtime(
        tmp_path,
        fake,
        open_event_ids=lambda: {"1", "2", "3", "4"},
        env={
            "CATALOGUE_CAPTURE_WORKERS": "1",
            "CATALOGUE_CAPTURE_MAX_RESPONSE_BYTES": str(BODY_CAP),
            "CATALOGUE_GAMMA_MAX_RETRIES": "0",
        },
    )
    try:
        with pytest.raises(ResponseTooLarge):
            runner.run_capture(runtime, "daily")
        batch = runtime.ledger.list_batches()[0]
        failed = runtime.ledger.latest_attempt(batch["batch_id"], "events_keyset_open")
        assert failed["fetched_seq"] == 1 and failed["fetched_cursor"] == "after-1"
        directory = scan_dir_for(
            runtime.settings, batch["observation_date"], batch["batch_id"], failed["scan_id"]
        )
        committed = {path: sha256_bytes(path.read_bytes()) for path in directory.glob("p000001.*")}
        calls.clear()
        broken["active"] = False
        runtime.client.close()
        runtime.client = _client(fake.handle)
        captured = runner.run_capture(runtime, "daily", resume=batch["batch_id"])
        assert captured.resumed and captured.status == "captured"
        assert calls == [("after-1", 100), ("after-1", 50), ("after-1", 25), ("after-2", 25)]
        scan = runtime.ledger.latest_attempt(batch["batch_id"], "events_keyset_open")
        assert scan["scan_id"] == failed["scan_id"]
        pages = list(
            iter_scan_pages(runtime.settings, runtime.ledger.get_batch(batch["batch_id"]), scan)
        )
        assert [manifest["seq"] for manifest, _ in pages] == [1, 2, 3]
        assert [manifest["params"]["limit"] for manifest, _ in pages] == [50, 25, 25]
        assert all(manifest["page_unit_revision"] == 3 for manifest, _ in pages)
        assert [row["id"] for _, rows in pages for row in rows] == ["1", "2", "3", "4"]
        assert {path: sha256_bytes(path.read_bytes()) for path in committed} == committed
        for manifest, _ in pages:
            assert (
                read_body(directory, manifest, trusted_root=runtime.settings.raw_dir)
                == native[manifest["input_cursor"]]
            )
        with Ledger(tmp_path / "rebuilt.sqlite") as restored:
            counts = runner.rebuild_from_raw(runtime.settings, restored)
            assert counts["pages"] == len(runtime.ledger.pages_for_batch(batch["batch_id"]))
            rebuilt_scan = restored.latest_attempt(batch["batch_id"], "events_keyset_open")
            replayed = list(
                iter_scan_pages(
                    runtime.settings, restored.get_batch(batch["batch_id"]), rebuilt_scan
                )
            )
            assert replayed == pages
            assert restored.get_batch(batch["batch_id"])["status"] == "captured"
    finally:
        runtime.client.close()
        runtime.ledger.close()
