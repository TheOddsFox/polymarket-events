"""Adaptive requests are durable native source units, never oversized merged pages."""

from __future__ import annotations

import copy
import json
from datetime import timedelta

import httpx
import pytest

from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, FakeClock, build_runtime
from fakes.world import World, make_event, make_market
from oddsfox_catalogue.capture import runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.reader import (
    DurabilityError,
    iter_scan_pages,
    read_page_records,
    scan_dir_for,
    validate_page_identity,
)
from oddsfox_catalogue.capture.writer import (
    manifest_path,
    read_body,
    read_manifest,
    write_page,
)
from oddsfox_catalogue.config import GammaSettings, Settings
from oddsfox_catalogue.gamma.http import GammaClient, MalformedResponse, RetriesExhausted
from oddsfox_catalogue.gamma.paginators import PageState, id_list_pages, id_range_pages
from oddsfox_catalogue.gamma.scans import ScanSpec
from oddsfox_catalogue.ids import ids_hash, iso_utc, make_page_id, sha256_bytes
from oddsfox_catalogue.inventory import coverage_unit, expand_coverage_id_range
from oddsfox_catalogue.load.runner import LoadRuntime, _rows_for_chunk

BATCH_ID = "20261008T060000Z-bootstrap"
BODY_CAP = 1024


def _client(handler, *, body_cap=BODY_CAP):
    clock = FakeClock()
    receipts = iter(FIXED_NOW + timedelta(seconds=index) for index in range(1000))
    return GammaClient(
        GammaSettings(base_url="https://gamma.fake.test", requests_per_second=1000),
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: next(receipts),
        max_body_bytes=body_cap,
    )


def _body(ids, *, key="events", padding=0):
    # Deliberately retain unusual whitespace, record order and an unrelated field.
    return json.dumps(
        {
            key: [
                {"id": str(identifier), "padding": "x" * padding} for identifier in reversed(ids)
            ],
            "source_note": "native",
        },
        indent=1,
    ).encode()


def _context(kind="id_range", *, lo=1, hi=4, step=4, ids=None, record_key="events"):
    params = {"lo": lo, "hi": hi, "step": step, "tail": False}
    targets = [] if ids is None else ids
    if kind == "keyset_ids":
        params = {"limit": 100, "id": [int(identifier) for identifier in targets]}
    spec = ScanSpec(
        "adaptive",
        kind,
        "/" + record_key + "/keyset",
        record_key,
        tuple(sorted(params.items())),
        tuple(str(identifier) for identifier in targets),
    )
    batch = {"batch_id": BATCH_ID, "observation_date": "2026-10-08"}
    scan = runner._scan_row(BATCH_ID, spec, attempt=1, plan_order=1, started_at=iso_utc(FIXED_NOW))
    return batch, scan


def _manifest(batch, scan, seq, start, end, *, terminal=False, revision=2, endpoint=None):
    if scan["kind"] == "id_range":
        wanted = list(range(start, end + 1))
    else:
        wanted = [int(value) for value in json.loads(scan["input_ids_json"])[start:end]]
    params = {"limit": len(wanted), "id": [str(value) for value in wanted]}
    if scan["record_key"] == "markets":
        params["include_tag"] = "true"
    return {
        "page_unit_revision": revision,
        "seq": seq,
        "page_id": make_page_id(scan["scan_id"], seq),
        "batch_id": batch["batch_id"],
        "scan_id": scan["scan_id"],
        "scan_name": scan["scan_name"],
        "attempt": scan["attempt"],
        "plan_order": scan["plan_order"],
        "observation_date": batch["observation_date"],
        "record_key": scan["record_key"],
        "endpoint": endpoint or scan["endpoint"],
        "params": params,
        "input_cursor": None,
        "output_cursor": None,
        "offset_start": start,
        "offset_end": end,
        "terminal": terminal,
        "http_status": 200,
        "record_count": 0,
        "ids_hash": ids_hash([]),
        "observed_at": iso_utc(FIXED_NOW),
    }


def test_bounded_leaves_are_not_reencoded_or_combined_into_an_oversized_page():
    seen = []
    bodies = {}

    def handler(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        seen.append(wanted)
        body = _body(wanted, padding=560)
        bodies[wanted] = body
        return httpx.Response(200, content=body)

    client = _client(handler)
    try:
        pages = list(
            id_range_pages(client, "/events/keyset", {"lo": 1, "hi": 2, "step": 2}, "events")
        )
        assert [(page.offset, page.output_offset) for page in pages] == [(1, 1), (2, 2)]
        assert [page.terminal for page in pages] == [False, True]
        assert [page.page_unit_revision for page in pages] == [2, 2]
        assert all(len(page.response.body) <= BODY_CAP for page in pages)
        assert sum(len(page.response.body) for page in pages) > BODY_CAP
        assert [page.response.body for page in pages] == [bodies[(1,)], bodies[(2,)]]
        assert all(page.endpoint == page.response.endpoint for page in pages)
        assert all(page.params == page.response.params for page in pages)
        assert pages[0].response.received_at < pages[1].response.received_at
        assert seen == [(1, 2), (1,), (2,)]
        assert client.stats.requests == 3
        assert client.stats.downloaded_bytes == sum(len(body) for body in bodies.values())
    finally:
        client.close()


def test_unequal_leaf_splits_preserve_native_order_and_original_window_boundary():
    seen = []

    def handler(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        seen.append(wanted)
        if len(wanted) > 50 or (wanted[0] == 1 and len(wanted) > 25):
            return httpx.Response(413, json={"error": "split"})
        return httpx.Response(200, content=_body(wanted))

    client = _client(handler, body_cap=16 * 1024)
    try:
        pages = list(
            id_range_pages(client, "/events/keyset", {"lo": 1, "hi": 103, "step": 100}, "events")
        )
        assert [(page.offset, page.output_offset) for page in pages] == [
            (1, 25),
            (26, 50),
            (51, 100),
            (101, 103),
        ]
        assert [row["id"] for row in pages[0].records] == [str(value) for value in range(25, 0, -1)]
        assert [page.seq for page in pages] == [1, 2, 3, 4]
        assert [page.terminal for page in pages] == [False, False, False, True]
        assert seen[-1] == (101, 102, 103)
    finally:
        client.close()


@pytest.mark.parametrize("kind", ["id_range", "keyset_ids"])
def test_a_later_failure_keeps_the_first_leaf_and_cold_resume_starts_after_it(kind):
    calls = []

    def failed(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        calls.append((request.url.path, wanted))
        if wanted == (1, 9) or wanted == (1, 2):
            return httpx.Response(413, json={"error": "split"})
        if wanted == (1,):
            return httpx.Response(200, content=_body(wanted))
        return httpx.Response(500, json={"error": "unavailable"})

    params = {"lo": 1, "hi": 2, "step": 2} if kind == "id_range" else {"id": [1, 9]}
    iterator = id_range_pages if kind == "id_range" else id_list_pages
    first_client = _client(failed)
    try:
        pages = iterator(first_client, "/events/keyset", params, "events")
        first = next(pages)
        assert [record["id"] for record in first.records] == ["1"]
        assert not first.terminal
        # A pending right branch must not be requested before the left can be committed.
        assert calls == [
            ("/events/keyset", tuple(params.get("id", [1, 2]))),
            ("/events/keyset", (1,)),
        ]
        with pytest.raises(RetriesExhausted):
            next(pages)
    finally:
        first_client.close()

    resumed_calls = []

    def recovered(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        resumed_calls.append(wanted)
        return httpx.Response(200, content=_body(wanted))

    second_client = _client(recovered)
    try:
        resumed = list(
            iterator(
                second_client,
                "/events/keyset",
                params,
                "events",
                PageState(seq=first.seq, offset=first.output_offset),
            )
        )
        assert resumed_calls == [(2,)] if kind == "id_range" else resumed_calls == [(9,)]
        assert [page.seq for page in resumed] == [2]
        assert resumed[-1].terminal
        assert resumed[0].offset == 2 if kind == "id_range" else resumed[0].offset == 1
    finally:
        second_client.close()


def test_resume_of_a_noncontiguous_frozen_list_uses_positions_not_numeric_ids():
    calls = []

    def handler(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        calls.append(wanted)
        return httpx.Response(200, content=_body(wanted))

    client = _client(handler)
    try:
        pages = list(
            id_list_pages(
                client,
                "/events/keyset",
                {"id": [7, 41, 900, 5000]},
                "events",
                PageState(seq=3, offset=2),
            )
        )
        assert calls == [(900, 5000)]
        assert [(page.seq, page.offset, page.output_offset, page.terminal) for page in pages] == [
            (4, 2, 4, True)
        ]
    finally:
        client.close()


def test_resume_mid_range_keeps_the_original_step_alignment():
    calls = []

    def handler(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        calls.append(wanted)
        return httpx.Response(200, content=_body(wanted))

    client = _client(handler)
    try:
        pages = list(
            id_range_pages(
                client,
                "/events/keyset",
                {"lo": 11, "hi": 20, "step": 4},
                "events",
                PageState(seq=9, offset=13),
            )
        )
        assert calls == [(14,), (15, 16, 17, 18), (19, 20)]
        assert [(page.seq, page.offset, page.output_offset) for page in pages] == [
            (10, 14, 14),
            (11, 15, 18),
            (12, 19, 20),
        ]
    finally:
        client.close()


@pytest.mark.parametrize("status", [200, 404])
def test_singleton_fallback_preserves_actual_unfiltered_native_evidence(tmp_path, status):
    calls = []

    def handler(request):
        calls.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/markets/keyset":
            return httpx.Response(413, json={"error": "use native lookup"})
        return httpx.Response(
            status, json={"id": "7", "closed": False} if status == 200 else {"error": "missing"}
        )

    client = _client(handler)
    try:
        [page] = id_range_pages(
            client, "/markets/keyset", {"lo": 7, "hi": 7, "step": 1, "closed": True}, "markets"
        )
    finally:
        client.close()
    assert calls[-1] == ("/markets/7", {"include_tag": "true"})
    assert page.endpoint == "/markets/7"
    assert page.params == page.response.params == {"include_tag": "true"}
    assert page.http_status == status and page.terminal
    assert page.records == ([{"id": "7", "closed": False}] if status == 200 else [])
    batch, scan = _context(lo=7, hi=7, step=1, record_key="markets")
    scan["params_json"] = json.dumps({"lo": 7, "hi": 7, "step": 1, "closed": True, "tail": False})
    manifest = _manifest(batch, scan, 1, 7, 7, terminal=True)
    manifest.update(
        endpoint=page.endpoint,
        params=dict(page.params),
        http_status=status,
        record_count=page.record_count,
        ids_hash=page.ids_hash,
    )
    validate_page_identity(batch, scan, manifest, seq=1)
    settings = Settings(tmp_path)
    directory = settings.raw_dir / "fixture"
    written = write_page(directory, 1, page.response.body, manifest)
    raw = read_manifest(written.manifest_path, trusted_root=settings.raw_dir)
    assert read_page_records(settings, directory, raw, "markets", scan=scan) == page.records
    unit = coverage_unit(raw, "id_range")
    assert unit["endpoint"] == "/markets/7" and unit["params"] == {"include_tag": "true"}
    assert expand_coverage_id_range(unit["id_range"]) == ["7"]
    assert unit["status"] == ("success" if status == 200 else "absent")


def test_successful_empty_list_is_not_converted_into_confirmed_absence():
    client = _client(lambda request: httpx.Response(200, json={"events": []}))
    try:
        [page] = id_range_pages(client, "/events/keyset", {"lo": 1, "hi": 2, "step": 2}, "events")
    finally:
        client.close()
    assert page.http_status == 200 and page.records == [] and page.terminal
    assert page.endpoint == "/events/keyset"


def test_oversized_singleton_lookup_fails_without_fabricating_absence():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/events/keyset":
            return httpx.Response(413, json={"error": "split"})
        return httpx.Response(200, content=_body([1], padding=BODY_CAP * 2))

    client = _client(handler)
    try:
        with pytest.raises(MalformedResponse):
            next(id_range_pages(client, "/events/keyset", {"lo": 1, "hi": 1, "step": 1}, "events"))
        assert calls == ["/events/keyset", "/events/1"]
        assert client.stats.requests == 2
        assert client.stats.downloaded_bytes > BODY_CAP
    finally:
        client.close()


@pytest.mark.parametrize("status", [400, 403])
def test_unresolved_native_singleton_error_cannot_be_committed_as_empty(status):
    def handler(request):
        return httpx.Response(
            413 if request.url.path == "/events/keyset" else status,
            json={"error": "unresolved"},
        )

    client = _client(handler)
    try:
        with pytest.raises(MalformedResponse):
            next(id_range_pages(client, "/events/keyset", {"lo": 1, "hi": 1, "step": 1}, "events"))
    finally:
        client.close()


@pytest.mark.parametrize(
    "body",
    [
        {"id": "8"},
        {"events": [{"id": "7"}, {"id": "7"}]},
        {"events": [{"id": "7"}], "next_cursor": "continuation"},
        {"events": []},
    ],
)
def test_checksums_do_not_make_wrong_singleton_evidence_usable(tmp_path, body):
    batch, scan = _context(lo=7, hi=7, step=1)
    manifest = _manifest(batch, scan, 1, 7, 7, endpoint="/events/7", terminal=True)
    records = [body] if "id" in body else body["events"]
    manifest.update(
        params={},
        record_count=len(records),
        ids_hash=ids_hash([row["id"] for row in records]),
    )
    validate_page_identity(batch, scan, manifest, seq=1)
    settings = Settings(tmp_path)
    directory = settings.raw_dir / "fixture"
    written = write_page(directory, 1, json.dumps(body).encode(), manifest)
    raw = read_manifest(written.manifest_path, trusted_root=settings.raw_dir)
    with pytest.raises(DurabilityError, match="committed source unit"):
        read_page_records(settings, directory, raw, "events", scan=scan)


def test_bulk_404_is_not_confirmed_native_absence_even_with_valid_checksums(tmp_path):
    batch, scan = _context()
    manifest = _manifest(batch, scan, 1, 1, 2)
    manifest["http_status"] = 404
    validate_page_identity(batch, scan, manifest, seq=1)
    settings = Settings(tmp_path)
    directory = settings.raw_dir / "fixture"
    written = write_page(directory, 1, b'{"error":"missing"}', manifest)
    raw = read_manifest(written.manifest_path, trusted_root=settings.raw_dir)
    with pytest.raises(DurabilityError, match="singleton"):
        read_page_records(settings, directory, raw, "events", scan=scan)


@pytest.mark.parametrize("kind", ["id_range", "keyset_ids"])
@pytest.mark.parametrize("revision", [True, False, 0, 3, "2", None])
def test_unknown_or_coerced_page_revisions_are_rejected(kind, revision):
    batch, scan = _context(kind, ids=[1, 9, 41, 100])
    manifest = _manifest(batch, scan, 1, 1 if kind == "id_range" else 0, 2, revision=revision)
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, manifest, seq=1)


@pytest.mark.parametrize("field", ["offset_start", "offset_end"])
@pytest.mark.parametrize("value", [True, False, "1", 1.0, None])
def test_leaf_offsets_are_literal_integers(field, value):
    batch, scan = _context()
    manifest = _manifest(batch, scan, 1, 1, 2)
    manifest[field] = value
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, manifest, seq=1)


@pytest.mark.parametrize("bounds", [(1, 2), (2, 3), (4, 4)])
def test_range_predecessor_rejects_overlap_or_a_gap(bounds):
    batch, scan = _context()
    previous = _manifest(batch, scan, 1, 1, 2)
    current = _manifest(batch, scan, 2, *bounds, terminal=bounds[1] == 4)
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, current, seq=2, previous=previous)


def test_adaptive_page_requires_its_exact_predecessor():
    batch, scan = _context()
    current = _manifest(batch, scan, 2, 3, 4, terminal=True)
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, current, seq=2)


@pytest.mark.parametrize("field", ["seq", "scan_id", "page_id"])
def test_an_unrelated_predecessor_cannot_authorize_a_leaf(field):
    batch, scan = _context()
    previous = _manifest(batch, scan, 1, 1, 2)
    current = _manifest(batch, scan, 2, 3, 4, terminal=True)
    previous[field] = 2 if field == "seq" else "different-source-unit"
    with pytest.raises(DurabilityError, match="predecessor"):
        validate_page_identity(batch, scan, current, seq=2, previous=previous)


def test_a_leaf_cannot_cross_a_sealed_original_window_boundary():
    batch, scan = _context(hi=8, step=4)
    previous = _manifest(batch, scan, 1, 1, 2)
    current = _manifest(batch, scan, 2, 3, 5)
    with pytest.raises(DurabilityError, match="window"):
        validate_page_identity(batch, scan, current, seq=2, previous=previous)


def test_native_leaf_revision_is_rejected_for_a_legacy_tail():
    batch, scan = _context()
    scan["params_json"] = json.dumps({"lo": 1, "step": 4, "tail": True})
    manifest = _manifest(batch, scan, 1, 1, 2)
    with pytest.raises(DurabilityError, match="finite"):
        validate_page_identity(batch, scan, manifest, seq=1)


@pytest.mark.parametrize(
    "mutation", ["premature_terminal", "wrong_query", "wrong_fallback", "continuation"]
)
def test_leaf_cannot_claim_a_different_source_unit(mutation):
    batch, scan = _context()
    manifest = _manifest(batch, scan, 1, 1, 2)
    if mutation == "premature_terminal":
        manifest["terminal"] = True
    elif mutation == "wrong_query":
        manifest["params"]["id"] = ["1", "3"]
    elif mutation == "wrong_fallback":
        manifest.update(endpoint="/events/1", params={})
    else:
        manifest["output_cursor"] = "more"
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, manifest, seq=1)


def test_old_fixed_prefix_can_resume_as_adaptive_leaves_without_changing_old_bytes(tmp_path):
    batch, scan = _context(hi=8, step=4)
    legacy = _manifest(batch, scan, 1, 1, 4)
    legacy.pop("page_unit_revision")
    legacy["params"] = {"limit": 4, "id": [1, 2, 3, 4]}
    body = _body([1, 2, 3, 4])
    legacy.update(record_count=4, ids_hash=ids_hash(["1", "2", "3", "4"]))
    settings = Settings(tmp_path)
    directory = settings.raw_dir / "fixture"
    written = write_page(directory, 1, body, legacy)
    paths = [written.manifest_path, written.gz_path]
    original = {path: sha256_bytes(path.read_bytes()) for path in paths}
    retained = read_manifest(written.manifest_path, trusted_root=settings.raw_dir)
    validate_page_identity(batch, scan, retained, seq=1)
    leaf = _manifest(batch, scan, 2, 5, 6)
    validate_page_identity(batch, scan, leaf, seq=2, previous=retained)
    final = _manifest(batch, scan, 3, 7, 8, terminal=True)
    validate_page_identity(batch, scan, final, seq=3, previous=leaf)
    assert {path: sha256_bytes(path.read_bytes()) for path in paths} == original
    assert read_body(directory, retained, trusted_root=settings.raw_dir) == body


def test_a_legacy_page_cannot_downgrade_an_adaptive_attempt():
    batch, scan = _context(hi=8, step=4)
    previous = _manifest(batch, scan, 1, 1, 4)
    current = _manifest(batch, scan, 2, 5, 8, terminal=True)
    current.pop("page_unit_revision")
    current["params"] = {"limit": 4, "id": [5, 6, 7, 8]}
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, current, seq=2, previous=previous)


def test_frozen_list_predecessor_rejects_numeric_offset_or_reordered_inputs():
    batch, scan = _context("keyset_ids", ids=[7, 41, 900, 5000])
    previous = _manifest(batch, scan, 1, 0, 2)
    current = _manifest(batch, scan, 2, 2, 4, terminal=True)
    validate_page_identity(batch, scan, current, seq=2, previous=previous)
    wrong = copy.deepcopy(current)
    wrong["params"]["id"] = ["5000", "900"]
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, wrong, seq=2, previous=previous)
    wrong = copy.deepcopy(current)
    wrong["offset_start"] = 900
    with pytest.raises(DurabilityError):
        validate_page_identity(batch, scan, wrong, seq=2, previous=previous)


@pytest.mark.parametrize("consumer", ["orphan", "loader"])
def test_indexed_consumers_read_actual_predecessor_revision_to_reject_downgrade(tmp_path, consumer):
    runtime, _ = build_runtime(tmp_path, FakeGamma(World()))
    batch_meta, scan = _context(hi=8, step=4)
    runtime.ledger.create_batch(
        BATCH_ID,
        "bootstrap",
        batch_meta["observation_date"],
        iso_utc(FIXED_NOW),
        "testsha",
        [scan],
        scope={"revision": 2, "kind": "catalogue", "sealed": True},
    )
    batch = runtime.ledger.get_batch(BATCH_ID)
    directory = scan_dir_for(runtime.settings, batch["observation_date"], BATCH_ID, scan["scan_id"])
    try:
        for seq, start, end in ((1, 1, 4), (2, 5, 8)):
            manifest = _manifest(batch, scan, seq, start, end, terminal=seq == 2)
            manifest.update(
                record_count=4,
                ids_hash=ids_hash([str(value) for value in range(start, end + 1)]),
                retries=0,
                latency_s=0.0,
            )
            if seq == 2:
                manifest.pop("page_unit_revision")
                manifest["params"]["id"] = list(range(start, end + 1))
            written = write_page(directory, seq, _body(range(start, end + 1)), manifest)
            retained = read_manifest(written.manifest_path, trusted_root=runtime.settings.raw_dir)
            if seq == 1 or consumer == "loader":
                runtime.ledger.record_page(
                    runner._row_from_manifest(retained, BATCH_ID, scan["scan_id"]),
                    scan["scan_id"],
                    seq == 2,
                )
        # SQLite deliberately does not carry a page revision column.
        assert "page_unit_revision" not in runtime.ledger.page_for_scan(scan["scan_id"], 1)
        with pytest.raises(DurabilityError, match="downgraded"):
            if consumer == "orphan":
                runner._adopt_durable_pages(
                    runtime, batch, runtime.ledger.get_scan(scan["scan_id"]), directory
                )
            else:
                second = next(
                    page for page in runtime.ledger.pages_for_batch(BATCH_ID) if page["seq"] == 2
                )
                _rows_for_chunk(LoadRuntime(runtime.settings, runtime.ledger), [second])
    finally:
        runtime.client.close()
        runtime.ledger.close()


@pytest.mark.parametrize("fallback", [False, True])
def test_confirmed_fallback_absence_is_final_but_empty_bulk_still_needs_confirmation(
    tmp_path, fallback
):
    world = World()
    world.add_event(make_event("1", "Known event"))
    world.add_direct_market(
        make_market("3", "Missing parent?", event_stub={"id": "9", "title": "Missing"})
    )
    fake = FakeGamma(world)
    native_requests = []

    def native_lookup(request):
        native_requests.append(request.url.path)
        return httpx.Response(
            404 if len(native_requests) == 1 else 500,
            json={"error": "absent" if len(native_requests) == 1 else "outage"},
        )

    fake.rules.append(
        Rule(lambda request: request.url.path == "/events/9", native_lookup, remaining=100)
    )
    if fallback:
        fake.rules.append(
            Rule(
                lambda request: (
                    request.url.path == "/events/keyset"
                    and request.url.params.get_list("id") == ["9"]
                ),
                lambda request: httpx.Response(413, json={"error": "native lookup required"}),
                remaining=100,
            )
        )
    runtime, _ = build_runtime(tmp_path, fake, env={"CATALOGUE_CAPTURE_WORKERS": "1"})
    try:
        summary = runner.run_capture(runtime, "bootstrap")
        assert summary.status == "captured"
        assert native_requests == ["/events/9"]
        phase_three = [
            row for row in runtime.ledger.list_scans(summary.batch_id) if row["phase"] == 3
        ]
        assert phase_three == [] if fallback else len(phase_three) == 1
        if not fallback:
            assert phase_three[0]["kind"] == "single_ids"
            assert json.loads(phase_three[0]["input_ids_json"]) == ["9"]
        phase_two = next(
            row for row in runtime.ledger.list_scans(summary.batch_id) if row["phase"] == 2
        )
        [page] = runtime.ledger.pages_for_scan(phase_two["scan_id"])
        assert page["http_status"] == (404 if fallback else 200)
        assert page["record_count"] == 0
        assert phase_two["status"] == "complete"
    finally:
        runtime.client.close()
        runtime.ledger.close()


def test_adaptive_orphan_is_adopted_and_raw_ledger_rebuild_keeps_every_leaf(tmp_path, monkeypatch):
    runtime, _ = build_runtime(tmp_path, FakeGamma(World()))
    batch_meta, scan = _context()
    runtime.ledger.create_batch(
        BATCH_ID,
        "bootstrap",
        batch_meta["observation_date"],
        iso_utc(FIXED_NOW),
        "testsha",
        [scan],
        scope={"revision": 2, "kind": "catalogue", "sealed": True},
    )
    batch = runtime.ledger.get_batch(BATCH_ID)
    runner._write_batch_marker(runtime, batch)
    directory = scan_dir_for(runtime.settings, batch["observation_date"], BATCH_ID, scan["scan_id"])
    runner._write_scan_marker(
        runtime, batch, runtime.ledger.get_scan(scan["scan_id"]), directory, "running"
    )

    def handler(request):
        wanted = tuple(int(value) for value in request.url.params.get_list("id"))
        if len(wanted) > 2:
            return httpx.Response(413, json={"error": "split"})
        return httpx.Response(200, content=_body(wanted))

    runtime.client.close()
    runtime.client = _client(handler)
    try:
        pages = list(
            id_range_pages(
                runtime.client, "/events/keyset", {"lo": 1, "hi": 4, "step": 4}, "events"
            )
        )
        runner._persist_page(runtime, batch, scan, directory, pages[0])
        first = runtime.ledger.page_for_scan(scan["scan_id"], 1)
        manifests_before = {path: path.read_bytes() for path in directory.glob("p000001.*")}

        def crash_after_files(label):
            if label == "after_page_rename":
                raise RuntimeError("injected between manifest and ledger commit")

        monkeypatch.setattr(runner, "fault_point", crash_after_files)
        with pytest.raises(RuntimeError, match="injected"):
            runner._persist_page(runtime, batch, scan, directory, pages[1])
        assert runtime.ledger.page_for_scan(scan["scan_id"], 2) is None
        assert manifest_path(directory, 2).is_file()
        monkeypatch.setattr(runner, "fault_point", lambda label: None)
        assert (
            runner._adopt_durable_pages(
                runtime, batch, runtime.ledger.get_scan(scan["scan_id"]), directory
            )
            == 1
        )
        captured = list(
            iter_scan_pages(runtime.settings, batch, runtime.ledger.get_scan(scan["scan_id"]))
        )
        assert [manifest["page_unit_revision"] for manifest, _ in captured] == [2, 2]
        assert [[record["id"] for record in records] for _, records in captured] == [
            ["2", "1"],
            ["4", "3"],
        ]
        assert runtime.ledger.page_for_scan(scan["scan_id"], 1) == first
        assert {path: path.read_bytes() for path in manifests_before} == manifests_before

        with Ledger(tmp_path / "rebuilt.sqlite") as rebuilt:
            counts = runner.rebuild_from_raw(runtime.settings, rebuilt)
            assert counts["pages"] == 2
            assert [
                (page["seq"], page["offset_start"], page["offset_end"])
                for page in rebuilt.pages_for_scan(scan["scan_id"])
            ] == [(1, 1, 2), (2, 3, 4)]
    finally:
        runtime.client.close()
        runtime.ledger.close()
