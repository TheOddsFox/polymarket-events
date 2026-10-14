"""Compact coverage normalizes only canonical request IDs, preserving frozen requests."""

import hashlib
import json

import pytest
from test_current_import import add_event_page, descriptor, release_fixture
from test_current_import_oracle import oracle

from oddsfox_catalogue import current_import as importer
from oddsfox_catalogue.capture.reader import DurabilityError
from oddsfox_catalogue.current_config import CurrentSettings
from oddsfox_catalogue.legacy_release import verify_legacy_release


def verified_page(tmp_path, ids, *, wire="int", page_overrides=None, compact=True, scan_ids=None):
    release, _, conn = release_fixture(tmp_path)
    raw_root = tmp_path / "raw"
    records = [{"id": "1", "markets": [{"id": "2"}]}]
    add_event_page(release, raw_root, records)
    page_path = next(raw_root.rglob("p000001.manifest.json"))
    scan_path = page_path.parent / "_scan.json"
    page = json.loads(page_path.read_text())
    scan = json.loads(scan_path.read_text())
    page["params"]["id"] = ids
    page["params"].update(page_overrides or {})
    scan["params"] = dict(page["params"])
    if scan_ids is not None:
        scan["input_ids"] = scan_ids
    if wire == "string":
        # Revision 2 records Gamma's encoded ID list and its frozen input offsets.
        page.update(page_unit_revision=2, offset_start=0, offset_end=1)
    page_path.write_text(json.dumps(page))
    scan_path.write_text(json.dumps(scan))
    coverage_path = release / "coverage.json"
    coverage = json.loads(coverage_path.read_text())
    unit = coverage["units"][0]
    if compact:
        unit["params"].pop("id")
        unit["id_range"] = {"start": "1", "end": "1"}
    else:
        unit["params"]["id"] = [1] if wire == "string" else ["1"]
    coverage_path.write_text(json.dumps(coverage))
    inventory = []
    for path in sorted(raw_root.rglob("*")):
        if path.is_file():
            entry = descriptor(path)
            entry.pop("file")
            inventory.append({"path": path.relative_to(raw_root).as_posix(), **entry})
    inventory_path = release / "capture-inventory.json"
    inventory_path.write_text(json.dumps(inventory))
    manifest_path = release / "release.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["coverage"] = descriptor(coverage_path)
    manifest["capture_inventory"] = descriptor(inventory_path)
    manifest_path.write_text(json.dumps(manifest))
    checksum = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    try:
        manifest = verify_legacy_release(release, manifest_sha256=checksum, connection=conn)
    finally:
        conn.close()
    return CurrentSettings(tmp_path / "current"), release, manifest, raw_root, records


def read_both(fixture):
    settings, release, manifest, raw_root, _ = fixture
    imported = list(importer._event_pages(settings, release, manifest, raw_root, None))
    independent = [
        (page, list(records))
        for page, records in oracle.event_pages(settings, release, manifest, raw_root)
    ]
    return imported, independent


def assert_both_reject(fixture):
    settings, release, manifest, raw_root, _ = fixture
    with pytest.raises((ValueError, DurabilityError)):
        list(importer._event_pages(settings, release, manifest, raw_root, None))
    with pytest.raises((ValueError, DurabilityError)):
        for _, records in oracle.event_pages(settings, release, manifest, raw_root):
            list(records)


@pytest.mark.parametrize("wire,ids", [("int", [1]), ("string", ["1"])])
def test_compact_coverage_accepts_canonical_int_and_string_wire_ids(tmp_path, wire, ids):
    fixture = verified_page(tmp_path, ids, wire=wire)
    imported, independent = read_both(fixture)
    assert len(imported) == len(independent) == 1
    page, records = independent[0]
    assert records == fixture[4]
    assert page["params"]["id"] == ids
    assert imported[0].memberships == {"1": ["2"]}
    assert {str(record["id"]): oracle.complete_members(record) for record in records} == imported[
        0
    ].memberships
    assert imported[0].received_at == page["observed_at"]
    assert imported[0].response_id == "legacy-event:" + page["page_id"]


@pytest.mark.parametrize(
    "ids",
    [
        pytest.param([True], id="boolean"),
        pytest.param([False], id="false"),
        pytest.param(["01"], id="leading-zero"),
        pytest.param([1.0], id="float"),
        pytest.param([0], id="zero"),
        pytest.param([-1], id="negative"),
        pytest.param(["1e0"], id="exponent"),
        pytest.param("1", id="scalar-string"),
        pytest.param(1, id="scalar-int"),
        pytest.param(None, id="null"),
        pytest.param({"id": 1}, id="object"),
        pytest.param([2], id="wrong-int-id"),
        pytest.param(["2"], id="wrong-string-id"),
        pytest.param([], id="empty"),
        pytest.param([1, 1], id="duplicate"),
    ],
)
def test_compact_coverage_rejects_invalid_or_different_ids_after_rehash(tmp_path, ids):
    assert_both_reject(verified_page(tmp_path, ids))


@pytest.mark.parametrize("wire,ids", [("int", [1]), ("string", ["1"])])
@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"limit": 2}, id="different-limit"),
        pytest.param({"limit": "1"}, id="string-limit"),
        pytest.param({"limit": 1.0}, id="float-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"closed": True}, id="extra-filter"),
    ],
)
def test_compact_coverage_preserves_all_non_id_parameter_checks(tmp_path, wire, ids, overrides):
    assert_both_reject(verified_page(tmp_path, ids, wire=wire, page_overrides=overrides))


@pytest.mark.parametrize("wire,ids", [("int", [1]), ("string", ["1"])])
def test_noncompact_coverage_retains_exact_request_id_types(tmp_path, wire, ids):
    assert_both_reject(verified_page(tmp_path, ids, wire=wire, compact=False))


@pytest.mark.parametrize("wire,ids", [("int", [1]), ("string", ["1"])])
def test_compact_coverage_never_bypasses_frozen_scan_input_ids(tmp_path, wire, ids):
    assert_both_reject(verified_page(tmp_path, ids, wire=wire, scan_ids=["2"]))
