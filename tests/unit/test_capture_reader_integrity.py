"""Valid checksums cannot substitute for a faithful declared source unit."""

from __future__ import annotations

import json

import pytest

from oddsfox_catalogue.capture.reader import DurabilityError, read_page_records
from oddsfox_catalogue.capture.writer import read_manifest, write_page
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.ids import canonical_json, ids_hash


def _read(tmp_path, *, kind, body, terminal=True, output_cursor=None, target="201", mark=999):
    settings = Settings(tmp_path)
    directory = settings.raw_dir / "synthetic"
    records = [body] if "id" in body else body["markets"]
    params = {"_high_water": mark} if kind == "keyset" else {}
    scan = {
        "scan_name": "synthetic",
        "kind": kind,
        "endpoint": "/markets/keyset" if kind == "keyset" else "/markets",
        "record_key": "markets",
        "params_json": json.dumps(params),
        "input_ids_json": json.dumps([target]),
    }
    write = write_page(
        directory,
        1,
        canonical_json(body).encode(),
        {
            "record_key": "markets",
            "http_status": 200,
            "record_count": len(records),
            "terminal": terminal,
            "input_cursor": None,
            "output_cursor": output_cursor,
            "params": {"id": [int(target)]},
            "ids_hash": ids_hash([str(row["id"]) for row in records]),
        },
    )
    manifest = read_manifest(write.manifest_path, trusted_root=settings.raw_dir)
    assert manifest is not None
    return read_page_records(settings, directory, manifest, "markets", scan=scan)


@pytest.mark.parametrize(
    ("terminal", "cursor"),
    [(True, None), (True, "continue"), (False, None)],
)
def test_keyset_declared_continuation_must_match_decoded_evidence(tmp_path, terminal, cursor):
    with pytest.raises(DurabilityError, match="committed source unit"):
        _read(
            tmp_path,
            kind="keyset",
            body={"markets": [{"id": "201"}], "next_cursor": "continue"},
            terminal=terminal,
            output_cursor=cursor,
        )


def test_keyset_stopping_at_sealed_mark_preserves_the_observed_cursor(tmp_path):
    rows = _read(
        tmp_path,
        kind="keyset",
        body={"markets": [{"id": "201"}], "next_cursor": "continue"},
        output_cursor="continue",
        mark=201,
    )
    assert rows == [{"id": "201"}]


@pytest.mark.parametrize("kind", ["single_ids", "keyset_ids"])
def test_requested_native_identity_is_checked_even_with_valid_hashes(tmp_path, kind):
    with pytest.raises(DurabilityError, match="committed source unit"):
        _read(tmp_path, kind=kind, body={"id": "202"}, target="201")


def test_single_lookup_cannot_hide_a_list_continuation(tmp_path):
    with pytest.raises(DurabilityError, match="committed source unit"):
        _read(
            tmp_path,
            kind="single_ids",
            body={"markets": [{"id": "201"}], "next_cursor": "continue"},
        )
