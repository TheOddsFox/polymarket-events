"""Small synthetic checks for the immutable legacy adapter and bounded seed."""

import hashlib
import json

import duckdb
import pytest

from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.contract import CONTRACT, NORMALIZATION_REVISION, projection_schemas
from oddsfox_catalogue.current_config import CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_import import _membership_response
from oddsfox_catalogue.current_import import import_release as import_phase
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.legacy_release import verify_legacy_release


def import_release(*args, **kwargs):
    while True:
        result = import_phase(*args, **kwargs)
        if result["complete"]:
            return result


def release_fixture(tmp_path, *, usable=False):
    validate_root(tmp_path / "current", initialize=True)
    root = tmp_path / "20261013T191053Z"
    root.mkdir()
    conn = duckdb.connect()
    tables = {}
    for name, schema in projection_schemas().items():
        conn.execute(
            "CREATE OR REPLACE TABLE fixture ("
            + ",".join(f'"{c["name"]}" {c["type"]}' for c in schema)
            + ")"
        )
        if name == "events":
            conn.execute(
                "INSERT INTO fixture (venue,event_id,title,last_observed_at) VALUES ('polymarket','1','seed','2026-10-12T00:00:00Z')"
            )
        if name == "markets":
            conn.execute(
                "INSERT INTO fixture (venue,market_id,question,usable,last_observed_at,observation_id) VALUES ('polymarket','2','seed market',false,'2026-10-12T00:00:00Z','obs')"
            )
        if usable and name == "markets":
            conn.execute(
                "UPDATE fixture SET usable=true,source_market_version='v1',protocol='ctf',condition_id='0x1111111111111111111111111111111111111111111111111111111111111111',identity_error=NULL"
            )
        if usable and name == "outcomes":
            conn.execute(
                "INSERT INTO fixture (venue,market_id,condition_id,outcome_index,outcome_label,asset_kind,asset_id,clob_token_id,observation_id) VALUES ('polymarket','2','0x1111111111111111111111111111111111111111111111111111111111111111',1,'Yes','ctf_token','123','123','obs')"
            )
        p = root / f"{name}.parquet"
        conn.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(p)])
        tables[name] = {**descriptor(p), **semantic_fingerprint(conn, "SELECT * FROM fixture")}
    batch = "20261010T142904Z-bootstrap"
    coverage = {
        "coverage_schema_revision": 1,
        "declared_scans_complete": True,
        "source_catalogue_complete": False,
        "discovery_limits": ["bounded"],
        "batches": [
            {
                "batch_id": batch,
                "mode": "bootstrap",
                "scope": {"revision": 2, "sealed": True, "high_water": {"events": 3, "markets": 4}},
            }
        ],
        "units": [],
    }
    inventory = [{"path": f"2026-10-10/{batch}/_batch.json", "bytes": 0, "sha256": "0" * 64}]
    for name, value in [("coverage", coverage), ("capture-inventory", inventory)]:
        (root / f"{name}.json").write_text(json.dumps(value))
    manifest = {
        "release_id": root.name,
        "created_at": "2026-10-13T19:10:53Z",
        "git_sha": None,
        "contract": CONTRACT,
        "normalization_revision": NORMALIZATION_REVISION,
        "projection_version": "v2",
        "batch_ids": [batch],
        "tables": tables,
        "capture_inventory": descriptor(root / "capture-inventory.json"),
        "coverage": descriptor(root / "coverage.json"),
        "build": {
            "model_revision": "0" * 64,
            "quality": {},
            "effective_vars": {},
            "certification_sha256": "1" * 64,
        },
    }
    (root / "release.json").write_text(json.dumps(manifest))
    conn.execute("DROP TABLE fixture")
    return root, hashlib.sha256((root / "release.json").read_bytes()).hexdigest(), conn


def descriptor(path):
    data = path.read_bytes()
    return {"file": path.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def test_seed_and_resume(tmp_path):
    release, checksum, conn = release_fixture(tmp_path)
    result = import_release(
        CurrentSettings(tmp_path / "current"),
        conn,
        release,
        manifest_sha256=checksum,
        raw_root=tmp_path / "raw",
    )
    assert result["valid"]
    assert result["seed"]["frontiers"] == {"events": 3, "markets": 4}
    assert conn.execute("SELECT question,usable FROM markets").fetchall() == [
        ("seed market", False)
    ]
    assert import_release(
        CurrentSettings(tmp_path / "current"),
        conn,
        release,
        manifest_sha256=checksum,
        raw_root=tmp_path / "raw",
    )["resumed"]
    with pytest.raises(CurrentError, match="different release"):
        import_release(
            CurrentSettings(tmp_path / "current"),
            conn,
            release,
            manifest_sha256="0" * 64,
            raw_root=tmp_path / "raw",
        )


def test_corrupt_release_rejected(tmp_path):
    release, checksum, conn = release_fixture(tmp_path)
    (release / "events.parquet").write_bytes(b"corrupt")
    with pytest.raises(CurrentError):
        verify_legacy_release(release, manifest_sha256=checksum, connection=conn)


def add_event_page(release, raw_root, records, *, day="13"):
    from oddsfox_catalogue.capture.writer import write_page
    from oddsfox_catalogue.ids import ids_hash

    manifest = json.loads((release / "release.json").read_text())
    batch = manifest["batch_ids"][0]
    scan_id = batch + ".events.a1"
    directory = raw_root / "2026-10-10" / batch / scan_id
    directory.mkdir(parents=True)
    scan = {
        "scan_id": scan_id,
        "scan_name": "events",
        "attempt": 1,
        "plan_order": 1,
        "kind": "keyset_ids",
        "endpoint": "/events/keyset",
        "record_key": "events",
        "params": {"id": [1], "limit": 1},
        "input_ids": ["1"],
    }
    (directory / "_scan.json").write_text(json.dumps(scan))
    page_id = scan_id + ".p000001"
    params = {"id": [1], "limit": 1}
    write_page(
        directory,
        1,
        json.dumps(records).encode(),
        {
            "page_id": page_id,
            "batch_id": batch,
            "scan_id": scan_id,
            "scan_name": "events",
            "attempt": 1,
            "plan_order": 1,
            "observation_date": "2026-10-10",
            "record_key": "events",
            "endpoint": "/events/keyset",
            "params": params,
            "observed_at": f"2026-10-{day}T00:00:00Z",
            "seq": 1,
            "terminal": True,
            "input_cursor": None,
            "output_cursor": None,
            "offset_start": None,
            "offset_end": None,
            "http_status": 200,
            "record_count": len(records),
            "ids_hash": ids_hash([r["id"] for r in records]),
        },
    )
    coverage = json.loads((release / "coverage.json").read_text())
    coverage["units"] = [
        {
            "batch_id": batch,
            "page_id": page_id,
            "endpoint": "/events/keyset",
            "params": params,
            "received_at": f"2026-10-{day}T00:00:00Z",
            "records": len(records),
            "status": "success",
        }
    ]
    (release / "coverage.json").write_text(json.dumps(coverage))
    inventory = [
        {
            "path": p.relative_to(raw_root).as_posix(),
            "bytes": p.stat().st_size,
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        }
        for p in sorted(directory.iterdir())
    ]
    (release / "capture-inventory.json").write_text(json.dumps(inventory))
    manifest["coverage"] = descriptor(release / "coverage.json")
    manifest["capture_inventory"] = descriptor(release / "capture-inventory.json")
    (release / "release.json").write_text(json.dumps(manifest))
    return hashlib.sha256((release / "release.json").read_bytes()).hexdigest()


def test_verified_event_pass_preserves_parquet_metadata(tmp_path):
    release, _, conn = release_fixture(tmp_path)
    raw_root = tmp_path / "raw"
    checksum = add_event_page(
        release,
        raw_root,
        [
            {
                "id": "1",
                "title": "recovered",
                "markets": [{"id": "2", "question": "new nested"}, {"id": "bad"}],
            }
        ],
    )
    import_release(
        CurrentSettings(tmp_path / "current"),
        conn,
        release,
        manifest_sha256=checksum,
        raw_root=raw_root,
    )
    assert conn.execute("SELECT title FROM events").fetchone() == ("seed",)
    assert conn.execute("SELECT question FROM markets").fetchone() == ("seed market",)
    assert conn.execute("SELECT unresolved FROM _membership").fetchone() == (True,)
    assert conn.execute("SELECT * FROM event_markets").fetchall() == []


def test_raw_corruption_retains_private_baseline(tmp_path):
    release, _, conn = release_fixture(tmp_path)
    raw_root = tmp_path / "raw"
    checksum = add_event_page(release, raw_root, [{"id": "1", "markets": []}])
    next(raw_root.rglob("*.json.gz")).write_bytes(b"corrupt")
    with pytest.raises(ValueError):
        import_release(
            CurrentSettings(tmp_path / "current"),
            conn,
            release,
            manifest_sha256=checksum,
            raw_root=raw_root,
        )
    assert conn.execute("SELECT count(*) FROM events").fetchone() == (1,)
    assert store.get_state(conn, "seed_pending")["stage"] == "evidence"
    assert store.get_state(conn, "seed") is None


def test_usable_seed_preserves_labels_and_identity(tmp_path):
    release, checksum, conn = release_fixture(tmp_path, usable=True)
    import_release(
        CurrentSettings(tmp_path / "current"),
        conn,
        release,
        manifest_sha256=checksum,
        raw_root=tmp_path / "raw",
    )
    assert conn.execute("SELECT outcome_label FROM outcomes").fetchall() == [("Yes",)]
    assert conn.execute("SELECT usable FROM markets").fetchone() == (True,)
    assert store.validate_store(conn)["valid"]


@pytest.mark.parametrize("children", [None, "bad", [{"id": "bad"}], [None]])
def test_membership_parser_incomplete(children):
    response = _membership_response(
        [{"id": "1", "markets": children}], received_at="2026-10-13T00:00:00Z", response_id="a"
    )
    assert response.memberships == {"1": None}


def test_membership_parser_ignores_metadata_and_rejects_conflicting_memberships():
    response = _membership_response(
        [{"id": "1", "title": 123, "markets": [{"id": "2", "conditionId": {}, "volume": "bad"}]}],
        received_at="2026-10-13T00:00:00Z",
        response_id="a",
    )
    assert response.memberships == {"1": ["2"]}
    assert not response.events and not response.markets and not response.outcomes
    with pytest.raises(CurrentError, match="conflicting event membership"):
        _membership_response(
            [{"id": "1", "markets": []}, {"id": "1", "markets": [{"id": "2"}]}],
            received_at=response.received_at,
            response_id="a",
        )


@pytest.mark.parametrize(
    "latest",
    [{}, {"markets": None}, {"markets": "bad"}, {"markets": [{"id": "2"}], "hasMore": True}],
)
def test_seed_latest_incomplete_preserves_complete_and_baseline(tmp_path, monkeypatch, latest):
    from oddsfox_catalogue import current_import as importer

    release, checksum, conn = release_fixture(tmp_path, usable=True)
    settings = CurrentSettings(tmp_path / "current")
    args = dict(manifest_sha256=checksum, raw_root=tmp_path / "raw")
    import_phase(settings, conn, release, **args)
    tables = ("events", "markets", "outcomes", "_receipts", "_identity", "_identity_claims")
    before = {
        name: conn.execute(f"SELECT * FROM {name} ORDER BY ALL").fetchall() for name in tables
    }

    def pages(*_):
        for day, rid, raw in [
            ("14", "z", {"id": "1", **latest}),
            ("13", "z", {"id": "1", "markets": [{"id": "2"}]}),
            ("13", "a", {"id": "1", "markets": []}),
        ]:
            yield _membership_response(
                [raw], received_at=f"2026-10-{day}T00:00:00Z", response_id=rid
            )

    monkeypatch.setattr(importer, "_event_pages", pages)
    assert import_release(settings, conn, release, **args)["valid"]
    assert conn.execute("SELECT event_id,market_id FROM event_markets").fetchall() == [("1", "2")]
    assert conn.execute(
        "SELECT response,unresolved,latest_response FROM _membership"
    ).fetchone() == ("z", True, "z")
    assert conn.execute("SELECT receipt < latest_receipt FROM _membership").fetchone() == (True,)
    assert before == {
        name: conn.execute(f"SELECT * FROM {name} ORDER BY ALL").fetchall() for name in tables
    }


def test_seed_empty_authoritative_and_unknown_parents_fail_closed(tmp_path, monkeypatch):
    from oddsfox_catalogue import current_import as importer

    release, checksum, conn = release_fixture(tmp_path)
    settings = CurrentSettings(tmp_path / "current")
    args = dict(manifest_sha256=checksum, raw_root=tmp_path / "raw")

    def pages(*_):
        for rid, members in [("a", [{"id": "2"}]), ("z", [])]:
            yield _membership_response(
                [{"id": "1", "markets": members}],
                received_at="2026-10-13T00:00:00Z",
                response_id=rid,
            )

    monkeypatch.setattr(importer, "_event_pages", pages)
    assert import_release(settings, conn, release, **args)["valid"]
    assert conn.execute("SELECT * FROM event_markets").fetchall() == []
    assert conn.execute("SELECT response,unresolved FROM _membership").fetchone() == ("z", False)


@pytest.mark.parametrize("event,markets", [("9", []), ("1", [{"id": "9"}])])
def test_seed_unknown_parent_rejected(tmp_path, monkeypatch, event, markets):
    from oddsfox_catalogue import current_import as importer

    release, checksum, conn = release_fixture(tmp_path)
    response = _membership_response(
        [{"id": event, "markets": markets}], received_at="2026-10-13T00:00:00Z", response_id="a"
    )
    monkeypatch.setattr(importer, "_event_pages", lambda *_: iter([response]))
    with pytest.raises(CurrentError):
        import_release(
            CurrentSettings(tmp_path / "current"),
            conn,
            release,
            manifest_sha256=checksum,
            raw_root=tmp_path / "raw",
        )
    assert store.generation(conn) == 0
    assert conn.execute("SELECT count(*) FROM events").fetchone() == (1,)
    assert conn.execute("SELECT count(*) FROM markets").fetchone() == (1,)
