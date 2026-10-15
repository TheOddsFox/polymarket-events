"""Small offline proofs for the full-domain capacity oracle."""

import hashlib
import importlib.util
import json
import shutil
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from fakes.world import make_event, make_market
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_publish import publish
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_store import apply, initialize

spec = importlib.util.spec_from_file_location(
    "capacity_content", Path(__file__).resolve().parents[2] / "scripts/verify-capacity-content.py"
)
oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(oracle)
STAMP = "2026-02-01T00:00:00+00:00"
RESPONSE = "1234567890abcdef1234567890abcdef"


def settings(root):
    return CurrentSettings(root, limits=replace(CurrentLimits(), free_reserve_bytes=1))


@pytest.fixture
def world(tmp_path):
    seed, root = tmp_path / "seed", tmp_path / "root"
    validate_root(seed, initialize=True)
    good = make_market("10", "Open", version="v1")
    bad = make_market("11", "Bad")
    bad["outcomes"] = []
    events = [make_event("1", "Event", markets=[good, bad]), make_event("2", "Empty")]
    with duckdb.connect(str(seed / "catalogue.duckdb")) as conn:
        initialize(conn)
        apply(
            conn,
            project_response(
                events, "events", received_at="2026-01-01T00:00:00Z", response_id="seed"
            ),
        )
        conn.execute(
            "UPDATE markets SET identity_error='outcomes must contain nonempty labels' WHERE market_id='11'"
        )
    shutil.copytree(seed, root)
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        publish(settings(root), conn)
    # Explicit wire shape: the seed's NULLs and both aliases are significant.
    records = []
    for event in events:
        record = {
            key: event.get(key) for key in ("id", "title", "slug", "active", "closed", "archived")
        }
        record["markets"] = []
        for market in event.get("markets", []):
            raw = {
                key: market.get(key)
                for key in ("id", "question", "slug", "active", "closed", "archived")
            }
            raw.update(
                conditionId=market.get("conditionId"),
                version=market.get("version"),
                events=[{"id": event["id"]}],
            )
            raw["outcomes"] = (
                json.loads(market["outcomes"])
                if isinstance(market["outcomes"], str)
                else market["outcomes"]
            )
            if market["id"] == "10":
                raw["clobTokenIds"] = json.loads(market["clobTokenIds"])
            record["markets"].append(raw)
        records.append(record)
    return seed, root, tmp_path / "audit.jsonl", records


def capture(world, records=None):
    seed, root, audit, defaults = world
    records = defaults if records is None else records
    body = json.dumps(records, separators=(",", ":")).encode()
    write_page(
        root / "raw" / RESPONSE,
        1,
        body,
        {
            "request": {
                "endpoint": "/events",
                "params": {"id": [int(r["id"]) for r in records], "limit": len(records)},
            },
            "received_at": STAMP,
            "status": 200,
        },
    )
    audit.write_text(
        json.dumps(
            {
                "request": 1,
                "monotonic_s": 1.0,
                "kind": "events",
                "status": 200,
                "ids": [r["id"] for r in records],
                "emitted_ids": [r["id"] for r in records],
                "complete_memberships": {r["id"]: [m["id"] for m in r["markets"]] for r in records},
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "body_bytes": len(body),
            }
        )
        + "\n"
    )
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        apply(conn, project_response(records, "events", received_at=STAMP, response_id=RESPONSE))
        publish(settings(root), conn)
    return oracle.verify(seed, root, audit)


def test_exact_domain_and_authoritative_empty_membership(world):
    report = capture(world)
    assert report["valid"]
    assert report["authenticated_responses"] == 1
    assert report["relations"]["event_markets"]["actual"]["rows"] == 2
    assert report["relations"]["markets"]["source"] != report["relations"]["markets"]["actual"]


@pytest.mark.parametrize(
    "fault", ["bodyhash", "receipt", "impostor", "untouched_error", "links", "alias"]
)
def test_rejects_unproven_or_changed_content(world, fault):
    seed, root, audit, records = world
    if fault == "untouched_error":
        audit.write_text("")
        with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
            conn.execute(
                "UPDATE markets SET identity_error='native_confirmation_required' WHERE market_id='11'"
            )
    elif fault == "impostor":
        records[0]["id"] = "99"
        with pytest.raises(ValueError):
            capture(world, records)
        return
    else:
        capture(world)
        if fault == "bodyhash":
            path = root / "raw" / RESPONSE / "p000001.manifest.json"
            manifest = json.loads(path.read_text())
            manifest["body_sha256"] = "0" * 64
            path.write_text(json.dumps(manifest))
        else:
            with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
                if fault == "receipt":
                    conn.execute(
                        "UPDATE _receipts SET receipt=receipt+INTERVAL '1 second' WHERE kind='market'"
                    )
                elif fault == "links":
                    conn.execute("INSERT INTO event_markets VALUES ('polymarket','2','10')")
                else:
                    conn.execute("UPDATE outcomes SET position_id='123' WHERE market_id='10'")
    with pytest.raises(ValueError):
        oracle.verify(seed, root, audit)


def test_rejects_partial_commit_even_when_public_rows_equal(world):
    seed, root, audit, _records = world
    capture(world)
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        # Empty event has no public row or link delta; restore only its progress.
        conn.execute(
            "UPDATE _receipts SET receipt='2026-01-01T00:00:00Z',response='seed' WHERE kind='event' AND id='2'"
        )
        conn.execute(
            "UPDATE _membership SET receipt='2026-01-01T00:00:00Z',response='seed',latest_receipt='2026-01-01T00:00:00Z',latest_response='seed' WHERE event_id='2'"
        )
    with pytest.raises(ValueError, match="partially applied"):
        oracle.verify(seed, root, audit)


def test_requires_audit_for_real_receipt(world):
    seed, root, audit, _records = world
    capture(world)
    audit.write_text("")
    with pytest.raises(ValueError, match="absent from source audit"):
        oracle.verify(seed, root, audit)


def test_rejects_frozen_text_changed_in_authenticated_body(world):
    _seed, _root, _audit, records = world
    records[0]["title"] = "Different source title"
    with pytest.raises(ValueError, match="differs from frozen source"):
        capture(world, records)


def test_native_market_response_binds_exact_aliases_and_nulls(world):
    seed, root, audit, records = world
    market = records[0]["markets"][0]
    body = json.dumps(market, separators=(",", ":")).encode()
    write_page(
        root / "raw" / RESPONSE,
        1,
        body,
        {
            "request": {"endpoint": "/markets/10", "params": {}},
            "received_at": STAMP,
            "status": 200,
        },
    )
    audit.write_text(
        json.dumps(
            {
                "kind": "markets",
                "status": 200,
                "ids": ["10"],
                "emitted_ids": ["10"],
                "complete_memberships": {},
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "body_bytes": len(body),
            }
        )
        + "\n"
    )
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        apply(conn, project_response([market], "markets", received_at=STAMP, response_id=RESPONSE))
        publish(settings(root), conn)
    assert oracle.verify(seed, root, audit)["authenticated_responses"] == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "INSERT INTO event_markets SELECT * FROM event_markets LIMIT 1",
        "UPDATE events SET title=NULL WHERE event_id='1'",
        "DELETE FROM outcomes WHERE market_id='10' AND outcome_index=1",
        "ALTER TABLE events ADD COLUMN extra VARCHAR",
        "DELETE FROM _receipts WHERE kind='market' AND id='10'",
    ],
)
def test_rejects_full_domain_schema_multiplicity_and_missing_rows(world, mutation):
    seed, root, audit, _records = world
    audit.write_text("")
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        conn.execute(mutation)
    with pytest.raises(ValueError):
        oracle.verify(seed, root, audit)


@pytest.mark.parametrize("fault", ["missing_pointer", "stale_generation", "corrupt_parquet"])
def test_requires_current_complete_actual_publication(world, fault):
    seed, root, audit, _records = world
    capture(world)
    pointer_path = root / "published" / "current.json"
    if fault == "missing_pointer":
        pointer_path.unlink()
    elif fault == "stale_generation":
        with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
            conn.execute(
                "UPDATE sync_state SET value=to_json(cast(value AS BIGINT)+1) WHERE key='generation'"
            )
    else:
        pointer = json.loads(pointer_path.read_text())
        release = root / "published" / "releases" / pointer["release_id"]
        (release / "markets.parquet").write_bytes(b"corrupt actual export")
    with pytest.raises((ValueError, OSError)):
        oracle.verify(seed, root, audit)


def test_binds_stable_pointer_and_actual_publication(world, monkeypatch):
    seed, root, audit, _records = world
    report = capture(world)
    binding = report["publication"]
    pointer_path = root / "published" / "current.json"
    assert binding["published_generation"] == binding["store_generation"]
    assert binding["pointer_sha256"] == hashlib.sha256(pointer_path.read_bytes()).hexdigest()
    assert (
        binding["relations"]["markets"]["semantic_sha256"]
        == report["relations"]["markets"]["actual"]["semantic_sha256"]
    )
    original = oracle.verify_current

    def change_pointer(*args, **kwargs):
        result = original(*args, **kwargs)
        pointer_path.write_text(pointer_path.read_text() + "\n")
        return result

    monkeypatch.setattr(oracle, "verify_current", change_pointer)
    with pytest.raises(ValueError, match="pointer changed"):
        oracle.verify(seed, root, audit)


def test_explicit_private_bounded_sql_spill(world, tmp_path):
    seed, root, audit, _records = world
    capture(world)
    temporary = tmp_path / "sql-spill"
    report = oracle.verify(seed, root, audit, temporary=temporary)
    assert report["sql_temporary"] == {"directory": str(temporary), "max_bytes": 8 * 1024**3}
    assert temporary.stat().st_mode & 0o777 == 0o700
    with pytest.raises(ValueError, match="must be fresh"):
        oracle.verify(seed, root, audit, temporary=temporary)
    with pytest.raises(ValueError, match="must be disjoint"):
        oracle.verify(seed, root, audit, temporary=root / "temporary")
