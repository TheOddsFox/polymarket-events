#!/usr/bin/env python3
"""Offline independent membership oracle for a pinned legacy import.

Validates latest complete event-owned memberships only, not all imported content.
Never called by the importer or runtime.
"""

import argparse
import hashlib
import json
import re
import time
from pathlib import Path
from types import SimpleNamespace

from oddsfox_catalogue.capture.reader import read_page_records, validate_page_identity
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, safe_path
from oddsfox_catalogue.current_publish import verify_current
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.legacy_release import (
    JSON_LIMIT,
    MANIFEST_LIMIT,
    _descriptor,
    _read_json,
    _validate_coverage,
    expand_coverage_id_range,
)


def canonical_id(value):
    return (type(value) is int and value > 0) or (
        isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value) is not None
    )


def complete_members(record):
    """Derive membership directly from event evidence, independently of projection."""
    if not isinstance(record, dict) or not canonical_id(record.get("id")):
        return None
    children = record.get("markets")
    if not isinstance(children, list) or any(
        not isinstance(child, dict) or not canonical_id(child.get("id")) for child in children
    ):
        return None
    for key, value in record.items():
        name = key.replace("_", "").lower()
        if name in {
            "truncated",
            "hasmore",
            "hasnext",
            "hasnextpage",
            "marketstruncated",
            "marketshasmore",
        } and value not in (False, None):
            return None
        if name in {"nextcursor", "next", "marketsnextcursor", "marketscursor"} and value:
            return None
        if name in {"pagination", "pageinfo", "marketspagination"} and value:
            return None
        if name in {
            "total",
            "totalcount",
            "marketcount",
            "marketscount",
            "totalmarkets",
            "marketstotal",
        } and (type(value) is not int or value != len(children)):
            return None
    return sorted({str(child["id"]) for child in children})


def event_pages(settings, release, manifest, raw_root):
    """Verify physical evidence and requests; decode each event page once."""
    for filename, key in (
        ("coverage.json", "coverage"),
        ("capture-inventory.json", "capture_inventory"),
    ):
        _descriptor(release / filename, manifest[key], release, max_bytes=JSON_LIMIT)
    inventory, _ = _read_json(release / "capture-inventory.json", release, JSON_LIMIT)
    coverage, _ = _read_json(release / "coverage.json", release, JSON_LIMIT)
    _validate_coverage(coverage, inventory, manifest["batch_ids"])
    entries = {entry["path"]: entry for entry in inventory}
    units = {unit["page_id"]: unit for unit in coverage["units"]}
    raw_settings = SimpleNamespace(
        raw_dir=raw_root,
        capture=SimpleNamespace(max_response_bytes=settings.limits.max_response_bytes),
    )
    previous, scans = {}, {}
    for relative in sorted(entries):
        if not relative.endswith(".manifest.json"):
            continue
        path = raw_root / relative
        page_id = path.parent.name + "." + path.name.removesuffix(".manifest.json")
        unit = units[page_id]
        if not unit["endpoint"].startswith("/events"):
            continue
        _descriptor(path, {**entries[relative], "file": path.name}, raw_root)
        page, _ = _read_json(path, raw_root, MANIFEST_LIMIT)
        if page.get("record_key") != "events" or page.get("page_id") != page_id:
            raise ValueError("inventoried event page identity differs")
        scan_path = path.parent / "_scan.json"
        scan_relative = scan_path.relative_to(raw_root).as_posix()
        if scan_relative not in entries:
            raise ValueError("event scan is not inventoried")
        if page["scan_id"] not in scans:
            _descriptor(scan_path, {**entries[scan_relative], "file": scan_path.name}, raw_root)
            scan, _ = _read_json(scan_path, raw_root, MANIFEST_LIMIT)
            scans[page["scan_id"]] = {
                **scan,
                "params_json": json.dumps(scan["params"]),
                "input_ids_json": json.dumps(scan.get("input_ids", [])),
            }
        scan = scans[page["scan_id"]]
        params = dict(unit["params"])
        actual_params = dict(page["params"])
        if "id_range" in unit:
            params["id"] = expand_coverage_id_range(unit["id_range"])
            if not isinstance(actual_params.get("id"), list) or any(
                not canonical_id(value) for value in actual_params["id"]
            ):
                raise ValueError("invalid event request ID list")
            actual_params["id"] = [str(value) for value in actual_params["id"]]
        if (
            any(
                page.get(key) != unit.get(other)
                for key, other in (
                    ("batch_id", "batch_id"),
                    ("endpoint", "endpoint"),
                    ("observed_at", "received_at"),
                    ("record_count", "records"),
                )
            )
            or actual_params != params
        ):
            raise ValueError("event page differs from coverage")
        body = path.parent / page["file"]
        body_relative = body.relative_to(raw_root).as_posix()
        if body_relative not in entries:
            raise ValueError("event body is not inventoried")
        _descriptor(body, {**entries[body_relative], "file": body.name}, raw_root)
        prior = previous.get(page["scan_id"])
        validate_page_identity(
            {"batch_id": page["batch_id"], "observation_date": path.parts[-4]},
            scan,
            page,
            seq=page["seq"],
            previous=prior,
        )
        records = read_page_records(
            raw_settings,
            path.parent,
            page,
            "events",
            scan=scan,
            previous=prior,
        )
        previous[page["scan_id"]] = page
        yield page, records


def membership_report(conn, pages, bridge):
    conn.execute("""CREATE TEMP TABLE oracle_receipts (
        event_id VARCHAR, received TIMESTAMPTZ, response VARCHAR, ids VARCHAR[])""")
    count = 0

    def append(batch):
        conn.execute(
            """INSERT INTO oracle_receipts
            SELECT json_extract_string(value,'$[0]'),
                json_extract_string(value,'$[1]')::TIMESTAMPTZ,
                json_extract_string(value,'$[2]'),
                from_json(json_extract(value,'$[3]'),'["VARCHAR"]')
            FROM json_each(?)""",
            [json.dumps(batch)],
        )

    for page, records in pages:
        count += 1
        batch = []
        for record in records:
            members = complete_members(record)
            if members is not None:
                batch.append(
                    (
                        str(record["id"]),
                        page["observed_at"],
                        "legacy-event:" + page["page_id"],
                        members,
                    )
                )
            if len(batch) == 100:
                append(batch)
                batch.clear()
        if batch:
            append(batch)
    conn.execute("""CREATE TEMP TABLE oracle_latest AS SELECT * FROM oracle_receipts
        QUALIFY row_number() OVER (
            PARTITION BY event_id ORDER BY received DESC,response DESC)=1""")
    conn.execute("""CREATE TEMP TABLE oracle_links AS
        SELECT DISTINCT 'polymarket' AS venue,event_id,unnest(ids) AS market_id
        FROM oracle_latest""")
    conn.execute("""CREATE TEMP TABLE oracle_actual AS
        SELECT venue,event_id,market_id FROM event_markets""")
    conn.execute(
        """CREATE TEMP TABLE oracle_bridge AS
        SELECT DISTINCT venue,event_id,market_id FROM read_parquet(?)""",
        [str(bridge)],
    )

    def difference(left, right):
        return conn.execute(f"""SELECT count(*) FROM (
            (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right}) UNION ALL
            (SELECT * FROM {right} EXCEPT ALL SELECT * FROM {left}))""").fetchone()[0]

    expected = semantic_fingerprint(conn, "SELECT * FROM oracle_links")
    actual = semantic_fingerprint(conn, "SELECT * FROM oracle_actual")
    bounds = conn.execute(
        "SELECT min(received),max(received),count(*) FROM oracle_latest"
    ).fetchone()
    return {
        "scope": "latest-complete event membership only; other imported content is not validated",
        "source_event_page_count": count,
        "expected": expected,
        "actual": actual,
        "membership_difference_count": difference("oracle_links", "oracle_actual"),
        "old_bridge_difference_count": difference("oracle_links", "oracle_bridge"),
        "latest_complete_receipt_min": str(bounds[0]) if bounds[0] else None,
        "latest_complete_receipt_max": str(bounds[1]) if bounds[1] else None,
        "events_with_complete_membership": bounds[2],
    }


def run(root, release_path, manifest_sha256, raw_root, output):
    started = time.monotonic()
    root, release, raw_root, output = map(safe_path, (root, release_path, raw_root, output))
    # Reports must never alter either canonical input tree, including its parent legacy root.
    protected = [root, raw_root, release]
    for path in (release, raw_root):
        protected.extend(
            parent
            for parent in path.parents
            if any(
                (parent / marker).exists()
                for marker in ("catalogue.json", "ledger.sqlite", "warehouse", ".state")
            )
        )
    if output.exists() or any(output.is_relative_to(path) for path in protected):
        raise ValueError("oracle output must be fresh and outside canonical roots")
    manifest, raw = _read_json(release / "release.json", release, MANIFEST_LIMIT)
    if (
        not re.fullmatch(r"[a-f0-9]{64}", manifest_sha256)
        or hashlib.sha256(raw).hexdigest() != manifest_sha256
    ):
        raise ValueError("release manifest checksum mismatch")
    bridge = release / "market_event_bridge.parquet"
    _descriptor(bridge, manifest["tables"]["market_event_bridge"], release)
    settings = CurrentSettings(root)
    with connect_store(settings, read_only=True) as conn:
        current = verify_current(settings, connection=conn)
        if current["published_generation"] != current["store_generation"]:
            raise ValueError("current publication is not bound to the imported store generation")
        report = membership_report(conn, event_pages(settings, release, manifest, raw_root), bridge)
        report.update(
            manifest_sha256=manifest_sha256, current=current, elapsed_s=time.monotonic() - started
        )
    with output.open("x") as handle:
        json.dump(report, handle, indent=2, default=str)
        handle.write("\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "release-path", "raw-root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    report = run(args.root, args.release_path, args.manifest_sha256, args.raw_root, args.output)
    return int(report["membership_difference_count"] != 0)


if __name__ == "__main__":
    raise SystemExit(main())
