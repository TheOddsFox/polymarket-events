#!/usr/bin/env python3
"""Bounded offline full-domain oracle for a frozen capacity source."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from uuid import UUID

import duckdb

from oddsfox_catalogue.capture.writer import read_body, read_regular_bytes
from oddsfox_catalogue.current_config import CurrentSettings, data_path, safe_path
from oddsfox_catalogue.current_contract import SCHEMAS, projection
from oddsfox_catalogue.current_publish import verify_current
from oddsfox_catalogue.fingerprints import semantic_fingerprint

AUDIT_LIMIT = 256 * 1024**2
REPORT_LIMIT = 4 * 1024**2


def require(ok, message):
    if not ok:
        raise ValueError(message)


def regular(path, limit):
    return read_regular_bytes(path, max_bytes=limit)


def rows(conn, table, key, ids):
    cursor = conn.execute(
        f"SELECT * FROM seed.{table} WHERE venue='polymarket' AND {key} IN (SELECT unnest(?))",
        [ids],
    )
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def expected_records(conn, kind, ids):
    """Reconstruct the frozen wire contract independently of the runtime projector."""
    base = rows(conn, kind, kind[:-1] + "_id", ids)
    links = rows(conn, "event_markets", "event_id", ids) if kind == "events" else []
    mids = list({row["market_id"] for row in links}) if kind == "events" else ids
    markets = rows(conn, "markets", "market_id", mids)
    outcomes = rows(conn, "outcomes", "market_id", mids)
    parents = rows(conn, "event_markets", "market_id", mids)
    outcome_map = {}
    parent_map = {}
    for row in sorted(outcomes, key=lambda r: (r["market_id"], r["outcome_index"])):
        outcome_map.setdefault(row["market_id"], []).append(row)
    for row in parents:
        parent_map.setdefault(row["market_id"], []).append({"id": row["event_id"]})
    market_map = {}
    for row in markets:
        mid = row["market_id"]
        raw = {key: row[key] for key in ("question", "slug", "active", "closed", "archived")}
        raw.update(
            id=mid,
            conditionId=row["condition_id"],
            version=row["source_market_version"],
            events=parent_map.get(mid, []),
        )
        identity = outcome_map.get(mid, [])
        raw["outcomes"] = [o["outcome_label"] for o in identity]
        for field, wire in (("clob_token_id", "clobTokenIds"), ("position_id", "positionIds")):
            if identity and all(o[field] is not None for o in identity):
                raw[wire] = [o[field] for o in identity]
        if not row["usable"]:
            require(
                row["identity_error"]
                in ("CTF market lacks clobTokenIds", "outcomes must contain nonempty labels"),
                "unsupported seed identity error",
            )
            if row["identity_error"] == "CTF market lacks clobTokenIds":
                raw["outcomes"] = ["Yes", "No"]
        market_map[mid] = raw
    if kind == "markets":
        return market_map
    membership = {}
    for row in links:
        membership.setdefault(row["event_id"], []).append(row["market_id"])
    result = {}
    for row in base:
        eid = row["event_id"]
        raw = {key: row[key] for key in ("title", "slug", "active", "closed", "archived")}
        raw.update(id=eid, markets=[market_map[mid] for mid in membership.get(eid, [])])
        result[eid] = raw
    return result


def same_record(actual, expected, kind):
    # SQL storage has no ordering contract for memberships or market parents.
    def normalized(record, event):
        record = dict(record)
        if event:
            record["markets"] = sorted(
                (normalized(m, False) for m in record["markets"]), key=lambda m: m["id"]
            )
        else:
            record["events"] = sorted(record["events"], key=lambda e: e["id"])
        return record

    require(
        json.dumps(normalized(actual, kind == "events"), sort_keys=True)
        == json.dumps(normalized(expected, kind == "events"), sort_keys=True),
        "retained response differs from frozen source content",
    )


def audit_index(path):
    # The file and every line are bounded before parsing; no provider calls.
    data = regular(path, AUDIT_LIMIT)
    result = {}
    for line in data.splitlines():
        row = json.loads(line)
        require(isinstance(row, dict), "invalid source audit")
        if row.get("status") == 200:
            key = (row.get("kind"), row.get("body_sha256"), row.get("body_bytes"))
            require(key[0] in ("events", "markets"), "invalid audit kind")
            result.setdefault(key, []).append(row)
    return result


def changed_pairs(conn):
    """Unchanged receipts stay byte-equivalent; every changed pair needs raw proof."""
    conn.execute(
        "CREATE TEMP TABLE changed (kind VARCHAR,id VARCHAR,receipt TIMESTAMPTZ,response VARCHAR)"
    )
    for table, keys, pairs in (
        ("_receipts", ["kind", "venue", "id"], [("receipt", "response")]),
        (
            "_membership",
            ["venue", "event_id"],
            [("receipt", "response"), ("latest_receipt", "latest_response")],
        ),
    ):
        join = " AND ".join(f"a.{key} IS NOT DISTINCT FROM b.{key}" for key in keys)
        key = "id" if table == "_receipts" else "event_id"
        require(
            conn.execute(
                f"SELECT 1 FROM main.{table} a FULL OUTER JOIN seed.{table} b ON {join} "
                f"WHERE a.{key} IS NULL OR b.{key} IS NULL OR a.venue<>'polymarket' LIMIT 1"
            ).fetchone()
            is None,
            f"{table} entity keys changed",
        )
        if table == "_membership":
            require(
                conn.execute(
                    f"SELECT 1 FROM main.{table} a JOIN seed.{table} b ON {join} "
                    "WHERE a.unresolved IS DISTINCT FROM b.unresolved LIMIT 1"
                ).fetchone()
                is None,
                "membership resolution changed",
            )
        for receipt, response in pairs:
            changed = f"(a.{receipt} IS DISTINCT FROM b.{receipt} OR a.{response} IS DISTINCT FROM b.{response})"
            newer = (
                f"a.{receipt} IS NOT NULL AND a.{response} IS NOT NULL AND "
                f"(b.{receipt} IS NULL OR a.{receipt}>b.{receipt} OR "
                f"(a.{receipt}=b.{receipt} AND a.{response}>b.{response}))"
            )
            require(
                conn.execute(
                    f"SELECT 1 FROM main.{table} a JOIN seed.{table} b ON {join} "
                    f"WHERE {changed} AND NOT coalesce(({newer}),false) LIMIT 1"
                ).fetchone()
                is None,
                "receipt is not genuinely newer",
            )
            kind = "a.kind" if table == "_receipts" else "'membership'"
            conn.execute(
                f"INSERT INTO changed SELECT {kind},a.{key},a.{receipt},a.{response} "
                f"FROM main.{table} a JOIN seed.{table} b ON {join} WHERE {changed}"
            )
    require(
        conn.execute(
            "SELECT 1 FROM changed WHERE kind NOT IN ('event','market','membership') LIMIT 1"
        ).fetchone()
        is None,
        "unknown receipt kind",
    )


def require_atomic_application(conn, present, stamp, response):
    """Every row in a retained response must have committed at least that stamp."""
    for kind in ("event", "market", "membership"):
        ids = [entity for item_kind, entity in present if item_kind == kind]
        if not ids:
            continue
        table = "_membership" if kind == "membership" else "_receipts"
        key = "event_id" if kind == "membership" else "id"
        condition = "" if kind == "membership" else f" AND r.kind='{kind}'"
        for receipt, response_column in (
            [("receipt", "response"), ("latest_receipt", "latest_response")]
            if kind == "membership"
            else [("receipt", "response")]
        ):
            missing = conn.execute(
                f"SELECT 1 FROM (SELECT unnest(?) id) wanted LEFT JOIN {table} r "
                f"ON r.venue='polymarket' AND r.{key}=wanted.id{condition} "
                f"WHERE r.{receipt} IS NULL OR r.{response_column} IS NULL "
                f"OR r.{receipt}<? OR (r.{receipt}=? AND r.{response_column}<?) LIMIT 1",
                [ids, stamp, stamp, response],
            ).fetchone()
            require(missing is None, "response was only partially applied")


def authenticate(conn, root, audit):
    count = 0
    last = ""
    while batch := conn.execute(
        "SELECT DISTINCT response FROM changed WHERE response>? ORDER BY response LIMIT 128", [last]
    ).fetchall():
        for (response,) in batch:
            require(
                isinstance(response, str)
                and re.fullmatch("[0-9a-f]{32}", response)
                and UUID(hex=response).hex == response,
                "invalid response UUID",
            )
            directory = root / "raw" / response
            manifest = json.loads(regular(directory / "p000001.manifest.json", 1024**2))
            require(
                manifest.get("status") == 200 and manifest.get("seq") == 1,
                "changed receipt lacks successful raw response",
            )
            endpoint = manifest["request"]["endpoint"]
            require(
                isinstance(endpoint, str)
                and re.fullmatch(r"/(events|markets)(/[1-9][0-9]{0,19})?", endpoint),
                "invalid raw response endpoint",
            )
            kind = endpoint.split("/")[1]
            params = manifest["request"]["params"]
            require(isinstance(params, dict), "invalid raw request parameters")
            body = read_body(directory, manifest, trusted_root=root, max_body_bytes=16 * 1024**2)
            candidates = audit.get((kind, manifest["body_sha256"], len(body)), [])
            require(candidates, "raw response absent from source audit")
            stamp = datetime.fromisoformat(manifest["received_at"].replace("Z", "+00:00"))
            require(stamp.tzinfo is not None, "naive response timestamp")
            payload = json.loads(body)
            records = payload if isinstance(payload, list) else [payload]
            require(
                all(isinstance(r, dict) and isinstance(r.get("id"), str) for r in records),
                "malformed response records",
            )
            ids = [r["id"] for r in records]
            require(len(ids) == len(set(ids)), "duplicate response entity")
            expected = expected_records(conn, kind, ids)
            require(set(expected) == set(ids), "response contains impostor entity")
            for record in records:
                same_record(record, expected[record["id"]], kind)
            memberships = (
                {r["id"]: [m["id"] for m in r["markets"]] for r in records}
                if kind == "events"
                else {}
            )
            requested = (
                [endpoint.split("/")[2]]
                if endpoint.count("/") == 2
                else [str(value) for value in params["id"]]
                if "id" in params
                else None
            )
            require(
                any(
                    row.get("emitted_ids") == ids
                    and row.get("complete_memberships") == memberships
                    and (requested is None or row.get("ids") == requested)
                    for row in candidates
                ),
                "source audit entity/membership mismatch",
            )
            present = {("event" if kind == "events" else "market", entity) for entity in ids}
            if kind == "events":
                present.update(("membership", entity) for entity in ids)
                present.update(("market", m["id"]) for r in records for m in r["markets"])
            for receipt_kind, entity, receipt in conn.execute(
                "SELECT DISTINCT kind,id,receipt FROM changed WHERE response=?", [response]
            ).fetchall():
                require(
                    receipt == stamp and (receipt_kind, entity) in present,
                    "receipt timestamp or actual response entity mismatch",
                )
            require_atomic_application(conn, present, stamp, response)
            count += 1
        last = batch[-1][0]
    return count


def verify(source, root, audit_path, temporary=None):
    temporary = data_path(Path(temporary)) if temporary is not None else None
    source, root = safe_path(Path(source)), safe_path(Path(root))
    require(
        source != root and source not in root.parents and root not in source.parents,
        "source and target must be disjoint",
    )
    for directory in (source, root):
        path = directory / "catalogue.duckdb"
        require(
            path.is_file() and not any(p.is_symlink() for p in (path, *path.parents)),
            "database must be regular without symlinks",
        )
    identities = {p: p.stat() for p in (source / "catalogue.duckdb", root / "catalogue.duckdb")}
    pointer_path = root / "published" / "current.json"
    pointer_before = regular(pointer_path, REPORT_LIMIT)
    audit = audit_index(audit_path)
    sql_config = {"memory_limit": "2GB", "threads": "2", "max_temp_directory_size": "0B"}
    if temporary is not None:
        require(not temporary.exists(), "SQL temporary directory must be fresh")
        for directory in (source, root):
            require(
                temporary != directory
                and temporary not in directory.parents
                and directory not in temporary.parents,
                "SQL temporary directory must be disjoint",
            )
        temporary.mkdir(mode=0o700)
        sql_config.update(temp_directory=str(temporary), max_temp_directory_size="8589934592B")
    with duckdb.connect(
        str(root / "catalogue.duckdb"),
        read_only=True,
        config=sql_config,
    ) as conn:
        conn.execute("SET TimeZone='UTC'")
        escaped = str(source / "catalogue.duckdb").replace("'", "''")
        conn.execute(f"ATTACH '{escaped}' AS seed (READ_ONLY)")
        changed_pairs(conn)
        authenticated = authenticate(conn, root, audit)
        report = {
            "valid": True,
            "authenticated_responses": authenticated,
            "relations": {},
            "sql_temporary": {
                "directory": str(temporary) if temporary else None,
                "max_bytes": 8 * 1024**3 if temporary else 0,
            },
        }
        for table, schema in SCHEMAS.items():
            for prefix in ("main", "seed"):
                actual = [
                    (r[0], r[1]) for r in conn.execute(f"DESCRIBE {prefix}.{table}").fetchall()
                ]
                require(actual == list(schema), f"exact schema mismatch: {prefix}.{table}")
            fields = projection(table)
            expected = f"SELECT {fields} FROM seed.{table}"
            if table == "markets":
                expected = (
                    "SELECT "
                    + ", ".join(
                        "CASE WHEN NOT usable AND market_id IN (SELECT id FROM changed WHERE kind='market') "
                        "THEN 'native_confirmation_required' ELSE identity_error END AS identity_error"
                        if name == "identity_error"
                        else f'"{name}"'
                        for name, _ in schema
                    )
                    + " FROM seed.markets"
                )
            mismatch = conn.execute(
                f"SELECT {fields} FROM (SELECT *,1::BIGINT n FROM ({expected}) UNION ALL "
                f"SELECT *,-1::BIGINT n FROM main.{table}) GROUP BY {fields} HAVING sum(n)<>0 LIMIT 1"
            ).fetchone()
            require(mismatch is None, f"exact full-domain content mismatch: {table}")
            report["relations"][table] = {
                "source": semantic_fingerprint(conn, f"SELECT {fields} FROM seed.{table}"),
                "expected": semantic_fingerprint(conn, expected),
                "actual": semantic_fingerprint(conn, f"SELECT {fields} FROM main.{table}"),
            }
        publication = verify_current(CurrentSettings(root), connection=conn)
        require(
            publication["published_generation"] == publication["store_generation"],
            "published generation is stale",
        )
        require(
            regular(pointer_path, REPORT_LIMIT) == pointer_before,
            "publication pointer changed during verification",
        )
        report["publication"] = {
            "release_id": publication["release_id"],
            "published_generation": publication["published_generation"],
            "store_generation": publication["store_generation"],
            "pointer_sha256": hashlib.sha256(pointer_before).hexdigest(),
            "manifest_sha256": json.loads(pointer_before)["manifest_sha256"],
            "relations": publication["relations"],
        }
        report["changed_receipts"] = conn.execute("SELECT count(*) FROM changed").fetchone()[0]
        for path, before in identities.items():
            after = path.stat()
            require(
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
                "database changed during verification",
            )
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "root", "audit", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument(
        "--temporary", type=Path, help="Fresh private SQL spill directory; bounded to 8 GiB"
    )
    args = parser.parse_args()
    args.output = data_path(args.output)
    require(not args.output.exists() and not args.output.is_symlink(), "output must be fresh")
    for directory in (args.source.absolute(), args.root.absolute()):
        require(
            not args.output.absolute().is_relative_to(directory), "output must be outside databases"
        )
    report = verify(args.source, args.root, args.audit, temporary=args.temporary)
    encoded = (json.dumps(report, indent=2) + "\n").encode()
    require(len(encoded) <= REPORT_LIMIT, "report byte limit exceeded")
    with args.output.open("xb") as handle:
        handle.write(encoded)


if __name__ == "__main__":
    main()
