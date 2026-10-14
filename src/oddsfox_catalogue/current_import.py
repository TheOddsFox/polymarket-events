"""Pinned legacy seed with verified membership and required missing-market recovery."""

import hashlib
import json
import time
from array import array
from bisect import bisect_left
from pathlib import Path
from types import SimpleNamespace

from . import current_store as store
from .capture.reader import read_page_records, validate_page_identity
from .current_config import admit
from .current_contract import SCHEMAS, CurrentError, ResponseRows, projection
from .current_rows import _incomplete, entity_id, project_response
from .legacy_release import (
    JSON_LIMIT,
    MANIFEST_LIMIT,
    _descriptor,
    _read_json,
    expand_coverage_id_range,
    verify_legacy_release,
)


def _event_pages(settings, release, manifest, raw_root, deadline, supplement=None):
    inventory, _ = _read_json(release / "capture-inventory.json", release, JSON_LIMIT)
    coverage, _ = _read_json(release / "coverage.json", release, JSON_LIMIT)
    entries = {e["path"]: e for e in inventory}
    units = {u["page_id"]: u for u in coverage["units"]}
    raw_settings = SimpleNamespace(
        raw_dir=raw_root,
        capture=SimpleNamespace(max_response_bytes=settings.limits.max_response_bytes),
    )
    previous = {}
    scans = {}
    for relative in sorted(entries):
        if not relative.endswith(".manifest.json"):
            continue
        page_id = (
            Path(relative).parent.name + "." + Path(relative).name.removesuffix(".manifest.json")
        )
        if not units.get(page_id, {}).get("endpoint", "").startswith("/events"):
            continue
        if deadline is not None and time.monotonic() >= deadline:
            raise CurrentError("import deadline exhausted; transaction rolled back")
        path = raw_root / relative
        _descriptor(path, {**entries[relative], "file": path.name}, raw_root)
        page, _ = _read_json(path, raw_root, 1024**2)
        if page.get("record_key") != "events":
            continue
        scan_path = path.parent / "_scan.json"
        scan_relative = scan_path.relative_to(raw_root).as_posix()
        if scan_relative not in entries:
            raise CurrentError("event scan is not inventoried")
        if page["scan_id"] not in scans:
            _descriptor(scan_path, {**entries[scan_relative], "file": "_scan.json"}, raw_root)
            scan, _ = _read_json(scan_path, raw_root, 1024**2)
            scans[page["scan_id"]] = {
                **scan,
                "params_json": json.dumps(scan["params"]),
                "input_ids_json": json.dumps(scan.get("input_ids", [])),
            }
        scan = scans[page["scan_id"]]
        unit = units.get(page.get("page_id"))
        if unit is None:
            raise CurrentError("event page has no coverage unit")
        params = dict(unit["params"])
        actual_params = dict(page["params"])
        if "id_range" in unit:
            params["id"] = expand_coverage_id_range(unit["id_range"])
            if not isinstance(actual_params.get("id"), list):
                raise CurrentError("invalid event request ID list")
            actual_params["id"] = [entity_id(value) for value in actual_params["id"]]
        if (
            any(
                page.get(k) != unit.get(u)
                for k, u in (
                    ("batch_id", "batch_id"),
                    ("endpoint", "endpoint"),
                    ("observed_at", "received_at"),
                    ("record_count", "records"),
                )
            )
            or actual_params != params
        ):
            raise CurrentError("event page differs from verified coverage")
        gz_path = path.parent / page["file"]
        gz_relative = gz_path.relative_to(raw_root).as_posix()
        if gz_relative not in entries:
            raise CurrentError("event body is not inventoried")
        _descriptor(gz_path, {**entries[gz_relative], "file": gz_path.name}, raw_root)
        prior = previous.get(page["scan_id"])
        validate_page_identity(
            {"batch_id": page["batch_id"], "observation_date": path.parts[-4]},
            scan,
            page,
            seq=page["seq"],
            previous=prior,
        )
        records = read_page_records(
            raw_settings, path.parent, page, "events", scan=scan, previous=prior
        )
        if supplement is not None:
            records = list(records)
            supplement(records, page["observed_at"], "legacy-event:" + page["page_id"])
        previous[page["scan_id"]] = page
        yield _membership_response(
            records,
            received_at=page["observed_at"],
            response_id="legacy-event:" + page["page_id"],
        )


def _membership_response(records, *, received_at, response_id):
    """Read only authoritative membership evidence, never raw entity snapshots."""
    result = ResponseRows(received_at=received_at, response_id=response_id)
    for raw in records:
        if not isinstance(raw, dict):
            raise CurrentError("response contains a nonrecord item")
        eid = entity_id(raw.get("id"))
        children = raw.get("markets")
        members = None
        if isinstance(children, list) and not _incomplete(raw, len(children)):
            try:
                members = sorted({entity_id(child.get("id")) for child in children}, key=int)
            except (CurrentError, AttributeError):
                members = None
        if eid in result.memberships and result.memberships[eid] != members:
            raise CurrentError(f"conflicting event membership {eid}")
        result.memberships[eid] = members
    return result


def _seed_market_keys(conn):
    """Keep exact canonical keys in packed low words, grouped by their high word."""
    keys = {}
    result = conn.execute("""SELECT market_id FROM markets WHERE venue='polymarket'
        ORDER BY length(market_id),market_id""")
    while rows := result.fetchmany(8192):
        for (value,) in rows:
            number = int(entity_id(value))
            high = number >> 64
            if high not in keys:
                keys[high] = array("Q")
            keys[high].append(number & (2**64 - 1))
    return keys


def _seed_has_market(keys, value):
    number = int(entity_id(value))
    bucket = keys.get(number >> 64)
    if bucket is None:
        return False
    low = number & (2**64 - 1)
    index = bisect_left(bucket, low)
    return index < len(bucket) and bucket[index] == low


def _supplement_evidence(conn, keys, records, received_at, response_id):
    rows = []
    for raw in records:
        if not isinstance(raw, dict):
            raise CurrentError("response contains a nonrecord item")
        if not isinstance(raw.get("markets"), list):
            continue
        for child in raw["markets"]:
            try:
                mid = entity_id(child.get("id"))
            except (CurrentError, AttributeError):
                continue
            if not _seed_has_market(keys, mid):
                retained = {
                    name: child.get(name)
                    for name in (
                        "question",
                        "slug",
                        "version",
                        "conditionId",
                        "active",
                        "closed",
                        "archived",
                        "outcomes",
                        "clobTokenIds",
                        "positionIds",
                    )
                }
                retained["id"] = mid
                rows.append([mid, received_at, response_id, json.dumps(retained)])
    if rows:
        conn.execute(
            """INSERT INTO seed_supplements
            SELECT r[1],r[2]::TIMESTAMPTZ,r[3],r[4]::JSON
            FROM (SELECT unnest(from_json(?,'[["VARCHAR"]]')) AS r)""",
            [json.dumps(rows)],
        )


def _finish_supplements(conn):
    conn.execute("""CREATE TEMP TABLE seed_selected AS
        WITH required AS (
            SELECT DISTINCT unnest(from_json(members,'["VARCHAR"]')) market_id
            FROM seed_complete)
        SELECT s.* FROM seed_supplements s JOIN required USING(market_id)
        WHERE NOT EXISTS (SELECT 1 FROM markets m
            WHERE m.venue='polymarket' AND m.market_id=s.market_id)
        QUALIFY dense_rank() OVER (
            PARTITION BY market_id ORDER BY receipt DESC,response DESC)=1""")
    if conn.execute("SELECT 1 FROM seed_selected LIMIT 1").fetchone() is None:
        conn.execute("DROP TABLE seed_selected")
        conn.execute("DROP TABLE seed_supplements")
        return
    conn.execute("""CREATE TEMP TABLE seed_projected (
        market_id VARCHAR,receipt TIMESTAMPTZ,response VARCHAR,market JSON,outcomes JSON)""")
    previous = None
    while True:
        predicate = "" if previous is None else "WHERE (receipt,response)>(?::TIMESTAMPTZ,?)"
        stamp = conn.execute(
            f"SELECT receipt,response FROM seed_selected {predicate} GROUP BY ALL "
            "ORDER BY receipt,response LIMIT 1",
            [] if previous is None else list(previous),
        ).fetchone()
        if stamp is None:
            break
        # A winning response is bounded by the verified source page's response limit.
        records = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT market FROM seed_selected WHERE receipt=? AND response=?", list(stamp)
            ).fetchall()
        ]
        projected = project_response(
            records, "markets", received_at=stamp[0].isoformat(), response_id=stamp[1]
        )
        outcomes = {}
        for row in projected.outcomes:
            outcomes.setdefault(row["market_id"], []).append(row)
        rows = [
            [
                row["market_id"],
                stamp[0].isoformat(),
                stamp[1],
                json.dumps(row),
                json.dumps(outcomes.get(row["market_id"], [])),
            ]
            for row in projected.markets
        ]
        conn.execute(
            """INSERT INTO seed_projected
            SELECT r[1],r[2]::TIMESTAMPTZ,r[3],r[4]::JSON,r[5]::JSON
            FROM (SELECT unnest(from_json(?,'[["VARCHAR"]]')) AS r)""",
            [json.dumps(rows)],
        )
        previous = stamp
    market_type = json.dumps(dict(SCHEMAS["markets"]))
    outcome_type = json.dumps([dict(SCHEMAS["outcomes"])])
    conn.execute(f"""INSERT INTO markets SELECT m.* FROM (
        SELECT from_json(market, '{market_type}') m FROM seed_projected)""")
    conn.execute(f"""INSERT INTO outcomes SELECT o.* FROM (
        SELECT unnest(from_json(outcomes, '{outcome_type}')) o FROM seed_projected)""")
    conn.execute("""INSERT INTO _receipts
        SELECT 'market','polymarket',market_id,receipt,response FROM seed_projected""")
    conn.execute("""INSERT INTO _identity
        SELECT 'polymarket',market_id,
            CASE WHEN json_array_length(outcomes)>0 THEN outcomes END,
            CASE WHEN json_array_length(outcomes)>0 THEN outcomes END,
            CASE WHEN json_array_length(outcomes)=0 THEN receipt END,
            CASE WHEN json_array_length(outcomes)=0 THEN response END
        FROM seed_projected""")
    conn.execute(f"""INSERT INTO _identity_claims
        SELECT o.venue,o.market_id,role,o.asset_kind,o.asset_id FROM (
            SELECT unnest(from_json(outcomes, '{outcome_type}')) o FROM seed_projected)
        CROSS JOIN (VALUES ('candidate'),('accepted')) roles(role)""")
    conn.execute("DROP TABLE seed_selected")
    conn.execute("DROP TABLE seed_projected")
    conn.execute("DROP TABLE seed_supplements")


def _membership_evidence(conn, response):
    rows = [
        [
            eid,
            response.received_at,
            response.response_id,
            None if members is None else json.dumps(members),
        ]
        for eid, members in response.memberships.items()
    ]
    if rows:
        conn.execute(
            """INSERT INTO seed_evidence
            SELECT r[1],r[2]::TIMESTAMPTZ,r[3],r[4]::JSON
            FROM (SELECT unnest(from_json(?,'[["VARCHAR"]]')) AS r)""",
            [json.dumps(rows)],
        )


def _finish_memberships(conn, *, supplement=False):
    for name, condition in (("seed_latest", ""), ("seed_complete", "WHERE members IS NOT NULL")):
        conn.execute(f"""CREATE TEMP TABLE {name} AS SELECT * FROM seed_evidence {condition}
            QUALIFY row_number() OVER (
                PARTITION BY event_id ORDER BY receipt DESC,response DESC)=1""")
    if supplement:
        _finish_supplements(conn)
        _identity_checks(conn)
    # Unknown parents remain evidence and fail closed, including empty/unknown arrays.
    if conn.execute("""SELECT 1 FROM seed_latest l LEFT JOIN events e
        ON e.venue='polymarket' AND e.event_id=l.event_id WHERE e.event_id IS NULL LIMIT 1""").fetchone():
        raise CurrentError("membership evidence references an unknown event")
    conn.execute("""INSERT INTO _membership
        SELECT 'polymarket',l.event_id,c.receipt,c.response,l.members IS NULL,l.receipt,l.response
        FROM seed_latest l LEFT JOIN seed_complete c USING(event_id)""")
    # Finish receipt joins before the large link primary-key index becomes resident.
    conn.execute("""INSERT INTO _membership
        SELECT e.venue,e.event_id,NULL,NULL,true,r.receipt,r.response
        FROM events e JOIN _receipts r ON r.kind='event' AND r.venue=e.venue AND r.id=e.event_id
        WHERE NOT EXISTS (SELECT 1 FROM _membership m WHERE m.venue=e.venue AND m.event_id=e.event_id)""")
    conn.execute("""INSERT INTO event_markets
        SELECT 'polymarket',event_id,unnest(from_json(members,'["VARCHAR"]'))
        FROM seed_complete""")
    for name in ("seed_latest", "seed_complete", "seed_evidence"):
        conn.execute(f"DROP TABLE {name}")


def _identity(conn):
    conn.execute("DELETE FROM _identity")
    conn.execute("DELETE FROM _identity_claims")
    # DuckDB's list aggregate cannot spill; bound its groups before building JSON maps.
    bounds = conn.execute("""SELECT venue,min(market_id),max(market_id) FROM (
        SELECT venue,market_id,(row_number() OVER (PARTITION BY venue ORDER BY market_id)-1)//50000 bucket
        FROM markets) GROUP BY venue,bucket ORDER BY venue,bucket""").fetchall()
    for venue, lo, hi in bounds:
        conn.execute(
            """INSERT INTO _identity
        SELECT m.venue,m.market_id, o.identity_map, o.identity_map,
        CASE WHEN o.identity_map IS NULL THEN r.receipt END,
        CASE WHEN o.identity_map IS NULL THEN r.response END
        FROM markets m JOIN _receipts r ON r.kind='market' AND r.venue=m.venue AND r.id=m.market_id
        LEFT JOIN (SELECT venue,market_id,to_json(list(o ORDER BY outcome_index)) identity_map
                   FROM outcomes o WHERE venue=? AND market_id BETWEEN ? AND ?
                   GROUP BY venue,market_id) o ON o.venue=m.venue AND o.market_id=m.market_id
        WHERE m.venue=? AND m.market_id BETWEEN ? AND ?""",
            [venue, lo, hi, venue, lo, hi],
        )
    try:
        conn.execute("""INSERT INTO _identity_claims
        SELECT venue,market_id,role,asset_kind,asset_id FROM outcomes
        CROSS JOIN (VALUES ('candidate'),('accepted')) roles(role)""")
    except Exception as exc:
        raise CurrentError(f"seed ownership claims failed: {exc}") from exc


def _identity_checks(conn):
    conn.execute("""UPDATE markets SET usable=false,identity_error=coalesce(nullif(identity_error,''),'invalid_identity')
        WHERE NOT EXISTS (SELECT 1 FROM outcomes o WHERE o.venue=markets.venue
                           AND o.market_id=markets.market_id)""")
    conn.execute("""CREATE TEMP TABLE conflicts AS
        WITH disputed AS (SELECT venue,asset_kind,asset_id FROM _identity_claims
            GROUP BY venue,asset_kind,asset_id HAVING min(market_id)<>max(market_id))
        SELECT DISTINCT c.venue,c.market_id FROM _identity_claims c JOIN disputed
        USING(venue,asset_kind,asset_id)""")
    conn.execute(
        "UPDATE markets SET usable=false,identity_error='asset_ownership_conflict' FROM conflicts c WHERE markets.venue=c.venue AND markets.market_id=c.market_id"
    )
    conn.execute("""UPDATE _identity SET invalidated_at=r.receipt,invalidated_response=r.response
        FROM conflicts c, _receipts r WHERE _identity.venue=c.venue
        AND _identity.market_id=c.market_id AND r.kind='market' AND r.venue=c.venue AND r.id=c.market_id""")
    conn.execute(
        "DELETE FROM outcomes WHERE EXISTS (SELECT 1 FROM markets m WHERE m.venue=outcomes.venue AND m.market_id=outcomes.market_id AND NOT m.usable)"
    )
    conn.execute("DROP TABLE conflicts")


def _seal(release, manifest_sha256):
    manifest, raw = _read_json(release / "release.json", release, MANIFEST_LIMIT)
    if hashlib.sha256(raw).hexdigest() != manifest_sha256:
        raise CurrentError("seed release changed during import")
    for name, entry in manifest["tables"].items():
        _descriptor(release / f"{name}.parquet", entry, release)
    for name, key in (
        ("coverage.json", "coverage"),
        ("capture-inventory.json", "capture_inventory"),
    ):
        _descriptor(release / name, manifest[key], release, max_bytes=JSON_LIMIT)
    return manifest


def import_release(settings, conn, release_path, *, manifest_sha256, raw_root, deadline=None):
    """Commit one private seed phase; callers reopen the connection between phases."""
    release, raw_root = Path(release_path).absolute(), Path(raw_root).absolute()
    store.initialize(conn)
    previous = store.get_state(conn, "seed")
    if previous:
        if previous["manifest_sha256"] != manifest_sha256:
            raise CurrentError("store already seeded from a different release")
        return {
            **store.validate_store(conn),
            "complete": True,
            "phase": "complete",
            "seed": previous,
            "resumed": True,
        }
    source = {
        "release_path": str(release),
        "manifest_sha256": manifest_sha256,
        "raw_root": str(raw_root),
    }
    pending = store.get_state(conn, "seed_pending")
    has_pending = conn.execute("SELECT 1 FROM sync_state WHERE key='seed_pending'").fetchone()
    if has_pending and (
        not isinstance(pending, dict)
        or set(pending) != {"version", "stage", "source"}
        or type(pending["version"]) is not int
        or pending["version"] != 2
        or not isinstance(pending["stage"], str)
        or pending["stage"] not in {"baseline", "evidence", "verify"}
        or pending["source"] != source
    ):
        raise CurrentError(
            "invalid seed_pending marker or seed source mismatch; use a fresh current root"
        )
    if pending is None or pending["stage"] == "baseline":
        if store.generation(conn) != 0 or any(
            conn.execute(f"SELECT 1 FROM {t} LIMIT 1").fetchone()
            for t in (
                *SCHEMAS,
                "_receipts",
                "_identity",
                "_identity_claims",
                "_membership",
                "_parent_repairs",
            )
        ):
            raise CurrentError("seed requires a pristine current store")
        if pending is None:
            pending = {"version": 2, "stage": "baseline", "source": source}
            store.set_state(conn, "seed_pending", pending)
    elif store.generation(conn) != 0:
        raise CurrentError("pending seed requires generation zero")
    phase = pending["stage"]
    if phase == "baseline":
        manifest = verify_legacy_release(release, manifest_sha256=manifest_sha256, connection=conn)
        projected = sum(
            manifest["tables"][name]["bytes"] for name in ("events", "markets", "outcomes")
        )
        admit(settings, additional_bytes=8 * projected + 1024**2)
    else:
        manifest = _seal(release, manifest_sha256)
    conn.execute("BEGIN")
    try:
        if phase == "baseline":
            for name in ("events", "markets", "outcomes"):
                conn.execute(
                    f"INSERT INTO {name} ({projection(name)}) SELECT {projection(name)} FROM read_parquet(?)",
                    [str(release / f"{name}.parquet")],
                )
            conn.execute(
                "DELETE FROM outcomes WHERE NOT EXISTS (SELECT 1 FROM markets m WHERE m.venue=outcomes.venue AND m.market_id=outcomes.market_id AND m.usable)"
            )
            # Original response IDs are unrecoverable; this namespace sorts below event recovery on ties.
            for name, kind, key in [
                ("events", "event", "event_id"),
                ("markets", "market", "market_id"),
            ]:
                response = (
                    "'legacy-baseline:' || coalesce(observation_id,'')"
                    if name == "markets"
                    else "'legacy-baseline:event:' || event_id"
                )
                conn.execute(
                    f"INSERT INTO _receipts SELECT ?,venue,{key},last_observed_at,{response} FROM read_parquet(?)",
                    [kind, str(release / f"{name}.parquet")],
                )
            _identity(conn)
            next_stage = "evidence"
        elif phase == "evidence":
            keys = _seed_market_keys(conn)
            conn.execute("""CREATE TEMP TABLE seed_supplements (
                market_id VARCHAR,receipt TIMESTAMPTZ,response VARCHAR,market JSON)""")
            # Keep append-only staging tables: dropped per-page tables retain
            # transaction memory until commit and exhaust the bounded import.
            conn.execute("""CREATE TEMP TABLE seed_evidence (
                event_id VARCHAR,receipt TIMESTAMPTZ,response VARCHAR,members JSON)""")

            def supplement(records, receipt, response_id):
                _supplement_evidence(conn, keys, records, receipt, response_id)

            for response in _event_pages(
                settings, release, manifest, raw_root, deadline, supplement
            ):
                admit(settings, additional_bytes=8 * settings.limits.max_response_bytes)
                _membership_evidence(conn, response)
            del keys
            _finish_memberships(conn, supplement=True)
            next_stage = "verify"
        else:
            result = store.validate_store(conn, allow_seeding=True)
            coverage, _ = _read_json(release / "coverage.json", release, JSON_LIMIT)
            frontiers = {
                kind: max(
                    (b["scope"].get("high_water", {}).get(kind, 0) for b in coverage["batches"]),
                    default=0,
                )
                for kind in ("events", "markets")
            }
            supplement_hash = hashlib.sha256()
            supplement_count = 0
            supplemental = conn.execute("""SELECT id FROM _receipts
                WHERE kind='market' AND venue='polymarket' AND response LIKE 'legacy-event:%'
                ORDER BY id""")
            while rows := supplemental.fetchmany(8192):
                for (mid,) in rows:
                    supplement_hash.update((mid + "\n").encode())
                    supplement_count += 1
            seed = {
                "supplement": {
                    "policy": "verified-event-required-markets-v1",
                    "markets": supplement_count,
                    "market_ids_sha256": supplement_hash.hexdigest(),
                },
                "manifest_sha256": manifest_sha256,
                "release_id": manifest["release_id"],
                "source_catalogue_complete": False,
                "frontiers": frontiers,
                "tie_break": "legacy-baseline:<observation_id> before legacy-event:<page_id>",
            }
            store.set_state(conn, "seed", seed)
            for kind, value in frontiers.items():
                store.set_state(conn, f"{kind}_frontier", value)
            store.set_state(
                conn,
                "discovery",
                {
                    "version": 2,
                    "next_lane": 0,
                    "pending": None,
                    "lanes": {
                        lane: {"frontier": frontiers[lane], "highwater": None, "phase": "tail"}
                        for lane in ("events", "markets")
                    },
                },
            )
            store.set_state(conn, "generation", 1)
            conn.execute("DELETE FROM sync_state WHERE key='seed_pending'")
        _seal(release, manifest_sha256)
        if phase != "verify":
            store.set_state(conn, "seed_pending", {**pending, "stage": next_stage})
        conn.execute("COMMIT")
        if phase == "verify":
            return {
                **result,
                "generation": 1,
                "complete": True,
                "phase": "complete",
                "seed": seed,
                "resumed": False,
            }
        return {"complete": False, "phase": next_stage}
    except BaseException as exc:
        try:
            conn.execute("ROLLBACK")
        except Exception as rollback_error:
            exc.add_note(f"rollback also failed: {rollback_error}")
        if isinstance(exc, Exception):
            raise CurrentError(f"seed {phase} failed: {exc}") from exc
        raise
