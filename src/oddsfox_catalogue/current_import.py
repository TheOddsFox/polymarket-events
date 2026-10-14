"""Pinned legacy seed and independently verified event-only membership recovery."""

import hashlib
import json
import time
from pathlib import Path
from types import SimpleNamespace

from . import current_store as store
from .capture.reader import read_page_records, validate_page_identity
from .current_config import admit
from .current_contract import SCHEMAS, CurrentError, columns, projection
from .current_rows import project_response
from .legacy_release import (
    JSON_LIMIT,
    MANIFEST_LIMIT,
    _descriptor,
    _read_json,
    expand_coverage_id_range,
    verify_legacy_release,
)


def _stage(conn, name, schema, rows):
    conn.execute(f"CREATE TEMP TABLE {name} ({schema})")
    if rows:
        conn.executemany(f"INSERT INTO {name} VALUES ({','.join('?' for _ in rows[0])})", rows)


def _event_pages(settings, release, manifest, raw_root, deadline):
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
        if "id_range" in unit:
            params["id"] = [int(v) for v in expand_coverage_id_range(unit["id_range"])]
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
            or page["params"] != params
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
        previous[page["scan_id"]] = page
        yield project_response(
            records,
            "events",
            received_at=page["observed_at"],
            response_id="legacy-event:" + page["page_id"],
        )


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
    conn.execute("""UPDATE markets SET usable=false,identity_error='invalid_identity'
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


def import_release(settings, conn, release_path, *, manifest_sha256, raw_root, deadline=None):
    """All-or-nothing seed; a repeated completed import verifies its pinned identity."""
    release, raw_root = Path(release_path).absolute(), Path(raw_root).absolute()
    store.initialize(conn)
    previous = store.get_state(conn, "seed")
    if previous:
        if previous["manifest_sha256"] != manifest_sha256:
            raise CurrentError("store already seeded from a different release")
        return {**store.validate_store(conn), "seed": previous, "resumed": True}
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
    manifest = verify_legacy_release(release, manifest_sha256=manifest_sha256, connection=conn)
    projected = sum(manifest["tables"][name]["bytes"] for name in ("events", "markets", "outcomes"))
    admit(settings, additional_bytes=8 * projected + 1024**2)
    conn.execute("BEGIN")
    phase = "public rows"
    try:
        for name in ("events", "markets", "outcomes"):
            phase = f"{name} rows"
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
        phase = "identity state"
        _identity(conn)
        phase = "event evidence"
        for response in _event_pages(settings, release, manifest, raw_root, deadline):
            admit(settings, additional_bytes=8 * settings.limits.max_response_bytes)
            _overlay(conn, response)
        conn.execute("""INSERT INTO _membership
            SELECT e.venue,e.event_id,NULL,NULL,true,r.receipt,r.response
            FROM events e JOIN _receipts r ON r.kind='event' AND r.venue=e.venue AND r.id=e.event_id
            WHERE NOT EXISTS (SELECT 1 FROM _membership m WHERE m.venue=e.venue AND m.event_id=e.event_id)""")
        coverage, _ = _read_json(release / "coverage.json", release, JSON_LIMIT)
        frontiers = {
            kind: max(
                (b["scope"].get("high_water", {}).get(kind, 0) for b in coverage["batches"]),
                default=0,
            )
            for kind in ("events", "markets")
        }
        seed = {
            "manifest_sha256": manifest_sha256,
            "release_id": manifest["release_id"],
            "source_catalogue_complete": False,
            "frontiers": frontiers,
            "tie_break": "legacy-baseline:<observation_id> before legacy-event:<page_id>",
        }
        store.set_state(conn, "seed", seed)
        for kind, value in frontiers.items():
            store.set_state(conn, f"{kind}_frontier", value)
        store.set_state(conn, "generation", 1)
        result = store.validate_store(conn)
        # Bind consumed source bytes to the immutable release, rather than trusting stats.
        _, raw_manifest = _read_json(release / "release.json", release, MANIFEST_LIMIT)
        if hashlib.sha256(raw_manifest).hexdigest() != manifest_sha256:
            raise CurrentError("seed release changed during import")
        for name, entry in manifest["tables"].items():
            _descriptor(release / f"{name}.parquet", entry, release)
        for name, key in (
            ("coverage.json", "coverage"),
            ("capture-inventory.json", "capture_inventory"),
        ):
            _descriptor(release / name, manifest[key], release, max_bytes=JSON_LIMIT)
        conn.execute("COMMIT")
        return {**result, "seed": seed, "resumed": False}
    except BaseException as exc:
        conn.execute("ROLLBACK")
        if isinstance(exc, Exception):
            raise CurrentError(f"seed {phase} failed: {exc}") from exc
        raise


def _overlay_identity(conn, stamp):
    """Advance bounded incoming maps without releasing accepted ownership."""
    conn.execute("""CREATE TEMP TABLE identity_update AS
        SELECT n.venue,n.market_id,
        CASE WHEN n.usable THEN o.candidate END incoming,
        i.candidate previous_candidate,i.accepted,
        i.invalidated_at,i.invalidated_response
        FROM newer n LEFT JOIN _identity i USING(venue,market_id)
        LEFT JOIN (SELECT venue,market_id,to_json(list(o ORDER BY outcome_index)) candidate
                   FROM incoming_outcomes o GROUP BY venue,market_id) o USING(venue,market_id)""")
    # Labels may change while the asset map stays fixed.
    key = """to_json(list(struct_pack(
        outcome_index:=json_extract(value,'$.outcome_index'),
        condition_id:=json_extract(value,'$.condition_id'),
        asset_kind:=json_extract(value,'$.asset_kind'),
        asset_id:=json_extract(value,'$.asset_id'),
        clob_token_id:=json_extract(value,'$.clob_token_id'),
        position_id:=json_extract(value,'$.position_id')) ORDER BY json_extract(value,'$.outcome_index')::INTEGER))"""
    conn.execute(
        f"""UPDATE identity_update SET invalidated_at=?::TIMESTAMPTZ,invalidated_response=?
        WHERE invalidated_at IS NULL AND (incoming IS NULL OR
        (accepted IS NOT NULL AND (SELECT {key} FROM json_each(incoming))
                                 IS DISTINCT FROM (SELECT {key} FROM json_each(accepted))))""",
        stamp,
    )
    conn.execute("""UPDATE identity_update SET accepted=incoming
        WHERE incoming IS NOT NULL AND invalidated_at IS NULL AND NOT EXISTS (
        SELECT 1 FROM incoming_outcomes o JOIN _identity_claims c
        ON c.venue=o.venue AND c.asset_kind=o.asset_kind AND c.asset_id=o.asset_id
        AND c.market_id<>o.market_id
        WHERE o.venue=identity_update.venue AND o.market_id=identity_update.market_id)""")
    conn.execute("""INSERT OR REPLACE INTO _identity
        SELECT venue,market_id,coalesce(incoming,previous_candidate),accepted,
        invalidated_at,invalidated_response FROM identity_update""")
    conn.execute("""CREATE TEMP TABLE impacted AS
        SELECT venue,market_id FROM newer UNION
        SELECT DISTINCT c.venue,c.market_id FROM _identity_claims c JOIN _identity_claims old
        ON old.venue=c.venue AND old.asset_kind=c.asset_kind AND old.asset_id=c.asset_id
        JOIN newer n ON n.venue=old.venue AND n.market_id=old.market_id""")
    conn.execute("""DELETE FROM _identity_claims WHERE EXISTS (
        SELECT 1 FROM newer n WHERE n.venue=_identity_claims.venue
        AND n.market_id=_identity_claims.market_id)""")
    for role in ("candidate", "accepted"):
        conn.execute(
            f"""INSERT INTO _identity_claims
            SELECT i.venue,i.market_id,?,json_extract_string(o.value,'$.asset_kind'),
            json_extract_string(o.value,'$.asset_id') FROM _identity i JOIN newer n USING(venue,market_id),
            LATERAL json_each(i.{role}) o""",
            [role],
        )
    conn.execute("""INSERT INTO impacted SELECT DISTINCT c.venue,c.market_id
        FROM _identity_claims c JOIN _identity_claims fresh
        ON fresh.venue=c.venue AND fresh.asset_kind=c.asset_kind AND fresh.asset_id=c.asset_id
        JOIN newer n ON n.venue=fresh.venue AND n.market_id=fresh.market_id
        WHERE NOT EXISTS (SELECT 1 FROM impacted p WHERE p.venue=c.venue AND p.market_id=c.market_id)""")
    conn.execute("INSERT OR REPLACE INTO markets SELECT * FROM newer")
    conn.execute("""CREATE TEMP TABLE conflicted AS SELECT DISTINCT a.venue,a.market_id
        FROM _identity_claims a JOIN _identity_claims b
        ON a.venue=b.venue AND a.asset_kind=b.asset_kind AND a.asset_id=b.asset_id
        AND a.market_id<>b.market_id JOIN impacted p ON p.venue=a.venue AND p.market_id=a.market_id""")
    conn.execute(
        """UPDATE _identity SET invalidated_at=?::TIMESTAMPTZ,invalidated_response=?
        WHERE EXISTS (SELECT 1 FROM conflicted c WHERE c.venue=_identity.venue AND c.market_id=_identity.market_id)
        AND (invalidated_at IS NULL OR (invalidated_at,invalidated_response)<(?::TIMESTAMPTZ,?))""",
        stamp + stamp,
    )
    conn.execute("""UPDATE markets SET usable=(i.candidate IS NOT NULL AND i.accepted IS NOT NULL
        AND i.invalidated_at IS NULL),identity_error=CASE WHEN c.market_id IS NOT NULL THEN 'asset_ownership_conflict'
        WHEN i.invalidated_at IS NOT NULL THEN 'native_confirmation_required'
        WHEN i.candidate IS NULL OR i.accepted IS NULL THEN 'invalid_identity' ELSE NULL END
        FROM _identity i JOIN impacted p USING(venue,market_id)
        LEFT JOIN conflicted c USING(venue,market_id)
        WHERE markets.venue=i.venue AND markets.market_id=i.market_id""")
    conn.execute(
        "DELETE FROM outcomes WHERE EXISTS (SELECT 1 FROM impacted p WHERE p.venue=outcomes.venue AND p.market_id=outcomes.market_id)"
    )
    fields = ",".join(f"json_extract_string(o.value,'$.{n}')::{t}" for n, t in SCHEMAS["outcomes"])
    conn.execute(f"""INSERT INTO outcomes SELECT {fields} FROM _identity i JOIN markets m USING(venue,market_id)
        JOIN impacted p USING(venue,market_id),LATERAL json_each(i.accepted) o WHERE m.usable""")
    for name in ("identity_update", "impacted", "conflicted"):
        conn.execute(f"DROP TABLE {name}")


def _overlay(conn, response):
    stamp = [response.received_at, response.response_id]
    for table, kind, key in [("events", "event", "event_id"), ("markets", "market", "market_id")]:
        rows = getattr(response, table)
        schema = ",".join(f'"{n}" {t}' for n, t in SCHEMAS[table])
        _stage(conn, "incoming", schema, [[r.get(n) for n in columns(table)] for r in rows])
        conn.execute(
            f"""CREATE TEMP TABLE newer AS SELECT i.* FROM incoming i
            LEFT JOIN _receipts r ON r.kind=? AND r.venue=i.venue AND r.id=i.{key}
            WHERE r.id IS NULL OR (r.receipt,r.response)<(?::TIMESTAMPTZ,?)""",
            [kind, *stamp],
        )
        if conn.execute("SELECT 1 FROM newer LIMIT 1").fetchone() is None:
            conn.execute("DROP TABLE newer")
            conn.execute("DROP TABLE incoming")
            continue
        if table == "markets":
            # Embedded identity changes require native confirmation; metadata still advances.
            _stage(
                conn,
                "incoming_outcomes",
                ",".join(f'"{n}" {t}' for n, t in SCHEMAS["outcomes"]),
                [[r.get(n) for n in columns("outcomes")] for r in response.outcomes],
            )
            _overlay_identity(conn, stamp)
            conn.execute("DROP TABLE incoming_outcomes")
        if table != "markets":
            conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM newer")
        conn.execute(
            f"INSERT OR REPLACE INTO _receipts SELECT ?,venue,{key},?::TIMESTAMPTZ,? FROM newer",
            [kind, *stamp],
        )
        conn.execute("DROP TABLE newer")
        conn.execute("DROP TABLE incoming")
    memberships = [
        ["polymarket", eid, members is None] for eid, members in response.memberships.items()
    ]
    _stage(
        conn,
        "incoming_membership",
        "venue VARCHAR,event_id VARCHAR,unresolved BOOLEAN",
        memberships,
    )
    conn.execute(
        """CREATE TEMP TABLE latest_membership AS SELECT i.*,
        (m.event_id IS NULL OR (m.latest_receipt,m.latest_response)<(?::TIMESTAMPTZ,?)) is_latest,
        (NOT i.unresolved AND (m.receipt IS NULL OR (m.receipt,m.response)<(?::TIMESTAMPTZ,?))) is_complete
        FROM incoming_membership i LEFT JOIN _membership m USING(venue,event_id)""",
        stamp + stamp,
    )
    conn.execute("""DELETE FROM event_markets WHERE EXISTS (SELECT 1 FROM latest_membership m
        WHERE m.venue=event_markets.venue AND m.event_id=event_markets.event_id AND m.is_complete)""")
    links = [
        ["polymarket", eid, mid]
        for eid, members in response.memberships.items()
        for mid in members or []
    ]
    _stage(conn, "incoming_links", "venue VARCHAR,event_id VARCHAR,market_id VARCHAR", links)
    conn.execute(
        "INSERT OR IGNORE INTO event_markets SELECT l.* FROM incoming_links l JOIN latest_membership m USING(venue,event_id) WHERE m.is_complete"
    )
    conn.execute(
        """INSERT OR REPLACE INTO _membership
        SELECT l.venue,l.event_id,CASE WHEN l.is_complete THEN ?::TIMESTAMPTZ ELSE m.receipt END,
        CASE WHEN l.is_complete THEN ? ELSE m.response END,
        CASE WHEN l.is_latest THEN l.unresolved ELSE m.unresolved END,
        CASE WHEN l.is_latest THEN ?::TIMESTAMPTZ ELSE m.latest_receipt END,
        CASE WHEN l.is_latest THEN ? ELSE m.latest_response END
        FROM latest_membership l LEFT JOIN _membership m USING(venue,event_id)""",
        stamp + stamp,
    )
    for name in ("incoming_links", "latest_membership", "incoming_membership"):
        conn.execute(f"DROP TABLE {name}")
