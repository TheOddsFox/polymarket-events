"""Transactional bounded current state and conservative identity ownership."""

import json
from datetime import UTC, datetime

from .current_contract import (
    KEYS,
    PROJECTION_VERSION,
    SCHEMAS,
    STORE_CONTRACT,
    CurrentError,
    ResponseRows,
    columns,
    native_integrity_queries,
    projection,
)
from .fingerprints import semantic_fingerprint


def initialize(conn):
    existing = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    if existing:
        required = set(SCHEMAS) | {
            "sync_state",
            "_receipts",
            "_membership",
            "_identity",
            "_identity_claims",
            "_parent_repairs",
        }
        if not required.issubset(existing):
            raise CurrentError("unrecognized or incomplete current store")
        _validate_schema(conn)
        return
    conn.execute("BEGIN TRANSACTION")
    try:
        for table, schema in SCHEMAS.items():
            fields = ", ".join(
                f'"{name}" {kind}' + (" NOT NULL" if name in KEYS[table] else "")
                for name, kind in schema
            )
            key = ", ".join(KEYS[table])
            constraint = "" if table == "event_markets" else f", PRIMARY KEY ({key})"
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({fields}{constraint})")
        conn.execute("CREATE TABLE IF NOT EXISTS sync_state (key VARCHAR PRIMARY KEY, value JSON)")
        conn.execute("""CREATE TABLE IF NOT EXISTS _receipts (
            kind VARCHAR, venue VARCHAR, id VARCHAR, receipt TIMESTAMPTZ, response VARCHAR,
            PRIMARY KEY(kind,venue,id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS _membership (
            venue VARCHAR,event_id VARCHAR,receipt TIMESTAMPTZ,response VARCHAR,
            unresolved BOOLEAN, latest_receipt TIMESTAMPTZ, latest_response VARCHAR, PRIMARY KEY(venue,event_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS _identity (
            venue VARCHAR,market_id VARCHAR,candidate JSON,accepted JSON,
            invalidated_at TIMESTAMPTZ,invalidated_response VARCHAR,
            PRIMARY KEY(venue,market_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS _identity_claims (
            venue VARCHAR NOT NULL,market_id VARCHAR NOT NULL,
            role VARCHAR NOT NULL CHECK(role IN ('candidate','accepted')),
            asset_kind VARCHAR NOT NULL,asset_id VARCHAR NOT NULL)""")
        conn.execute("CREATE TABLE IF NOT EXISTS _parent_repairs (event_id VARCHAR PRIMARY KEY)")
        for key, value in [
            ("store_contract", STORE_CONTRACT),
            ("projection_version", PROJECTION_VERSION),
            ("generation", 0),
        ]:
            if get_state(conn, key) is None:
                set_state(conn, key, value)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def get_state(conn, key, default=None):
    row = conn.execute("SELECT value FROM sync_state WHERE key=?", [key]).fetchone()
    return default if row is None else json.loads(row[0])


def set_state(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO sync_state VALUES (?, ?)", [key, json.dumps(value)])


def generation(conn):
    return get_state(conn, "generation", 0)


def _identity_key(mapping):
    return [
        tuple(
            row.get(key)
            for key in (
                "outcome_index",
                "condition_id",
                "asset_kind",
                "asset_id",
                "clob_token_id",
                "position_id",
            )
        )
        for row in mapping or []
    ]


def record_attempts(conn, lane, ids, when):
    """Record scheduler attempts for ids that exist after this response.

    Window gaps are not rows. ``lane`` ``native`` records against markets.
    """
    table, key = ("events", "event_id") if lane == "events" else ("markets", "market_id")
    source = f"SELECT {key} id FROM {table}"
    if lane == "events":
        source += " UNION SELECT event_id FROM _parent_repairs"
    conn.execute(
        f"""INSERT OR REPLACE INTO _reconcile_attempts
        SELECT ?, id, ? FROM ({source}) WHERE id IN (SELECT unnest(?))""",
        [lane, when, [str(value) for value in ids]],
    )


def apply(conn, response: ResponseRows, progress=None, *, attempts=None):
    """Apply one response as a set. ``attempts`` commits in this transaction."""
    stamp = _receipt_stamp(response)
    conn.execute("BEGIN TRANSACTION")
    try:
        result = _apply_set(conn, response, stamp, progress, attempts)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return result


_WRITE_CHUNK = 1000


def _receipt_stamp(response):
    stamp = (
        datetime.fromisoformat(response.received_at.replace("Z", "+00:00")),
        response.response_id,
    )
    if stamp[0].tzinfo is None:
        raise CurrentError("receipt must include timezone")
    return (stamp[0].astimezone(UTC), stamp[1])


def _chunks(rows):
    rows = list(rows)
    for start in range(0, len(rows), _WRITE_CHUNK):
        yield rows[start : start + _WRITE_CHUNK]


def _placeholders(count, width):
    row = "(" + ",".join("?" * width) + ")"
    return ",".join(row for _ in range(count))


def _flat(rows):
    return [value for row in rows for value in row]


def _select_in(conn, query, keys, width):
    found = []
    for chunk in _chunks(keys):
        found.extend(
            conn.execute(
                query.format(values=_placeholders(len(chunk), width)), _flat(chunk)
            ).fetchall()
        )
    return found


def _write_values(conn, sql, rows, width):
    for chunk in _chunks(rows):
        conn.execute(sql.format(values=_placeholders(len(chunk), width)), _flat(chunk))


def _row_tuple(table, row):
    return tuple(row.get(name) for name in columns(table))


def _load_keyed(conn, query, keys, width, key_length):
    return {
        tuple(row[:key_length]): row[key_length:] for row in _select_in(conn, query, keys, width)
    }


def _apply_set(conn, response, stamp, progress, attempts):
    event_keys = list({(row["venue"], row["event_id"]) for row in response.events})
    # A repeated id in one response keeps the first row, matching the receipt gate.
    event_rows = {}
    for row in response.events:
        event_rows.setdefault((row["venue"], row["event_id"]), row)
    stored_events = (
        _load_keyed(
            conn,
            "SELECT s.venue, s.event_id, "
            + ", ".join(f's."{name}"' for name in columns("events"))
            + " FROM events s JOIN (VALUES {values}) k(venue, event_id) "
            "ON s.venue=k.venue AND s.event_id=k.event_id",
            event_keys,
            2,
            2,
        )
        if event_keys
        else {}
    )
    event_receipts = (
        _load_keyed(
            conn,
            "SELECT r.venue, r.id, r.receipt, r.response FROM _receipts r "
            "JOIN (VALUES {values}) k(venue, id) ON r.kind='event' AND r.venue=k.venue AND r.id=k.id",
            event_keys,
            2,
            2,
        )
        if event_keys
        else {}
    )
    membership_keys = []
    for event_id in response.memberships:
        event = next((row for row in response.events if row["event_id"] == event_id), None)
        venue = event["venue"] if event else "polymarket"
        membership_keys.append((venue, event_id))
    memberships = {
        key: {
            "receipt": row[0],
            "response": row[1],
            "unresolved": row[2],
            "latest_receipt": row[3],
            "latest_response": row[4],
        }
        for key, row in (
            _load_keyed(
                conn,
                "SELECT m.venue, m.event_id, m.receipt, m.response, m.unresolved, "
                "m.latest_receipt, m.latest_response FROM _membership m "
                "JOIN (VALUES {values}) k(venue, event_id) "
                "ON m.venue=k.venue AND m.event_id=k.event_id",
                membership_keys,
                2,
                2,
            ).items()
            if membership_keys
            else []
        )
    }
    link_rows = (
        _select_in(
            conn,
            "SELECT s.venue, s.event_id, s.market_id FROM event_markets s "
            "JOIN (VALUES {values}) k(venue, event_id) ON s.venue=k.venue AND s.event_id=k.event_id "
            "ORDER BY s.venue, s.event_id, s.market_id",
            membership_keys,
            2,
        )
        if membership_keys
        else []
    )
    links = {}
    for venue, event_id, market_id in link_rows:
        links.setdefault((venue, event_id), []).append((venue, event_id, market_id))

    changed = False
    event_writes = {}
    receipt_writes = {}
    for key, row in event_rows.items():
        before = [stored_events[key]] if key in stored_events else []
        projected = [_row_tuple("events", row)]
        old = event_receipts.get(key)
        newer = not (old and stamp <= old)
        if newer and before != projected:
            event_writes[key] = projected[0]
            stored_events[key] = projected[0]
        if newer:
            receipt_writes[("event", *key)] = ("event", *key, *stamp)
        after = [stored_events[key]] if key in stored_events else []
        changed |= before != after

    membership_writes = {}
    link_deletes = []
    link_inserts = []
    repair_deletes = []
    for event_id, members in response.memberships.items():
        event = next((row for row in response.events if row["event_id"] == event_id), None)
        venue = event["venue"] if event else "polymarket"
        key = (venue, event_id)
        old = memberships.get(key)
        old_receipt = None if old is None else (old["receipt"], old["response"])
        old_latest = None if old is None else (old["latest_receipt"], old["latest_response"])
        latest = old is None or stamp > old_latest
        if old is None:
            old = {
                "receipt": None,
                "response": None,
                "unresolved": True,
                "latest_receipt": stamp[0],
                "latest_response": stamp[1],
            }
            memberships[key] = old
            membership_writes[key] = old
        if latest:
            old["unresolved"] = members is None
            old["latest_receipt"], old["latest_response"] = stamp
            membership_writes[key] = old
        if members is None or (old_receipt and old_receipt[0] and stamp <= old_receipt):
            continue
        before = links.get(key, [])
        expected = sorted((venue, event_id, market_id) for market_id in set(members))
        if before != expected:
            link_deletes.append(key)
            link_inserts.extend(expected)
            links[key] = expected
        old["receipt"], old["response"] = stamp
        membership_writes[key] = old
        repair_deletes.append(event_id)
        changed |= before != links.get(key, [])

    market_rows = {}
    for row in response.markets:
        market_rows.setdefault((row["venue"], row["market_id"]), row)
    market_keys = list(market_rows)
    identities = {
        key: row
        for key, row in (
            _load_keyed(
                conn,
                "SELECT i.venue, i.market_id, i.candidate, i.accepted, "
                "i.invalidated_at, i.invalidated_response FROM _identity i "
                "JOIN (VALUES {values}) k(venue, market_id) "
                "ON i.venue=k.venue AND i.market_id=k.market_id",
                market_keys,
                2,
                2,
            ).items()
            if market_keys
            else []
        )
    }
    asset_keys = {(row["venue"], row["asset_kind"], row["asset_id"]) for row in response.outcomes}
    for key, old in identities.items():
        for mapping in old[:2]:
            for outcome in json.loads(mapping) if mapping else []:
                asset_keys.add((key[0], outcome["asset_kind"], outcome["asset_id"]))
    claims = {}
    matched = (
        _select_in(
            conn,
            "SELECT c.venue, c.market_id, c.role, c.asset_kind, c.asset_id FROM _identity_claims c "
            "JOIN (VALUES {values}) k(venue, asset_kind, asset_id) "
            "ON c.venue=k.venue AND c.asset_kind=k.asset_kind AND c.asset_id=k.asset_id",
            asset_keys,
            3,
        )
        if asset_keys
        else []
    )
    impacted = set(market_keys)
    for venue, market_id, role, kind, asset in matched:
        claims.setdefault((venue, kind, asset), {}).setdefault(market_id, set()).add(role)
        impacted.add((venue, market_id))
    extra_markets = [key for key in impacted if key not in market_keys and key not in identities]
    if extra_markets:
        identities.update(
            _load_keyed(
                conn,
                "SELECT i.venue, i.market_id, i.candidate, i.accepted, "
                "i.invalidated_at, i.invalidated_response FROM _identity i "
                "JOIN (VALUES {values}) k(venue, market_id) "
                "ON i.venue=k.venue AND i.market_id=k.market_id",
                extra_markets,
                2,
                2,
            )
        )
    before_markets = (
        _load_keyed(
            conn,
            "SELECT s.venue, s.market_id, "
            + ", ".join(f's."{name}"' for name in columns("markets"))
            + " FROM markets s JOIN (VALUES {values}) k(venue, market_id) "
            "ON s.venue=k.venue AND s.market_id=k.market_id",
            impacted,
            2,
            2,
        )
        if impacted
        else {}
    )
    outcome_lists = {}
    if impacted:
        for row in _select_in(
            conn,
            "SELECT "
            + ", ".join(f's."{name}"' for name in columns("outcomes"))
            + " FROM outcomes s JOIN (VALUES {values}) k(venue, market_id) "
            "ON s.venue=k.venue AND s.market_id=k.market_id",
            impacted,
            2,
        ):
            outcome_lists.setdefault(row[:2], []).append(row)
    before_outcomes = {
        key: sorted(outcome_lists.get(key, []), key=lambda item: item[:2] + (item[3],))
        for key in impacted
    }
    market_receipts = (
        _load_keyed(
            conn,
            "SELECT r.venue, r.id, r.receipt, r.response FROM _receipts r "
            "JOIN (VALUES {values}) k(venue, id) ON r.kind='market' AND r.venue=k.venue AND r.id=k.id",
            list({*market_keys, *[("polymarket", key) for key in response.absent_market_ids]}),
            2,
            2,
        )
        if market_keys or response.absent_market_ids
        else {}
    )
    stored_claims = {}
    if market_keys:
        for claim in _select_in(
            conn,
            "SELECT c.venue, c.market_id, c.role, c.asset_kind, c.asset_id FROM _identity_claims c "
            "JOIN (VALUES {values}) k(venue, market_id) "
            "ON c.venue=k.venue AND c.market_id=k.market_id",
            market_keys,
            2,
        ):
            stored_claims.setdefault(claim[:2], []).append(claim)

    market_state = {key: list(before_markets[key]) for key in before_markets}
    market_columns = columns("markets")
    outcome_state = {key: list(rows) for key, rows in before_outcomes.items()}
    identity_writes = {}
    claim_replacements = {}
    for key, row in market_rows.items():
        venue, market_id = key
        old_receipt = market_receipts.get(key)
        if old_receipt and stamp <= old_receipt:
            continue
        receipt_writes[("market", *key)] = ("market", *key, *stamp)
        market_receipts[key] = stamp
        outcomes = [
            item
            for item in response.outcomes
            if item["venue"] == venue and item["market_id"] == market_id
        ]
        candidate = (
            sorted(outcomes, key=lambda item: item["outcome_index"])
            if row.get("usable") and outcomes
            else None
        )
        old = identities.get(key)
        accepted = json.loads(old[1]) if old and old[1] else None
        invalid = (old[2], old[3]) if old and old[2] else None
        if invalid is None and (
            candidate is None
            or (accepted is not None and _identity_key(candidate) != _identity_key(accepted))
        ):
            invalid = stamp
        native = market_id in response.native_market_ids
        ownership_conflict = any(
            any(
                owner != market_id
                for owner in claims.get((venue, item["asset_kind"], item["asset_id"]), {})
            )
            for item in candidate or []
        )
        if (
            candidate
            and native
            and not ownership_conflict
            and (invalid is None or stamp >= invalid)
        ):
            accepted, invalid = candidate, None
        elif candidate and invalid is None and not ownership_conflict:
            accepted = candidate
        if candidate is None and old and old[0]:
            candidate = json.loads(old[0])
        projected = list(_row_tuple("markets", row))
        if key not in before_markets or list(before_markets[key]) != projected:
            market_state[key] = projected
        identity_values = (
            candidate if candidate else None,
            accepted if accepted else None,
            invalid[0] if invalid else None,
            invalid[1] if invalid else None,
        )
        old_values = (
            (
                json.loads(old[0]) if old[0] else None,
                json.loads(old[1]) if old[1] else None,
                old[2],
                old[3],
            )
            if old
            else None
        )
        if old_values != identity_values:
            identities[key] = (
                json.dumps(candidate) if candidate else None,
                json.dumps(accepted) if accepted else None,
                identity_values[2],
                identity_values[3],
            )
            identity_writes[key] = identities[key]
        for (claim_venue, _kind, _asset), owners in claims.items():
            if claim_venue == venue:
                owners.pop(market_id, None)
        replacement = []
        for role, mapping in [("candidate", candidate), ("accepted", accepted)]:
            for outcome in mapping or []:
                kind, asset = outcome["asset_kind"], outcome["asset_id"]
                claims.setdefault((venue, kind, asset), {}).setdefault(market_id, set()).add(role)
                replacement.append((venue, market_id, role, kind, asset))
        if sorted(stored_claims.get(key, [])) != sorted(set(replacement)):
            claim_replacements[key] = list(set(replacement))

    conflicts = {
        (venue, market_id)
        for (venue, _kind, _asset), owners in claims.items()
        if len(owners) > 1
        for market_id in owners
    }
    outcome_deletes = []
    outcome_inserts = []
    for venue, market_id in impacted:
        key = (venue, market_id)
        identity = identities.get(key)
        conflict = key in conflicts
        if conflict and identity is not None:
            invalidated = identity[2]
            if invalidated is None or (identity[2], identity[3]) < stamp:
                identity = (identity[0], identity[1], stamp[0], stamp[1])
                identities[key] = identity
                identity_writes[key] = identity
        usable = bool(identity and identity[0] and identity[1] and not identity[2] and not conflict)
        error = (
            None
            if usable
            else (
                "asset_ownership_conflict"
                if conflict
                else "native_confirmation_required"
                if identity and identity[2]
                else "invalid_identity"
            )
        )
        if key in market_state:
            usable_at = market_columns.index("usable")
            error_at = market_columns.index("identity_error")
            current = market_state[key]
            if current[usable_at] is not usable or current[error_at] != error:
                current[usable_at] = usable
                current[error_at] = error
        parsed = json.loads(identity[1]) if usable else []
        expected = sorted(
            (tuple(outcome.get(name) for name in columns("outcomes")) for outcome in parsed),
            key=lambda item: item[:2] + (item[3],),
        )
        if before_outcomes.get(key, []) != expected:
            outcome_deletes.append(key)
            outcome_inserts.extend(expected)
            outcome_state[key] = expected
        market_changed = ([tuple(before_markets[key])] if key in before_markets else []) != (
            [tuple(market_state[key])] if key in market_state else []
        )
        outcome_changed = before_outcomes.get(key, []) != outcome_state.get(key, [])
        changed |= market_changed or outcome_changed

    absent_events = sorted(response.absent_event_ids)
    existing_events = {
        event_id
        for venue, event_id in event_writes
        if venue == "polymarket" and event_id in response.absent_event_ids
    }
    if absent_events:
        existing_events.update(
            row[0]
            for row in _select_in(
                conn,
                "SELECT event_id FROM events WHERE venue='polymarket' AND event_id IN "
                "(SELECT * FROM (VALUES {values}) k(event_id))",
                [(event_id,) for event_id in absent_events],
                1,
            )
        )
    absent_memberships = {}
    if existing_events:
        absent_memberships = _load_keyed(
            conn,
            "SELECT m.event_id, m.venue, m.receipt, m.response, m.unresolved, "
            "m.latest_receipt, m.latest_response FROM _membership m "
            "JOIN (VALUES {values}) k(event_id) ON m.venue='polymarket' AND m.event_id=k.event_id",
            [(event_id,) for event_id in existing_events],
            1,
            1,
        )
    for event_id in absent_events:
        key = ("polymarket", event_id)
        if key in membership_writes and event_id in existing_events:
            row = dict(membership_writes[key])
            if row["receipt"] is not None and (row["receipt"], row["response"]) >= stamp:
                continue
            row["receipt"], row["response"] = stamp
            membership_writes[key] = row
            continue
        if event_id not in existing_events or (event_id,) not in absent_memberships:
            continue
        venue, receipt, response_id, unresolved, latest_receipt, latest_response = (
            absent_memberships[(event_id,)]
        )
        if receipt is not None and (receipt, response_id) >= stamp:
            continue
        key = (venue, event_id)
        row = membership_writes.get(key) or memberships.get(key)
        if row is None:
            row = {
                "receipt": stamp[0],
                "response": stamp[1],
                "unresolved": unresolved,
                "latest_receipt": latest_receipt,
                "latest_response": latest_response,
            }
        else:
            row = dict(row)
            row["receipt"], row["response"] = stamp
        membership_writes[key] = row

    absent_market_ids = [
        market_id
        for market_id in sorted(response.absent_market_ids)
        if ("polymarket", market_id) not in market_state
        and ("polymarket", market_id) not in before_markets
    ]
    absent_present = (
        {
            row[0]
            for row in _select_in(
                conn,
                "SELECT market_id FROM markets WHERE venue='polymarket' AND market_id IN "
                "(SELECT * FROM (VALUES {values}) k(market_id))",
                [(market_id,) for market_id in absent_market_ids],
                1,
            )
        }
        if absent_market_ids
        else set()
    )
    for market_id in sorted(response.absent_market_ids):
        key = ("polymarket", market_id)
        if (
            key not in market_state
            and key not in before_markets
            and market_id not in absent_present
        ):
            continue
        old = market_receipts.get(key)
        if not (old and stamp <= old):
            receipt_writes[("market", *key)] = ("market", *key, *stamp)

    _write_values(
        conn,
        "INSERT OR REPLACE INTO events ("
        + projection("events")
        + ") SELECT * FROM (VALUES {values}) v("
        + projection("events")
        + ")",
        event_writes.values(),
        len(columns("events")),
    )
    _write_values(
        conn,
        "INSERT OR REPLACE INTO _receipts (kind, venue, id, receipt, response) "
        "SELECT * FROM (VALUES {values}) v(kind, venue, id, receipt, response)",
        receipt_writes.values(),
        5,
    )
    _write_values(
        conn,
        "DELETE FROM event_markets USING (VALUES {values}) k(venue, event_id) "
        "WHERE event_markets.venue=k.venue AND event_markets.event_id=k.event_id",
        link_deletes,
        2,
    )
    _write_values(
        conn,
        "INSERT INTO event_markets ("
        + projection("event_markets")
        + ") SELECT * FROM (VALUES {values}) v("
        + projection("event_markets")
        + ")",
        link_inserts,
        3,
    )
    # Membership receipt lands after the link replacement so a failure there rolls it back.
    _write_values(
        conn,
        "INSERT OR REPLACE INTO _membership "
        "(venue, event_id, receipt, response, unresolved, latest_receipt, latest_response) "
        "SELECT * FROM (VALUES {values}) v("
        "venue, event_id, receipt, response, unresolved, latest_receipt, latest_response)",
        [
            (
                venue,
                event_id,
                row["receipt"],
                row["response"],
                row["unresolved"],
                row["latest_receipt"],
                row["latest_response"],
            )
            for (venue, event_id), row in membership_writes.items()
        ],
        7,
    )
    _write_values(
        conn,
        "DELETE FROM _parent_repairs USING (VALUES {values}) k(event_id) "
        "WHERE _parent_repairs.event_id=k.event_id",
        [(event_id,) for event_id in repair_deletes],
        1,
    )
    changed_markets = [
        market_state[key]
        for key in market_state
        if key not in before_markets or list(before_markets[key]) != list(market_state[key])
    ]
    _write_values(
        conn,
        "INSERT OR REPLACE INTO markets ("
        + projection("markets")
        + ") SELECT * FROM (VALUES {values}) v("
        + projection("markets")
        + ")",
        changed_markets,
        len(columns("markets")),
    )
    _write_values(
        conn,
        "INSERT OR REPLACE INTO _identity "
        "(venue, market_id, candidate, accepted, invalidated_at, invalidated_response) "
        "SELECT * FROM (VALUES {values}) v("
        "venue, market_id, candidate, accepted, invalidated_at, invalidated_response)",
        [(*key, *row) for key, row in identity_writes.items()],
        6,
    )
    _write_values(
        conn,
        "DELETE FROM _identity_claims USING (VALUES {values}) k(venue, market_id) "
        "WHERE _identity_claims.venue=k.venue AND _identity_claims.market_id=k.market_id",
        list(claim_replacements),
        2,
    )
    _write_values(
        conn,
        "INSERT INTO _identity_claims (venue, market_id, role, asset_kind, asset_id) "
        "SELECT * FROM (VALUES {values}) v(venue, market_id, role, asset_kind, asset_id)",
        [row for rows in claim_replacements.values() for row in rows],
        5,
    )
    _write_values(
        conn,
        "DELETE FROM outcomes USING (VALUES {values}) k(venue, market_id) "
        "WHERE outcomes.venue=k.venue AND outcomes.market_id=k.market_id",
        outcome_deletes,
        2,
    )
    _write_values(
        conn,
        "INSERT INTO outcomes ("
        + projection("outcomes")
        + ") SELECT * FROM (VALUES {values}) v("
        + projection("outcomes")
        + ")",
        outcome_inserts,
        len(columns("outcomes")),
    )
    for event_id in response.parent_event_ids:
        conn.execute("INSERT OR IGNORE INTO _parent_repairs VALUES (?)", [event_id])
    if attempts:
        record_attempts(conn, *attempts)
    for key, value in (progress or {}).items():
        set_state(conn, key, value)
    if changed:
        set_state(conn, "generation", generation(conn) + 1)
    return {
        "generation": generation(conn),
        "changed": bool(changed),
        "events": len(response.events),
        "markets": len(response.markets),
    }


def _validate_schema(conn):
    if (
        get_state(conn, "store_contract") != STORE_CONTRACT
        or get_state(conn, "projection_version") != PROJECTION_VERSION
    ):
        raise CurrentError("incompatible current store")
    if type(generation(conn)) is not int or generation(conn) < 0:
        raise CurrentError("invalid generation")
    for table, schema in SCHEMAS.items():
        actual = {r[0]: r[1] for r in conn.execute(f"DESCRIBE {table}").fetchall()}
        if any(actual.get(name) != kind for name, kind in schema):
            raise CurrentError(f"invalid {table} types")


def validate_public(conn, sources):
    for table, source in sources.items():
        keys = ", ".join(KEYS[table])
        nulls = " OR ".join(f'"{key}" IS NULL' for key in KEYS[table])
        if conn.execute(f"SELECT 1 FROM {source} WHERE {nulls} LIMIT 1").fetchone():
            raise CurrentError(f"null {table} key")
        if conn.execute(
            f"SELECT 1 FROM {source} GROUP BY {keys} HAVING count(*)>1 LIMIT 1"
        ).fetchone():
            raise CurrentError(f"duplicate {table} key")
    events, markets, links, outcomes = (sources[t] for t in SCHEMAS)
    checks = native_integrity_queries(events, markets, outcomes) + [
        f"SELECT 1 FROM {links} l LEFT JOIN {events} e USING(venue,event_id) LEFT JOIN {markets} m USING(venue,market_id) WHERE e.event_id IS NULL OR m.market_id IS NULL",
        f"SELECT 1 FROM {outcomes} o LEFT JOIN {markets} m USING(venue,market_id) WHERE m.market_id IS NULL OR m.usable IS DISTINCT FROM true",
        f"SELECT 1 FROM {outcomes} WHERE asset_kind IS NULL OR asset_id IS NULL",
    ]
    for query in checks:
        if conn.execute(query + " LIMIT 1").fetchone():
            raise CurrentError("release relational integrity violation")


def validate_store(conn):
    return _validate_store(conn)[0]


def _validate_store(conn):
    """Return validation and freshly compared source evidence, never accept a proof."""
    _validate_schema(conn)
    validate_public(conn, {table: table for table in SCHEMAS})
    for query in [
        "SELECT 1 FROM _identity_claims GROUP BY venue,market_id,asset_kind,asset_id "
        "HAVING count(*) FILTER (WHERE role='candidate')>1 OR count(*) FILTER (WHERE role='accepted')>1",
        "SELECT 1 FROM markets m LEFT JOIN (SELECT venue,market_id, "
        "accepted IS NULL AS missing,invalidated_at IS NOT NULL AS invalid "
        "FROM _identity) i USING(venue,market_id) "
        "WHERE m.usable AND (i.market_id IS NULL OR i.missing OR i.invalid)",
        "SELECT 1 FROM outcomes o ANTI JOIN _identity_claims c ON c.venue=o.venue AND c.market_id=o.market_id AND c.role='accepted' AND c.asset_kind=o.asset_kind AND c.asset_id=o.asset_id",
        "WITH disputed AS (SELECT venue,asset_kind,asset_id FROM _identity_claims "
        "GROUP BY venue,asset_kind,asset_id HAVING min(market_id)<>max(market_id)) "
        "SELECT 1 FROM _identity_claims a JOIN disputed USING(venue,asset_kind,asset_id) "
        "JOIN markets m ON m.venue=a.venue AND m.market_id=a.market_id WHERE m.usable",
    ]:
        if conn.execute(query + " LIMIT 1").fetchone():
            raise CurrentError("current store integrity violation")
    structure = json.dumps([{name: kind for name, kind in SCHEMAS["outcomes"]}])
    mapped = (
        " FROM (SELECT i.venue AS owner_venue,i.market_id AS owner_market_id, "
        + f"unnest(from_json(i.accepted, '{structure}')) AS mapping "
        + "FROM _identity i JOIN markets m USING(venue,market_id) WHERE m.usable) maps"
    )
    accepted = (
        "SELECT "
        + "CASE WHEN owner_venue IS NOT DISTINCT FROM mapping.venue "
        + "AND owner_market_id IS NOT DISTINCT FROM mapping.market_id "
        + "THEN mapping.venue ELSE NULL END AS venue, "
        + ", ".join(f'mapping."{name}" AS "{name}"' for name in columns("outcomes")[1:])
        + mapped
    )
    accepted_fingerprint = semantic_fingerprint(conn, accepted)
    source_fingerprint = semantic_fingerprint(
        conn, f"SELECT {projection('outcomes')} FROM outcomes"
    )
    if accepted_fingerprint != source_fingerprint:
        raise CurrentError("current outcomes differ from accepted native mappings")
    return {"valid": True, "generation": generation(conn)}, source_fingerprint


def summary(conn, now=None, repair_limit=100):
    if not isinstance(repair_limit, int) or isinstance(repair_limit, bool) or repair_limit < 0:
        raise CurrentError("repair_limit must be a nonnegative integer")
    return {
        "generation": generation(conn),
        "counts": {
            table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in SCHEMAS
        },
        "identity_repair_count": conn.execute(
            "SELECT count(DISTINCT market_id) FROM markets WHERE NOT usable"
        ).fetchone()[0],
        "parent_event_repair_count": conn.execute(
            "SELECT count(*) FROM _parent_repairs"
        ).fetchone()[0],
        "identity_repairs": [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT market_id FROM markets WHERE NOT usable ORDER BY market_id LIMIT ?",
                [repair_limit],
            ).fetchall()
        ],
        "parent_event_repairs": [
            r[0]
            for r in conn.execute(
                "SELECT event_id FROM _parent_repairs ORDER BY event_id LIMIT ?", [repair_limit]
            ).fetchall()
        ],
        "unresolved_memberships": conn.execute(
            "SELECT count(*) FROM _membership WHERE unresolved"
        ).fetchone()[0],
    }
