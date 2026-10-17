"""Frozen row-at-a-time apply, the oracle for the set-based writer.

Copied from current_store before the set-based rewrite. Do not import production
apply; this module is the behavior the rewrite must match.
"""

import json
from datetime import UTC, datetime

from oddsfox_catalogue.current_contract import (
    KEYS,
    CurrentError,
    ResponseRows,
    columns,
    projection,
)


def get_state(conn, key, default=None):
    row = conn.execute("SELECT value FROM sync_state WHERE key=?", [key]).fetchone()
    return default if row is None else json.loads(row[0])


def set_state(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO sync_state VALUES (?, ?)", [key, json.dumps(value)])


def generation(conn):
    return get_state(conn, "generation", 0)


def _put(conn, table, row):
    names = columns(table)
    insert = "INSERT" if table == "event_markets" else "INSERT OR REPLACE"
    conn.execute(
        f"{insert} INTO {table} ({projection(table)}) VALUES ({', '.join('?' for _ in names)})",
        [row.get(k) for k in names],
    )


def _newer(conn, kind, venue, key, stamp):
    old = conn.execute(
        "SELECT receipt,response FROM _receipts WHERE kind=? AND venue=? AND id=?",
        [kind, venue, key],
    ).fetchone()
    if old and stamp <= tuple(old):
        return False
    if old:
        conn.execute(
            "UPDATE _receipts SET receipt=?,response=? WHERE kind=? AND venue=? AND id=?",
            [*stamp, kind, venue, key],
        )
    else:
        conn.execute("INSERT INTO _receipts VALUES (?,?,?,?,?)", [kind, venue, key, *stamp])
    return True


def _snapshot(conn, table, predicate, params):
    return conn.execute(
        f"SELECT {projection(table)} FROM {table} WHERE {predicate} ORDER BY {', '.join(KEYS[table])}",
        params,
    ).fetchall()


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


def reference_apply(conn, response: ResponseRows, progress=None):
    stamp = (
        datetime.fromisoformat(response.received_at.replace("Z", "+00:00")),
        response.response_id,
    )
    if stamp[0].tzinfo is None:
        raise CurrentError("receipt must include timezone")
    stamp = (stamp[0].astimezone(UTC), stamp[1])
    conn.execute("BEGIN TRANSACTION")
    try:
        changed = False
        for row in response.events:
            venue, key = row["venue"], row["event_id"]
            before = _snapshot(conn, "events", "venue=? AND event_id=?", [venue, key])
            if _newer(conn, "event", venue, key, stamp) and before != [
                tuple(row.get(name) for name in columns("events"))
            ]:
                _put(conn, "events", row)
            changed |= before != _snapshot(conn, "events", "venue=? AND event_id=?", [venue, key])
        for event_id, members in response.memberships.items():
            event = next((e for e in response.events if e["event_id"] == event_id), None)
            venue = event["venue"] if event else "polymarket"
            old = conn.execute(
                "SELECT receipt,response,latest_receipt,latest_response FROM _membership WHERE venue=? AND event_id=?",
                [venue, event_id],
            ).fetchone()
            latest = old is None or stamp > tuple(old[2:])
            if old is None:
                conn.execute(
                    "INSERT INTO _membership VALUES (?,?,NULL,NULL,true,?,?)",
                    [venue, event_id, *stamp],
                )
            if latest:
                conn.execute(
                    "UPDATE _membership SET unresolved=?,latest_receipt=?,latest_response=? WHERE venue=? AND event_id=?",
                    [members is None, *stamp, venue, event_id],
                )
            if members is None or (old and old[0] and stamp <= tuple(old[:2])):
                continue
            before = _snapshot(conn, "event_markets", "venue=? AND event_id=?", [venue, event_id])
            expected_links = sorted((venue, event_id, market_id) for market_id in set(members))
            if before != expected_links:
                conn.execute(
                    "DELETE FROM event_markets WHERE venue=? AND event_id=?", [venue, event_id]
                )
                for market_id in set(members):
                    _put(
                        conn,
                        "event_markets",
                        dict(venue=venue, event_id=event_id, market_id=market_id),
                    )
            conn.execute(
                "UPDATE _membership SET receipt=?,response=? WHERE venue=? AND event_id=?",
                [*stamp, venue, event_id],
            )
            conn.execute("DELETE FROM _parent_repairs WHERE event_id=?", [event_id])
            changed |= before != _snapshot(
                conn, "event_markets", "venue=? AND event_id=?", [venue, event_id]
            )
        impacted = set()
        before_markets = {}
        before_outcomes = {}

        def remember(venue, market_id):
            key = (venue, market_id)
            if key not in impacted:
                impacted.add(key)
                before_markets[key] = _snapshot(conn, "markets", "venue=? AND market_id=?", key)
                before_outcomes[key] = _snapshot(conn, "outcomes", "venue=? AND market_id=?", key)

        old_identities = {}
        asset_keys = {(o["venue"], o["asset_kind"], o["asset_id"]) for o in response.outcomes}
        for row in response.markets:
            key = (row["venue"], row["market_id"])
            old = conn.execute(
                "SELECT candidate,accepted,invalidated_at,invalidated_response FROM _identity WHERE venue=? AND market_id=?",
                key,
            ).fetchone()
            old_identities[key] = old
            for mapping in old[:2] if old else []:
                for outcome in json.loads(mapping) if mapping else []:
                    asset_keys.add((key[0], outcome["asset_kind"], outcome["asset_id"]))
        claims = {}
        if asset_keys:
            values = ",".join("(?,?,?)" for _ in asset_keys)
            matched = conn.execute(
                "SELECT c.venue,c.market_id,c.role,c.asset_kind,c.asset_id FROM _identity_claims c "
                f"JOIN (VALUES {values}) k(venue,asset_kind,asset_id) "
                "ON c.venue=k.venue AND c.asset_kind=k.asset_kind AND c.asset_id=k.asset_id",
                [value for key in asset_keys for value in key],
            ).fetchall()
            for venue, market_id, role, kind, asset in matched:
                claims.setdefault((venue, kind, asset), {}).setdefault(market_id, set()).add(role)
                remember(venue, market_id)
        replacements = {}
        for row in response.markets:
            venue, market_id = row["venue"], row["market_id"]
            remember(venue, market_id)
            if not _newer(conn, "market", venue, market_id, stamp):
                continue
            outcomes = [
                o for o in response.outcomes if o["venue"] == venue and o["market_id"] == market_id
            ]
            candidate = (
                sorted(outcomes, key=lambda o: o["outcome_index"])
                if row.get("usable") and outcomes
                else None
            )
            old = old_identities[(venue, market_id)]
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
                    for owner in claims.get((venue, o["asset_kind"], o["asset_id"]), {})
                )
                for o in candidate or []
            )
            if (
                candidate
                and native
                and not ownership_conflict
                and (invalid is None or stamp >= invalid)
            ):
                # A native response can resolve its own map change, but not older invalidation.
                accepted, invalid = candidate, None
            elif candidate and invalid is None and not ownership_conflict:
                accepted = candidate
            if candidate is None and old and old[0]:
                candidate = json.loads(old[0])
            if before_markets[(venue, market_id)] != [
                tuple(row.get(name) for name in columns("markets"))
            ]:
                _put(conn, "markets", row)
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
                conn.execute(
                    "INSERT OR REPLACE INTO _identity VALUES (?,?,?,?,?,?)",
                    [
                        venue,
                        market_id,
                        json.dumps(candidate) if candidate else None,
                        json.dumps(accepted) if accepted else None,
                        identity_values[2],
                        identity_values[3],
                    ],
                )
            for (claim_venue, _kind, _asset), owners in claims.items():
                if claim_venue == venue:
                    owners.pop(market_id, None)
            replacement = []
            for role, mapping in [("candidate", candidate), ("accepted", accepted)]:
                for outcome in mapping or []:
                    kind, asset = outcome["asset_kind"], outcome["asset_id"]
                    claims.setdefault((venue, kind, asset), {}).setdefault(market_id, set()).add(
                        role
                    )
                    replacement.append((venue, market_id, role, kind, asset))
            replacements[(venue, market_id)] = replacement
        if replacements:
            values = ",".join("(?,?)" for _ in replacements)
            stored_claims = {}
            for claim in conn.execute(
                "SELECT c.venue,c.market_id,c.role,c.asset_kind,c.asset_id FROM _identity_claims c "
                f"JOIN (VALUES {values}) k(venue,market_id) "
                "ON c.venue=k.venue AND c.market_id=k.market_id",
                [value for key in replacements for value in key],
            ).fetchall():
                stored_claims.setdefault(claim[:2], []).append(claim)
            replacements = {
                key: replacement
                for key, replacement in replacements.items()
                if sorted(stored_claims.get(key, [])) != sorted(set(replacement))
            }
        if replacements:
            values = ",".join("(?,?)" for _ in replacements)
            conn.execute(
                "DELETE FROM _identity_claims USING "
                f"(VALUES {values}) k(venue,market_id) WHERE "
                "_identity_claims.venue=k.venue AND _identity_claims.market_id=k.market_id",
                [value for key in replacements for value in key],
            )
            rows = list({row for replacement in replacements.values() for row in replacement})
            if rows:
                conn.executemany("INSERT INTO _identity_claims VALUES (?,?,?,?,?)", rows)
        conflicts = {
            (venue, market_id)
            for (venue, _kind, _asset), owners in claims.items()
            if len(owners) > 1
            for market_id in owners
        }
        for venue, market_id in impacted:
            identity = conn.execute(
                "SELECT candidate,accepted,invalidated_at FROM _identity WHERE venue=? AND market_id=?",
                [venue, market_id],
            ).fetchone()
            conflict = (venue, market_id) in conflicts
            if conflict:
                conn.execute(
                    "UPDATE _identity SET invalidated_at=?, invalidated_response=? "
                    "WHERE venue=? AND market_id=? AND "
                    "(invalidated_at IS NULL OR (invalidated_at, invalidated_response) < (?, ?))",
                    [*stamp, venue, market_id, *stamp],
                )
            usable = bool(
                identity and identity[0] and identity[1] and not identity[2] and not conflict
            )
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
            conn.execute(
                "UPDATE markets SET usable=?,identity_error=? WHERE venue=? AND market_id=? "
                "AND (usable IS DISTINCT FROM ? OR identity_error IS DISTINCT FROM ?)",
                [usable, error, venue, market_id, usable, error],
            )
            outcomes = json.loads(identity[1]) if usable else []
            expected_outcomes = sorted(
                (tuple(outcome.get(name) for name in columns("outcomes")) for outcome in outcomes),
                key=lambda row: row[:2] + (row[3],),
            )
            if before_outcomes[(venue, market_id)] != expected_outcomes:
                conn.execute(
                    "DELETE FROM outcomes WHERE venue=? AND market_id=?", [venue, market_id]
                )
                for outcome in outcomes:
                    _put(conn, "outcomes", outcome)
            changed |= before_markets[(venue, market_id)] != _snapshot(
                conn, "markets", "venue=? AND market_id=?", [venue, market_id]
            ) or before_outcomes[(venue, market_id)] != _snapshot(
                conn, "outcomes", "venue=? AND market_id=?", [venue, market_id]
            )
        # Confirmed absence is a receipt, not data: it never changes rows or the generation.
        for event_id in sorted(response.absent_event_ids):
            conn.execute(
                "UPDATE _membership SET receipt=?,response=? WHERE venue=? AND event_id=? "
                "AND (receipt IS NULL OR (receipt,response)<(?,?)) AND EXISTS "
                "(SELECT 1 FROM events e WHERE e.venue=_membership.venue "
                "AND e.event_id=_membership.event_id)",
                [*stamp, "polymarket", event_id, *stamp],
            )
        for market_id in sorted(response.absent_market_ids):
            if conn.execute(
                "SELECT 1 FROM markets WHERE venue=? AND market_id=?", ["polymarket", market_id]
            ).fetchone():
                _newer(conn, "market", "polymarket", market_id, stamp)
        for event_id in response.parent_event_ids:
            conn.execute("INSERT OR IGNORE INTO _parent_repairs VALUES (?)", [event_id])
        for key, value in (progress or {}).items():
            set_state(conn, key, value)
        if changed:
            set_state(conn, "generation", generation(conn) + 1)
        result = {
            "generation": generation(conn),
            "changed": bool(changed),
            "events": len(response.events),
            "markets": len(response.markets),
        }
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return result
