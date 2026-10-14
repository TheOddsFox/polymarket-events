"""Bounded due work, with successful receipts kept separate from retry fairness."""

import time
from datetime import UTC, datetime, timedelta

from . import current_store
from .current_config import admit
from .current_sync import fetch_apply, resume_pending
from .gamma.http import CaptureStopped, RequestBudgetExceeded


def _now():
    return datetime.now(UTC)


def _initialize(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS _reconcile_attempts (
        kind VARCHAR, id VARCHAR, last_attempt TIMESTAMPTZ,
        PRIMARY KEY(kind,id))""")


def _entities(lane):
    if lane == "events":
        return """SELECT e.event_id id,e.closed,m.receipt,
            coalesce(m.unresolved,true) unresolved FROM events e
            LEFT JOIN _membership m USING(venue,event_id)"""
    return """SELECT m.market_id id,m.closed,r.receipt,false unresolved FROM markets m
        LEFT JOIN _receipts r ON r.kind='market' AND r.venue=m.venue AND r.id=m.market_id"""


def _due(conn, lane, cold, now, interval):
    entities = _entities(lane)
    if lane == "events":
        entities = f"""SELECT id,
            CASE WHEN EXISTS (SELECT 1 FROM _parent_repairs p WHERE p.event_id=e.id)
            THEN NULL ELSE closed END closed, receipt,
            unresolved OR EXISTS (SELECT 1 FROM _parent_repairs p WHERE p.event_id=e.id)
            unresolved FROM ({entities}) e"""
    if lane == "events" and not cold:
        entities += """ UNION ALL SELECT p.event_id,NULL,NULL,true FROM _parent_repairs p
            WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.event_id=p.event_id)"""
    return conn.execute(
        f"""SELECT e.id,e.receipt FROM ({entities}) e
        LEFT JOIN _reconcile_attempts a ON a.kind=? AND a.id=e.id
        WHERE (e.closed IS TRUE)=? AND
        (e.receipt IS NULL OR e.receipt<=? OR e.unresolved) AND
        (a.last_attempt IS NULL OR a.last_attempt<=?)
        ORDER BY a.last_attempt NULLS FIRST,e.receipt NULLS FIRST,try_cast(e.id AS BIGINT)
        LIMIT 100""",
        [lane, cold, now - timedelta(seconds=interval), now - timedelta(hours=1)],
    ).fetchall()


def _attempt(conn, lane, ids, now):
    table, key = ("events", "event_id") if lane == "events" else ("markets", "market_id")
    # Empty numeric windows must not create one scheduler row per absent source ID.
    source = f"SELECT {key} id FROM {table}"
    if lane == "events":
        source += " UNION SELECT event_id FROM _parent_repairs"
    conn.execute(
        f"""INSERT OR REPLACE INTO _reconcile_attempts
        SELECT ?, id, ? FROM ({source}) WHERE id IN (SELECT unnest(?))""",
        [lane, now, [str(value) for value in ids]],
    )


def _cold(conn, settings, now, state):
    discovery = current_store.get_state(conn, "discovery", {}).get("lanes", {})
    for offset in range(4):
        slot = (state["cold_slot"] + offset) % 4
        lane = ("events", "markets")[slot % 2]
        if slot < 2:
            rows = _due(conn, lane, True, now, settings.limits.cold_recheck_s)
            if rows:
                return lane, [r[0] for r in rows], slot, None
        else:
            window = state["windows"].get(lane, {"next": 1, "cycle": None})
            if (
                window["cycle"]
                and now.timestamp() - window["cycle"] < settings.limits.cold_recheck_s
            ):
                continue
            high = discovery.get(lane, {}).get("frontier", 0)
            if high < 1:
                continue
            lo = min(window["next"], high)
            ids = list(range(lo, min(lo + 99, high) + 1))
            next_window = {"next": ids[-1] + 1, "cycle": None}
            if ids[-1] == high:
                next_window = {"next": 1, "cycle": now.timestamp()}
            return lane, ids, slot, next_window
    return None


def reconcile(settings, conn, client, *, deadline):
    """Run 3 active units per cold unit, retaining finite rotation checkpoints."""
    reserve = max(
        settings.limits.publication_reserve_s,
        1.5 * current_store.get_state(conn, "previous_publish_seconds", 0),
    )
    stop = deadline - reserve
    counters = {"responses": 0, "active_batches": 0, "cold_batches": 0}
    if time.monotonic() >= stop:
        return {**counters, "source_complete": False}
    # Frozen foreign checkpoints must finish before this scheduler advances its own state.
    try:
        counters["responses"] += resume_pending(settings, conn, client)["responses"]
    except (CaptureStopped, RequestBudgetExceeded):
        return {**counters, "source_complete": False}
    _initialize(conn)
    while time.monotonic() < stop:
        now = _now()
        state = current_store.get_state(
            conn, "reconcile", {"turn": 0, "active_lane": 0, "cold_slot": 0, "windows": {}}
        )
        active = None
        for offset in range(2):
            lane = ("events", "markets")[(state["active_lane"] + offset) % 2]
            rows = _due(conn, lane, False, now, settings.limits.active_recheck_s)
            if rows:
                active = (lane, rows)
                break
        cold = _cold(conn, settings, now, state)
        if active is None and cold is None:
            break
        # A near-target active receipt gets the next slot, even on a cold turn.
        urgent = active and any(
            receipt is None or (now - receipt).total_seconds() >= 5 * 3600
            for _, receipt in active[1]
        )
        use_active = active is not None and (cold is None or state["turn"] < 3 or urgent)
        if use_active:
            lane, rows = active
            ids = [r[0] for r in rows]
            state["active_lane"] = 1 - ("events", "markets").index(lane)
            state["turn"] = min(3, state["turn"] + 1)
            counter = "active_batches"
        else:
            lane, ids, slot, window = cold
            state["cold_slot"] = (slot + 1) % 4
            if window is not None:
                state["windows"][lane] = window
            state["turn"] = 0
            counter = "cold_batches"
        if time.monotonic() >= stop:
            break
        admit(settings, additional_bytes=65536)
        try:
            result = fetch_apply(settings, conn, client, lane, ids, progress={"reconcile": state})
        except (CaptureStopped, RequestBudgetExceeded):
            break
        _attempt(conn, lane, ids, now)
        counters["responses"] += result["responses"]
        counters[counter] += 1
        # Only changed/invalid identities need native confirmation; one ID per budget check.
        repairs = conn.execute(
            """SELECT DISTINCT m.market_id FROM markets m
            LEFT JOIN _reconcile_attempts a ON a.kind='native' AND a.id=m.market_id
            WHERE NOT m.usable
            AND (a.last_attempt IS NULL OR a.last_attempt<=?) AND (
                (?='markets' AND m.market_id IN (SELECT unnest(?))) OR
                (?='events' AND EXISTS (SELECT 1 FROM event_markets e
                    WHERE e.market_id=m.market_id AND e.event_id IN (SELECT unnest(?)))))
            ORDER BY m.market_id LIMIT 100""",
            [
                now - timedelta(hours=1),
                lane,
                [str(key) for key in ids],
                lane,
                [str(key) for key in ids],
            ],
        ).fetchall()
        for (key,) in repairs:
            if time.monotonic() >= stop:
                break
            try:
                counters["responses"] += fetch_apply(
                    settings, conn, client, "markets", [key], native=True
                )["responses"]
                _attempt(conn, "native", [key], now)
            except (CaptureStopped, RequestBudgetExceeded):
                return {**counters, "source_complete": False}
    return {**counters, "source_complete": False}


def repair_pending(settings, conn, client, *, deadline):
    """One bounded parent unit after discovery; never a sweep of known entities."""
    reserve = max(
        settings.limits.publication_reserve_s,
        1.5 * current_store.get_state(conn, "previous_publish_seconds", 0),
    )
    if time.monotonic() >= deadline - reserve:
        return {"responses": 0}
    _initialize(conn)
    now = _now()
    ids = [
        row[0]
        for row in conn.execute(
            """SELECT p.event_id FROM _parent_repairs p LEFT JOIN _reconcile_attempts a
        ON a.kind='events' AND a.id=p.event_id
        WHERE a.last_attempt IS NULL OR a.last_attempt<=?
        ORDER BY a.last_attempt NULLS FIRST,p.event_id LIMIT 100""",
            [now - timedelta(hours=1)],
        ).fetchall()
    ]
    if not ids:
        return {"responses": 0}
    result = fetch_apply(settings, conn, client, "events", ids)
    _attempt(conn, "events", ids, now)
    return result


def freshness(conn, *, now=None):
    """Observed age and coverage, never a source-completeness claim."""
    now = now or _now()
    result = {"source_complete": False}
    for cold, prefix, interval in [(False, "active", 6 * 3600), (True, "cold", 30 * 86400)]:
        row = conn.execute(
            f"""SELECT count(*),count(*) FILTER (WHERE receipt IS NULL OR receipt<=?
            OR unresolved),min(receipt) FROM ({_entities("events")} UNION ALL
            {_entities("markets")}) WHERE (closed IS TRUE)=?""",
            [now - timedelta(seconds=interval), cold],
        ).fetchone()
        result.update(
            {
                f"{prefix}_count": row[0],
                f"{prefix}_overdue_count": row[1],
                f"{prefix}_oldest_complete_receipt": row[2].isoformat() if row[2] else None,
            }
        )
    result["overdue_count"] = result["active_overdue_count"]
    result["cold_cycle"] = current_store.get_state(conn, "reconcile", {}).get("windows", {})
    return result
