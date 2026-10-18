"""Bounded due work, with successful receipts kept separate from retry fairness."""

import time
from collections import deque
from datetime import UTC, datetime, timedelta

from . import current_store
from .current_config import ACTIVE_FRESHNESS_S, COLD_FRESHNESS_S, admit, publication_reserve
from .current_sync import (
    ResponsePrefetcher,
    fetch_apply,
    fetch_unit,
    first_bulk_request,
    resume_pending,
)
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


def _due(conn, lane, cold, now, interval, *, pass_started=None):
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
    if pass_started is None:
        bypass, threshold = lane == "markets" and cold, now - timedelta(seconds=interval)
    else:
        # A forced pass visits every known entity, open or closed, until its receipt is newer.
        bypass, threshold = True, pass_started
    return conn.execute(
        f"""SELECT e.id,e.receipt FROM ({entities}) e
        LEFT JOIN _reconcile_attempts a ON a.kind=? AND a.id=e.id
        WHERE (? OR (e.closed IS TRUE)=?) AND
        (e.receipt IS NULL OR e.receipt<=? OR e.unresolved) AND
        (a.last_attempt IS NULL OR a.last_attempt<=?)
        ORDER BY a.last_attempt NULLS FIRST,e.receipt NULLS FIRST,try_cast(e.id AS HUGEINT)
        LIMIT 100""",
        [lane, bypass, cold, threshold, now - timedelta(hours=1)],
    ).fetchall()


def _attempt(conn, lane, ids, now):
    # Empty numeric windows must not create one scheduler row per absent source ID.
    current_store.record_attempts(conn, lane, ids, now)


def repair_identities(settings, conn, client, lane, ids, now, stop, *, response_counter=None):
    """Confirm changed or invalid native identities related to one applied batch."""
    if not ids or time.monotonic() >= stop:
        return 0
    _initialize(conn)
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
    responses = 0
    for (key,) in repairs:
        if time.monotonic() >= stop:
            break
        count = fetch_apply(
            settings,
            conn,
            client,
            "markets",
            [key],
            native=True,
            attempts=("native", [key], now),
        )["responses"]
        if response_counter is None:
            responses += count
        else:
            response_counter["responses"] += count
    return responses


def _cold(conn, settings, now, state, *, pass_started=None):
    discovery = current_store.get_state(conn, "discovery", {}).get("lanes", {})
    for offset in range(4):
        slot = (state["cold_slot"] + offset) % 4
        lane = ("events", "markets")[slot % 2]
        if slot < 2:
            rows = _due(
                conn,
                lane,
                True,
                now,
                settings.limits.cold_recheck_s,
                pass_started=pass_started,
            )
            if rows:
                return lane, [r[0] for r in rows], slot, None
        else:
            if pass_started is not None:
                continue
            window = state["windows"].get(lane, {"next": 1, "cycle": None})
            if (
                pass_started is None
                and window.get("started_at") is None
                and window["cycle"] is not None
                and now.timestamp() - window["cycle"]
                < min(settings.limits.cold_recheck_s, COLD_FRESHNESS_S // 2)
            ):
                continue
            certificate = current_store.get_state(conn, "bulk_policy", {})
            keyset = (
                certificate.get("version") == 3
                and certificate.get("origin") == settings.gamma.base_url
                and certificate.get("capabilities", {}).get("keyset_discovery", {}).get("supported")
                is True
            )
            if keyset:
                started_at = window.get("started_at", now.timestamp())
                next_window = {
                    "next": 1,
                    "cycle": None,
                    "started_at": started_at,
                    "high": None,
                    "keyset": True,
                }
                return lane, [], slot, next_window
            high = window.get("high")
            if high is None:
                high = discovery.get(lane, {}).get("frontier", 0)
            if high < 1:
                continue
            started_at = window.get("started_at", now.timestamp())
            lo = min(window["next"], high)
            ids = list(range(lo, min(lo + 99, high) + 1))
            next_window = {
                "next": ids[-1] + 1,
                "cycle": None,
                "started_at": started_at,
                "high": high,
            }
            if ids[-1] == high:
                next_window = {"next": 1, "cycle": started_at}
            return lane, ids, slot, next_window
    return None


def _confirm_omitted(settings, conn, client, lane, omitted):
    """Native lookups settle known entities a bounded list omitted; only a 404 proves absence.

    Unknown IDs (window gaps) are skipped, so a sparse window costs no native requests.
    """
    if not omitted:
        return 0
    table, key = ("events", "event_id") if lane == "events" else ("markets", "market_id")
    known = [
        row[0]
        for row in conn.execute(
            f"SELECT {key} FROM {table} WHERE {key} IN (SELECT unnest(?))", [omitted]
        ).fetchall()
    ]
    if not known:
        return 0
    return fetch_apply(settings, conn, client, lane, known, native=True, absence=True)["responses"]


def _pass_started(conn):
    """Start time of an unfinished forced full pass, otherwise None."""
    state = current_store.get_state(conn, "full_pass") or {}
    if state.get("pass_started_at") is None or state.get("completed_at") is not None:
        return None
    return datetime.fromisoformat(state["pass_started_at"])


def start_full_pass(conn, *, now=None):
    """Begin a forced pass; an unfinished pass keeps its start and resumes from receipts."""
    if _pass_started(conn) is not None:
        return
    started = now or _now()
    current_store.set_state(
        conn,
        "full_pass",
        {"pass_started_at": started.isoformat(), "completed_at": None, "cursor_after": None},
    )


def _remaining(conn, started):
    """Known events and markets without a receipt newer than the pass start."""
    return conn.execute(
        f"""SELECT count(*) FROM (
            SELECT receipt FROM ({_entities("events")})
            UNION ALL SELECT receipt FROM ({_entities("markets")})
        ) WHERE receipt IS NULL OR receipt<=?""",
        [started],
    ).fetchone()[0]


def _complete_pass(conn, now):
    started = _pass_started(conn)
    if started is None or _remaining(conn, started) != 0:
        return
    state = current_store.get_state(conn, "full_pass")
    state["completed_at"] = now.isoformat()
    current_store.set_state(conn, "full_pass", state)


def full_pass_status(conn, *, now=None):
    """Forced-pass progress; never a source-completeness claim."""
    now = now or _now()
    state = current_store.get_state(conn, "full_pass") or {}
    started = state.get("pass_started_at")
    return {
        "started_at": started,
        "completed_at": state.get("completed_at"),
        "remaining": None if started is None else _remaining(conn, datetime.fromisoformat(started)),
        "overdue": freshness(conn, now=now)["active_overdue_count"],
        "unresolved": conn.execute("SELECT count(*) FROM _membership WHERE unresolved").fetchone()[
            0
        ],
        "identity_repair_count": conn.execute(
            "SELECT count(DISTINCT market_id) FROM markets WHERE NOT usable"
        ).fetchone()[0],
        "parent_event_repair_count": conn.execute(
            "SELECT count(*) FROM _parent_repairs"
        ).fetchone()[0],
    }


def _stale_event_ids(conn, started, after):
    """Known events a forced pass still has to visit, in numeric id order."""
    return [
        row[0]
        for row in conn.execute(
            """SELECT id FROM (
                SELECT e.event_id id, m.receipt, coalesce(m.unresolved, true) unresolved
                FROM events e LEFT JOIN _membership m USING (venue, event_id)
                UNION ALL
                SELECT p.event_id, NULL, true FROM _parent_repairs p
                WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.event_id=p.event_id)
            ) WHERE (receipt IS NULL OR receipt<=? OR unresolved)
            AND (? IS NULL OR try_cast(id AS HUGEINT) > ?)
            ORDER BY try_cast(id AS HUGEINT)""",
            [started, after, after],
        ).fetchall()
    ]


def _urgent_active_ids(conn, settings, now):
    rows = _due(conn, "events", False, now, settings.limits.active_recheck_s)
    if not rows:
        return None
    urgent = any(
        receipt is None or (now - receipt).total_seconds() >= ACTIVE_FRESHNESS_S - 2 * 3600
        for _, receipt in rows
    )
    if not urgent:
        return None
    return [row[0] for row in rows]


def _pass_checkpoint(conn, cursor):
    state = dict(current_store.get_state(conn, "full_pass") or {})
    if cursor is not None:
        state["cursor_after"] = int(cursor)
    reconcile_state = current_store.get_state(
        conn, "reconcile", {"turn": 0, "cold_slot": 0, "windows": {}}
    )
    return {"full_pass": state, "reconcile": reconcile_state}


def _take_pending(queue, skipped, limit):
    """Consume the next bounded set of stale IDs, ignoring settled urgent work."""
    ids = []
    while queue and len(ids) < limit:
        key = queue.popleft()
        if key not in skipped:
            ids.append(key)
    return ids


def _peek_pending(queue, skipped, limit):
    """Preview eligible queue IDs for prefetch without changing the queue."""
    ids = []
    for key in queue:
        if key not in skipped:
            ids.append(key)
            if len(ids) == limit:
                break
    return ids


def _reconcile_pass(settings, conn, client, stop, counters):
    """Walk stale events in id order, then the markets those events did not renew."""
    started = _pass_started(conn)
    if started is not None and _remaining(conn, started) == 0:
        return {**counters, "source_complete": False}
    stored = current_store.get_state(conn, "full_pass") or {}
    pending = _stale_event_ids(conn, started, stored.get("cursor_after"))
    if not pending and stored.get("cursor_after") is not None:
        # Older checkpoints may have advanced past unresolved or oversized IDs.
        pending = _stale_event_ids(conn, started, None)
    pending = deque(pending)
    skipped = set()
    prefetcher = ResponsePrefetcher(client, deadline=stop)
    try:
        while time.monotonic() < stop:
            now = _now()
            while pending and pending[0] in skipped:
                pending.popleft()
            urgent = _urgent_active_ids(conn, settings, now)
            if urgent:
                lane, ids, advance = "events", urgent, None
                counter = "active_batches"
            elif pending:
                ids = _take_pending(pending, skipped, 100)
                lane, advance, counter = "events", ids[-1], "cold_batches"
            else:
                rows = _due(
                    conn,
                    "markets",
                    True,
                    now,
                    settings.limits.cold_recheck_s,
                    pass_started=started,
                )
                if not rows:
                    break
                lane, ids, advance = "markets", [row[0] for row in rows], None
                counter = "cold_batches"
            preview = _peek_pending(pending, skipped, 100)
            if preview and lane == "events" and advance is not None:
                prefetcher.submit(first_bulk_request(settings, conn, "events", preview))
            if time.monotonic() >= stop:
                break
            admit(settings, additional_bytes=65536)
            try:
                result, omitted = fetch_unit(
                    settings,
                    conn,
                    client,
                    lane,
                    ids,
                    progress=_pass_checkpoint(conn, None),
                    attempts=(lane, ids, now),
                    prefetcher=prefetcher,
                )
            except (CaptureStopped, RequestBudgetExceeded):
                break
            counters["responses"] += result["responses"]
            counters[counter] += 1
            # The cursor moves only after omissions are settled, so a deadline here
            # retries the batch instead of skipping a known entity forever.
            if omitted and time.monotonic() >= stop:
                break
            if omitted:
                try:
                    counters["responses"] += _confirm_omitted(settings, conn, client, lane, omitted)
                except (CaptureStopped, RequestBudgetExceeded):
                    break
            if lane == "events" and counter == "active_batches":
                skipped.update(ids)
            if advance is not None:
                state = dict(current_store.get_state(conn, "full_pass") or {})
                state["cursor_after"] = int(advance)
                current_store.set_state(conn, "full_pass", state)
            try:
                repair_identities(
                    settings, conn, client, lane, ids, now, stop, response_counter=counters
                )
            except (CaptureStopped, RequestBudgetExceeded):
                return {**counters, "source_complete": False}
    finally:
        prefetcher.close()
    return {**counters, "source_complete": False}


def reconcile(settings, conn, client, *, deadline):
    """Run bounded reconcile units; a forced pass is recorded complete once remaining hits 0."""
    result = _reconcile_units(settings, conn, client, deadline=deadline)
    _complete_pass(conn, _now())
    return result


def _reconcile_units(settings, conn, client, *, deadline):
    """Run 3 active units per cold unit, retaining finite rotation checkpoints."""
    reserve = publication_reserve(settings, conn)
    stop = deadline - reserve
    counters = {"responses": 0, "active_batches": 0, "cold_batches": 0}
    if time.monotonic() >= stop:
        return {**counters, "source_complete": False}
    # Frozen foreign checkpoints must finish before this scheduler advances its own state.
    try:
        resumed = resume_pending(settings, conn, client)
        counters["responses"] += resumed["responses"]
        repair_resumed_identities(
            settings, conn, client, resumed, now=_now(), stop=stop, response_counter=counters
        )
    except (CaptureStopped, RequestBudgetExceeded):
        return {**counters, "source_complete": False}
    _initialize(conn)
    if _pass_started(conn) is not None:
        return _reconcile_pass(settings, conn, client, stop, counters)
    while time.monotonic() < stop:
        now = _now()
        state = current_store.get_state(
            conn, "reconcile", {"turn": 0, "cold_slot": 0, "windows": {}}
        )
        pass_started = _pass_started(conn)
        # A finished forced pass must not keep spending the bypassed window scans.
        if pass_started is not None and _remaining(conn, pass_started) == 0:
            break
        rows = _due(
            conn,
            "events",
            False,
            now,
            settings.limits.active_recheck_s,
            pass_started=pass_started,
        )
        active = ("events", rows) if rows else None
        cold = _cold(conn, settings, now, state, pass_started=pass_started)
        if active is None and cold is None:
            break
        # A near-target active receipt gets the next slot, even on a cold turn.
        urgent = active and any(
            receipt is None or (now - receipt).total_seconds() >= ACTIVE_FRESHNESS_S - 2 * 3600
            for _, receipt in active[1]
        )
        use_active = active is not None and (cold is None or state["turn"] < 3 or urgent)
        if use_active:
            lane, rows = active
            ids = [r[0] for r in rows]
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
        if not ids and window is not None and window.get("keyset"):
            from .current_keyset import cold_page

            try:
                result = cold_page(
                    settings,
                    conn,
                    client,
                    lane,
                    reconcile_state=state,
                    now=now,
                )
            except (CaptureStopped, RequestBudgetExceeded):
                break
            counters["responses"] += result.get("responses", 0)
            counters["cold_batches"] += 1
            try:
                repair_resumed_identities(
                    settings, conn, client, result, now=now, stop=stop, response_counter=counters
                )
            except (CaptureStopped, RequestBudgetExceeded):
                return {**counters, "source_complete": False}
            if result.get("complete"):
                state["windows"][lane] = {"next": 1, "cycle": window["started_at"]}
                current_store.set_state(conn, "reconcile", state)
            continue
        try:
            result, omitted = fetch_unit(
                settings,
                conn,
                client,
                lane,
                ids,
                progress={"reconcile": state},
                attempts=(lane, ids, now),
            )
        except (CaptureStopped, RequestBudgetExceeded):
            break
        counters["responses"] += result["responses"]
        counters[counter] += 1
        # Only a forced pass must settle every known entity, so only it spends native lookups
        # on omissions; ordinary rechecks keep retrying omitted IDs on their normal schedule.
        if pass_started is not None and time.monotonic() < stop:
            try:
                counters["responses"] += _confirm_omitted(settings, conn, client, lane, omitted)
            except (CaptureStopped, RequestBudgetExceeded):
                break
        # Only changed/invalid identities need native confirmation; one ID per budget check.
        try:
            repair_identities(
                settings, conn, client, lane, ids, now, stop, response_counter=counters
            )
        except (CaptureStopped, RequestBudgetExceeded):
            return {**counters, "source_complete": False}
    return {**counters, "source_complete": False}


def repair_resumed_identities(settings, conn, client, resumed, *, now, stop, response_counter=None):
    """Repair native identities reported by an applied keyset cold page."""
    repair = resumed.get("identity_repairs")
    if repair is None:
        return 0
    return repair_identities(
        settings,
        conn,
        client,
        repair["lane"],
        repair["ids"],
        now,
        stop,
        response_counter=response_counter,
    )


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
    result = fetch_apply(settings, conn, client, "events", ids, attempts=("events", ids, now))
    return result


def freshness(conn, *, now=None):
    """Observed age and coverage, never a source-completeness claim."""
    now = now or _now()
    result = {"source_complete": False}
    for cold, prefix, interval in [
        (False, "active", ACTIVE_FRESHNESS_S),
        (True, "cold", COLD_FRESHNESS_S),
    ]:
        entities = (
            f"SELECT * FROM ({_entities('events')}) WHERE (closed IS TRUE)={str(cold).lower()}"
        )
        if cold:
            entities += f" UNION ALL {_entities('markets')}"
        row = conn.execute(
            f"""SELECT count(*),count(*) FILTER (WHERE receipt IS NULL OR receipt<=?
            OR unresolved),min(receipt) FROM ({entities})""",
            [now - timedelta(seconds=interval)],
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
    frontiers = current_store.get_state(conn, "discovery", {}).get("lanes", {})
    result["cold_ranges"] = {}
    for lane in ("events", "markets"):
        window = result["cold_cycle"].get(lane, {})
        started = window.get("started_at", window.get("cycle"))
        age = None if started is None else max(0, now.timestamp() - started)
        result["cold_ranges"][lane] = {
            "age_s": age,
            "overdue": None if age is None else age >= COLD_FRESHNESS_S,
            "unstarted": started is None and frontiers.get(lane, {}).get("frontier", 0) > 0,
        }
    return result
