"""Durable, bounded keyset pages for periodic cold discovery."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime
from types import SimpleNamespace

from . import current_store, current_sync
from .capture.writer import read_body
from .current_config import admit, validate_root
from .current_contract import CurrentError
from .current_rows import entity_id, project_response
from .current_sync import (
    POLICY,
    _pending_manifest,
    _project,
    _records,
    _safe_pending_raw_dir,
    _write_response,
)
from .gamma.http import CursorExpired, ResponseTooLarge
from .gamma.paginators import unpack

LANES = ("events", "markets")
PAGE_LIMIT = 100


def _require_all_lifecycle_event_highwater(certificate):
    """Reject keyset certificates whose event highwater may omit lifecycles."""
    if certificate.get("lanes", {}).get("events", {}).get("passes") != [None]:
        raise CurrentError(
            "keyset discovery requires certified all-lifecycle event highwater coverage"
        )


def _passes(lane):
    return (None,) if lane == "events" else (False, True)


def _pass_key(lane, closed):
    return "all" if lane == "events" else ("closed" if closed else "open")


def _validate_state(state):
    if not isinstance(state, dict) or state.get("version") != 1:
        raise CurrentError("incompatible keyset discovery state")
    if set(state.get("lanes", {})) != set(LANES):
        raise CurrentError("incomplete keyset discovery state")
    for lane in LANES:
        item = state["lanes"][lane]
        expected = {"all"} if lane == "events" else {"open", "closed"}
        if not isinstance(item, dict) or set(item.get("passes", {})) != expected:
            raise CurrentError("invalid keyset lifecycle progress")
        for progress in item["passes"].values():
            if (
                not isinstance(progress, dict)
                or (
                    progress.get("highwater") is not None
                    and (type(progress["highwater"]) is not int or progress["highwater"] < 0)
                )
                or (
                    progress.get("cursor") is not None
                    and (not isinstance(progress["cursor"], str) or not progress["cursor"])
                )
                or type(progress.get("last_id")) is not int
                or progress["last_id"] < 0
                or type(progress.get("page_limit")) is not int
                or not 1 <= progress["page_limit"] <= PAGE_LIMIT
                or type(progress.get("done")) is not bool
            ):
                raise CurrentError("invalid keyset progress values")


def _parse(response, lane, *, after, previous_id, highwater, closed):
    if response.status != 200 or not isinstance(response.json, dict):
        raise CurrentError(f"/{lane}/keyset returned an invalid page envelope")
    rows, cursor = unpack(response.json, lane)
    if cursor is not None and (not cursor or cursor == after):
        raise CurrentError("keyset cursor did not advance")
    if set(response.json) - {lane, "next_cursor"}:
        raise CurrentError("unexpected keyset page envelope fields")
    accepted = []
    last_accepted_id = previous_id
    crossed_highwater = False
    for row in rows:
        if not isinstance(row, dict):
            raise CurrentError("nonrecord keyset response item")
        key = entity_id(row.get("id"))
        numeric = int(key)
        if numeric <= previous_id:
            raise CurrentError("keyset IDs are not strictly increasing")
        _records(SimpleNamespace(status=200, json=row), lane, closed=closed)
        if lane == "markets" and (
            type(row.get("closed")) is not bool or row["closed"] is not closed
        ):
            raise CurrentError("market keyset lifecycle differs from frozen pass")
        previous_id = numeric
        if highwater is not None and numeric > highwater:
            crossed_highwater = True
        elif not crossed_highwater:
            accepted.append(row)
            last_accepted_id = numeric
    return accepted, cursor, last_accepted_id, crossed_highwater


def validate_pending(settings, conn, pending):
    try:
        current_sync.require_acquisition_allowed(settings, conn)
        lane = pending["lane"]
        if lane not in LANES or pending["kind"] not in {"highwater", "page"}:
            raise CurrentError("invalid keyset pending unit")
        certificate = current_store.get_state(conn, "bulk_policy", {})
        if (
            certificate.get("version") != 3
            or certificate.get("policy") != POLICY
            or certificate.get("origin") != settings.gamma.base_url
            or certificate.get("capabilities", {}).get("keyset_discovery", {}).get("supported")
            is not True
            or pending["certificate"] != certificate
        ):
            raise CurrentError("incompatible pending keyset certificate")
        _require_all_lifecycle_event_highwater(certificate)
        request = pending["request"]
        _safe_pending_raw_dir(settings, pending.get("raw_id"))
        params = request["params"]
        expected_endpoint = f"/{lane}" if pending["kind"] == "highwater" else f"/{lane}/keyset"
        if request["endpoint"] != expected_endpoint:
            raise CurrentError("invalid keyset endpoint")
        expected = {"limit", "order", "ascending"}
        if pending["kind"] == "page" and "after_cursor" in params:
            expected.add("after_cursor")
        if lane == "markets":
            expected.add("closed")
            if pending["kind"] == "page":
                expected.add("include_tag")
        if set(params) != expected:
            raise CurrentError("unexpected frozen keyset parameters")
        if params.get("order") != "id":
            raise CurrentError("invalid keyset ordering")
        if pending["kind"] == "highwater":
            if params.get("ascending") is not False or params.get("limit") != 1:
                raise CurrentError("invalid keyset highwater parameters")
        elif params.get("ascending") is not True:
            raise CurrentError("invalid keyset ordering")
        if type(params.get("limit")) is not int or not 1 <= params["limit"] <= PAGE_LIMIT:
            raise CurrentError("invalid keyset page limit")
        if lane == "events" and "closed" in params:
            raise CurrentError("event keyset cannot use a lifecycle filter")
        if lane == "markets" and type(params.get("closed")) is not bool:
            raise CurrentError("market keyset requires a frozen lifecycle pass")
        if (
            pending["kind"] == "page"
            and lane == "markets"
            and params.get("include_tag") is not True
        ):
            raise CurrentError("market keyset page must include tags")
        if lane == "markets" and (
            type(pending.get("closed")) is not bool or pending["closed"] is not params.get("closed")
        ):
            raise CurrentError("frozen lifecycle pass mismatch")
        if lane == "events" and pending.get("closed") is not None:
            raise CurrentError("event keyset cannot freeze a lifecycle pass")
        cursor = params.get("after_cursor")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise CurrentError("invalid frozen keyset cursor")
        if pending["kind"] == "page" and (
            type(pending["highwater"]) is not int or pending["highwater"] < 0
        ):
            raise CurrentError("invalid frozen keyset highwater")
        progress = current_store.get_state(conn, "keyset_discovery")["lanes"][lane]["passes"][
            _pass_key(lane, pending["closed"])
        ]
        if pending["kind"] == "highwater" and (
            pending.get("highwater") is not None
            or progress["highwater"] is not None
            or progress["cursor"] is not None
            or progress["last_id"] != pending["previous_id"]
        ):
            raise CurrentError("pending keyset highwater differs from saved progress")
        if pending["kind"] == "page" and (
            progress["highwater"] != pending["highwater"]
            or progress["cursor"] != params.get("after_cursor")
            or progress["page_limit"] != params["limit"]
            or progress["last_id"] != pending["previous_id"]
        ):
            raise CurrentError("pending keyset page differs from saved progress")
    except (KeyError, TypeError, ValueError) as exc:
        raise CurrentError("incompatible pending keyset request") from exc


def _read_pending(settings, conn, client, pending):
    validate_pending(settings, conn, pending)
    request = pending["request"]
    raw_dir = _safe_pending_raw_dir(settings, pending["raw_id"])
    manifest = _pending_manifest(raw_dir, settings.root)
    if manifest is None:
        response = client.get(request["endpoint"], request["params"])
        if pending["kind"] == "highwater":
            rows = _records(response, pending["lane"], closed=pending["closed"])
            if len(rows) > 1:
                raise CurrentError("keyset highwater response exceeds limit")
            _project(rows, pending["lane"], response)
        else:
            if not isinstance(response.json, dict):
                raise CurrentError("keyset page envelope is invalid")
            raw_rows, _ = unpack(response.json, pending["lane"])
            if len(raw_rows) > request["params"]["limit"]:
                raise CurrentError("keyset page exceeds its frozen limit")
            _parse(
                response,
                pending["lane"],
                after=request["params"].get("after_cursor"),
                previous_id=pending["previous_id"],
                highwater=pending["highwater"],
                closed=pending["closed"],
            )
            # Validate even records newer than the frozen boundary before
            # deliberately excluding them from this sweep.
            _project(raw_rows, pending["lane"], response)
        _write_response(settings, raw_dir, response, request)
        manifest = _pending_manifest(raw_dir, settings.root)
    if manifest["request"] != request:
        raise CurrentError("keyset raw request mismatch")
    body = read_body(
        raw_dir,
        manifest,
        trusted_root=settings.root,
        max_body_bytes=settings.limits.max_response_bytes,
    )
    response = SimpleNamespace(status=manifest["status"], json=json.loads(body))
    return response, manifest, raw_dir


def resume_pending(settings, conn, client):
    pending = current_store.get_state(conn, "keyset_pending")
    if pending is None:
        return {"responses": 0}
    try:
        response, manifest, raw_dir = _read_pending(settings, conn, client, pending)
    except CursorExpired as exc:
        key = (pending["lane"], pending["closed"])
        restarted = getattr(client, "_keyset_cursor_restarts", set())
        if key in restarted:
            raise CurrentError("keyset cursor expired twice in one invocation") from exc
        restarted.add(key)
        client._keyset_cursor_restarts = restarted
        state = copy.deepcopy(current_store.get_state(conn, "keyset_discovery"))
        progress = state["lanes"][pending["lane"]]["passes"][
            _pass_key(pending["lane"], pending["closed"])
        ]
        progress.update(cursor=None, last_id=0)
        conn.execute("BEGIN TRANSACTION")
        try:
            current_store.set_state(conn, "keyset_discovery", state)
            current_store.set_state(conn, "keyset_pending", None)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        return {"responses": 0, "cursor_restart": True}
    except ResponseTooLarge:
        if pending["kind"] != "page" or pending["request"]["params"]["limit"] == 1:
            raise
        state = copy.deepcopy(current_store.get_state(conn, "keyset_discovery"))
        progress = state["lanes"][pending["lane"]]["passes"][
            _pass_key(pending["lane"], pending["closed"])
        ]
        progress.update(cursor=None, last_id=0, page_limit=max(1, progress["page_limit"] // 2))
        conn.execute("BEGIN TRANSACTION")
        try:
            current_store.set_state(conn, "keyset_discovery", state)
            current_store.set_state(conn, "keyset_pending", None)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        return {"responses": 0, "page_restart": True}
    lane = pending["lane"]
    next_state = copy.deepcopy(current_store.get_state(conn, "keyset_discovery"))
    progress = next_state["lanes"][lane]["passes"][_pass_key(lane, pending["closed"])]
    if pending["kind"] == "highwater":
        rows = _records(response, lane, closed=pending["closed"])
        if len(rows) > 1:
            raise CurrentError("keyset highwater response exceeds limit")
        progress["highwater"] = int(rows[0]["id"]) if rows else 0
        conn.execute("BEGIN TRANSACTION")
        try:
            current_store.set_state(conn, "keyset_discovery", next_state)
            current_store.set_state(conn, "keyset_pending", None)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        return {"responses": 1, "records": 0, "highwater": progress["highwater"]}
    previous = progress["last_id"]
    if (
        not isinstance(response.json, dict)
        or len(response.json.get(lane, [])) > pending["request"]["params"]["limit"]
    ):
        raise CurrentError("keyset page exceeds its frozen limit")
    rows, cursor, last_id, crossed_highwater = _parse(
        response,
        lane,
        after=pending["request"]["params"].get("after_cursor"),
        previous_id=previous,
        highwater=pending["highwater"],
        closed=pending["closed"],
    )
    projected = project_response(
        rows,
        lane,
        received_at=manifest["received_at"],
        response_id=pending["raw_id"],
    )
    projected.raw_path = str(raw_dir / "p000001.manifest.json")
    progress.update(cursor=cursor, last_id=last_id)
    if (
        cursor is None
        or crossed_highwater
        or (pending["highwater"] > 0 and last_id >= pending["highwater"])
    ):
        progress.update(done=True, cursor=None)
        next_state["next_lane"] = (LANES.index(lane) + 1) % len(LANES)
    current_store.apply(
        conn,
        projected,
        {
            "keyset_discovery": next_state,
            "keyset_pending": None,
            **({"reconcile": pending["reconcile"]} if pending.get("reconcile") else {}),
        },
        attempts=(
            (
                lane,
                [int(row["id"]) for row in rows],
                datetime.fromisoformat(manifest["received_at"]),
            )
            if pending.get("reconcile") is not None
            else None
        ),
    )
    return {
        "responses": 1,
        "records": len(rows),
        "last_id": last_id,
        "complete": all(item["done"] for item in next_state["lanes"][lane]["passes"].values()),
    }


def cold_page(settings, conn, client, lane, *, reconcile_state, now, cycle_started=None):
    """Commit one frozen keyset page for a due cold-discovery lane."""
    validate_root(settings.root)
    from .current_reconcile import _initialize

    _initialize(conn)
    current_sync.require_acquisition_allowed(settings, conn)
    if lane not in LANES:
        raise CurrentError("unknown keyset lane")
    certificate = current_store.get_state(conn, "bulk_policy", {})
    if (
        certificate.get("version") != 3
        or certificate.get("origin") != settings.gamma.base_url
        or certificate.get("capabilities", {}).get("keyset_discovery", {}).get("supported")
        is not True
    ):
        raise CurrentError("a matching keyset capability certificate is required")
    _require_all_lifecycle_event_highwater(certificate)
    state = current_store.get_state(conn, "keyset_discovery") or {
        "version": 1,
        "next_lane": 0,
        "lanes": {
            key: {
                "cycle_started": None,
                "passes": {
                    _pass_key(key, closed): {
                        "highwater": None,
                        "cursor": None,
                        "last_id": 0,
                        "page_limit": PAGE_LIMIT,
                        "done": False,
                    }
                    for closed in _passes(key)
                },
            }
            for key in LANES
        },
    }
    _validate_state(state)
    pending = current_store.get_state(conn, "keyset_pending")
    if pending is not None:
        return resume_pending(settings, conn, client)
    lane_progress = state["lanes"][lane]
    window = (reconcile_state or {}).get("windows", {}).get(lane, {})
    if cycle_started is None:
        cycle_started = window.get("started_at")
    if lane_progress.get("cycle_started") != cycle_started:
        lane_progress["cycle_started"] = cycle_started
        for progress in lane_progress["passes"].values():
            progress.update(
                highwater=None, cursor=None, last_id=0, page_limit=PAGE_LIMIT, done=False
            )
    pass_keys = ["all"] if lane == "events" else ["open", "closed"]
    progress_key = next(
        (key for key in pass_keys if not lane_progress["passes"][key]["done"]), None
    )
    if progress_key is None:
        return {"responses": 0, "records": 0, "complete": True}
    closed_value = None if lane == "events" else progress_key == "closed"
    pass_key = progress_key
    progress = lane_progress["passes"][pass_key]
    if progress["highwater"] is None:
        params = {"limit": 1, "order": "id", "ascending": False}
        if closed_value is not None:
            params["closed"] = closed_value
        pending = {
            "kind": "highwater",
            "lane": lane,
            "closed": closed_value,
            "highwater": None,
            "certificate": certificate,
            "request": {"endpoint": f"/{lane}", "params": params},
            "raw_id": uuid.uuid4().hex,
            "previous_id": progress["last_id"],
            "reconcile": reconcile_state,
        }
    else:
        if progress["highwater"] == 0:
            progress["done"] = True
            current_store.set_state(conn, "keyset_discovery", state)
            complete = all(item["done"] for item in lane_progress["passes"].values())
            return {"responses": 0, "records": 0, "complete": complete}
        params = {"limit": progress["page_limit"], "order": "id", "ascending": True}
        if closed_value is not None:
            params["closed"] = closed_value
            params["include_tag"] = True
        if progress["cursor"] is not None:
            params["after_cursor"] = progress["cursor"]
        pending = {
            "kind": "page",
            "lane": lane,
            "closed": closed_value,
            "highwater": progress["highwater"],
            "certificate": certificate,
            "request": {"endpoint": f"/{lane}/keyset", "params": params},
            "raw_id": uuid.uuid4().hex,
            "previous_id": progress["last_id"],
            "reconcile": reconcile_state,
        }
    conn.execute("BEGIN TRANSACTION")
    try:
        current_store.set_state(conn, "keyset_discovery", state)
        current_store.set_state(conn, "keyset_pending", pending)
        if reconcile_state is not None:
            current_store.set_state(conn, "reconcile", reconcile_state)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    admit(settings, additional_bytes=65536)
    return resume_pending(settings, conn, client)
