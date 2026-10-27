"""Sealed, independently resumable Gamma ID windows with durable raw receipts."""

import copy
import json
import queue
import re
import threading
import time
import uuid
from datetime import datetime
from types import SimpleNamespace

from . import current_store
from .capture.writer import read_body, read_manifest, write_page
from .current_config import admit, publication_reserve, safe_path, validate_root
from .current_contract import PROJECTION_VERSION, CurrentError
from .current_rows import _incomplete, entity_id, project_response
from .gamma.http import (
    CaptureStopped,
    CursorExpired,
    MalformedResponse,
    RequestBudgetExceeded,
    ResponseTooLarge,
    RetriesExhausted,
)
from .gamma.paginators import _window_params, unpack
from .platform_support import is_symlink

LANES = ("events", "markets")
_RAW_ID = re.compile(r"[0-9a-f]{32}\Z")
POLICY = {
    "version": 2,
    "projection_version": PROJECTION_VERSION,
    "filter": {},
    "endpoints": {lane: f"/{lane}" for lane in LANES},
    "bulk": {lane: _window_params([1], lane, None) for lane in LANES},
    "order": "id",
    "ascending": False,
}


def _pending_manifest(directory, root):
    path = directory / "p000001.manifest.json"
    manifest = read_manifest(path, trusted_root=root)
    if manifest is None and (path.exists() or is_symlink(path)):
        raise CurrentError("corrupt pending response manifest")
    return manifest


def _safe_pending_raw_dir(settings, raw_id):
    if not isinstance(raw_id, str) or _RAW_ID.fullmatch(raw_id) is None:
        raise CurrentError("unsafe pending raw identity")
    raw_root = safe_path(settings.raw_dir)
    raw_dir = safe_path(raw_root / raw_id)
    if raw_dir.parent != raw_root:
        raise CurrentError("pending raw identity escaped the raw directory")
    return raw_dir


def _records(response, lane, wanted=None, *, native=False, closed=None):
    if response.status != 200:
        raise CurrentError(f"/{lane}: unexpected HTTP {response.status}")
    records, cursor = (
        ([response.json], None)
        if isinstance(response.json, dict) and "id" in response.json
        else unpack(response.json, lane)
    )
    if (
        isinstance(response.json, dict)
        and "id" not in response.json
        and _incomplete(response.json, len(records))
    ):
        raise CurrentError("incomplete bounded response envelope")
    if cursor is not None:
        raise CurrentError("bounded response contains a cursor")
    if (
        isinstance(response.json, dict)
        and "id" not in response.json
        and ({"events", "markets"} - {lane}) & response.json.keys()
    ):
        raise CurrentError("mismatched response envelope")
    seen = set()
    for record in records:
        if not isinstance(record, dict):
            raise CurrentError("nonrecord response item")
        key = entity_id(record.get("id"))
        if key in seen or (wanted is not None and int(key) not in wanted):
            raise CurrentError("duplicate or unrequested ID")
        seen.add(key)
        if (
            closed is not None
            and type(record.get("closed")) is bool
            and record["closed"] is not closed
        ):
            raise CurrentError("response lifecycle differs from frozen pass")
        for field in ("active", "closed", "archived"):
            if record.get(field) is not None and type(record[field]) is not bool:
                raise CurrentError(f"invalid {field}")
    if native and len(records) != 1:
        raise CurrentError("native response must contain its requested ID")
    return records


def _project(records, lane, response):
    return project_response(
        records, lane, received_at=response.received_at.isoformat(), response_id=uuid.uuid4().hex
    )


def _probe_pass(client, lane, ids, native, closed):
    response = client.get(f"/{lane}", _window_params(ids, lane, closed))
    records = _records(response, lane, ids, closed=closed)
    for record in records:
        if entity_id(record["id"]) not in native:
            raise CurrentError("bulk contains absent native control")
        left = _project([record], lane, response)
        right = _project([native[entity_id(record["id"])]], lane, response)
        if (left.events, left.markets, left.outcomes, left.memberships) != (
            right.events,
            right.markets,
            right.outcomes,
            right.memberships,
        ):
            raise CurrentError("bulk/native projection mismatch")
        if lane == "events" and any(v is None for v in left.memberships.values()):
            raise CurrentError("bulk event membership is incomplete")
    return {entity_id(record["id"]) for record in records}


def probe_policy(settings, client, *, event_ids, market_ids):
    """Certify each lane using bounded native open, closed and absent controls."""
    validate_root(settings.root)
    certificate = {**POLICY, "origin": settings.gamma.base_url, "lanes": {}}
    for lane, values in zip(LANES, (event_ids, market_ids), strict=True):
        if not isinstance(values, dict) or set(values) != {"open", "closed", "absent"}:
            raise CurrentError("probe requires explicit open, closed and absent controls")
        controls = {
            status: {int(entity_id(v)) for v in entries} for status, entries in values.items()
        }
        if any(not entries for entries in controls.values()):
            raise CurrentError("probe control groups cannot be empty")
        ids = sorted(set.union(*controls.values()))
        if len(ids) != sum(map(len, controls.values())):
            raise CurrentError("probe controls overlap")
        if not 3 <= len(ids) <= 100:
            raise CurrentError("probe requires 3..100 known open, closed and absent IDs")
        native = {}
        try:
            for key in ids:
                response = client.get(f"/{lane}/{key}", {})
                if response.status == 404:
                    if key not in controls["absent"]:
                        raise CurrentError("known control returned 404")
                    continue
                record = _records(response, lane, [key], native=True)[0]
                if key in controls["absent"] or record.get("closed") is not (
                    key in controls["closed"]
                ):
                    raise CurrentError("native lifecycle differs from control")
                native[str(key)] = record

            unfiltered = _probe_pass(client, lane, ids, native, None)
            passes = [None] if unfiltered == set(native) else [False, True]
            if passes == [False, True] and (
                _probe_pass(client, lane, ids, native, False)
                | _probe_pass(client, lane, ids, native, True)
                != set(native)
            ):
                raise CurrentError("bulk omissions differ from native controls")
            for closed in passes:
                params = {"limit": 1, "order": "id", "ascending": False}
                if closed is not None:
                    params["closed"] = closed
                high = _records(client.get(f"/{lane}", params), lane, closed=closed)
                known = [
                    int(key)
                    for key, row in native.items()
                    if closed is None or row.get("closed") is closed
                ]
                if len(high) != 1 or int(high[0]["id"]) < max(known):
                    raise CurrentError("pass highwater excludes a known control")
            certificate["lanes"][lane] = {
                "supported": True,
                "controls": ids,
                "passes": passes,
                "lifecycle_coverage": "known-controls-only",
            }
        except (CurrentError, ValueError, MalformedResponse) as exc:
            certificate["lanes"][lane] = {"supported": False, "error": str(exc)}
    return certificate


def _pending_work(conn):
    discovery = current_store.get_state(conn, "discovery", {})
    return (
        current_store.get_state(conn, "fetch_pending") is not None
        or (isinstance(discovery, dict) and discovery.get("pending") is not None)
        or current_store.get_state(conn, "keyset_pending") is not None
    )


def acquisition_block(conn):
    """Return the frozen certificate that must remain fenced after a reprobe."""
    return current_store.get_state(conn, "_acquisition_block")


def require_acquisition_allowed(settings, conn):
    block = acquisition_block(conn)
    if (
        isinstance(block, dict)
        and block.get("origin") == settings.gamma.base_url
        and block.get("certificate") == current_store.get_state(conn, "bulk_policy", {})
    ):
        raise CurrentError("acquisition is blocked until a successful policy reprobe")


def semantic_lane_proofs(lanes):
    """Ignore diagnostic metadata while comparing certified lifecycle coverage."""
    if not isinstance(lanes, dict):
        return None
    return {
        lane: {
            "supported": proof.get("supported") if isinstance(proof, dict) else None,
            "passes": proof.get("passes") if isinstance(proof, dict) else None,
        }
        for lane in LANES
        for proof in (lanes.get(lane),)
    }


def _same_projection(left, right, lane):
    stamp = "2000-01-01T00:00:00+00:00"
    a = project_response([left], lane, received_at=stamp, response_id="probe")
    b = project_response([right], lane, received_at=stamp, response_id="probe")
    return (a.events, a.markets, a.outcomes, a.memberships) == (
        b.events,
        b.markets,
        b.outcomes,
        b.memberships,
    )


def _probe_market_closed_any(client, ids, native):
    ids = sorted(int(entity_id(value)) for value in ids)
    if not 3 <= len(ids) <= 100:
        raise CurrentError("closed=any probe requires 3..100 market IDs")
    response = client.get("/markets", _window_params(ids, "markets", "any"))
    records = _records(response, "markets", ids)
    listed = {entity_id(row["id"]): row for row in records}
    expected = set(native)
    if set(listed) != expected:
        raise CurrentError("closed=any omissions differ from native controls")
    for key, record in listed.items():
        if not _same_projection(record, native[key], "markets"):
            raise CurrentError("closed=any/native projection mismatch")


def _probe_keyset_pass(client, lane, closed, expected_ids):
    params = {"limit": 10, "order": "id", "ascending": True}
    if closed is not None:
        params["closed"] = closed
    if lane == "markets":
        params["include_tag"] = True
    endpoint = f"/{lane}/keyset"
    previous = 0
    seen = set()
    cursor = None
    for _ in range(2):
        request_params = dict(params)
        if cursor is not None:
            request_params["after_cursor"] = cursor
        response = client.get(endpoint, request_params)
        if response.status != 200 or not isinstance(response.json, dict):
            raise CurrentError("keyset endpoint did not return a page envelope")
        records, next_cursor = unpack(response.json, lane)
        if set(response.json) - {lane, "next_cursor"} or len(records) > params["limit"]:
            raise CurrentError("invalid keyset page envelope")
        for record in records:
            key = entity_id(record.get("id"))
            number = int(key)
            if number <= previous or key in seen:
                raise CurrentError("keyset page IDs are not strictly increasing")
            checked = _records(SimpleNamespace(status=200, json=record), lane, closed=closed)[0]
            if lane == "markets" and (
                type(record.get("closed")) is not bool or record["closed"] is not closed
            ):
                raise CurrentError("keyset market lifecycle differs from frozen pass")
            native_response = client.get(f"/{lane}/{key}", {})
            native = _records(native_response, lane, [number], native=True)[0]
            if not _same_projection(checked, native, lane):
                raise CurrentError("keyset/native projection mismatch")
            if lane == "events" and any(
                value is None for value in _project([checked], lane, response).memberships.values()
            ):
                raise CurrentError("keyset event membership is incomplete")
            seen.add(key)
            previous = number
        if next_cursor is not None and (not next_cursor or next_cursor == cursor):
            raise CurrentError("keyset cursor did not advance")
        if next_cursor is None:
            break
        cursor = next_cursor
    expected = sorted({entity_id(value) for value in expected_ids}, key=int)
    for offset in range(0, len(expected), 100):
        batch = expected[offset : offset + 100]
        control_params = {"limit": len(batch), "order": "id", "ascending": True, "id": batch}
        if closed is not None:
            control_params["closed"] = closed
        if lane == "markets":
            control_params["include_tag"] = True
        cursor = None
        last_control_id = 0
        seen_controls = set()
        for _ in range(len(batch) + 1):
            request_params = dict(control_params)
            if cursor is not None:
                request_params["after_cursor"] = cursor
            response = client.get(endpoint, request_params)
            if response.status != 200 or not isinstance(response.json, dict):
                raise CurrentError("keyset control query did not return a page envelope")
            control_rows, next_cursor = unpack(response.json, lane)
            if set(response.json) - {lane, "next_cursor"} or len(control_rows) > len(batch):
                raise CurrentError("invalid keyset control page envelope")
            for record in control_rows:
                checked = _records(SimpleNamespace(status=200, json=record), lane, closed=closed)[0]
                key = entity_id(checked["id"])
                if key not in batch or key in seen_controls or int(key) <= last_control_id:
                    raise CurrentError("keyset control query returned duplicate or unordered IDs")
                if lane == "markets" and (
                    type(record.get("closed")) is not bool or record["closed"] is not closed
                ):
                    raise CurrentError(
                        "keyset market control lifecycle differs from requested pass"
                    )
                native_response = client.get(f"/{lane}/{key}", {})
                native = _records(native_response, lane, [int(key)], native=True)[0]
                if not _same_projection(checked, native, lane):
                    raise CurrentError("keyset control/native projection mismatch")
                seen_controls.add(key)
                last_control_id = int(key)
            if next_cursor is None:
                break
            if not next_cursor or next_cursor == cursor:
                raise CurrentError("keyset control cursor did not advance")
            cursor = next_cursor
        if seen_controls != set(batch):
            raise CurrentError("keyset control query omitted a known ID")


def _probe_keyset(client, event_ids, market_ids):
    for lane in LANES:
        for closed in (None,) if lane == "events" else (False, True):
            controls = event_ids if lane == "events" else market_ids
            expected = (
                [*controls["open"], *controls["closed"]]
                if lane == "events"
                else controls["closed" if closed else "open"]
            )
            _probe_keyset_pass(client, lane, closed, expected)


def probe_api_optimizations(
    settings,
    conn,
    client,
    *,
    event_ids,
    market_ids,
    market_closed_any=True,
    keyset_discovery=True,
):
    """Probe optional API capabilities without replacing a working certificate on failure."""
    validate_root(settings.root)
    if _pending_work(conn):
        raise CurrentError("cannot replace acquisition certificate while work is pending")
    base = probe_policy(settings, client, event_ids=event_ids, market_ids=market_ids)
    if not all(lane.get("supported") for lane in base["lanes"].values()):
        raise CurrentError("base provider policy failed; existing certificate retained")
    capabilities = {}
    if market_closed_any:
        try:
            native = {}
            groups = market_ids
            for status in ("open", "closed"):
                for value in groups[status]:
                    key = entity_id(value)
                    response = client.get(f"/markets/{key}", {})
                    row = _records(response, "markets", [int(key)], native=True)[0]
                    if row.get("closed") is not (status == "closed"):
                        raise CurrentError("native lifecycle differs from control")
                    native[key] = row
            absent = {entity_id(value) for value in groups["absent"]}
            for value in absent:
                response = client.get(f"/markets/{value}", {})
                if response.status != 404:
                    raise CurrentError("absent native control did not return 404")
            _probe_market_closed_any(client, [*native, *absent], native)
            capabilities["market_closed_any"] = {
                "supported": True,
                "controls": sorted([*native, *absent], key=int),
                "lifecycle_coverage": "known-controls-only",
            }
        except (
            CurrentError,
            ValueError,
            MalformedResponse,
            RetriesExhausted,
            CursorExpired,
        ) as exc:
            capabilities["market_closed_any"] = {"supported": False, "error": str(exc)}
    if keyset_discovery:
        event_passes = base["lanes"].get("events", {}).get("passes")
        if event_passes != [None]:
            # Keyset bootstrap freezes one unfiltered event highwater.  It is
            # safe only when the base certificate proves that endpoint covers
            # every lifecycle; split legacy passes cannot establish that.
            capabilities["keyset_discovery"] = {
                "supported": False,
                "error": "keyset event highwater requires certified all-lifecycle event coverage",
            }
        else:
            try:
                from . import current_keyset

                _probe_keyset(client, event_ids, market_ids)
                capabilities["keyset_discovery"] = {
                    "supported": True,
                    "page_limit": current_keyset.PAGE_LIMIT,
                    "order": "id-ascending",
                    "lifecycle_coverage": "events-all-markets-open-and-closed",
                    "proof": "two-page-native-parity",
                }
            except (
                CurrentError,
                ValueError,
                MalformedResponse,
                RetriesExhausted,
                CursorExpired,
            ) as exc:
                capabilities["keyset_discovery"] = {"supported": False, "error": str(exc)}
    return {
        "version": 3,
        "policy": POLICY,
        "origin": settings.gamma.base_url,
        "lanes": base["lanes"],
        "capabilities": capabilities,
    }


def _certificate(settings, conn, lane):
    require_acquisition_allowed(settings, conn)
    certificate = current_store.get_state(conn, "bulk_policy", {})
    v3 = certificate.get("version") == 3
    policy = certificate.get("policy") if v3 else certificate
    if (
        (
            v3
            and (not isinstance(policy, dict) or any(policy.get(k) != v for k, v in POLICY.items()))
        )
        or (not v3 and any(certificate.get(k) != v for k, v in POLICY.items()))
        or certificate.get("origin") != settings.gamma.base_url
        or certificate.get("lanes", {}).get(lane, {}).get("supported") is not True
    ):
        raise CurrentError("a matching bulk policy probe certificate is required")
    passes = certificate["lanes"][lane].get("passes")
    if passes != [None] and not (
        isinstance(passes, list) and len(passes) == 2 and passes[0] is False and passes[1] is True
    ):
        raise CurrentError("certificate has unsupported lifecycle passes")
    return certificate, passes


def market_closed_any_supported(settings, conn):
    certificate = current_store.get_state(conn, "bulk_policy", {})
    return (
        certificate.get("version") == 3
        and certificate.get("origin") == settings.gamma.base_url
        and certificate.get("policy") == POLICY
        and certificate.get("capabilities", {}).get("market_closed_any", {}).get("supported")
        is True
    )


def _api_certificate_version(conn):
    return 3 if current_store.get_state(conn, "bulk_policy", {}).get("version") == 3 else 2


def _pending_policy(conn):
    return (
        {**POLICY, "acquisition_certificate_version": 3}
        if _api_certificate_version(conn) == 3
        else POLICY
    )


def _policy_matches(certificate):
    policy = certificate.get("policy") if certificate.get("version") == 3 else certificate
    return isinstance(policy, dict) and all(policy.get(k) == v for k, v in POLICY.items())


def validate_discovery_pending(settings, conn, work):
    """Validate saved highwater requests against their original certificate."""
    try:
        lane = work["lane"]
        certificate, passes = _certificate(settings, conn, lane)
        if (
            work["kind"] != "highwater"
            or work["policy"] != POLICY
            or work["certificate"] != certificate
            or type(work["maximum"]) is not int
            or work["maximum"] < 0
            or not isinstance(work["heads"], list)
            or any(type(head) is not int or head < 0 for head in work["heads"])
            or work["maximum"] != max(work["heads"], default=0)
            or len(work["heads"]) + len(work["remaining"]) != len(passes)
        ):
            raise CurrentError("incompatible pending discovery policy")
        _safe_pending_raw_dir(settings, work.get("raw_id"))
        requests = []
        for closed in passes:
            params = {"limit": 1, "order": "id", "ascending": False}
            if closed is not None:
                params["closed"] = closed
            requests.append({"endpoint": f"/{lane}", "params": params})
        if not any(work["remaining"] == requests[i:] for i in range(len(requests))):
            raise CurrentError("invalid frozen pending highwater request")
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise CurrentError("incompatible pending discovery; explicit recovery required") from exc


def discover(settings, conn, client, *, deadline):
    """Freeze pass heads, then commit each whole numeric window atomically."""
    validate_root(settings.root)
    require_acquisition_allowed(settings, conn)
    certificate = current_store.get_state(conn, "bulk_policy", {})
    if not _policy_matches(certificate) or certificate.get("origin") != settings.gamma.base_url:
        raise CurrentError("a matching bulk policy probe certificate is required")
    for lane in LANES:
        if certificate.get("lanes", {}).get(lane, {}).get("supported") is True:
            _certificate(settings, conn, lane)
    state = current_store.get_state(
        conn,
        "discovery",
        {
            "version": 2,
            "next_lane": 0,
            "lanes": {
                lane: {"frontier": 0, "highwater": None, "phase": "discover"} for lane in LANES
            },
            "pending": None,
        },
    )
    if state.get("version") != 2:
        raise CurrentError("incompatible discovery policy; explicit reset required")
    reserve = publication_reserve(settings, conn)
    stop = deadline - reserve
    counters = {"requests": 0, "responses": 0, "records": 0}
    visited = set()
    keyset_capability = certificate.get("capabilities", {}).get("keyset_discovery", {})
    bootstrap_state = current_store.get_state(conn, "keyset_bootstrap_complete", False)
    initial_frontiers = all(
        item.get("frontier") == 0 and item.get("highwater") is None
        for item in state["lanes"].values()
    )
    if (
        keyset_capability.get("supported") is True
        and bootstrap_state is not True
        and state["pending"] is None
        and initial_frontiers
    ):
        from .current_keyset import cold_page

        # Bootstrap pages share the validated durable page machinery, while the
        # private marker keeps their lifecycle passes separate from later cold slots.
        key_state = current_store.get_state(conn, "keyset_discovery")
        next_lane = 0 if key_state is None else key_state.get("next_lane", 0)
        while time.monotonic() < stop:
            key_state = current_store.get_state(conn, "keyset_discovery")
            if key_state is not None and all(
                progress["done"]
                for lane_progress in key_state["lanes"].values()
                for progress in lane_progress["passes"].values()
            ):
                break
            lanes = list(LANES)
            lane = lanes[next_lane % len(lanes)]
            try:
                result = cold_page(
                    settings,
                    conn,
                    client,
                    lane,
                    reconcile_state=None,
                    now=None,
                    cycle_started="bootstrap-v1",
                )
            except (CaptureStopped, RequestBudgetExceeded):
                break
            counters["responses"] += result.get("responses", 0)
            counters["records"] += result.get("records", 0)
            key_state = current_store.get_state(conn, "keyset_discovery")
            next_lane = (LANES.index(lane) + 1) % len(LANES)
            if result.get("complete"):
                next_lane = (LANES.index(lane) + 1) % len(LANES)
            # Stop after a network-free empty highwater completion only if the
            # other lane still has work; the loop's persisted state is authoritative.
        key_state = current_store.get_state(conn, "keyset_discovery")
        if key_state is not None and all(
            progress["done"]
            for lane_progress in key_state["lanes"].values()
            for progress in lane_progress["passes"].values()
        ):
            event_highwater = key_state["lanes"]["events"]["passes"]["all"]["highwater"] or 0
            market_highwater = max(
                key_state["lanes"]["markets"]["passes"][name]["highwater"] or 0
                for name in ("open", "closed")
            )
            next_state = copy.deepcopy(state)
            next_state["lanes"]["events"]["frontier"] = event_highwater
            next_state["lanes"]["markets"]["frontier"] = market_highwater
            current_store.set_state(conn, "discovery", next_state)
            current_store.set_state(conn, "keyset_bootstrap_complete", True)
            state = next_state
        elif initial_frontiers:
            # Do not create a numeric highwater unit while the keyset bootstrap
            # has a durable page pending; resume that exact page first.
            return {
                **counters,
                "frontiers": state["lanes"],
                "pending": state["pending"],
                "backlog": sum(
                    max(0, (value["highwater"] or value["frontier"]) - value["frontier"])
                    for value in state["lanes"].values()
                ),
                "source_complete": False,
            }
    while time.monotonic() < stop:
        pending = state["pending"]
        if pending is None:
            choices = [
                i
                for i, lane in enumerate(LANES)
                if lane not in visited
                and certificate.get("lanes", {}).get(lane, {}).get("supported") is True
            ]
            if not choices:
                break
            index = state["next_lane"] if state["next_lane"] in choices else choices[0]
            lane = LANES[index]
            _, passes = _certificate(settings, conn, lane)
            progress = state["lanes"][lane]
            if progress["highwater"] is not None:
                end = min(progress["frontier"] + 100, progress["highwater"])
                next_state = copy.deepcopy(state)
                next_progress = next_state["lanes"][lane]
                next_progress["frontier"] = end
                if end == progress["highwater"]:
                    next_progress.update(highwater=None, phase="tail")
                next_state["next_lane"] = 1 - index
                try:
                    result = fetch_apply(
                        settings,
                        conn,
                        client,
                        lane,
                        list(range(progress["frontier"] + 1, end + 1)),
                        progress={"discovery": next_state},
                    )
                except (RequestBudgetExceeded, CaptureStopped):
                    break
                counters["responses"] += result["responses"]
                counters["requests"] += result["requests"]
                counters["records"] += result["records"]
                state = next_state
                if end == progress["highwater"]:
                    visited.add(lane)
                continue
            requests = []
            for closed in passes:
                params = {"limit": 1, "order": "id", "ascending": False}
                if closed is not None:
                    params["closed"] = closed
                requests.append({"endpoint": f"/{lane}", "params": params})
            pending = {
                "lane": lane,
                "kind": "highwater",
                "policy": POLICY,
                "certificate": certificate,
                "remaining": requests,
                "maximum": 0,
                "heads": [],
                "raw_id": uuid.uuid4().hex,
            }
            state["pending"] = pending
            admit(settings, additional_bytes=65536)
            current_store.set_state(conn, "discovery", state)
        lane = pending["lane"]
        validate_discovery_pending(settings, conn, pending)
        request = pending["remaining"][0]
        raw_dir = settings.raw_dir / pending["raw_id"]
        manifest = _pending_manifest(raw_dir, settings.root)
        if manifest is None:
            try:
                response = client.get(request["endpoint"], request["params"])
                counters["requests"] += 1
            except (RequestBudgetExceeded, CaptureStopped):
                break
            records = _records(response, lane, closed=request["params"].get("closed"))
            if len(records) > 1:
                raise CurrentError("highwater response exceeds limit")
            _project(records, lane, response)
            _write_response(settings, raw_dir, response, request)
            manifest = _pending_manifest(raw_dir, settings.root)
        if manifest["request"] != request:
            raise CurrentError("raw request receipt mismatch")
        body = read_body(
            raw_dir,
            manifest,
            trusted_root=settings.root,
            max_body_bytes=settings.limits.max_response_bytes,
        )
        records = _records(
            SimpleNamespace(status=manifest["status"], json=json.loads(body)),
            lane,
            closed=request["params"].get("closed"),
        )
        if len(records) > 1:
            raise CurrentError("highwater response exceeds limit")
        rows = project_response(
            records, lane, received_at=manifest["received_at"], response_id=pending["raw_id"]
        )
        rows.raw_path = str(raw_dir / "p000001.manifest.json")
        next_state = copy.deepcopy(state)
        next_pending = next_state["pending"]
        next_pending["heads"].append(int(records[0]["id"]) if records else 0)
        next_pending["maximum"] = max(next_pending["heads"])
        next_pending["remaining"] = next_pending["remaining"][1:]
        next_pending["raw_id"] = uuid.uuid4().hex
        if not next_pending["remaining"]:
            highwater = next_pending["maximum"]
            progress = next_state["lanes"][lane]
            if highwater < progress["frontier"]:
                raise CurrentError("source highwater regressed")
            progress["highwater"] = highwater
            if highwater == progress["frontier"]:
                visited.add(lane)
                progress.update(highwater=None, phase="tail")
            next_state["pending"] = None
            next_state["next_lane"] = 1 - LANES.index(lane)
        admit(settings, additional_bytes=max(65536, len(body) * 4))
        current_store.apply(conn, rows, {"discovery": next_state})
        state = next_state
        counters["responses"] += 1
        counters["records"] += len(records)
    return {
        **counters,
        "frontiers": state["lanes"],
        "pending": state["pending"],
        "backlog": sum(
            max(0, (v["highwater"] or v["frontier"]) - v["frontier"])
            for v in state["lanes"].values()
        ),
        "source_complete": False,
    }


def _write_response(settings, raw_dir, response, request):
    if len(response.body) > settings.limits.max_response_bytes:
        raise CurrentError("response body allowance exceeded")
    admit(settings, additional_bytes=2 * len(response.body) + 65536)
    write_page(
        raw_dir,
        1,
        response.body,
        {
            "request": request,
            "received_at": response.received_at.isoformat(),
            "status": response.status,
        },
    )
    settings.meter.add(len(response.body))


class ResponsePrefetcher:
    """Fetch the next known pass request while the current response is applied.

    The worker shares the parent's rate limit and stats. Depth is two bodies.
    A body is used only when its request matches the frozen one; anything else
    is discarded and still counts, because the worker already performed the GET.
    """

    def __init__(self, client, *, deadline):
        self.worker = client.spawn()
        self.deadline = deadline
        self.discarded = 0
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._pending = []
        self._inflight = None
        self._done = queue.Queue()
        self._thread = threading.Thread(target=self._run, name="catalogue-prefetch", daemon=True)
        self._thread.start()

    def submit(self, request):
        if self._stop.is_set() or time.monotonic() >= self.deadline:
            return
        with self._lock:
            if len(self._pending) >= 2 or self._inflight == request or request in self._pending:
                return
            self._pending.append(request)
        self._wake.set()

    def _run(self):
        while not self._stop.is_set():
            with self._lock:
                request = self._pending.pop(0) if self._pending else None
                if request is not None:
                    self._inflight = request
            if request is None:
                self._wake.wait(0.05)
                self._wake.clear()
                continue
            if self._stop.is_set() or time.monotonic() >= self.deadline:
                self._abandon()
                return
            try:
                payload = self.worker.get(request["endpoint"], request["params"])
            except BaseException as exc:
                payload = exc
            with self._lock:
                self._done.put((request, payload))
                self._inflight = None
        self._abandon()

    def _abandon(self):
        with self._lock:
            self._pending.clear()
            self._inflight = None

    def take(self, request):
        held = []
        try:
            while True:
                with self._lock:
                    try:
                        got, payload = self._done.get_nowait()
                    except queue.Empty:
                        waiting = request in self._pending or self._inflight == request
                        item = None
                    else:
                        waiting = False
                        item = (got, payload)
                if item is None:
                    if waiting and self._thread.is_alive() and not self._stop.is_set():
                        self._wake.wait(0.01)
                        continue
                    return None
                got, payload = item
                if got == request:
                    if isinstance(payload, BaseException):
                        raise payload
                    return payload
                held.append((got, payload))
        finally:
            for item in held:
                self._done.put(item)

    def close(self):
        self._stop.set()
        self._wake.set()
        self._abandon()
        self.worker._deadline = min(getattr(self.worker, "_deadline", 0), self.worker._clock())
        self._thread.join(timeout=5)
        self.worker.close()
        while True:
            try:
                self._done.get_nowait()
            except queue.Empty:
                break
            self.discarded += 1


def first_bulk_request(settings, conn, lane, ids):
    """The first certified list request for these ids, matching ``fetch_unit``."""
    _certificate_value, passes = _certificate(settings, conn, lane)
    wanted = [int(entity_id(value)) for value in ids]
    return {"endpoint": f"/{lane}", "params": _window_params(wanted, lane, passes[0])}


def fetch_apply(
    settings, conn, client, kind, ids, *, progress=None, native=False, absence=False, attempts=None
):
    """Persist and apply a bounded repair unit; callers own its scheduling checkpoint."""
    return fetch_unit(
        settings,
        conn,
        client,
        kind,
        ids,
        progress=progress,
        native=native,
        absence=absence,
        attempts=attempts,
    )[0]


def fetch_unit(
    settings,
    conn,
    client,
    kind,
    ids,
    *,
    progress=None,
    native=False,
    absence=False,
    attempts=None,
    prefetcher=None,
):
    """Like ``fetch_apply``, also returning the bulk IDs no listing contained.

    An omission is not absence. A forced pass settles each omitted known entity with a
    native lookup, where only a 404 proves it. ``absence`` records those 404s as receipts;
    it is stored with the pending unit, so a resumed unit keeps the mode it started with.
    """
    validate_root(settings.root)
    require_acquisition_allowed(settings, conn)
    lane = {"event": "events", "market": "markets"}.get(kind, kind)
    if lane not in LANES:
        raise CurrentError("unknown repair lane")
    wanted = [int(entity_id(value)) for value in ids]
    if not wanted or len(wanted) > 100 or len(set(wanted)) != len(wanted):
        raise CurrentError("repair requests require 1..100 distinct IDs")
    certificate, passes = (None, [None]) if native else _certificate(settings, conn, lane)
    if not native and lane == "markets" and market_closed_any_supported(settings, conn):
        passes = ["any"]
    existing = current_store.get_state(conn, "fetch_pending")
    identity = {
        "lane": lane,
        "ids": wanted,
        "native": native,
        "progress": progress or {},
        "policy": _pending_policy(conn),
        "certificate": certificate,
        "passes": passes,
        "api_certificate_version": _api_certificate_version(conn),
    }
    attempt_metadata = (
        _pending_attempts_metadata(attempts, lane, wanted, native=native)
        if attempts is not None
        else None
    )
    if existing is not None:
        validate_fetch_pending(settings, conn, existing)
        expected_identity = dict(identity)
        if "api_certificate_version" not in existing["identity"]:
            expected_identity.pop("api_certificate_version")
        if existing["identity"] != expected_identity:
            raise CurrentError("resume the pending repair before starting another")
        if attempt_metadata is not None:
            if "attempts" in existing:
                if existing["attempts"] != attempt_metadata:
                    raise CurrentError("resume the pending repair before starting another")
            else:
                existing = {**existing, "attempts": attempt_metadata}
    requests = (
        [{"endpoint": f"/{lane}/{key}", "params": {}} for key in wanted]
        if native
        else [
            {"endpoint": f"/{lane}", "params": _window_params(wanted, lane, closed)}
            for closed in passes
        ]
    )
    work = existing or {
        "identity": identity,
        "remaining": requests,
        "raw_id": uuid.uuid4().hex,
        "absence": bool(absence) and native,
        **({"attempts": attempt_metadata} if attempt_metadata is not None else {}),
    }
    if attempts is None:
        attempts = _pending_attempts_from_work(work)
    absence = work.get("absence", False)
    admit(settings, additional_bytes=65536)
    current_store.set_state(conn, "fetch_pending", work)
    responses = requests_made = record_count = 0
    listed = set()
    while work["remaining"]:
        request = work["remaining"][0]
        if prefetcher is not None and len(work["remaining"]) > 1:
            prefetcher.submit(work["remaining"][1])
        endpoint, params = request["endpoint"], request["params"]
        batch = [int(endpoint.rsplit("/", 1)[1])] if native else params["id"]
        raw_dir = settings.raw_dir / work["raw_id"]
        manifest = _pending_manifest(raw_dir, settings.root)
        if manifest is None:
            try:
                response = prefetcher.take(request) if prefetcher is not None else None
                if response is None:
                    response = client.get(endpoint, params)
                requests_made += 1
            except ResponseTooLarge:
                if len(batch) == 1:
                    raise
                middle = len(batch) // 2
                work["remaining"][:1] = [
                    {"endpoint": endpoint, "params": {**params, "id": part, "limit": len(part)}}
                    for part in (batch[:middle], batch[middle:])
                ]
                current_store.set_state(conn, "fetch_pending", work)
                continue
            if native and response.status == 404:
                records = []
            else:
                records = _records(
                    response,
                    lane,
                    batch,
                    native=native,
                    closed=None if params.get("closed") == "any" else params.get("closed"),
                )
            _project(records, lane, response)
            _write_response(settings, raw_dir, response, request)
            manifest = _pending_manifest(raw_dir, settings.root)
        if manifest["request"] != {"endpoint": endpoint, "params": params}:
            raise CurrentError("raw repair request mismatch")
        body = read_body(
            raw_dir,
            manifest,
            trusted_root=settings.root,
            max_body_bytes=settings.limits.max_response_bytes,
        )
        if native and manifest["status"] == 404:
            records = []
        else:
            records = _records(
                SimpleNamespace(status=manifest["status"], json=json.loads(body)),
                lane,
                batch,
                native=native,
                closed=None if params.get("closed") == "any" else params.get("closed"),
            )
        if not native:
            listed.update(entity_id(record["id"]) for record in records)
        rows = project_response(
            records,
            lane,
            received_at=manifest["received_at"],
            response_id=work["raw_id"],
            native_market_ids=batch if native and lane == "markets" else (),
        )
        rows.raw_path = str(raw_dir / "p000001.manifest.json")
        if absence and manifest["status"] == 404:
            absent = rows.absent_event_ids if lane == "events" else rows.absent_market_ids
            absent.update(entity_id(key) for key in batch)
        next_work = {**work, "remaining": work["remaining"][1:], "raw_id": uuid.uuid4().hex}
        checkpoint = {"fetch_pending": next_work if next_work["remaining"] else None}
        if not next_work["remaining"]:
            checkpoint.update(progress or {})
        admit(settings, additional_bytes=max(65536, 4 * len(body)))
        current_store.apply(
            conn, rows, checkpoint, attempts=attempts if not next_work["remaining"] else None
        )
        responses += 1
        record_count += len(records)
        work = next_work
    omitted = [] if native else sorted(set(map(str, wanted)) - listed, key=int)
    result = {"responses": responses, "requests": requests_made, "records": record_count}
    return result, omitted


def validate_fetch_pending(settings, conn, work):
    """Reject incompatible work rather than regenerate a saved request."""
    try:
        identity = work["identity"]
        lane, native = identity["lane"], identity["native"]
        if lane not in LANES or type(native) is not bool:
            raise CurrentError("incompatible pending repair policy")
        api_version = identity.get("api_certificate_version", 2)
        expected_policy = (
            {**POLICY, "acquisition_certificate_version": 3} if api_version == 3 else POLICY
        )
        if identity["policy"] != expected_policy:
            raise CurrentError("incompatible pending repair policy")
        if api_version != _api_certificate_version(conn):
            raise CurrentError("incompatible pending acquisition certificate")
        absence = work.get("absence", False)
        if type(absence) is not bool or (absence and not native):
            raise CurrentError("incompatible pending repair policy")
        _safe_pending_raw_dir(settings, work.get("raw_id"))
        certificate, passes = (None, [None]) if native else _certificate(settings, conn, lane)
        if not native and lane == "markets" and market_closed_any_supported(settings, conn):
            passes = ["any"]
        if identity["certificate"] != certificate or identity["passes"] != passes:
            raise CurrentError("incompatible pending repair certificate")
        ids = identity["ids"]
        if not ids or len(ids) > 100 or len(set(ids)) != len(ids):
            raise CurrentError("invalid pending repair IDs")
        _pending_attempts_from_work(work)
        if not work["remaining"]:
            raise CurrentError("empty pending repair")
        for request in work["remaining"]:
            params = request["params"]
            if native:
                key = int(request["endpoint"].rsplit("/", 1)[1])
                valid = key in ids and request == {"endpoint": f"/{lane}/{key}", "params": {}}
            else:
                batch = params["id"]
                closed = params.get("closed")
                valid = (
                    bool(batch)
                    and len(batch) <= 100
                    and len(batch) == len(set(batch))
                    and set(batch) <= set(ids)
                    and any(type(closed) is type(value) and closed == value for value in passes)
                    and request
                    == {"endpoint": f"/{lane}", "params": _window_params(batch, lane, closed)}
                )
            if not valid:
                raise CurrentError("invalid frozen pending request")
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise CurrentError("incompatible pending repair; explicit recovery required") from exc


def resume_pending(settings, conn, client):
    """Resume a durable repair using only its frozen request and checkpoint."""
    if current_store.get_state(conn, "keyset_pending") is not None:
        if current_store.get_state(conn, "fetch_pending") is not None:
            raise CurrentError("multiple frozen acquisition requests require recovery")
        from .current_keyset import resume_pending as resume_keyset

        return resume_keyset(settings, conn, client)
    work = current_store.get_state(conn, "fetch_pending")
    if work is None:
        return {"responses": 0}
    validate_fetch_pending(settings, conn, work)
    identity = work["identity"]
    return fetch_apply(
        settings,
        conn,
        client,
        identity["lane"],
        identity["ids"],
        progress=identity["progress"],
        native=identity["native"],
        attempts=_pending_attempts_from_work(work),
    )


def _pending_attempts_metadata(attempts, request_lane, ids, *, native):
    """Encode the scheduler attempt committed with the final response as JSON."""
    try:
        lane, attempt_ids, when = attempts
        if (
            lane not in (*LANES, "native")
            or not isinstance(attempt_ids, (list, tuple))
            or not isinstance(when, datetime)
            or when.utcoffset() is None
        ):
            raise ValueError
        metadata = [lane, list(attempt_ids), when.isoformat()]
        _pending_attempts_from_metadata(
            metadata, {"lane": request_lane, "native": native, "ids": ids}
        )
        return metadata
    except (TypeError, ValueError, OverflowError) as exc:
        raise CurrentError("invalid scheduler attempt metadata") from exc


def _pending_attempts_from_work(work):
    """Validate and restore optional frozen attempt metadata; old checkpoints omit it."""
    if "attempts" not in work:
        return None
    return _pending_attempts_from_metadata(work["attempts"], work["identity"])


def _pending_attempts_from_metadata(metadata, identity):
    try:
        lane, ids, timestamp = metadata
        when = datetime.fromisoformat(timestamp)
        valid_ids = (
            type(ids) is list
            and bool(ids)
            and len(ids) <= 100
            and all(type(value) in (int, str) for value in ids)
            and len({str(value) for value in ids}) == len(ids)
            and {str(value) for value in ids} == {str(value) for value in identity["ids"]}
        )
        valid_lane = lane == identity["lane"] or (
            lane == "native" and identity["lane"] == "markets" and identity["native"]
        )
        if (
            not valid_lane
            or not valid_ids
            or when.tzinfo is None
            or when.utcoffset() is None
            or timestamp != when.isoformat()
        ):
            raise ValueError
        return lane, ids, when
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise CurrentError("incompatible pending scheduler attempt metadata") from exc
