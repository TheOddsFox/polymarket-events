"""Sealed, independently resumable Gamma ID windows with durable raw receipts."""

import copy
import json
import time
import uuid
from types import SimpleNamespace

from . import current_store
from .capture.writer import read_body, read_manifest, write_page
from .current_config import admit, validate_root
from .current_contract import PROJECTION_VERSION, CurrentError
from .current_rows import _incomplete, entity_id, project_response
from .gamma.http import CaptureStopped, MalformedResponse, RequestBudgetExceeded, ResponseTooLarge
from .gamma.paginators import _window_params, unpack

LANES = ("events", "markets")
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
    if manifest is None and (path.exists() or path.is_symlink()):
        raise CurrentError("corrupt pending response manifest")
    return manifest


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


def _certificate(settings, conn, lane):
    certificate = current_store.get_state(conn, "bulk_policy", {})
    if (
        any(certificate.get(k) != v for k, v in POLICY.items())
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
    certificate = current_store.get_state(conn, "bulk_policy", {})
    if (
        any(certificate.get(k) != v for k, v in POLICY.items())
        or certificate.get("origin") != settings.gamma.base_url
    ):
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
    reserve = max(
        settings.limits.publication_reserve_s,
        1.5 * current_store.get_state(conn, "previous_publish_seconds", 0),
    )
    stop = deadline - reserve
    counters = {"requests": 0, "responses": 0, "records": 0}
    visited = set()
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


def fetch_apply(settings, conn, client, kind, ids, *, progress=None, native=False):
    """Persist and apply a bounded repair unit; callers own its scheduling checkpoint."""
    validate_root(settings.root)
    lane = {"event": "events", "market": "markets"}.get(kind, kind)
    if lane not in LANES:
        raise CurrentError("unknown repair lane")
    wanted = [int(entity_id(value)) for value in ids]
    if not wanted or len(wanted) > 100 or len(set(wanted)) != len(wanted):
        raise CurrentError("repair requests require 1..100 distinct IDs")
    certificate, passes = (None, [None]) if native else _certificate(settings, conn, lane)
    existing = current_store.get_state(conn, "fetch_pending")
    identity = {
        "lane": lane,
        "ids": wanted,
        "native": native,
        "progress": progress or {},
        "policy": POLICY,
        "certificate": certificate,
        "passes": passes,
    }
    if existing is not None:
        validate_fetch_pending(settings, conn, existing)
        if existing["identity"] != identity:
            raise CurrentError("resume the pending repair before starting another")
    requests = (
        [{"endpoint": f"/{lane}/{key}", "params": {}} for key in wanted]
        if native
        else [
            {"endpoint": f"/{lane}", "params": _window_params(wanted, lane, closed)}
            for closed in passes
        ]
    )
    work = existing or {"identity": identity, "remaining": requests, "raw_id": uuid.uuid4().hex}
    admit(settings, additional_bytes=65536)
    current_store.set_state(conn, "fetch_pending", work)
    responses = requests_made = record_count = 0
    while work["remaining"]:
        request = work["remaining"][0]
        endpoint, params = request["endpoint"], request["params"]
        batch = [int(endpoint.rsplit("/", 1)[1])] if native else params["id"]
        raw_dir = settings.raw_dir / work["raw_id"]
        manifest = _pending_manifest(raw_dir, settings.root)
        if manifest is None:
            try:
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
                    response, lane, batch, native=native, closed=params.get("closed")
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
                closed=params.get("closed"),
            )
        rows = project_response(
            records,
            lane,
            received_at=manifest["received_at"],
            response_id=work["raw_id"],
            native_market_ids=batch if native and lane == "markets" else (),
        )
        rows.raw_path = str(raw_dir / "p000001.manifest.json")
        next_work = {**work, "remaining": work["remaining"][1:], "raw_id": uuid.uuid4().hex}
        checkpoint = {"fetch_pending": next_work if next_work["remaining"] else None}
        if not next_work["remaining"]:
            checkpoint.update(progress or {})
        admit(settings, additional_bytes=max(65536, 4 * len(body)))
        current_store.apply(conn, rows, checkpoint)
        responses += 1
        record_count += len(records)
        work = next_work
    return {"responses": responses, "requests": requests_made, "records": record_count}


def validate_fetch_pending(settings, conn, work):
    """Reject incompatible work rather than regenerate a saved request."""
    try:
        identity = work["identity"]
        lane, native = identity["lane"], identity["native"]
        if lane not in LANES or type(native) is not bool or identity["policy"] != POLICY:
            raise CurrentError("incompatible pending repair policy")
        certificate, passes = (None, [None]) if native else _certificate(settings, conn, lane)
        if identity["certificate"] != certificate or identity["passes"] != passes:
            raise CurrentError("incompatible pending repair certificate")
        ids = identity["ids"]
        if not ids or len(ids) > 100 or len(set(ids)) != len(ids):
            raise CurrentError("invalid pending repair IDs")
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
                    and any(closed is value for value in passes)
                    and request
                    == {"endpoint": f"/{lane}", "params": _window_params(batch, lane, closed)}
                )
            if not valid:
                raise CurrentError("invalid frozen pending request")
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise CurrentError("incompatible pending repair; explicit recovery required") from exc


def resume_pending(settings, conn, client):
    """Resume a durable repair using only its frozen request and checkpoint."""
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
    )
