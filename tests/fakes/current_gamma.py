"""Frozen Gamma captures for real HTTP workflow proofs and deterministic benchmarks."""

from __future__ import annotations

import copy
import json
import threading
import time
from collections import deque
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import httpx

from fakes.world import make_event, make_market


def default_records():
    markets = {
        key: make_market(
            key, f"Market {key}?", version=version, closed=closed, event_stub={"id": parent}
        )
        for key, version, closed, parent in (
            ("11", "v1", False, "1"),
            ("12", "v2", False, "1"),
            ("21", "v1", True, "2"),
        )
    }
    events = {
        "1": make_event("1", "Open event", markets=[markets["11"], markets["12"]]),
        "2": make_event("2", "Closed event", markets=[markets["21"]], closed=True),
    }
    return events, markets


class CurrentGammaServer:
    """Replace whole captures between rounds; requests never mutate source records.

    ``market_default_open`` models closed omissions from market lists; explicit
    false passes include unknown status, while native endpoints remain unfiltered.
    ``failure_plan`` is a sequence of response status codes, consumed per request.
    A 200 entry serves the normal frozen record. ``handle`` also works with a
    MockTransport when loopback sockets are unavailable.
    """

    def __init__(self, events=None, markets=None, delay_s=0, *, market_default_open=False):
        if delay_s < 0:
            raise ValueError("fixture delay cannot be negative")
        self.delay_s = delay_s
        self.market_default_open = market_default_open
        self.failure_plan = deque()
        self._lock = threading.Lock()
        self._responses = []
        self._server = self._thread = None
        if events is None and markets is None:
            events, markets = default_records()
        self.replace(events=events or {}, markets=markets)

    def replace(self, *, events, markets=None):
        events = copy.deepcopy(events)
        if markets is None:
            markets = {m["id"]: m for event in events.values() for m in event["markets"]}
        markets = copy.deepcopy(markets)
        for records in (events, markets):
            if any(str(key) != record.get("id") for key, record in records.items()):
                raise ValueError("fixture keys must match canonical record IDs")
        for event in events.values():
            for market in event.get("markets", []):
                if market != markets.get(market["id"]):
                    raise ValueError("embedded and native markets must share one frozen capture")
        with self._lock:
            self._events, self._markets = events, markets

    @property
    def events(self):
        with self._lock:
            return copy.deepcopy(self._events)

    @property
    def markets(self):
        with self._lock:
            return copy.deepcopy(self._markets)

    @property
    def stats(self):
        with self._lock:
            responses = copy.deepcopy(self._responses)
        return {
            "http_attempts": len(responses),
            "downloaded_bytes": sum(row["bytes"] for row in responses),
            "requests": [row["path"] for row in responses],
            "responses": responses,
            "exhausted": False,
        }

    def handle(self, request):
        if self.delay_s:
            time.sleep(self.delay_s)
        parsed = urlsplit(str(request.url))
        params = parse_qs(parsed.query)
        parts = parsed.path.strip("/").split("/")
        with self._lock:
            planned = self.failure_plan.popleft() if self.failure_plan else 200
            status, payload = 200, []
            if planned != 200:
                status, payload = planned, {"error": "planned fixture failure"}
            elif len(parts) not in (1, 2) or parts[0] not in {"events", "markets"}:
                status, payload = 404, {"error": "unknown fixture endpoint"}
            else:
                records = self._events if parts[0] == "events" else self._markets
                if len(parts) == 2:
                    payload = records.get(parts[1])
                    if payload is None:
                        status, payload = 404, {"error": "not found"}
                elif (
                    set(params) - {"id", "limit", "order", "ascending", "include_tag", "closed"}
                    or "closed" in params
                    and (
                        not self.market_default_open
                        or parts[0] != "markets"
                        or params["closed"] not in (["false"], ["true"])
                    )
                ):
                    status, payload = 400, {"error": "filtered fixture requests are forbidden"}
                else:
                    query = {key: value for key, value in params.items() if key != "closed"}
                    if self.market_default_open and parts[0] == "markets":
                        closed = params.get("closed", ["false"])[0] == "true"
                        records = {
                            key: row
                            for key, row in records.items()
                            if (row.get("closed") is True) == closed
                        }
                    if "id" in query:
                        wanted = set(query["id"])
                        payload = [
                            records[key] for key in sorted(records, key=int) if key in wanted
                        ]
                    elif query == {"limit": ["1"], "order": ["id"], "ascending": ["false"]}:
                        payload = [records[max(records, key=int)]] if records else []
                    else:
                        status, payload = 400, {"error": "expected bounded IDs or highwater probe"}
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            self._responses.append(
                {"path": request.url.raw_path.decode(), "status": status, "bytes": len(body)}
            )
        return httpx.Response(status, content=body, headers={"Content-Type": "application/json"})

    @property
    def url(self):
        if self._server is None:
            raise RuntimeError("start the fixture before requesting its URL")
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self):
        if self._server is not None:
            raise RuntimeError("fixture is already started")
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                response = fixture.handle(httpx.Request("GET", fixture.url + self.path))
                self.send_response(response.status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response.content)))
                self.end_headers()
                with suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(response.content)

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def close(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                raise RuntimeError("fixture server did not stop")
            self._server = self._thread = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *_args):
        self.close()
