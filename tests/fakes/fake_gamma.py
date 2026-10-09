"""In-process fake of the Gamma endpoints the pipeline uses.

Serves a mutable :class:`World` through ``httpx.MockTransport``. Behaviour
follows the documented contract:

* keyset endpoints order by ``id`` ascending, honour ``after_cursor``, and
  return ``next_cursor`` only when a full page came back;
* ``offset`` is rejected on keyset endpoints with 422;
* ``/events?archived=true`` pages by ``offset``;
* ``/events/{id}`` returns 404 for unknown IDs;
* nested market ``events`` are identity stubs.

Faults are scripted with rules that match the path and are consumed once used.
"""

from __future__ import annotations

import base64
import json
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

from fakes.world import World

EVENT_BY_ID = re.compile(r"^/events/(\d+)$")
MARKET_BY_ID = re.compile(r"^/markets/(\d+)$")
ID_LIST_CAP = 100
OFFSET_CEILING = 3000


def _encode_cursor(last_id: int) -> str:
    raw = json.dumps({"after": last_id}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> int:
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        return int(json.loads(base64.urlsafe_b64decode(padded))["after"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("invalid cursor") from exc


@dataclass
class Rule:
    matches: Callable[[httpx.Request], bool]
    respond: Callable[[httpx.Request], httpx.Response]
    remaining: int = 1
    label: str = ""


@dataclass
class FakeGamma:
    world: World
    page_cap: int = 100
    requests: list[tuple[str, dict[str, list[str]]]] = field(default_factory=list)
    rules: list[Rule] = field(default_factory=list)
    # Events reachable only by ID (for example, archived or unindexed ones).
    hidden_from_lists: set[str] = field(default_factory=set)
    # Raw values served in place of a nested event market (for malformed-payload tests).
    nested_overrides: dict[str, Any] = field(default_factory=dict)
    # Keyset cursors at or past this id return HTTP 500. None disables the fault.
    deep_cursor_after: int | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    # Fault scripting ------------------------------------------------------------
    def fail_status(
        self, path: str, status: int, times: int = 1, retry_after: str | None = None
    ) -> None:
        headers = {"Retry-After": retry_after} if retry_after is not None else {}

        def respond(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status, headers=headers, json={"type": "error", "error": "injected"}
            )

        self.rules.append(
            Rule(lambda r: r.url.path == path, respond, times, label=f"status {status} {path}")
        )

    def expire_cursor(self, path: str, times: int = 1) -> None:
        """Reject the next request on ``path`` that carries an ``after_cursor``."""

        def respond(_: httpx.Request) -> httpx.Response:
            return httpx.Response(422, json={"type": "validation error", "error": "cursor expired"})

        self.rules.append(
            Rule(
                lambda r: r.url.path == path and "after_cursor" in r.url.params,
                respond,
                times,
                label=f"expire cursor {path}",
            )
        )

    def malformed(self, path: str, times: int = 1) -> None:
        self.rules.append(
            Rule(
                lambda r: r.url.path == path,
                lambda _: httpx.Response(200, content=b"<html>not json</html>"),
                times,
                label=f"malformed {path}",
            )
        )

    def transport_error(self, path: str, times: int = 1) -> None:
        def respond(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("injected connection reset", request=request)

        self.rules.append(Rule(lambda r: r.url.path == path, respond, times, label=f"net {path}"))

    def crash_on(self, path: str, times: int = 1) -> None:
        """Raise a non-retryable exception mid-run, as a process crash would interrupt it."""

        def respond(_: httpx.Request) -> httpx.Response:
            raise SystemExit("simulated crash")

        self.rules.append(Rule(lambda r: r.url.path == path, respond, times, label=f"crash {path}"))

    def calls_to(self, path: str) -> int:
        return sum(1 for p, _ in self.requests if p == path)

    # Request handling -----------------------------------------------------------
    def handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            params: dict[str, list[str]] = {}
            for key in request.url.params:
                params.setdefault(key, []).extend(request.url.params.get_list(key))
            self.requests.append((request.url.path, params))

            chosen: Rule | None = None
            for rule in list(self.rules):
                if rule.remaining > 0 and rule.matches(request):
                    rule.remaining -= 1
                    if rule.remaining == 0:
                        self.rules.remove(rule)
                    chosen = rule
                    break
            if chosen is None:
                return self._route(request, params)
        # Fault responses run outside the lock so a scripted wait cannot
        # block the sibling scan that is supposed to make progress.
        return chosen.respond(request)

    def _route(self, request: httpx.Request, params: dict[str, list[str]]) -> httpx.Response:
        path = request.url.path
        if path == "/events/keyset":
            return self._keyset_events(params)
        if path == "/markets/keyset":
            return self._keyset_markets(params)
        if path == "/events":
            return self._event_list(params)
        if path == "/markets":
            return self._market_list(params)
        match = EVENT_BY_ID.match(path)
        if match:
            return self._event_by_id(match.group(1))
        market_match = MARKET_BY_ID.match(path)
        if market_match:
            return self._market_by_id(market_match.group(1))
        return httpx.Response(404, json={"type": "not found", "error": "no such route"})

    def _too_many_ids(self, params: dict[str, list[str]]) -> httpx.Response | None:
        if "id" in params and len(params["id"]) > ID_LIST_CAP:
            return httpx.Response(
                422,
                json={"type": "validation error", "error": "id list is longer than 100"},
            )
        return None

    def _offset_too_large(self, params: dict[str, list[str]]) -> httpx.Response | None:
        offset = int(params.get("offset", ["0"])[0])
        if offset >= OFFSET_CEILING:
            return httpx.Response(
                422,
                json={
                    "type": "validation error",
                    "error": "offset too large, use /events/keyset for deeper pagination",
                },
            )
        return None

    # Helpers --------------------------------------------------------------------
    def _limit(self, params: dict[str, list[str]]) -> int:
        raw = params.get("limit", ["20"])[0]
        return max(1, min(int(raw), self.page_cap))

    def _render_event(self, event_id: str) -> dict[str, Any]:
        event = json.loads(json.dumps(self.world.events[event_id]))
        event["markets"] = [
            json.loads(json.dumps(self._nested_market(m["id"])))
            for m in self.world.events[event_id]["markets"]
        ]
        return event

    def _nested_market(self, market_id: str) -> Any:
        if market_id in self.nested_overrides:
            return self.nested_overrides[market_id]
        return self.world.markets[market_id]

    def _keyset_events(self, params: dict[str, list[str]]) -> httpx.Response:
        if "offset" in params:
            return httpx.Response(
                422, json={"type": "validation error", "error": "offset is not allowed"}
            )
        rejected = self._too_many_ids(params)
        if rejected is not None:
            return rejected
        after: int | None = None
        if "after_cursor" in params:
            try:
                after = _decode_cursor(params["after_cursor"][0])
            except ValueError:
                return httpx.Response(
                    422, json={"type": "validation error", "error": "invalid cursor"}
                )
        ids = sorted(int(i) for i in self.world.events)
        if "id" in params:
            wanted = {int(i) for i in params["id"]}
            ids = [i for i in ids if i in wanted]
        else:
            if params.get("closed") == ["true"]:
                ids = [i for i in ids if self.world.events[str(i)]["closed"]]
            elif params.get("closed") == ["false"]:
                ids = [i for i in ids if not self.world.events[str(i)]["closed"]]
        if "id" not in params:
            ids = [i for i in ids if str(i) not in self.hidden_from_lists]
        if after is not None:
            ids = [i for i in ids if i > after]
        if (
            self.deep_cursor_after is not None
            and after is not None
            and after >= self.deep_cursor_after
            and "id" not in params
        ):
            return httpx.Response(500, json={"type": "error", "error": "deep cursor"})

        limit = self._limit(params)
        page = ids[:limit]
        body: dict[str, Any] = {"events": [self._render_event(str(i)) for i in page]}
        if len(page) == limit and page:
            body["next_cursor"] = _encode_cursor(page[-1])
        return httpx.Response(200, json=body)

    def _keyset_markets(self, params: dict[str, list[str]]) -> httpx.Response:
        rejected = self._too_many_ids(params)
        if rejected is not None:
            return rejected
        after: int | None = None
        if "after_cursor" in params:
            try:
                after = _decode_cursor(params["after_cursor"][0])
            except ValueError:
                return httpx.Response(
                    422, json={"type": "validation error", "error": "invalid cursor"}
                )
        markets = sorted(self.world.markets.values(), key=lambda m: int(m["id"]))
        if "id" in params:
            wanted = {int(i) for i in params["id"]}
            markets = [m for m in markets if int(m["id"]) in wanted]
        # Gamma's /markets/keyset defaults to open markets when closed is omitted.
        closed = params.get("closed", ["false"])
        if closed == ["true"]:
            markets = [m for m in markets if m["closed"]]
        else:
            markets = [m for m in markets if not m["closed"]]
        if after is not None:
            markets = [m for m in markets if int(m["id"]) > after]
        if (
            self.deep_cursor_after is not None
            and after is not None
            and after >= self.deep_cursor_after
            and "id" not in params
        ):
            return httpx.Response(500, json={"type": "error", "error": "deep cursor"})
        include_tag = params.get("include_tag") == ["true"]
        limit = self._limit(params)
        page = markets[:limit]
        rendered = []
        for market in page:
            item = json.loads(json.dumps(market))
            if not include_tag:
                item.pop("tags", None)
            rendered.append(item)
        body: dict[str, Any] = {"markets": rendered}
        if len(page) == limit and page:
            body["next_cursor"] = _encode_cursor(int(page[-1]["id"]))
        return httpx.Response(200, json=body)

    def _event_list(self, params: dict[str, list[str]]) -> httpx.Response:
        rejected = self._offset_too_large(params)
        if rejected is not None:
            return rejected
        offset = int(params.get("offset", ["0"])[0])
        limit = self._limit(params)
        events = sorted(self.world.events.values(), key=lambda e: int(e["id"]))
        if params.get("archived") == ["true"]:
            events = [e for e in events if e["archived"]]
        if params.get("ascending", ["true"])[0] == "false":
            events.reverse()
        page = events[offset : offset + limit]
        return httpx.Response(200, json=[self._render_event(e["id"]) for e in page])

    def _market_list(self, params: dict[str, list[str]]) -> httpx.Response:
        rejected = self._offset_too_large(params)
        if rejected is not None:
            return rejected
        offset = int(params.get("offset", ["0"])[0])
        limit = self._limit(params)
        markets = sorted(self.world.markets.values(), key=lambda m: int(m["id"]))
        if params.get("ascending", ["true"])[0] == "false":
            markets.reverse()
        page = markets[offset : offset + limit]
        rendered = [json.loads(json.dumps(market)) for market in page]
        return httpx.Response(200, json=rendered)

    def _event_by_id(self, event_id: str) -> httpx.Response:
        if event_id not in self.world.events:
            return httpx.Response(404, json={"type": "not found", "error": "Event not found"})
        return httpx.Response(200, json=self._render_event(event_id))

    def _market_by_id(self, market_id: str) -> httpx.Response:
        if market_id not in self.world.markets:
            return httpx.Response(404, json={"type": "not found", "error": "Market not found"})
        return httpx.Response(200, json=json.loads(json.dumps(self.world.markets[market_id])))
