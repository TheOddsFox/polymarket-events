"""Frozen, bounded loopback Gamma source for capacity characterization."""

import hashlib
import json
import os
import re
import stat
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb

from oddsfox_catalogue.current_config import safe_path

MAX_RESPONSE_BYTES = 16 * 1024**2
MAX_AUDIT_BYTES = 128 * 1024**2
_ID = re.compile(r"[1-9][0-9]{0,19}\Z")


class CapacitySource:
    def __init__(
        self, seed_root: Path, audit_path: Path, *, delay_s=0.3, max_audit_bytes=MAX_AUDIT_BYTES
    ):
        self.seed_root = safe_path(Path(seed_root)).resolve(strict=True)
        self.db_path = self.seed_root / "catalogue.duckdb"
        audit_path = safe_path(Path(audit_path))
        if audit_path.is_relative_to(self.seed_root):
            raise ValueError("audit must be disjoint from seed")
        for parent in audit_path.parents:
            if parent.is_symlink() or not parent.is_dir():
                raise ValueError("unsafe audit parent")
        if self.db_path.is_symlink() or not stat.S_ISREG(self.db_path.stat().st_mode):
            raise ValueError("seed database must be regular")
        if not 0 <= delay_s <= 10 or not 0 < max_audit_bytes <= MAX_AUDIT_BYTES:
            raise ValueError("invalid source limits")
        before = self.db_path.stat()
        self.conn = duckdb.connect(
            str(self.db_path),
            read_only=True,
            config={"memory_limit": "2GB", "threads": "2", "max_temp_directory_size": "0B"},
        )
        self.lock = threading.RLock()
        self.delay_s = delay_s
        self.max_audit_bytes = max_audit_bytes
        self.stats = {
            "requests": 0,
            "audit_bytes": 0,
            "max_audit_bytes": max_audit_bytes,
            "response_bytes": 0,
            "faults": 0,
            "http_attempts": 0,
            "downloaded_bytes": 0,
            "paths": {},
            "emitted_events": 0,
            "emitted_markets": 0,
            "source_identity": {
                "device": before.st_dev,
                "inode": before.st_ino,
                "size": before.st_size,
                "mtime_ns": before.st_mtime_ns,
            },
        }
        self.controls = {}
        self.high = {}
        self.faults = False
        self.failure = None
        try:
            for kind in ("events", "markets"):
                singular = kind[:-1]
                key = f"{singular}_id"
                for closed, label in ((False, "open"), (True, "closed")):
                    value = self.conn.execute(
                        f"SELECT min(try_cast({key} AS HUGEINT)) FROM {kind} "
                        "WHERE venue='polymarket' AND closed IS NOT DISTINCT FROM ?",
                        [closed],
                    ).fetchone()[0]
                    if value is None:
                        raise ValueError("seed lacks lifecycle controls")
                    self.controls[f"{label}_{singular}_id"] = str(value)
                for closed in (None, False, True):
                    clause = "" if closed is None else " AND coalesce(closed,false)=?"
                    value = self.conn.execute(
                        f"SELECT max(try_cast({key} AS HUGEINT)) FROM {kind} "
                        "WHERE venue='polymarket'" + clause,
                        [] if closed is None else [closed],
                    ).fetchone()[0]
                    self.high[kind, closed] = None if value is None else str(value)
                self.controls[f"absent_{singular}_id"] = str(int(self.high[kind, None]) + 1)
            after = self.db_path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise ValueError("seed changed during initial read")
            fd = os.open(audit_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                os.close(fd)
                raise ValueError("audit must be regular")
            self.audit = os.fdopen(fd, "wb", buffering=0)
            source = self

            class Handler(BaseHTTPRequestHandler):
                def setup(self):
                    super().setup()
                    self.connection.settimeout(10)

                def log_message(self, *_args):
                    pass

                def do_GET(self):
                    source._serve(self)

            self.server = HTTPServer(("127.0.0.1", 0), Handler)
            self.url = f"http://127.0.0.1:{self.server.server_port}"
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.thread.start()
        except BaseException:
            if hasattr(self, "audit"):
                self.audit.close()
            self.conn.close()
            raise

    def _check_identity(self):
        current = self.db_path.stat()
        expected = self.stats["source_identity"]
        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
            expected["device"],
            expected["inode"],
            expected["size"],
            expected["mtime_ns"],
        ):
            raise RuntimeError("source database identity changed")

    def _bounded_append(self, target, value):
        self._budget += len(json.dumps(value, separators=(",", ":")).encode())
        if self._budget > MAX_RESPONSE_BYTES * 4:
            raise OverflowError
        target.append(value)

    def _query(self, sql, ids):
        """Materialize only this request, with a budget checked after every row."""
        self.stats["sql_queries"] = self.stats.get("sql_queries", 0) + 1
        cursor = self.conn.cursor()
        rows = []
        try:
            cursor.execute(sql, [ids])
            names = [column[0] for column in cursor.description]
            while (row := cursor.fetchone()) is not None:
                self._bounded_append(rows, dict(zip(names, row, strict=True)))
        finally:
            cursor.close()
        return rows

    def records(self, kind, ids, closed=None):
        if (
            kind not in ("events", "markets")
            or len(ids) > 100
            or any(not _ID.fullmatch(str(i)) for i in ids)
        ):
            raise ValueError("invalid IDs")
        with self.lock:
            self._check_identity()
            try:
                return self._records(kind, list(map(str, ids)), closed)
            finally:
                self._check_identity()

    def _records(self, kind, ids, closed):
        self._budget = 0
        rows = self._query(
            f"SELECT * FROM {kind} WHERE venue='polymarket' "
            f"AND {kind[:-1]}_id IN (SELECT unnest(?))",
            ids,
        )
        rows = [r for r in rows if closed is None or bool(r["closed"]) == closed]
        if kind == "events":
            event_ids = [r["event_id"] for r in rows]
            links = self._query(
                "SELECT event_id,market_id FROM event_markets WHERE venue='polymarket' "
                "AND event_id IN (SELECT unnest(?))",
                event_ids,
            )
            market_ids = list({r["market_id"] for r in links})
            market_rows = self._query(
                "SELECT * FROM markets WHERE venue='polymarket' "
                "AND market_id IN (SELECT unnest(?))",
                market_ids,
            )
        else:
            links = []
            market_rows = rows
            market_ids = [r["market_id"] for r in rows]
        outcomes = self._query(
            "SELECT market_id,outcome_label,clob_token_id,position_id FROM outcomes "
            "WHERE venue='polymarket' AND market_id IN (SELECT unnest(?)) "
            "ORDER BY market_id,outcome_index",
            market_ids,
        )
        parents = self._query(
            "SELECT market_id,event_id FROM event_markets WHERE venue='polymarket' "
            "AND market_id IN (SELECT unnest(?))",
            market_ids,
        )
        outcome_map = {}
        for row in outcomes:
            outcome_map.setdefault(row["market_id"], []).append(row)
        parent_map = {}
        for row in parents:
            parent_map.setdefault(row["market_id"], []).append({"id": row["event_id"]})
        markets = {}
        for row in market_rows:
            key = row["market_id"]
            raw = {name: row[name] for name in ("question", "slug", "active", "closed", "archived")}
            raw.update(
                id=key,
                conditionId=row["condition_id"],
                version=row["source_market_version"],
                events=parent_map.get(key, []),
            )
            identity = outcome_map.get(key, [])
            raw["outcomes"] = [o["outcome_label"] for o in identity]
            for field, name in (("clob_token_id", "clobTokenIds"), ("position_id", "positionIds")):
                if identity and all(o[field] is not None for o in identity):
                    raw[name] = [o[field] for o in identity]
            if not row["usable"]:
                error = row["identity_error"]
                if error == "CTF market lacks clobTokenIds":
                    raw["outcomes"] = ["Yes", "No"]
                elif error != "outcomes must contain nonempty labels":
                    raise RuntimeError("unsupported unusable source identity")
            markets[key] = raw
        result = []
        if kind == "markets":
            for key in ids:
                if key in markets:
                    self._bounded_append(result, markets[key])
        else:
            membership = {}
            for link in links:
                membership.setdefault(link["event_id"], []).append(link["market_id"])
            events = {r["event_id"]: r for r in rows}
            for key in ids:
                if key not in events:
                    continue
                row = events[key]
                raw = {
                    name: row[name] for name in ("title", "slug", "active", "closed", "archived")
                }
                raw.update(id=key, markets=[markets[mid] for mid in membership.get(key, [])])
                self._bounded_append(result, raw)
        if len(json.dumps(result, separators=(",", ":")).encode()) > MAX_RESPONSE_BYTES:
            raise OverflowError
        return result

    def _request(self, target):
        if len(target) > 8192:
            raise ValueError
        parsed = urlsplit(target)
        parts = parsed.path.split("/")
        if len(parts) not in (2, 3) or parts[0] or parts[1] not in ("events", "markets"):
            raise ValueError
        kind = parts[1]
        query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=110)
        if len(parts) == 3:
            if query or not _ID.fullmatch(parts[2]):
                raise ValueError
            return kind, [parts[2]], None, True
        if set(query) - {"id", "limit", "closed", "order", "ascending", "include_tag"}:
            raise ValueError
        for key, values in query.items():
            if key != "id" and len(values) != 1:
                raise ValueError
        closed = query.get("closed", [None])[0]
        if closed not in (None, "true", "false"):
            raise ValueError
        closed = (False if kind == "markets" else None) if closed is None else closed == "true"
        if "include_tag" in query and (kind != "markets" or query["include_tag"] != ["true"]):
            raise ValueError
        ids = query.get("id")
        if ids is None:
            if (
                query.get("limit") != ["1"]
                or query.get("order") != ["id"]
                or query.get("ascending") != ["false"]
            ):
                raise ValueError
            ids = [self.high[kind, closed]] if self.high[kind, closed] else []
        elif (
            len(ids) > 100
            or query.get("limit") != [str(len(ids))]
            or "order" in query
            or "ascending" in query
        ):
            raise ValueError
        if any(not _ID.fullmatch(i) for i in ids):
            raise ValueError
        return kind, ids, closed, False

    def _serve(self, handler):
        self.stats["requests"] += 1
        number = self.stats["requests"]
        self.stats["http_attempts"] = number
        route = urlsplit(handler.path).path.split("/")
        route_key = (
            "/" + route[1]
            if len(route) in (2, 3) and route[1] in ("events", "markets")
            else "invalid"
        )
        if len(route) == 3 and route_key != "invalid":
            route_key += "/id"
        self.stats["paths"][route_key] = self.stats["paths"].get(route_key, 0) + 1
        kind, ids, records, native = None, [], [], False
        status = 200
        try:
            if self.failure:
                raise RuntimeError("source failed")
            self._check_identity()
            kind, ids, closed, native = self._request(handler.path)
            time.sleep(self.delay_s)
            if self.faults and number % 1000 == 0:
                status, payload = 503, {"error": "injected failure"}
                self.stats["faults"] += 1
            else:
                records = self.records(kind, ids, closed)
                status = 404 if native and not records else 200
                payload = records[0] if native and records else records
            self._check_identity()
            body = json.dumps(payload, separators=(",", ":")).encode()
            if len(body) > MAX_RESPONSE_BYTES:
                raise OverflowError
        except ValueError:
            status, body = 400, b'{"error":"invalid request"}'
        except OverflowError:
            status, body = 413, b'{"error":"response too large"}'
        except Exception:
            self.failure = "source read failed"
            status, body = 500, b'{"error":"source failed"}'
        entry = {
            "request": number,
            "monotonic_s": time.monotonic(),
            "kind": kind,
            "status": status,
            "ids": ids,
            "emitted_ids": [r["id"] for r in records] if status == 200 else [],
            "complete_memberships": {
                r["id"]: [m["id"] for m in r["markets"]] for r in records if "markets" in r
            }
            if status == 200
            else {},
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "body_bytes": len(body),
        }
        encoded = (json.dumps(entry, separators=(",", ":")) + "\n").encode()
        if self.stats["audit_bytes"] + len(encoded) > self.max_audit_bytes:
            self.failure = "audit limit exceeded"
            status, body = 507, b'{"error":"audit limit exceeded"}'
        else:
            self.audit.write(encoded)
            self.stats["audit_bytes"] += len(encoded)
        self.stats["response_bytes"] += len(body)
        self.stats["downloaded_bytes"] += len(body)
        if status == 200:
            self.stats["emitted_events"] += len(records) if kind == "events" else 0
            self.stats["emitted_markets"] += (
                sum(len(r["markets"]) for r in records) if kind == "events" else len(records)
            )
        try:
            handler.send_response(status)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            handler.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def enable_faults(self):
        self.faults = True

    def close(self):
        if getattr(self, "server", None) is not None:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join()
            self.server = None
            self.audit.close()
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
