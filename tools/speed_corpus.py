"""Pinned disk-backed recorded Gamma fixture for offline performance experiments."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import Iterable
from contextlib import closing, suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from oddsfox_catalogue.ids import MAX_CANONICAL_BYTES, canonical_json
from oddsfox_catalogue.normalization import ID_RE

ENTITIES = {"events", "markets"}
DELAY_ROUTES = {"high_water", "keyset", "single"}
FLAGS = {"include_tag", "include_chat", "include_template", "include_best_lines"}


def _safe_path(path: Path, *, exists: bool) -> Path:
    if ".." in Path(path).parts:
        raise ValueError("corpus paths must not traverse parents")
    path = Path(os.path.abspath(path))
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("corpus paths must not contain symlinks")
    if not path.parent.is_dir() or (exists and not path.is_file()):
        raise ValueError("corpus requires an existing directory and regular file")
    return path


def _id(value) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("invalid corpus ID")
    value = str(value)
    if not ID_RE.fullmatch(value):
        raise ValueError("invalid corpus ID")
    return value


def _policy(high_water, delays):
    if set(high_water) != ENTITIES or any(
        type(v) is not int or v < 0 or v >= 10**20 for v in high_water.values()
    ):
        raise ValueError("high_water requires bounded nonnegative events and markets IDs")
    if set(delays) != DELAY_ROUTES or any(
        isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0
        for v in delays.values()
    ):
        raise ValueError("delays require positive finite high_water, keyset and single seconds")


def _hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def build_corpus(
    pages: Iterable[tuple[str, list[dict]]],
    path: Path,
    *,
    high_water: dict[str, int],
    delays: dict[str, float],
) -> dict:
    """Index verified top-level records; conflicting versions invalidate the corpus."""
    _policy(high_water, delays)
    path = _safe_path(path, exists=False)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    try:
        with closing(sqlite3.connect(path)) as db:
            db.execute(
                "CREATE TABLE records(entity TEXT, id TEXT, body TEXT, closed INTEGER, PRIMARY KEY(entity,id))"
            )
            db.execute("CREATE INDEX ordered_records ON records(entity,closed,length(id),id)")
            for entity, records in pages:
                if entity not in ENTITIES:
                    raise ValueError("unsupported corpus entity")
                for record in records:
                    if not isinstance(record, dict):
                        raise ValueError("corpus records must be objects")
                    entity_id = _id(record.get("id"))
                    closed = record.get("closed")
                    if closed is not None and type(closed) is not bool:
                        raise ValueError("corpus closed flags must be boolean or null")
                    body = canonical_json(record)
                    old = db.execute(
                        "SELECT body FROM records WHERE entity=? AND id=?", (entity, entity_id)
                    ).fetchone()
                    if old is not None:
                        if old[0] != body:
                            raise ValueError(
                                f"ambiguous corpus record: {entity}/{entity_id} (ambiguity_count=1)"
                            )
                        continue
                    db.execute(
                        "INSERT INTO records VALUES (?,?,?,?)",
                        (entity, entity_id, body, None if closed is None else int(closed)),
                    )
            counts = {
                entity: db.execute(
                    "SELECT count(*) FROM records WHERE entity=?", (entity,)
                ).fetchone()[0]
                for entity in sorted(ENTITIES)
            }
            manifest = {
                "schema_version": 1,
                "counts": counts,
                "high_water": dict(high_water),
                "delays": dict(delays),
                "ambiguous_records": 0,
                "delay_policy_sha256": hashlib.sha256(canonical_json(delays).encode()).hexdigest(),
            }
            db.execute("CREATE TABLE metadata(body TEXT NOT NULL)")
            db.execute("INSERT INTO metadata VALUES (?)", (canonical_json(manifest),))
            db.commit()
        return {**manifest, "corpus_sha256": _hash_file(path)}
    except BaseException:
        path.unlink(missing_ok=True)
        raise


class RecordedGammaServer:
    """Strict loopback-only fixture with real route delays and workflow-wide caps."""

    def __init__(self, path: Path, *, max_requests: int, max_download_bytes: int):
        self.path = _safe_path(path, exists=True)
        if any(type(v) is not int or v < 1 for v in (max_requests, max_download_bytes)):
            raise ValueError("fixture caps must be positive integers")
        self.max_requests = max_requests
        self.max_download_bytes = max_download_bytes
        with closing(self._connect()) as db:
            self.manifest = json.loads(db.execute("SELECT body FROM metadata").fetchone()[0])
        _policy(self.manifest["high_water"], self.manifest["delays"])
        self._lock = threading.Lock()
        self._requests = self._bytes = self._errors = 0
        self._statuses = Counter()

    def _connect(self):
        return sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)

    def accounting(self) -> dict:
        with self._lock:
            return {
                "requests": self._requests,
                "bytes": self._bytes,
                "statuses": dict(self._statuses),
                "errors": self._errors,
            }

    def __enter__(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                with owner._lock:
                    owner._requests += 1
                    capped = owner._requests > owner.max_requests
                status, body = 429, b""
                if not capped:
                    try:
                        if (
                            self.headers.get_all("Host") != [owner.url.removeprefix("http://")]
                            or self.headers.get("Authorization")
                            or self.headers.get("Proxy-Authorization")
                        ):
                            raise ValueError("invalid local request authority")
                        body = owner._query(self.path, stream=True)
                        status = 200
                    except LookupError:
                        status, body = 404, b'{"error":"not found"}'
                    except (ValueError, sqlite3.Error):
                        status, body = 400, b'{"error":"invalid fixture query"}'
                length = len(body) if isinstance(body, bytes) else body[0]
                with owner._lock:
                    if owner._bytes + length > owner.max_download_bytes:
                        status, body, length = 429, b"", 0
                    # Reserve the full response even if the client stops reading early.
                    owner._bytes += length
                    owner._statuses[str(status)] += 1
                    owner._errors += status not in {200, 404}
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(length))
                self.end_headers()
                with suppress(BrokenPipeError, ConnectionResetError):
                    if isinstance(body, bytes):
                        self.wfile.write(body)
                    else:
                        chunks = body[1]
                        try:
                            for chunk in chunks:
                                self.wfile.write(chunk)
                        finally:
                            chunks.close()

            def _reject(self):
                with owner._lock:
                    owner._requests += 1
                    owner._statuses["405"] += 1
                    owner._errors += 1
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = do_CONNECT = (
                do_TRACE
            ) = _reject

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def _query(self, target: str, *, stream: bool = False):
        if len(target) > 8192:
            raise ValueError("query exceeds fixture bound")
        url = urlsplit(target)
        if url.scheme or url.netloc or url.fragment or "%" in url.path:
            raise ValueError("unsupported fixture path")
        parts = url.path.split("/")
        if len(parts) not in {2, 3} or parts[0] or parts[1] not in ENTITIES:
            raise ValueError("unsupported fixture path")
        entity = parts[1]
        query = parse_qs(url.query, keep_blank_values=True, strict_parsing=True, max_num_fields=120)
        if any(len(v) != 1 for k, v in query.items() if k != "id"):
            raise ValueError("duplicate query flag")
        if any(query[k][0] not in {"true", "false"} for k in FLAGS & query.keys()):
            raise ValueError("invalid payload flag")
        route = "high_water" if len(parts) == 2 else "keyset" if parts[2] == "keyset" else "single"
        allowed = FLAGS | (
            {"limit", "order", "ascending"}
            if route == "high_water"
            else {"id", "limit", "closed", "after_cursor", "order", "ascending"}
            if route == "keyset"
            else set()
        )
        if query.keys() - allowed:
            raise ValueError("unsupported query parameter")
        if route == "high_water":
            if {k: query.get(k) for k in ("limit", "order", "ascending")} != {
                "limit": ["1"],
                "order": ["id"],
                "ascending": ["false"],
            }:
                raise ValueError("only frozen high-water list queries are supported")
            mark = self.manifest["high_water"][entity]
            result = b"[]" if mark == 0 else ('[{"id":"' + str(mark) + '"}]').encode()
        elif route == "single":
            entity_id = _id(parts[2])
            with closing(self._connect()) as db:
                row = db.execute(
                    "SELECT body FROM records WHERE entity=? AND id=?", (entity, entity_id)
                ).fetchone()
            time.sleep(self.manifest["delays"][route])
            if row is None:
                raise LookupError("absent corpus ID")
            return row[0].encode()
        else:
            if query.get("order", ["id"]) != ["id"] or query.get("ascending", ["true"]) != ["true"]:
                raise ValueError("keyset ordering must be ascending ID")
            limit_text = query.get("limit", ["100"])[0]
            if (
                not limit_text.isascii()
                or not limit_text.isdecimal()
                or str(int(limit_text)) != limit_text
                or not 1 <= int(limit_text) <= 100
            ):
                raise ValueError("limit must be 1..100")
            limit = int(limit_text)
            closed = query.get("closed", [None])[0]
            if closed not in {None, "true", "false"}:
                raise ValueError("invalid closed filter")
            ids = [_id(v) for v in query.get("id", [])]
            if "id" in query and (not ids or len(ids) > 100 or len(set(ids)) != len(ids)):
                raise ValueError("ID lists must contain at most 100 distinct IDs")
            signature = hashlib.sha256(
                canonical_json({k: v for k, v in query.items() if k != "after_cursor"}).encode()
            ).hexdigest()
            last = "0"
            if "after_cursor" in query:
                try:
                    cursor = query["after_cursor"][0]
                    if len(cursor) > 256:
                        raise ValueError("cursor too long")
                    saved = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
                    last = _id(saved["id"])
                    if saved["query"] != signature:
                        raise ValueError("cursor belongs to another query")
                except (ValueError, KeyError, TypeError) as exc:
                    raise ValueError("invalid fixture cursor") from exc
            sql = "SELECT rowid,id,length(CAST(body AS BLOB)) FROM records WHERE entity=? AND (length(id)>? OR (length(id)=? AND id>?))"
            values = [entity, len(last), len(last), last]
            if closed is not None or entity == "markets":
                sql += " AND closed=?"
                values.append(int(closed == "true"))
            if ids:
                sql += " AND id IN (" + ",".join("?" for _ in ids) + ")"
                values.extend(ids)
            sql += " ORDER BY length(id),id LIMIT ?"
            values.append(limit + 1)
            with closing(self._connect()) as db:
                rows = db.execute(sql, values).fetchall()
            more = len(rows) > limit
            rows = rows[:limit]
            last_id = rows[-1][1] if rows else None
            cursor = (
                None
                if not more
                else base64.urlsafe_b64encode(
                    canonical_json({"id": last_id, "query": signature}).encode()
                ).decode()
            )
            prefix = ('{"' + entity + '":[').encode()
            suffix = ('],"next_cursor":' + json.dumps(cursor) + "}").encode()
            length = (
                len(prefix) + len(suffix) + sum(size for _, _, size in rows) + max(0, len(rows) - 1)
            )
            time.sleep(self.manifest["delays"][route])
            chunks = self._page_chunks(entity, rows, prefix, suffix)
            if stream:
                return length, chunks
            if length > MAX_CANONICAL_BYTES:
                raise ValueError("use streaming for oversized fixture responses")
            return b"".join(chunks)
        time.sleep(self.manifest["delays"][route])
        return result

    def _page_chunks(self, entity, rows, prefix, suffix):
        yield prefix
        with closing(self._connect()) as db:
            for index, (row_id, _, size) in enumerate(rows):
                if index:
                    yield b","
                with db.blobopen("records", "body", row_id, readonly=True) as body:
                    for _ in range(0, size, 64 * 1024):
                        yield body.read(64 * 1024)
        yield suffix
