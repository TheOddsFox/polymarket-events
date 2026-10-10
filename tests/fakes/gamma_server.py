"""Bounded loopback adapter for the existing synthetic Gamma transport."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

from fakes.fake_gamma import FakeGamma


class GammaServer:
    def __init__(self, gamma: FakeGamma):
        self.gamma = gamma
        self.responses: list[dict[str, object]] = []
        self._lock = threading.Lock()
        self._bytes = 0
        self._exhausted = False
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                with fixture._lock:
                    if len(fixture.responses) >= 100 or fixture._bytes >= 64 * 1024**2:
                        fixture._exhausted = True
                        self.send_error(429, "fixture request allowance exhausted")
                        return
                    host = f"127.0.0.1:{self.server.server_port}"
                    if (
                        self.headers.get("Host") != host
                        or self.headers.get("Content-Length", "0") != "0"
                    ):
                        fixture._exhausted = True
                        self.send_error(400, "invalid fixture request")
                        return
                    response = fixture.gamma.handle(
                        httpx.Request("GET", f"http://{host}{self.path}")
                    )
                    body = response.content
                    fixture.responses.append(
                        {"path": self.path, "status": response.status_code, "bytes": len(body)}
                    )
                    fixture._bytes += len(body)
                self.send_response(response.status_code)
                self.send_header(
                    "Content-Type", response.headers.get("content-type", "application/json")
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def accounting(self) -> dict[str, object]:
        with self._lock:
            return {
                "http_attempts": len(self.responses),
                "downloaded_bytes": self._bytes,
                "responses": list(self.responses),
                "exhausted": self._exhausted,
            }

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("fixture server did not stop")
