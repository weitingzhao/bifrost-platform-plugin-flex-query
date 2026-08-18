"""Minimal HTTP health for the worker process."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any


def start_health_server(port: int, state: dict[str, Any]) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _fmt: str, *_args: Any) -> None:  # noqa: D401
            return

        def do_GET(self) -> None:  # noqa: N802
            if self.path not in ("/health", "/healthz", "/ready"):
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps(
                {
                    "ok": True,
                    "status": "ok",
                    "pool": "flex",
                    "jobs_done": int(state.get("jobs_done") or 0),
                    "jobs_failed": int(state.get("jobs_failed") or 0),
                    "last_kind": state.get("last_kind") or "",
                    "last_claim_at": state.get("last_claim_at") or "",
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    httpd = ThreadingHTTPServer(("0.0.0.0", int(port)), Handler)
    t = Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd
