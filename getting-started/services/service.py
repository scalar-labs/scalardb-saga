#!/usr/bin/env python3
"""A stand-in participant service for the ScalarDB Saga getting-started example.

One script serves all three participants; SERVICE_NAME selects which one a container is. Any POST
answers 200 with a JSON body carrying an id the saga captures, except for paths listed in
FAIL_PATHS, which answer 422 — a non-retryable failure, so the saga compensates instead of retrying.

Every request and response is printed, so `docker compose logs -f` shows the saga driving the
services forward and, on failure, unwinding them in reverse.

Environment:
  SERVICE_NAME   name used in logs and in the returned id field (default "service")
  PORT           port to listen on (default 8080)
  FAIL_PATHS     comma-separated paths that answer 422 (default none)
  DELAY_SECONDS  seconds to wait before answering, to make a slow participant visible (default 0)
"""
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

NAME = os.environ.get("SERVICE_NAME", "service")
PORT = int(os.environ.get("PORT", "8080"))
FAIL_PATHS = {path for path in os.environ.get("FAIL_PATHS", "").split(",") if path}
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "0"))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        print(f"[{NAME}] POST {self.path} <- {body}", flush=True)

        try:
            request = json.loads(body) if body else {}
        except json.JSONDecodeError:
            request = {}

        if DELAY_SECONDS:
            time.sleep(DELAY_SECONDS)

        if self.path in FAIL_PATHS:
            status = 422
            payload = {"error": f"{NAME} rejected {self.path}"}
        else:
            status = 200
            payload = {f"{NAME}_id": f"{NAME}-{request.get('orderId', 'unknown')}"}

        response = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)
        print(f"[{NAME}] POST {self.path} -> {status} {json.dumps(payload)}", flush=True)

    def log_message(self, *args):
        """Silences the default per-request line; the handler prints its own."""


if __name__ == "__main__":
    print(f"[{NAME}] listening on 0.0.0.0:{PORT}", flush=True)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
