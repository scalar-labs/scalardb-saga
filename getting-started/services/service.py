#!/usr/bin/env python3
"""A stand-in participant service for the ScalarDB Saga getting-started example.

One script serves all three participants; SERVICE_NAME selects which one a container is. Any POST
answers 200 with a JSON body carrying an id the saga captures, except for paths listed in
FAIL_PATHS, which answer 422 — a non-retryable failure, so the saga compensates instead of
retrying — and paths listed in FLAKY_PATHS, which answer 503 — a retryable failure — until the
same saga has failed FLAKY_FAILURES times on that path, after which they succeed.

Every request and response is printed, so `docker compose logs -f` shows the saga driving the
services forward and, on failure, unwinding them in reverse. When EVENT_SINK_URL is set, the same
information is also posted there (one event on receipt, one after answering), which is how the
dashboard shows the calls live. Delivery is best effort: a slow or absent sink never delays or
fails a response.

The dashboard paces the sagas it starts: POST /_pace with {"sagaId": ..., "delayMs": ...}
registers an extra wait applied to every later call carrying that X-Saga-Id, which is how the
pace slider slows a saga's steps and compensations to a watchable speed without affecting sagas
started with curl. The wait is capped well below the engine's per-call timeout and recovery
staleness window, so a paced saga can never look abandoned to the recovery sweeper.

Environment:
  SERVICE_NAME    name used in logs and in the returned id field (default "service")
  PORT            port to listen on (default 8080)
  FAIL_PATHS      comma-separated paths that answer 422 (default none)
  FAIL_RETRYABLE_PATHS  comma-separated paths that always answer 503 (default none)
  FLAKY_PATHS     comma-separated paths that answer 503 until a saga's attempts run out (default none)
  FLAKY_FAILURES  attempts that fail on a flaky path before it succeeds (default 2)
  DELAY_SECONDS   seconds to wait before answering, to make a slow participant visible (default 0)
  EVENT_SINK_URL  URL to POST request and response events to (default none)
"""
import json
import os
import threading
import time
import urllib.request
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAME = os.environ.get("SERVICE_NAME", "service")
PORT = int(os.environ.get("PORT", "8080"))
FAIL_PATHS = {path for path in os.environ.get("FAIL_PATHS", "").split(",") if path}
FAIL_RETRYABLE_PATHS = {
    path for path in os.environ.get("FAIL_RETRYABLE_PATHS", "").split(",") if path
}
FLAKY_PATHS = {path for path in os.environ.get("FLAKY_PATHS", "").split(",") if path}
FLAKY_FAILURES = int(os.environ.get("FLAKY_FAILURES", "2"))
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "0"))
EVENT_SINK_URL = os.environ.get("EVENT_SINK_URL", "")

MAX_DELAY_MS = 2000

# How many attempts each saga has already failed on a flaky path. Bounded so a long-running demo
# cannot grow it without limit; evicting the oldest saga is harmless (it would merely fail again).
flaky_lock = threading.Lock()
flaky_attempts = OrderedDict()
MAX_TRACKED_SAGAS = 1000

# The extra wait each paced saga's calls get, registered through /_pace. Bounded the same way;
# an evicted or never-registered saga simply runs at full speed.
pace_lock = threading.Lock()
pace_delays = OrderedDict()


def set_pace(saga_id, delay_ms):
    with pace_lock:
        pace_delays[saga_id] = delay_ms
        pace_delays.move_to_end(saga_id)
        while len(pace_delays) > MAX_TRACKED_SAGAS:
            pace_delays.popitem(last=False)


def pace_for(saga_id):
    with pace_lock:
        return pace_delays.get(saga_id, 0)


def flaky_should_fail(saga_id, path):
    """Counts an attempt and reports whether this one should still fail."""
    with flaky_lock:
        key = (saga_id, path)
        attempts = flaky_attempts.get(key, 0)
        if attempts >= FLAKY_FAILURES:
            return False
        flaky_attempts[key] = attempts + 1
        flaky_attempts.move_to_end(key)
        while len(flaky_attempts) > MAX_TRACKED_SAGAS:
            flaky_attempts.popitem(last=False)
        return True


def post_event(event):
    """Fire-and-forget POST to the event sink; the participant never depends on it."""
    if not EVENT_SINK_URL:
        return

    def post():
        try:
            request = urllib.request.Request(
                EVENT_SINK_URL,
                data=json.dumps(event).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(request, timeout=1)
        except Exception:
            pass

    threading.Thread(target=post, daemon=True).start()


def now():
    return datetime.now(timezone.utc).isoformat()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        started = time.monotonic()
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""

        # The dashboard's pace registration: internal, so not logged and not fed to the sink.
        if self.path == "/_pace":
            self.receive_pace(body)
            return

        print(f"[{NAME}] POST {self.path} <- {body}", flush=True)

        try:
            request = json.loads(body) if body else {}
        except json.JSONDecodeError:
            request = {}

        event_id = str(uuid.uuid4())
        saga_id = self.headers.get("X-Saga-Id", "")
        step = self.headers.get("X-Saga-Step", "")
        post_event(
            {
                "id": event_id,
                "phase": "request",
                "ts": now(),
                "service": NAME,
                "method": "POST",
                "path": self.path,
                "sagaId": saga_id,
                "step": step,
                "body": body,
            }
        )

        if DELAY_SECONDS:
            time.sleep(DELAY_SECONDS)
        delay_ms = pace_for(saga_id)
        if delay_ms:
            time.sleep(delay_ms / 1000)

        if self.path in FAIL_PATHS:
            status = 422
            payload = {"error": f"{NAME} rejected {self.path}"}
        elif self.path in FAIL_RETRYABLE_PATHS:
            status = 503
            payload = {"error": f"{NAME} unavailable on {self.path}"}
        elif self.path in FLAKY_PATHS and flaky_should_fail(saga_id, self.path):
            status = 503
            payload = {"error": f"{NAME} temporarily unavailable on {self.path}"}
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
        post_event(
            {
                "id": event_id,
                "phase": "response",
                "ts": now(),
                "service": NAME,
                "method": "POST",
                "path": self.path,
                "sagaId": saga_id,
                "step": step,
                "status": status,
                "response": json.dumps(payload),
                "durationMs": int((time.monotonic() - started) * 1000),
            }
        )

    def receive_pace(self, body):
        try:
            request = json.loads(body)
            saga_id = str(request["sagaId"])
            delay_ms = min(max(int(request.get("delayMs", 0)), 0), MAX_DELAY_MS)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self.send_response(400)
            self.end_headers()
            return
        set_pace(saga_id, delay_ms)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        """Silences the default per-request line; the handler prints its own."""


if __name__ == "__main__":
    print(f"[{NAME}] listening on 0.0.0.0:{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
