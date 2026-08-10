#!/usr/bin/env python3
"""The live dashboard for the ScalarDB Saga getting-started example.

Serves the static UI, and keeps every open browser in sync with the saga server over one SSE
stream per tab. The saga server is never modified for this: a poller thread reads the same REST
API any client could (the admin listing for discovery and status transitions, the per-saga detail
for the durable event timeline), the participant services post the HTTP calls they receive to
/feed, and saga launches from the UI are proxied so the browser never needs CORS.

Realtime state rules, chosen to survive the API's semantics:

- A RUNNING saga's updatedAt is frozen at its last status transition, so the first listing pass is
  unbounded (a watermark started at "now" would permanently miss in-flight sagas), and afterwards
  the listing is only discovery and transitions; step-by-step progress comes from detail polls of
  every unsettled saga.
- Listing and detail results can arrive out of order, so merging is monotonic: a listing snapshot
  wins only when strictly newer by updatedAt, and a detail result wins only when its append-only
  timeline is at least as long as the one already held.
- A saga settles only when a detail response itself shows COMPLETED or COMPENSATED; that response
  already carries the final timeline, so nothing remains to fetch. ESCALATED also settles, but any
  strictly newer listing snapshot un-settles a saga again, which is how a reset or force-complete
  (or recovery) resumes polling.
- The browser's EventSource reconnects by itself and every connection starts with a full-state
  init event, so updates are whole objects and rendering is idempotent; nothing depends on a
  client seeing every event.

Environment:
  SAGA_SERVER_URL   base URL of the saga server (default http://saga-server:12080)
  PORT              port to listen on (default 12090)
  POLL_INTERVAL_MS  listing/detail poll interval (default 500)
  FEED_BUFFER       participant call events kept for replay (default 500)
  DEFINITIONS_DIR   the saga definitions the server registers (default /definitions)
"""
import json
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SAGA_SERVER_URL = os.environ.get("SAGA_SERVER_URL", "http://saga-server:12080").rstrip("/")
PARTICIPANT_URLS = [u.rstrip("/") for u in os.environ.get("PARTICIPANT_URLS", "").split(",") if u]
MAX_PACE_MS = 2000
PORT = int(os.environ.get("PORT", "12090"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_MS", "500")) / 1000
FEED_BUFFER = int(os.environ.get("FEED_BUFFER", "500"))
DEFINITIONS_DIR = Path(os.environ.get("DEFINITIONS_DIR", "/definitions"))
STATIC_DIR = Path(__file__).resolve().parent / "static"

WATERMARK_OVERLAP = timedelta(seconds=5)
SETTLED_STATUSES = {"COMPLETED", "COMPENSATED", "ESCALATED"}
MAX_PAGES_PER_TICK = 10
MAX_HYDRATION_PAGES = 50
MAX_PENDING_SAGAS = 50
MAX_PENDING_EVENTS_PER_SAGA = 50

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
}


def parse_instant(value):
    """A server timestamp as a datetime, or None; never trusts this host's clock."""
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def format_instant(dt):
    """A datetime as the ISO-8601 UTC instant form the server's query parser accepts."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


PLACEHOLDER = re.compile(r"\$\{([^}]+)\}")

# The call nodes a step may carry: execution and compensation in SAGA mode; reservation,
# confirmation, and cancellation in TCC mode.
PHASE_NODES = ("execution", "compensation", "reservation", "confirmation", "cancellation")


def load_definitions():
    """Parses the mounted definition files into {name: {mode, steps, inputKeys, json}} for the UI.

    inputKeys is what the launcher renders a field for: every ${...} key the definition's steps
    reference, minus the keys a phase's output mapping captures — those enter the context from an
    earlier call's response, not from the caller's input.
    """
    definitions = {}
    for file in sorted(DEFINITIONS_DIR.glob("*.json")):
        try:
            text = file.read_text()
            definition = json.loads(text)
            steps = []
            placeholders = []
            outputs = set()
            for step in definition.get("steps", []):
                entry = {"name": step["name"], "service": step.get("service", "")}
                for phase in PHASE_NODES:
                    node = step.get(phase) or {}
                    entry[phase + "Path"] = node.get("path", "")
                    outputs.update(node.get("output", {}).keys())
                steps.append(entry)
                for match in PLACEHOLDER.finditer(json.dumps(step)):
                    if match.group(1) not in placeholders:
                        placeholders.append(match.group(1))
            input_keys = [key for key in placeholders if key not in outputs]
            definitions[definition["name"]] = {
                "mode": definition.get("mode", "SAGA"),
                "steps": steps,
                "inputKeys": input_keys,
                # The registered file as written, so the UI can show the definition being executed.
                "json": text.rstrip(),
            }
        except Exception:
            continue  # a malformed file is the saga server's problem to report, not ours
    return definitions


class State:
    """Everything the dashboard knows, under one lock; every mutation returns what to broadcast."""

    def __init__(self):
        self.lock = threading.Lock()
        self.sagas = {}  # sagaId -> {"saga": snapshot, "timeline": [...], "settled": bool}
        self.watermark = None  # datetime of the newest updatedAt seen in a listing
        self.hydrated = False
        self.connected = False
        self.feed = deque(maxlen=FEED_BUFFER)
        self.pending_feed = OrderedDict()  # sagaId -> [events for a saga not discovered yet]
        self.definitions = load_definitions()
        self.clients = []
        self.tick_count = 0

    # --- merging ---------------------------------------------------------------

    def apply_listing_snapshot(self, snapshot):
        """Merges one listing row; returns (saga event or None, flushed feed events)."""
        with self.lock:
            saga_id = snapshot["sagaId"]
            entry = self.sagas.get(saga_id)
            if entry is None:
                entry = {"saga": snapshot, "timeline": [], "settled": False}
                self.sagas[saga_id] = entry
                return self.saga_event(entry), self.attach_pending(saga_id)
            if self.strictly_newer(snapshot, entry["saga"]):
                entry["saga"] = snapshot
                entry["settled"] = False  # a transition happened; poll detail again
                return self.saga_event(entry), []
            return None, []

    def apply_detail(self, saga_id, snapshot, timeline):
        """Merges one detail response; returns a saga event, or None when nothing changed."""
        with self.lock:
            entry = self.sagas.get(saga_id)
            if entry is None or len(timeline) < len(entry["timeline"]):
                return None
            changed = snapshot != entry["saga"] or len(timeline) != len(entry["timeline"])
            entry["saga"] = snapshot
            entry["timeline"] = timeline
            entry["settled"] = snapshot.get("status") in SETTLED_STATUSES
            return self.saga_event(entry) if changed else None

    def strictly_newer(self, snapshot, stored):
        new = parse_instant(snapshot.get("updatedAt"))
        old = parse_instant(stored.get("updatedAt"))
        return new is not None and old is not None and new > old

    def saga_event(self, entry):
        return {"saga": entry["saga"], "timeline": entry["timeline"]}

    def unsettled_ids(self):
        with self.lock:
            return [saga_id for saga_id, entry in self.sagas.items() if not entry["settled"]]

    def advance_watermark(self, snapshots):
        with self.lock:
            for snapshot in snapshots:
                updated = parse_instant(snapshot.get("updatedAt"))
                if updated is not None and (self.watermark is None or updated > self.watermark):
                    self.watermark = updated

    def listing_window(self):
        with self.lock:
            if not self.hydrated or self.watermark is None:
                return None
            return format_instant(self.watermark - WATERMARK_OVERLAP)

    def missing_definitions(self):
        with self.lock:
            return any(
                entry["saga"].get("sagaName") not in self.definitions
                for entry in self.sagas.values()
            )

    def reload_definitions(self):
        definitions = load_definitions()
        with self.lock:
            changed = definitions != self.definitions
            self.definitions = definitions
        return changed

    def definitions_payload(self):
        with self.lock:
            return dict(self.definitions)

    # --- participant feed ------------------------------------------------------

    def add_feed_event(self, event):
        """Buffers or accepts one feed event; returns it if it should broadcast now."""
        with self.lock:
            saga_id = event.get("sagaId", "")
            if saga_id and saga_id not in self.sagas:
                pending = self.pending_feed.setdefault(saga_id, [])
                if len(pending) < MAX_PENDING_EVENTS_PER_SAGA:
                    pending.append(event)
                while len(self.pending_feed) > MAX_PENDING_SAGAS:
                    self.pending_feed.popitem(last=False)
                return None
            self.feed.append(event)
            return event

    def attach_pending(self, saga_id):
        """Moves a newly discovered saga's buffered feed events into the visible feed."""
        events = self.pending_feed.pop(saga_id, [])
        self.feed.extend(events)
        return events

    # --- SSE -------------------------------------------------------------------

    def init_payload(self):
        with self.lock:
            return {
                "connected": self.connected,
                "definitions": self.definitions,
                "sagas": {saga_id: self.saga_event(e) for saga_id, e in self.sagas.items()},
                "feed": list(self.feed),
            }

    def register_client(self):
        client = {"queue": queue.Queue(maxsize=256), "dead": False}
        with self.lock:
            self.clients.append(client)
        return client

    def unregister_client(self, client):
        with self.lock:
            if client in self.clients:
                self.clients.remove(client)

    def broadcast(self, event_type, data):
        with self.lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client["queue"].put_nowait((event_type, data))
            except queue.Full:
                client["dead"] = True  # its EventSource will reconnect and resync from init

    def set_connected(self, connected):
        with self.lock:
            changed = self.connected != connected
            self.connected = connected
        if changed:
            self.broadcast("status", {"connected": connected})


STATE = State()


# --- saga-server client --------------------------------------------------------


def register_pace(saga_id, pace_ms):
    """Tells every participant how long to hold this saga's calls; best effort by design — a
    participant that misses the registration just answers that saga at full speed."""
    for base in PARTICIPANT_URLS:
        try:
            request = urllib.request.Request(
                base + "/_pace",
                data=json.dumps({"sagaId": saga_id, "delayMs": pace_ms}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(request, timeout=1)
        except Exception:
            pass


def server_get(path, params=None):
    url = SAGA_SERVER_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.loads(response.read())


def poll_forever():
    """Discovers and follows sagas; one failure means disconnected, never a crash."""
    backoff = POLL_INTERVAL
    while True:
        try:
            tick()
            STATE.set_connected(True)
            backoff = POLL_INTERVAL
            time.sleep(POLL_INTERVAL)
        except Exception:
            STATE.set_connected(False)
            time.sleep(backoff)
            backoff = min(backoff * 2, 5)


def tick():
    window = STATE.listing_window()
    max_pages = MAX_PAGES_PER_TICK if STATE.hydrated else MAX_HYDRATION_PAGES
    params = {"pageSize": "100"}
    if window is not None:
        params["updatedAfter"] = window
    for _ in range(max_pages):
        page = server_get("/sagas", params)
        snapshots = page.get("sagas", [])
        STATE.advance_watermark(snapshots)
        for snapshot in snapshots:
            saga_event, flushed = STATE.apply_listing_snapshot(snapshot)
            if saga_event is not None:
                STATE.broadcast("saga", saga_event)
            for event in flushed:
                STATE.broadcast("call", event)
        token = page.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token
    STATE.hydrated = True

    # Re-read the mounted directory when a saga references an unknown definition, and every few
    # seconds regardless, so a definition a reader adds shows up in the launcher without a restart.
    STATE.tick_count += 1
    if (STATE.tick_count % 10 == 0 or STATE.missing_definitions()) and STATE.reload_definitions():
        STATE.broadcast("definitions", STATE.definitions_payload())

    for saga_id in STATE.unsettled_ids():
        detail = server_get(f"/sagas/{urllib.parse.quote(saga_id)}/detail")
        saga_event = STATE.apply_detail(saga_id, detail["saga"], detail.get("timeline", []))
        if saga_event is not None:
            STATE.broadcast("saga", saga_event)


# --- HTTP handler --------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"status": "ok"})
        elif self.path == "/events":
            self.serve_events()
        else:
            self.serve_static()

    def do_POST(self):
        if self.path == "/feed":
            self.receive_feed()
        elif self.path == "/api/sagas":
            self.proxy_start_saga()
        else:
            self.send_json(404, {"error": "unknown path"})

    # --- routes ---------------------------------------------------------------

    def serve_static(self):
        name = self.path.split("?", 1)[0]
        if name == "/":
            name = "/index.html"
        elif name.startswith("/static/"):
            name = name[len("/static") :]
        file = (STATIC_DIR / name.lstrip("/")).resolve()
        if not file.is_relative_to(STATIC_DIR) or not file.is_file():
            self.send_json(404, {"error": "not found"})
            return
        body = file.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPES.get(file.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def serve_events(self):
        client = STATE.register_client()
        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            self.write_event("init", STATE.init_payload())
            while not client["dead"]:
                try:
                    event_type, data = client["queue"].get(timeout=15)
                    self.write_event(event_type, data)
                except queue.Empty:
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            STATE.unregister_client(client)

    def write_event(self, event_type, data):
        payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
        self.wfile.write(payload.encode("utf-8"))
        self.wfile.flush()

    def receive_feed(self):
        event = self.read_json()
        if event is None:
            self.send_json(400, {"error": "malformed feed event"})
            return
        accepted = STATE.add_feed_event(event)
        if accepted is not None:
            STATE.broadcast("call", accepted)
        self.send_response(204)
        self.end_headers()

    def proxy_start_saga(self):
        launch = self.read_json()
        if not isinstance(launch, dict):
            self.send_json(400, {"error": "malformed launch request"})
            return
        try:
            pace_ms = min(max(int(launch.pop("paceMs", 0)), 0), MAX_PACE_MS)
        except (TypeError, ValueError):
            pace_ms = 0
        # The saga id is chosen here, not by the server, so every participant can know the saga's
        # pace before its first step arrives; PUT /sagas/{id} makes the server adopt the id.
        saga_id = str(uuid.uuid4())
        if pace_ms:
            register_pace(saga_id, pace_ms)
        request = urllib.request.Request(
            f"{SAGA_SERVER_URL}/sagas/{saga_id}?async=true",
            data=json.dumps(launch).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status, payload = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, payload = error.code, error.read()
        except urllib.error.URLError:
            self.send_json(502, {"error": "saga server unreachable"})
            return
        if status == 202:
            try:
                saga_event, flushed = STATE.apply_listing_snapshot(json.loads(payload))
                if saga_event is not None:
                    STATE.broadcast("saga", saga_event)
                for event in flushed:
                    STATE.broadcast("call", event)
            except (json.JSONDecodeError, KeyError):
                pass  # the listing poll will discover it within a tick
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    # --- helpers --------------------------------------------------------------

    def read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def read_json(self):
        try:
            return json.loads(self.read_body())
        except json.JSONDecodeError:
            return None

    def send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Silences per-request logging; the poller's connectivity is what matters."""


if __name__ == "__main__":
    threading.Thread(target=poll_forever, daemon=True).start()
    print(f"[dashboard] listening on 0.0.0.0:{PORT}, watching {SAGA_SERVER_URL}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
