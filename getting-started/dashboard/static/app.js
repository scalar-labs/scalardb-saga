"use strict";

// All rendering is idempotent over full objects: the SSE stream replays complete state on every
// (re)connection, so nothing here depends on having seen every event.

const state = {
  definitions: {},
  sagas: new Map(), // sagaId -> {saga, timeline}
  calls: new Map(), // feed event id -> merged request/response call
  callOrder: [], // feed event ids, arrival order
  selectedId: null,
  serverConnected: true, // dashboard <-> saga-server
  sseConnected: false, // browser <-> dashboard
  feedMineOnly: true,
  launchValues: {}, // launcher field values, kept across definition switches
  defStepName: null, // step whose object is highlighted in the definition panel
};
let scrollDefinitionToStep = false;

const MAX_FEED_ROWS = 200;
let orderCounter = 1000 + Math.floor(Math.random() * 9000);

// --- SSE ------------------------------------------------------------------------

function connect() {
  const source = new EventSource("/events");
  source.addEventListener("init", (e) => rebuild(JSON.parse(e.data)));
  source.addEventListener("saga", (e) => upsertSaga(JSON.parse(e.data)));
  source.addEventListener("call", (e) => addCall(JSON.parse(e.data), true));
  source.addEventListener("definitions", (e) => {
    state.definitions = JSON.parse(e.data);
    renderLaunchControls();
    renderDetail();
  });
  source.addEventListener("status", (e) => {
    state.serverConnected = JSON.parse(e.data).connected;
    renderBanner();
  });
  source.onopen = () => {
    state.sseConnected = true;
    renderBanner();
  };
  source.onerror = () => {
    state.sseConnected = false;
    renderBanner();
  };
}

function rebuild(payload) {
  state.definitions = payload.definitions || {};
  state.sagas = new Map(Object.entries(payload.sagas || {}));
  state.calls = new Map();
  state.callOrder = [];
  for (const event of payload.feed || []) addCall(event, false);
  state.serverConnected = !!payload.connected;
  if (state.selectedId && !state.sagas.has(state.selectedId)) state.selectedId = null;
  renderLaunchControls();
  renderAll();
}

function upsertSaga(entry) {
  state.sagas.set(entry.saga.sagaId, entry);
  renderList();
  if (entry.saga.sagaId === state.selectedId) renderDetail();
  renderFeed(); // direction labels and attempt badges may resolve once the saga is known
}

function addCall(event, render) {
  let call = state.calls.get(event.id);
  if (!call) {
    call = { id: event.id };
    state.calls.set(event.id, call);
    state.callOrder.push(event.id);
    while (state.callOrder.length > MAX_FEED_ROWS) {
      state.calls.delete(state.callOrder.shift());
    }
  }
  Object.assign(call, event);
  if (render) {
    renderFeed();
    if (event.sagaId && event.sagaId === state.selectedId) renderDetail(); // per-step logs live there
  }
}

// --- launching ------------------------------------------------------------------

// The launcher covers every definition the dashboard found in the mounted definitions directory:
// one field per input key the definition reads. The pace slider applies to every launch — the
// dashboard registers the pace with the participants, so no definition needs to know about it.

function renderLaunchControls() {
  const select = document.getElementById("in-definition");
  const current = select.value;
  select.replaceChildren();
  const names = Object.keys(state.definitions).sort();
  for (const name of names) select.append(new Option(name, name));
  if (names.includes(current)) select.value = current;
  else if (names.includes("order-saga-paced")) select.value = "order-saga-paced";
  renderLaunchInputs();
}

function renderLaunchInputs() {
  const definition = state.definitions[document.getElementById("in-definition").value];
  const container = document.getElementById("dyn-inputs");
  container.replaceChildren();
  for (const key of (definition && definition.inputKeys) || []) {
    if (!(key in state.launchValues)) state.launchValues[key] = defaultInputFor(key);
    const label = el("label", "", `${key} `);
    const field = document.createElement("input");
    field.size = 7;
    field.dataset.key = key;
    field.value = state.launchValues[key];
    field.oninput = () => {
      state.launchValues[key] = field.value;
    };
    label.append(field);
    container.append(label);
  }
}

function defaultInputFor(key) {
  if (key === "orderId") return nextOrderId();
  return { amount: "100", item: "widget", quantity: "2" }[key] ?? "";
}

async function launch() {
  const name = document.getElementById("in-definition").value;
  const definition = state.definitions[name];
  if (!definition) return;
  const input = {};
  for (const field of document.querySelectorAll("#dyn-inputs input")) {
    input[field.dataset.key] = field.value;
  }
  setButtonsEnabled(false);
  try {
    const response = await fetch("/api/sagas", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sagaName: name,
        input,
        paceMs: Number(document.getElementById("in-pace").value),
      }),
    });
    const body = await response.json().catch(() => null);
    if (response.status === 202 && body && body.sagaId) {
      state.selectedId = body.sagaId;
      if ("orderId" in input) state.launchValues.orderId = nextOrderId();
      renderLaunchInputs();
      renderAll();
    } else {
      toast(`Launch failed (${response.status}): ${body && body.error ? body.error : "see server logs"}`);
    }
  } catch (e) {
    toast("Launch failed: dashboard unreachable");
  } finally {
    setButtonsEnabled(true);
  }
}

function nextOrderId() {
  orderCounter += 1;
  return `o-${orderCounter}`;
}

function setButtonsEnabled(enabled) {
  for (const button of document.querySelectorAll("#launch-form button")) button.disabled = !enabled;
}

// --- derivations ----------------------------------------------------------------

function stepsFor(entry) {
  const definition = state.definitions[entry.saga.sagaName];
  if (definition) return definition.steps;
  // Unknown definition: degrade to what the timeline has shown so far.
  const steps = [];
  for (const event of entry.timeline) {
    if (event.stepIndex != null && event.stepName) {
      steps[event.stepIndex] = { name: event.stepName, service: "", executionPath: "", compensationPath: "" };
    }
  }
  for (let i = 0; i < steps.length; i++) {
    if (!steps[i]) steps[i] = { name: `step ${i}`, service: "", executionPath: "", compensationPath: "" };
  }
  return steps;
}

function deriveStepStates(entry, steps) {
  const states = steps.map(() => "pending");
  let lastCompleted = -1;
  let lowestCompensated = -1;
  let failedAt = -1;
  for (const event of entry.timeline) {
    const i = event.stepIndex;
    if (i == null || i < 0 || i >= steps.length) continue;
    if (event.type === "STEP_COMPLETED") {
      states[i] = "completed";
      lastCompleted = Math.max(lastCompleted, i);
    } else if (event.type === "STEP_FAILED") {
      states[i] = "failed";
      failedAt = i;
    } else if (event.type === "STEP_COMPENSATED") {
      states[i] = "compensated";
      lowestCompensated = lowestCompensated === -1 ? i : Math.min(lowestCompensated, i);
    } else if (event.type === "STEP_COMPENSATION_FAILED") {
      states[i] = "compensation-failed";
    }
  }
  // There is no step-started event; the active step is inferred from status + last outcome.
  const status = entry.saga.status;
  if (status === "RUNNING") {
    const active = lastCompleted + 1;
    if (active < steps.length && states[active] === "pending") states[active] = "executing";
  } else if (status === "COMPENSATING") {
    // The engine compensates the failed step too, then walks backward.
    const active = lowestCompensated === -1 ? (failedAt !== -1 ? failedAt : lastCompleted) : lowestCompensated - 1;
    if (active >= 0 && (states[active] === "failed" || states[active] === "completed")) {
      states[active] = "compensating";
    }
  }
  return states;
}

// The participant's own log for one step of the selected saga: every call that hit it, in order,
// so retries and the compensation call pile up right under the service they reached.
function stepLog(entry, step) {
  const log = el("div", "step-log");
  const rows = [];
  for (const id of state.callOrder) {
    const call = state.calls.get(id);
    const mine = call.step === step.name || (call.step && call.step.startsWith(step.name + "."));
    if (call.sagaId !== entry.saga.sagaId || !mine) continue;
    rows.push(call);
  }
  for (const call of rows.slice(-6)) {
    const direction = callDirection(call);
    const row = el("div", `sl-row${call.status == null ? " in-flight" : ""}`);
    row.append(el("span", `sl-path ${isUndoDirection(direction) ? "sl-comp" : "sl-exec"}`, call.path));
    if (call.status == null) {
      row.append(el("span", "feed-pending", "…"));
    } else {
      row.append(el("span", `feed-status s${Math.floor(call.status / 100)}xx`, String(call.status)));
    }
    row.title = `request: ${call.body || ""}\nresponse: ${call.response || ""}`;
    log.append(row);
  }
  return log;
}

// A TCC definition compiles to 2N engine steps — every reservation (indices 0..N-1), then every
// confirmation (N..2N-1), named "<step>.reserve" and "<step>.confirm" — so compiled index i maps
// to box i % N, and a cancelled reservation arrives as an ordinary STEP_COMPENSATED.
function deriveTccStepStates(entry, steps) {
  const n = steps.length;
  const states = steps.map(() => "pending");
  let lastCompleted = -1;
  let lowestCancelled = -1;
  let failedAt = -1;
  for (const event of entry.timeline) {
    const i = event.stepIndex;
    if (i == null || i < 0 || i >= 2 * n) continue;
    if (event.type === "STEP_COMPLETED") {
      lastCompleted = Math.max(lastCompleted, i);
      states[i % n] = i < n ? "reserved" : "confirmed";
    } else if (event.type === "STEP_FAILED") {
      states[i % n] = i < n ? "reserve-failed" : "confirm-failed";
      if (i < n) failedAt = i;
    } else if (event.type === "STEP_COMPENSATED") {
      states[i % n] = "cancelled";
      lowestCancelled = lowestCancelled === -1 ? i % n : Math.min(lowestCancelled, i % n);
    } else if (event.type === "STEP_COMPENSATION_FAILED") {
      states[i % n] = "cancel-failed";
    }
  }
  const status = entry.saga.status;
  if (status === "RUNNING") {
    const active = lastCompleted + 1;
    if (active < 2 * n) {
      const box = active % n;
      if (active < n && states[box] === "pending") states[box] = "reserving";
      else if (active >= n && states[box] === "reserved") states[box] = "confirming";
    }
  } else if (status === "COMPENSATING") {
    const active =
      lowestCancelled === -1 ? (failedAt !== -1 ? failedAt : lastCompleted) : lowestCancelled - 1;
    if (
      active >= 0 &&
      active < n &&
      (states[active] === "reserved" || states[active] === "reserve-failed")
    ) {
      states[active] = "cancelling";
    }
  }
  return states;
}

// Maps each TCC state onto the SAGA state family whose colors it reuses; states absent here
// (pending, reserved, confirming) have their own styling.
const TCC_FAMILY = {
  reserving: "executing",
  confirmed: "completed",
  cancelling: "compensating",
  cancelled: "compensated",
  "reserve-failed": "failed",
  "confirm-failed": "failed",
  "cancel-failed": "compensation-failed",
};

function stepOf(definition, callStep) {
  if (!definition || !callStep) return null;
  return definition.steps.find((s) => s.name === callStep || callStep.startsWith(s.name + "."));
}

function callDirection(call) {
  const entry = call.sagaId ? state.sagas.get(call.sagaId) : null;
  const definition = entry ? state.definitions[entry.saga.sagaName] : null;
  const step = stepOf(definition, call.step);
  if (!step) return "";
  if (call.path === step.reservationPath) return "reserve";
  if (call.path === step.confirmationPath) return "confirm";
  if (call.path === step.cancellationPath) return "cancel";
  if (call.path === step.compensationPath) return "compensation";
  if (call.path === step.executionPath) return "execution";
  return "";
}

function isUndoDirection(direction) {
  return direction === "compensation" || direction === "cancel";
}

function attemptNumber(call) {
  const direction = callDirection(call);
  if (!call.sagaId || direction === "" || isUndoDirection(direction)) return 0;
  let attempt = 0;
  for (const id of state.callOrder) {
    const other = state.calls.get(id);
    if (other.sagaId === call.sagaId && other.step === call.step && other.path === call.path) {
      attempt += 1;
      if (other.id === call.id) return attempt;
    }
  }
  return attempt;
}

// --- rendering ------------------------------------------------------------------

function renderAll() {
  renderBanner();
  renderList();
  renderDetail();
  renderFeed();
}

function renderBanner() {
  const banner = document.getElementById("banner");
  if (!state.sseConnected) {
    banner.textContent = "Reconnecting to the dashboard…";
  } else if (!state.serverConnected) {
    banner.textContent = "Saga server unreachable — retrying…";
  } else {
    banner.classList.add("hidden");
    return;
  }
  banner.classList.remove("hidden");
}

function sortedSagas() {
  return [...state.sagas.values()].sort((a, b) =>
    (b.saga.updatedAt || "").localeCompare(a.saga.updatedAt || "")
  );
}

function renderList() {
  const list = document.getElementById("saga-list");
  list.replaceChildren();
  const sagas = sortedSagas();
  document.getElementById("saga-empty").classList.toggle("hidden", sagas.length > 0);
  for (const entry of sagas) {
    const item = el("li", entry.saga.sagaId === state.selectedId ? "selected" : "");
    const top = el("div", "saga-row");
    top.append(el("span", "saga-name", entry.saga.sagaName), statusBadge(entry.saga.status));
    const bottom = el("div", "saga-row saga-row-sub");
    bottom.append(
      el("code", "saga-id", entry.saga.sagaId.slice(0, 8)),
      el("span", "saga-time", timeOf(entry.saga.updatedAt))
    );
    item.append(top, bottom);
    item.onclick = () => {
      if (state.selectedId !== entry.saga.sagaId) state.defStepName = null;
      state.selectedId = entry.saga.sagaId;
      renderAll();
    };
    list.append(item);
  }
}

function renderDetail() {
  const entry = state.selectedId ? state.sagas.get(state.selectedId) : null;
  document.getElementById("detail-empty").classList.toggle("hidden", !!entry);
  document.getElementById("detail").classList.toggle("hidden", !entry);
  if (!entry) {
    const definition = document.getElementById("definition");
    definition.textContent = "Select a saga to see its definition.";
    definition.className = "empty";
    return;
  }
  document.getElementById("definition").className = "";

  document.getElementById("detail-name").textContent = entry.saga.sagaName;
  document.getElementById("detail-id").textContent = entry.saga.sagaId;
  const status = document.getElementById("detail-status");
  status.textContent = entry.saga.status;
  status.className = `badge st-${entry.saga.status}`;

  renderPipeline(entry);
  renderTimeline(entry);
  renderDefinition(entry);
}

// Finds the text range of each step object inside the raw definition JSON, in array order, so a
// clicked step box can highlight its own definition verbatim. Brace matching must be
// string-aware: the ${...} placeholders put braces inside string literals.
function stepTextRanges(text) {
  const ranges = [];
  let depth = 0;
  let inString = false;
  let escaped = false;
  let lastString = "";
  let current = "";
  let inSteps = false;
  let stepsDepth = 0;
  let start = -1;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (c === "\\") escaped = true;
      else if (c === '"') {
        inString = false;
        lastString = current;
      } else current += c;
    } else if (c === '"') {
      inString = true;
      current = "";
    } else if (c === "{" || c === "[") {
      if (!inSteps && c === "[" && lastString === "steps" && depth === 1) {
        inSteps = true;
        stepsDepth = depth + 1;
      }
      depth++;
      if (inSteps && c === "{" && depth === stepsDepth + 1) start = i;
    } else if (c === "}" || c === "]") {
      if (inSteps && c === "}" && depth === stepsDepth + 1) ranges.push({ start, end: i + 1 });
      depth--;
      if (inSteps && c === "]" && depth === stepsDepth - 1) inSteps = false;
    }
  }
  return ranges;
}

// Tokenizes JSON text into colored spans: keys, strings, numbers, literals, and — inside string
// values — the ${input} placeholders and $.output paths that carry the saga's data flow.
const JSON_TOKEN = /("(?:[^"\\]|\\.)*")(\s*:)?|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|\b(true|false|null)\b/g;
const DATA_FLOW = /\$\{[^}]+\}|\$(?:\.[A-Za-z_][\w]*)+|\$body/g;

function appendColoredJson(container, text) {
  JSON_TOKEN.lastIndex = 0;
  let last = 0;
  let match;
  while ((match = JSON_TOKEN.exec(text)) !== null) {
    if (match.index > last) container.append(text.slice(last, match.index));
    if (match[1] !== undefined) {
      if (match[2]) {
        container.append(el("span", "j-key", match[1]), match[2]);
      } else {
        appendColoredString(container, match[1]);
      }
    } else if (match[3] !== undefined) {
      container.append(el("span", "j-num", match[3]));
    } else {
      container.append(el("span", "j-lit", match[4]));
    }
    last = match.index + match[0].length;
  }
  container.append(text.slice(last));
}

function appendColoredString(container, quoted) {
  const span = el("span", "j-str");
  DATA_FLOW.lastIndex = 0;
  let last = 0;
  let match;
  while ((match = DATA_FLOW.exec(quoted)) !== null) {
    if (match.index > last) span.append(quoted.slice(last, match.index));
    span.append(el("span", "j-flow", match[0]));
    last = match.index + match[0].length;
  }
  span.append(quoted.slice(last));
  container.append(span);
}

function renderDefinition(entry) {
  const container = document.getElementById("definition");
  const definition = state.definitions[entry.saga.sagaName];
  if (!definition || !definition.json) {
    container.textContent = "(not in conf/definitions)";
    return;
  }
  const text = definition.json;
  const ranges = stepTextRanges(text);
  container.replaceChildren();
  if (ranges.length !== definition.steps.length) {
    appendColoredJson(container, text); // unexpected shape; colored but no step highlighting
    return;
  }
  let cursor = 0;
  ranges.forEach((range, i) => {
    appendColoredJson(container, text.slice(cursor, range.start));
    const span = el("span", "def-step");
    if (definition.steps[i].name === state.defStepName) span.classList.add("active");
    appendColoredJson(span, text.slice(range.start, range.end));
    container.append(span);
    cursor = range.end;
  });
  appendColoredJson(container, text.slice(cursor));
  if (scrollDefinitionToStep) {
    scrollDefinitionToStep = false;
    const active = container.querySelector(".def-step.active");
    const pane = document.getElementById("definition-pane");
    if (active) pane.scrollTop = Math.max(0, active.offsetTop - 12);
  }
}


function renderPipeline(entry) {
  const steps = stepsFor(entry);
  const definition = state.definitions[entry.saga.sagaName];
  const tcc = definition && definition.mode === "TCC";
  const states = tcc ? deriveTccStepStates(entry, steps) : deriveStepStates(entry, steps);
  const pipeline = document.getElementById("pipeline");
  pipeline.replaceChildren();
  if (steps.length === 0) {
    pipeline.append(el("p", "empty", "No step information yet."));
    return;
  }
  const forward = el("div", "pipe-row");
  steps.forEach((step, i) => {
    if (i > 0) forward.append(el("span", "pipe-arrow", "→"));
    const cls = tcc ? `${states[i]} ${TCC_FAMILY[states[i]] || ""}`.trim() : states[i];
    const stack = el("div", `step-stack ${cls}`);
    const box = el("div", `step ${cls}`);
    box.append(el("div", "step-name", step.name), el("div", "step-service", step.service));
    box.append(el("div", "step-state", states[i].replace("-", " ")));
    box.title = "click to highlight this step in the definition";
    box.onclick = (event) => {
      event.stopPropagation(); // the clear-on-outside-click listener must not see this click
      state.defStepName = state.defStepName === step.name ? null : step.name;
      scrollDefinitionToStep = state.defStepName !== null;
      renderDetail();
    };
    stack.append(box, document.getElementById("db-template").content.cloneNode(true));
    stack.append(stepLog(entry, step));
    forward.append(stack);
  });
  pipeline.append(forward);
  if (tcc && states.some((s) => s.startsWith("cancel"))) {
    pipeline.append(el("div", "pipe-note", "← the reservations are cancelled right-to-left; nothing was confirmed"));
  } else if (states.some((s) => s.startsWith("compensat"))) {
    pipeline.append(el("div", "pipe-note", "← compensation runs right-to-left, starting at the failed step"));
  }
}

function renderTimeline(entry) {
  const list = document.getElementById("timeline");
  list.replaceChildren();
  for (const event of entry.timeline) {
    const row = el("li", "");
    row.append(el("span", "tl-time", timeOf(event.timestamp)), typeBadge(event.type));
    if (event.stepName) row.append(el("span", "tl-step", event.stepName));
    if (event.resultingStatus) row.append(statusBadge(event.resultingStatus));
    if (event.operator) row.append(el("span", "tl-operator", `by ${event.operator}`));
    if (event.detail) {
      const detail = el("span", "tl-detail", event.detail);
      detail.title = event.detail;
      row.append(detail);
    }
    list.append(row);
  }
  list.scrollTop = list.scrollHeight;
}

function renderFeed() {
  const list = document.getElementById("feed");
  list.replaceChildren();
  const mineOnly = state.feedMineOnly && state.selectedId;
  let shown = 0;
  for (const id of state.callOrder) {
    const call = state.calls.get(id);
    if (mineOnly && call.sagaId !== state.selectedId) continue;
    shown += 1;
    const row = el("li", call.status == null ? "in-flight" : "");
    row.append(el("span", "tl-time", timeOf(call.ts)), el("span", "feed-service", `[${call.service}]`));
    row.append(el("span", "feed-call", `${call.method} ${call.path}`));
    const direction = callDirection(call);
    if (direction) row.append(el("span", `feed-dir ${direction}`, direction));
    const attempt = attemptNumber(call);
    if (attempt > 1) row.append(el("span", "feed-attempt", `attempt ${attempt}`));
    if (call.status == null) {
      row.append(el("span", "feed-pending", "…"));
    } else {
      row.append(el("span", `feed-status s${Math.floor(call.status / 100)}xx`, String(call.status)));
      if (call.durationMs != null) row.append(el("span", "feed-duration", `${call.durationMs} ms`));
    }
    const body = call.status == null ? call.body : call.response;
    if (body) {
      const preview = el("span", "feed-body", body);
      preview.title = `request: ${call.body || ""}\nresponse: ${call.response || ""}`;
      row.append(preview);
    }
    list.append(row);
  }
  document.getElementById("feed-empty").classList.toggle("hidden", shown > 0);
  list.scrollTop = list.scrollHeight;
}

// --- small helpers --------------------------------------------------------------

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

function statusBadge(status) {
  return el("span", `badge st-${status}`, status);
}

function typeBadge(type) {
  const kind = type.startsWith("SAGA_") ? "saga" : "step";
  const bad = type.includes("FAILED") || type.includes("ESCALATED");
  return el("span", `badge ev-${kind}${bad ? " ev-bad" : ""}`, type);
}

function timeOf(instant) {
  const date = new Date(instant);
  return isNaN(date) ? "" : date.toLocaleTimeString(undefined, { hour12: false }) +
    "." + String(date.getMilliseconds()).padStart(3, "0");
}

let toastTimer = null;
function toast(message) {
  const node = document.getElementById("toast");
  node.textContent = message;
  node.classList.remove("hidden");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => node.classList.add("hidden"), 5000);
}

// --- wiring ---------------------------------------------------------------------

document.getElementById("in-pace").oninput = (e) => {
  document.getElementById("pace-value").textContent = `${e.target.value} ms`;
};
document.getElementById("feed-mine").onchange = (e) => {
  state.feedMineOnly = e.target.checked;
  renderFeed();
};
document.getElementById("in-definition").onchange = renderLaunchInputs;
document.getElementById("btn-launch").onclick = launch;

// Clicking anywhere other than a step box clears the definition highlight. Step boxes stop their
// clicks from propagating, so any click that reaches here landed elsewhere.
document.addEventListener("click", () => {
  if (state.defStepName === null) return;
  state.defStepName = null;
  renderDetail();
});

// The side panes and the participant-calls pane are resizable by dragging their inner borders;
// each chosen size sticks (localStorage). sign says which drag direction grows the pane.
function attachResize(handleId, pane, { axis, sign, min, maxFraction, storageKey }) {
  const limit = () =>
    Math.round((axis === "x" ? window.innerWidth : window.innerHeight) * maxFraction);
  const clamp = (value) => Math.min(Math.max(value, min), limit());
  const measure = () => (axis === "x" ? pane.offsetWidth : pane.offsetHeight);
  const property = axis === "x" ? "width" : "height";
  const saved = Number(localStorage.getItem(storageKey));
  if (saved) pane.style[property] = `${clamp(saved)}px`;
  document.getElementById(handleId).onpointerdown = (down) => {
    down.preventDefault();
    const handle = down.target;
    const start = measure();
    handle.setPointerCapture(down.pointerId);
    handle.onpointermove = (move) => {
      const delta = axis === "x" ? move.clientX - down.clientX : move.clientY - down.clientY;
      pane.style[property] = `${clamp(start + sign * delta)}px`;
    };
    handle.onpointerup = () => {
      handle.onpointermove = handle.onpointerup = null;
      localStorage.setItem(storageKey, String(measure()));
    };
  };
}

attachResize("saga-list-resize", document.getElementById("saga-list-pane"), {
  axis: "x", sign: 1, min: 180, maxFraction: 0.4, storageKey: "sagaPaneWidth",
});
attachResize("definition-resize", document.getElementById("definition-pane"), {
  axis: "x", sign: -1, min: 240, maxFraction: 0.5, storageKey: "definitionPaneWidth",
});
attachResize("feed-resize", document.getElementById("feed-pane"), {
  axis: "y", sign: -1, min: 110, maxFraction: 0.6, storageKey: "feedPaneHeight",
});

connect();
