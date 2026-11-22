// What a call can read from the saga context, and how a sample response becomes output. No Preact
// import, so the UI tests can call it through the page.

const PLACEHOLDER = /\$\{([^}]*)\}/g;

/** The phases whose call undoes a step: they may not read the step's own output. */
const BACKWARD_PHASES = new Set(["compensation", "cancellation"]);

/**
 * The keys of the sample saga input, which every step can read. The input is a JSON object; text
 * that is not one gives no keys and the reason.
 */
export function inputKeys(text) {
  if (text === null || text === undefined || text.trim() === "") {
    return { keys: [], error: null };
  }
  let value;
  try {
    value = JSON.parse(text);
  } catch (error) {
    return { keys: [], error: `Not valid JSON: ${error.message}` };
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { keys: [], error: "The saga input is a JSON object, such as {\"orderId\": \"o-1\"}." };
  }
  return { keys: Object.keys(value), error: null };
}

/**
 * The calls that have run before a step's call in the given phase, in the engine's order, as
 * [step index, phase] pairs. A saga runs each execution in turn. A TCC definition runs every
 * reservation first and then every confirmation, so a confirmation follows all the reservations.
 * A compensation or a cancellation follows the forward calls of the steps before it.
 */
function callsBefore(model, index, phase) {
  const calls = [];
  if (model.mode !== "TCC") {
    for (let i = 0; i < index; i++) {
      calls.push([i, "execution"]);
    }
    return calls;
  }
  const confirming = phase === "confirmation";
  const reserved = confirming ? (model.steps ?? []).length : index;
  for (let i = 0; i < reserved; i++) {
    calls.push([i, "reservation"]);
  }
  if (confirming) {
    for (let i = 0; i < index; i++) {
      calls.push([i, "confirmation"]);
    }
  }
  return calls;
}

/**
 * The forward phase of a step whose output its own compensation or cancellation may not read, as
 * the server checks it.
 */
function ownForwardPhase(model) {
  return model.mode === "TCC" ? "reservation" : "execution";
}

/** The keys a call's output captures, exactly as written: the engine does not trim them. */
function outputKeys(call) {
  return (call?.output ?? []).map((row) => row.name ?? "").filter((key) => key !== "");
}

/**
 * The keys a step's call in the given phase can read: the saga input, then those captured with
 * output by the calls the engine runs before it, in order, each with where it comes from. A later
 * capture of the same key replaces the earlier one, as it does in the engine. In a compensation or
 * a cancellation, the step's own outputs are listed too, marked own, because the server rejects a
 * call that reads them.
 */
export function keysFor(model, index, phase, input = []) {
  const steps = model.steps ?? [];
  const keys = new Map();
  const add = (key, entry) => {
    keys.delete(key);
    keys.set(key, entry);
  };
  for (const key of input) {
    add(key, { key, from: "input" });
  }
  for (const [at, runs] of callsBefore(model, index, phase)) {
    const step = steps[at];
    for (const key of outputKeys(step?.phases?.[runs])) {
      add(key, { key, from: step.name || "a step" });
    }
  }
  if (BACKWARD_PHASES.has(phase)) {
    const step = steps[index];
    for (const key of outputKeys(step?.phases?.[ownForwardPhase(model)])) {
      add(key, { key, from: step.name || "this step", own: true });
    }
  }
  return [...keys.values()];
}

/** The keys a call reads with ${key}, in its path, query, JSON body and text body. */
export function referencedKeys(call) {
  const keys = new Set();
  const scan = (text) => {
    for (const match of String(text ?? "").matchAll(PLACEHOLDER)) {
      if (match[1] !== "") {
        keys.add(match[1]);
      }
    }
  };
  const walk = (value) => {
    if (!value) {
      return;
    }
    if (value.kind === "context" && value.key) {
      keys.add(value.key);
    } else if (value.kind === "template" || (value.kind === "fixed" && value.type === "string")) {
      // The engine resolves ${key} in every string of a JSON body, a fixed one too.
      scan(value.text);
    } else if (value.kind === "object") {
      value.fields.forEach((field) => walk(field.value));
    } else if (value.kind === "list") {
      value.items.forEach(walk);
    }
  };
  if (!call) {
    return keys;
  }
  scan(call.path);
  (call.query ?? []).forEach((row) => scan(row.value));
  walk(call.jsonBody);
  scan(call.stringBody);
  return keys;
}

/** The keys a call reads that are not available to it, or are its own step's output. */
export function unavailableKeys(call, keys) {
  const usable = new Set(keys.filter((key) => !key.own).map((key) => key.key));
  return [...referencedKeys(call)].filter((key) => !usable.has(key));
}

/**
 * The keys calls read that no step captures and the input does not hold: when a file is opened
 * these are taken as input keys, so a working definition does not open covered in red.
 */
export function missingInputKeys(model, input = []) {
  const produced = new Set(input);
  const read = new Set();
  for (const step of model.steps ?? []) {
    for (const call of Object.values(step?.phases ?? {})) {
      outputKeys(call).forEach((key) => produced.add(key));
      referencedKeys(call).forEach((key) => read.add(key));
    }
  }
  return [...read].filter((key) => !produced.has(key));
}

/**
 * The sample input with the given keys added, each set to null, or null when the sample is not a
 * JSON object the keys can be added to.
 */
export function withInputKeys(text, keys) {
  if (!keys.length) {
    return text;
  }
  let value = {};
  if (text !== null && text !== undefined && text.trim() !== "") {
    try {
      value = JSON.parse(text);
    } catch {
      return null;
    }
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      return null;
    }
  }
  const added = keys.filter((key) => !Object.hasOwn(value, key));
  if (!added.length) {
    return text;
  }
  const entries = added.map((key) => `  ${JSON.stringify(key)}: null`).join(",\n");
  if (text === null || text === undefined || text.trim() === "") {
    return `{\n${entries}\n}`;
  }
  // The entries go in as text before the closing brace, so the rest stays exactly as written: a
  // parse and re-encode would round large numbers and drop the user's layout.
  const close = text.lastIndexOf("}");
  const before = text.slice(0, close).trimEnd();
  const separator = Object.keys(value).length > 0 ? "," : "";
  return `${before}${separator}\n${entries}\n${text.slice(close)}`;
}

/** "inventory_id" or "tracking-no" as a context key: inventoryId, trackingNo. */
export function suggestKey(name) {
  const words = String(name)
    .split(/[^A-Za-z0-9]+|(?<=[a-z0-9])(?=[A-Z])/)
    .filter((word) => word !== "");
  if (!words.length) {
    return "value";
  }
  const [first, ...rest] = words;
  const key = first.toLowerCase() + rest.map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join("");
  return /^[0-9]/.test(key) ? `field${key}` : key;
}

/**
 * The key to capture a field under: the suggestion, or, when another key already has that name,
 * the suggestion after the step's name (amount in charge becomes chargeAmount), then a number.
 */
export function collisionFreeKey(suggestion, stepName, taken) {
  const used = new Set(taken);
  if (!used.has(suggestion)) {
    return suggestion;
  }
  const prefix = suggestKey(stepName || "step");
  const prefixed = prefix + suggestion.charAt(0).toUpperCase() + suggestion.slice(1);
  if (!used.has(prefixed)) {
    return prefixed;
  }
  for (let n = 2; ; n++) {
    if (!used.has(`${prefixed}${n}`)) {
      return `${prefixed}${n}`;
    }
  }
}

/**
 * The fields of a sample response as a tree the capture panel shows. Each node has the expression
 * that captures it, or the reason it cannot be captured: the engine reads $.a.b by splitting on
 * dots and cannot index into a list, so a name with a dot, an empty name, and anything inside a
 * list are shown but not capturable.
 */
export function responseTree(value, path = "$", reason = null) {
  if (Array.isArray(value)) {
    return value.map((item, index) => ({
      name: `[${index}]`,
      value: item,
      expression: null,
      reason: "Items of a list cannot be captured",
      children: isContainer(item) ? responseTree(item, null, "Items of a list cannot be captured") : [],
    }));
  }
  return Object.entries(value).map(([name, child]) => {
    const own = reason ?? (name === "" ? "An empty field name cannot be captured" : name.includes(".") ? "A field name with a dot cannot be captured" : null);
    const expression = own === null && path !== null ? `${path}.${name}` : null;
    return {
      name,
      value: child,
      expression,
      reason: own,
      children: isContainer(child) ? responseTree(child, expression, own) : [],
    };
  });
}

function isContainer(value) {
  return value !== null && typeof value === "object";
}

/**
 * Whether a sample response has the value an output expression reads: true, false, or null when
 * there is nothing to check against (no sample, a sample that is not JSON, or $body).
 */
export function sampleHas(sample, expression) {
  if (sample === null || sample === undefined || expression === "$body" || !expression.startsWith("$.")) {
    return null;
  }
  let current = sample;
  for (const segment of expression.slice(2).split(".")) {
    if (current === null || typeof current !== "object" || Array.isArray(current) || !Object.hasOwn(current, segment)) {
      return false;
    }
    current = current[segment];
  }
  return !holdsNull(current);
}

/** Whether a value is null or holds a null at any depth: the engine refuses such output. */
function holdsNull(value) {
  if (value === null) {
    return true;
  }
  return typeof value === "object" && Object.values(value).some(holdsNull);
}

/** A sample response's text parsed as JSON, or null with the reason when it is not JSON. */
export function parseSample(text) {
  if (text === null || text === undefined || text.trim() === "") {
    return { value: null, error: null };
  }
  try {
    return { value: JSON.parse(text), error: null };
  } catch (error) {
    return { value: null, error: `Not JSON, so only the whole body ($body) can be captured: ${error.message}` };
  }
}
