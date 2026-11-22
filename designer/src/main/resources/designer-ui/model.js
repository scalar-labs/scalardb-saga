// Pure helpers over the editor model, the JSON shape EditorModel documents on the Java side. No
// Preact import, so the UI tests can call them through the page.
//
// Numbers are kept as their text and never parsed here: the Java side writes them back exactly.

export const SAGA_PHASES = ["execution", "compensation"];
export const TCC_PHASES = ["reservation", "confirmation", "cancellation"];
export const FORWARD_PHASES = new Set(["execution", "reservation", "confirmation"]);
export const METHODS = ["POST", "PUT", "PATCH", "GET", "DELETE"];

/** The phases a definition of this mode has, in the order the file lists them. */
export function phasesFor(mode) {
  return mode === "TCC" ? TCC_PHASES : SAGA_PHASES;
}

/** A new, empty SAGA definition. */
export function newDefinition(name) {
  return {
    name,
    version: "1",
    mode: "SAGA",
    recoveryStrategy: null,
    timeoutMillis: null,
    defaultRetryPolicy: null,
    steps: [],
    extras: [],
  };
}

export function newCall() {
  return {
    method: "POST",
    path: "",
    query: null,
    jsonBody: null,
    stringBody: null,
    contentType: null,
    output: null,
    async: null,
    callbackTimeoutMillis: null,
    extras: [],
  };
}

/** A retry policy with every field left to the engine's default. */
export function newRetryPolicy() {
  return {
    maxAttempts: null,
    initialIntervalMillis: null,
    backoffMultiplier: null,
    maxIntervalMillis: null,
    extras: [],
  };
}

/** A new step with an unused name and an empty call for each of the mode's phases. */
export function newStep(model, base = "step") {
  const phases = {};
  for (const phase of phasesFor(model.mode)) {
    phases[phase] = newCall();
  }
  return {
    id: mintId(model),
    name: uniqueName(model, base),
    stepClass: null,
    service: "",
    transport: null,
    pivot: null,
    timeoutMillis: null,
    retryPolicy: null,
    phases,
    extras: [],
  };
}

/** A copy of a step, inserted after it, with a new id and an unused name such as ship-2. */
export function duplicateStep(model, index) {
  const copy = structuredClone(model.steps[index]);
  copy.id = mintId(model);
  if ("name" in copy) {
    copy.name = uniqueName(model, copy.name || "step");
  }
  model.steps.splice(index + 1, 0, copy);
  return copy;
}

/** Moves a step from one position to another, in place. */
export function moveStep(model, from, to) {
  if (to < 0 || to >= model.steps.length || from === to) {
    return;
  }
  const [step] = model.steps.splice(from, 1);
  model.steps.splice(to, 0, step);
}

/**
 * The first of base, base-2, base-3, ... that no step uses. A trailing -N on base is dropped first,
 * so duplicating ship-2 gives ship-3, not ship-2-2.
 */
export function uniqueName(model, base) {
  const used = new Set((model.steps ?? []).map((step) => step.name));
  if (!used.has(base)) {
    return base;
  }
  const stem = base.replace(/-\d+$/, "") || base;
  for (let n = 2; ; n++) {
    const candidate = `${stem}-${n}`;
    if (!used.has(candidate)) {
      return candidate;
    }
  }
}

// Ids only ever go up within a page, so an id a deleted step had is never handed out again while
// Undo can still bring that step back.
let lastId = 0;

/** An id no step of the model has. The Java side gives opened steps s1, s2, ...; ours start u. */
export function mintId(model) {
  const used = new Set((model.steps ?? []).map((step) => step.id));
  let id;
  do {
    id = `u${++lastId}`;
  } while (used.has(id));
  return id;
}

/**
 * The object with one known field set. A file can hold a known field with a value of the wrong
 * type, which the model keeps in extras with the field itself null; once the form sets the field,
 * the extra is dropped, since export refuses a field that is both edited and kept as it was.
 */
export function withField(object, key, value) {
  if (key === "extras" || value === null) {
    return { ...object, [key]: value };
  }
  return { ...object, [key]: value, extras: (object.extras ?? []).filter((extra) => extra.name !== key) };
}

/** A step that is a class step or not an object at all: shown, kept, but not edited. */
export function isReadOnly(step) {
  return "raw" in step || (step.stepClass !== null && step.stepClass !== undefined);
}

/** A body value of the given kind, carrying over what it can from the value it replaces. */
export function convertValue(value, kind) {
  switch (kind) {
    case "context":
      return { kind, key: value?.kind === "context" ? value.key : "" };
    case "template":
      return {
        kind,
        text:
          value?.kind === "context" && value.key
            ? "${" + value.key + "}"
            : value?.kind === "fixed" && value.type === "string"
              ? value.text
              : "",
      };
    case "fixed":
      return {
        kind,
        type: "string",
        text: value?.kind === "template" && !value.text.includes("${") ? value.text : "",
      };
    case "object":
      return { kind, fields: [] };
    case "list":
      return { kind, items: [] };
    default:
      throw new Error(`Unknown body value kind ${kind}`);
  }
}

/** The text a number field holds, or null when it is empty, which leaves the field out. */
export function numberOrNull(text) {
  return text.trim() === "" ? null : text.trim();
}

/**
 * The version with its last number raised by one, keeping its width: 1.9 becomes 1.10, v2 becomes
 * v3, rc09 becomes rc10. Null when the version has no number to raise.
 */
export function bumpVersion(version) {
  const match = /^(.*?)(\d+)(\D*)$/.exec(version ?? "");
  if (!match) {
    return null;
  }
  const [, head, digits, tail] = match;
  const raised = (BigInt(digits) + 1n).toString().padStart(digits.length, "0");
  return head + raised + tail;
}

export const RECOVERY_STRATEGIES = ["BACKWARD", "FORWARD", "MIXED"];

/** The index of the step marked as the pivot, or -1. */
export function pivotIndex(model) {
  return (model.steps ?? []).findIndex((step) => step?.pivot === true);
}

/**
 * Sets the recovery strategy, in place. Choosing MIXED marks the second-to-last step as the pivot
 * when none is marked; any other strategy clears the pivot, which only MIXED allows. BACKWARD, the
 * default, leaves the field out.
 */
export function setRecoveryStrategy(model, strategy) {
  Object.assign(model, withField(model, "recoveryStrategy", strategy === "BACKWARD" ? null : strategy));
  const steps = model.steps ?? [];
  if (strategy === "MIXED") {
    if (pivotIndex(model) < 0 && steps.length > 0) {
      setPivot(model, Math.max(steps.length - 2, 0));
    }
  } else {
    setPivot(model, -1);
  }
}

/** Marks one step as the pivot and clears the mark on every other, in place; -1 clears them all. */
export function setPivot(model, index) {
  (model.steps ?? []).forEach((step, i) => {
    if (step && !("raw" in step) && (i === index || step.pivot !== null)) {
      Object.assign(step, withField(step, "pivot", i === index ? true : null));
    }
  });
}

/**
 * Why a step's compensation can never run, or null when it can. Under FORWARD nothing is
 * compensated; under MIXED, nothing after the pivot is.
 */
export function compensationNeverRuns(model, index) {
  if (model.mode === "TCC") {
    return null;
  }
  if (model.recoveryStrategy === "FORWARD") {
    return "Never runs: under FORWARD a failed step is retried forward, and nothing is compensated.";
  }
  const pivot = pivotIndex(model);
  if (model.recoveryStrategy === "MIXED" && pivot >= 0 && index > pivot) {
    return "Never runs: this step comes after the pivot, so a failure here is retried forward.";
  }
  return null;
}

const MODE_PHASES = {
  TCC: [
    ["execution", "reservation"],
    ["compensation", "cancellation"],
  ],
  SAGA: [
    ["reservation", "execution"],
    ["cancellation", "compensation"],
  ],
};

/**
 * Switches the mode, in place, and returns what the switch drops, in words. SAGA to TCC turns each
 * execution into a reservation and each compensation into a cancellation, leaves the confirmation
 * to be written, and drops the recovery strategy and the pivot, which TCC does not have. TCC to
 * SAGA reverses this and drops the confirmations.
 */
export function switchMode(model, mode) {
  const dropped = [];
  if (mode === "TCC") {
    if (model.recoveryStrategy !== null && model.recoveryStrategy !== undefined) {
      dropped.push(`the recovery strategy ${model.recoveryStrategy}`);
    }
    Object.assign(model, withField(model, "recoveryStrategy", null));
  }
  for (const step of model.steps ?? []) {
    if (!step || "raw" in step) {
      continue;
    }
    const name = step.name || "a step";
    // A class step keeps its phases, but its pivot mark goes like any other: TCC has no pivot.
    if (mode === "TCC" && step.pivot !== null && step.pivot !== undefined) {
      dropped.push(`the pivot mark on ${name}`);
      Object.assign(step, withField(step, "pivot", null));
    }
    if (isReadOnly(step)) {
      continue;
    }
    const phases = { ...step.phases };
    for (const [from, to] of MODE_PHASES[mode] ?? []) {
      if (from in phases) {
        if (phases[from] !== null && phases[to] !== null && phases[to] !== undefined) {
          dropped.push(`the ${to} call of ${name}, replaced by its ${from}`);
        }
        phases[to] = phases[from] ?? phases[to] ?? null;
        delete phases[from];
      }
    }
    if (mode === "TCC") {
      phases.confirmation = phases.confirmation ?? null;
    } else if ("confirmation" in phases) {
      if (phases.confirmation !== null) {
        dropped.push(`the confirmation call of ${name}`);
      }
      delete phases.confirmation;
    }
    step.phases = phases;
  }
  Object.assign(model, withField(model, "mode", mode));
  return dropped;
}
