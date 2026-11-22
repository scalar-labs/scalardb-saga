// What a call can read from the saga context. No Preact import, so the UI tests can call it
// through the page.

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
 * The keys a step's call in the given phase can read: those captured with output by the calls the
 * engine runs before it, in order, each with the step that captures it. A later capture of the
 * same key replaces the earlier one, as it does in the engine.
 */
export function keysFor(model, index, phase) {
  const steps = model.steps ?? [];
  const keys = new Map();
  for (const [at, runs] of callsBefore(model, index, phase)) {
    const step = steps[at];
    const call = step?.phases?.[runs];
    if (!call?.output) {
      continue;
    }
    for (const row of call.output) {
      const key = row.name.trim();
      if (key) {
        keys.delete(key);
        keys.set(key, { key, from: step.name || "a step" });
      }
    }
  }
  return [...keys.values()];
}
