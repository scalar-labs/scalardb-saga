// What a step can read from the saga context. No Preact import, so the UI tests can call it
// through the page.
import { FORWARD_PHASES } from "./model.js";

/**
 * The keys earlier steps capture with output, in order, each with the step that captures it. A
 * later capture of the same key replaces the earlier one, as it does in the engine.
 */
export function keysBefore(model, index) {
  const keys = new Map();
  for (const step of (model.steps ?? []).slice(0, index)) {
    for (const [phase, call] of Object.entries(step.phases || {})) {
      if (!FORWARD_PHASES.has(phase) || !call || !call.output) {
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
  }
  return [...keys.values()];
}
