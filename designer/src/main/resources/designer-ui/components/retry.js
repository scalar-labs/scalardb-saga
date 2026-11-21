import { html } from "../html.js";
import { withField } from "../model.js";
import { Extras, NumberField } from "./fields.js";

/** A retry policy's four numbers; an empty field leaves it to the engine's default. */
export function RetryFields({ policy, onChange }) {
  const set = (key, value) => onChange(withField(policy, key, value));
  return html`<div class="grid2">
      <${NumberField}
        label="Max attempts"
        value=${policy.maxAttempts}
        placeholder="3"
        onChange=${(value) => set("maxAttempts", value)}
      />
      <${NumberField}
        label="First wait (ms)"
        value=${policy.initialIntervalMillis}
        placeholder="1000"
        onChange=${(value) => set("initialIntervalMillis", value)}
      />
      <${NumberField}
        label="Backoff multiplier"
        value=${policy.backoffMultiplier}
        placeholder="2.0"
        onChange=${(value) => set("backoffMultiplier", value)}
      />
      <${NumberField}
        label="Longest wait (ms)"
        value=${policy.maxIntervalMillis}
        placeholder="30000"
        onChange=${(value) => set("maxIntervalMillis", value)}
      />
    </div>
    <${Extras} extras=${policy.extras} where="the retry policy" onChange=${(extras) => set("extras", extras)} />`;
}
