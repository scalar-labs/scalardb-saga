import { html } from "../html.js";
import { newRetryPolicy, withField } from "../model.js";
import { Extras, NumberField, Problems } from "./fields.js";
import { RetryFields } from "./retry.js";

/** What applies to the whole definition: its problems, the saga timeout, the default retry policy. */
export function DefinitionPanel({ model, problems, onChange }) {
  const set = (key, value) => onChange(withField(model, key, value));
  return html`<section class="panel" aria-label="Definition">
    <div class="panel-h"><h2 class="lbl">Definition</h2></div>
    <div class="panel-b">
      <${Problems} problems=${problems} />
      <${NumberField}
        label="Saga timeout (ms)"
        value=${model.timeoutMillis}
        placeholder="none"
        onChange=${(value) => set("timeoutMillis", value)}
      />
      <label class="toggle">
        <input
          type="checkbox"
          checked=${model.defaultRetryPolicy !== null}
          onChange=${(event) =>
            set("defaultRetryPolicy", event.currentTarget.checked ? newRetryPolicy() : null)}
        />
        A default retry policy for every step
      </label>
      ${model.defaultRetryPolicy !== null &&
      html`<${RetryFields}
        policy=${model.defaultRetryPolicy}
        onChange=${(policy) => set("defaultRetryPolicy", policy)}
      />`}
      <${Extras} extras=${model.extras} where="the definition" onChange=${(extras) => set("extras", extras)} />
    </div>
  </section>`;
}
