import { html } from "../html.js";
import { compensationNeverRuns, isReadOnly, newRetryPolicy, phasesFor, setPivot, withField } from "../model.js";
import { CallEditor } from "./call.js";
import { Extras, NumberField, Problems, TextField } from "./fields.js";
import { icons } from "./icons.js";
import { RetryFields } from "./retry.js";

/** The editor of the selected step. */
export function StepEditor({ model, index, problems, keysFor, sample, services, onChange, onModel, onSample, onDelete, onDuplicate }) {
  const step = model.steps[index];
  const set = (key, value) => onChange(withField(step, key, value));
  const head = html`<div class="ed-acts">
    <button class="btn small" type="button" onClick=${onDuplicate}>${icons.copy} Duplicate</button>
    <button class="btn small danger" type="button" onClick=${onDelete}>${icons.trash} Delete</button>
  </div>`;
  if (isReadOnly(step)) {
    return html`<section class="editor" aria-label="Step">
      <div class="ed-head">
        <h2>${step.name || "A step the form cannot edit"}</h2>
        ${head}
      </div>
      <${Problems} problems=${problems} />
      ${"raw" in step
        ? html`<p class="hint">This entry in "steps" is not an object, so it is kept as it is:</p>
            <pre class="code">${step.raw}</pre>`
        : html`<p class="hint">
            A class step, defined in code by <code class="mono">${step.stepClass}</code>. The designer
            keeps it as it is; a declarative definition loaded by the server cannot use it.
          </p>`}
    </section>`;
  }
  const phases = phasesFor(model.mode);
  // The sample response belongs to the step's first call, the one that captures its output.
  const sampled = phases[0];
  const here = keysFor(sampled);
  const mixed = model.mode !== "TCC" && model.recoveryStrategy === "MIXED";
  // Phases the file has that this mode does not use, such as an execution in a TCC definition: kept
  // and shown, so that the parser's problem about them can be fixed here.
  const others = Object.keys(step.phases).filter((phase) => !phases.includes(phase));
  return html`<section class="editor" aria-label="Step">
    <div class="ed-head">
      <div class="grid2 grow">
        <${TextField} label="Step name" value=${step.name} onChange=${(value) => set("name", value)} />
        <${TextField}
          label="Service"
          value=${step.service}
          list=${services.length ? "service-names" : undefined}
          placeholder="payment"
          onChange=${(value) => set("service", value)}
        />
      </div>
      ${head}
    </div>
    ${services.length > 0 &&
    html`<datalist id="service-names">
      ${services.map((service) => html`<option value=${service} />`)}
    </datalist>`}
    <${Problems} problems=${problems} />
    ${mixed &&
    html`<label class="toggle pivot">
      <input
        type="checkbox"
        checked=${step.pivot === true}
        onChange=${(event) => {
          const checked = event.currentTarget.checked;
          onModel((m) => setPivot(m, checked ? index : -1));
        }}
      />
      Pivot: the point of no return. A failure after it is retried forward, never compensated.
    </label>`}
    <div class="context-bar" role="group" aria-label="Context here">
      <span class="lbl">Context here</span>
      ${here.length === 0
        ? html`<span class="hint">Nothing yet: add keys to the sample input, or capture output in an earlier step.</span>`
        : here.map((key) => html`<span class="tok static" title=${`from ${key.from}`}>${key.key}<small> · ${key.from}</small></span>`)}
    </div>
    ${[...phases, ...others].map(
      (phase) => html`<${CallEditor}
        key=${phase}
        stepName=${step.name || "step"}
        phase=${phase}
        call=${step.phases[phase] ?? null}
        keys=${keysFor(phase)}
        neverRuns=${phase === "compensation" ? compensationNeverRuns(model, index) : null}
        sample=${phase === sampled ? (sample ?? null) : undefined}
        onSample=${onSample}
        onChange=${(call) =>
          onChange({
            ...step,
            phases: { ...step.phases, [phase]: call },
            // A phase the file held with the wrong type is an extra; setting the call replaces it.
            extras: (step.extras ?? []).filter((extra) => call === null || extra.name !== phase),
          })}
      />`,
    )}
    <section class="sub boxed" aria-label="Retry and timeout">
      <h3>Retry and timeout</h3>
      <label class="toggle">
        <input
          type="checkbox"
          checked=${step.retryPolicy !== null}
          onChange=${(event) => set("retryPolicy", event.currentTarget.checked ? newRetryPolicy() : null)}
        />
        A retry policy of its own, instead of the definition's
      </label>
      ${step.retryPolicy !== null &&
      html`<${RetryFields} policy=${step.retryPolicy} onChange=${(policy) => set("retryPolicy", policy)} />`}
      <div class="grid2">
        <${NumberField}
          label="Step timeout (ms)"
          value=${step.timeoutMillis}
          placeholder="none"
          onChange=${(value) => set("timeoutMillis", value)}
        />
        ${step.transport !== null &&
        html`<${TextField}
          label="Transport"
          value=${step.transport}
          nullable
          onChange=${(value) => set("transport", value)}
        />`}
      </div>
    </section>
    <${Extras} extras=${step.extras} where="this step" onChange=${(extras) => set("extras", extras)} />
  </section>`;
}
