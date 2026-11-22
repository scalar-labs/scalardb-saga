import { html } from "../html.js";
import { inputKeys } from "../context.js";
import { RECOVERY_STRATEGIES } from "../model.js";

const STRATEGY_TEXT = {
  BACKWARD: "Every completed step is compensated, and the saga ends COMPENSATED. For when every step can be undone.",
  FORWARD: "Nothing is compensated: recovery retries the failed step until it succeeds. For when no step can be undone, but every step succeeds eventually.",
  MIXED: "A failure at or before the pivot compensates; a failure after it is retried forward. For a workflow with a point of no return.",
};

/** "When a step fails": the recovery strategy of a SAGA definition. */
export function RecoveryPanel({ model, onStrategy }) {
  const current = model.recoveryStrategy ?? "BACKWARD";
  return html`<section class="panel" aria-label="When a step fails">
    <div class="panel-h"><h2 class="lbl">When a step fails</h2></div>
    <div class="panel-b" role="radiogroup" aria-label="Recovery strategy">
      ${!RECOVERY_STRATEGIES.includes(current) &&
      html`<p class="err-text">The file's recovery strategy ${current} is not one the engine knows; choose one.</p>`}
      ${RECOVERY_STRATEGIES.map(
        (strategy) => html`<label class="choice">
          <input
            type="radio"
            name="recovery-strategy"
            value=${strategy}
            checked=${current === strategy}
            onChange=${() => onStrategy(strategy)}
          />
          <span>
            <strong class="mono">${strategy}</strong>${strategy === "BACKWARD" ? " (default)" : ""}
            <small class="hint">${STRATEGY_TEXT[strategy]}</small>
          </span>
        </label>`,
      )}
    </div>
  </section>`;
}

/**
 * The sample saga input: the keys every step can read, with sample values. Designer-only data,
 * saved beside the definition, never in it.
 */
export function SampleInputPanel({ text, onChange }) {
  const { keys, error } = inputKeys(text);
  return html`<section class="panel" aria-label="Sample input">
    <div class="panel-h"><h2 class="lbl">Sample input</h2></div>
    <div class="panel-b">
      <p class="hint">The saga's input, a JSON object. Its keys are in the context of every step.</p>
      <textarea
        class="in mono body-text"
        aria-label="Sample input"
        placeholder='{"orderId": "o-1", "amount": 100}'
        value=${text ?? ""}
        onInput=${(event) => onChange(event.currentTarget.value === "" ? null : event.currentTarget.value)}
      />
      ${error && html`<p class="err-text" role="note">${error}</p>`}
      ${keys.length > 0 &&
      html`<div class="chips" aria-label="Input keys">
        ${keys.map((key) => html`<span class="tok static">${key}</span>`)}
      </div>`}
    </div>
  </section>`;
}
