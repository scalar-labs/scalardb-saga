import { html } from "../html.js";
import { collisionFreeKey, parseSample, responseTree, suggestKey } from "../context.js";

const BODY = "$body";

/**
 * A step's sample response, pasted as text, shown as a tree of its fields. Capture on a field adds
 * an output row for it, under a suggested key that no other key here uses; capturing it again
 * removes the row. The sample is designer-only data: it is saved beside the definition, never in
 * it.
 */
export function SampleResponse({ text, call, stepName, label, keys, onText, onOutput }) {
  const { value, error } = parseSample(text);
  const rows = call.output ?? [];
  const taken = [...keys.filter((key) => !key.own).map((key) => key.key), ...rows.map((row) => row.name)];
  const capturedAs = (expression) => rows.find((row) => row.value === expression)?.name ?? null;
  const toggle = (expression, fieldName) => {
    if (capturedAs(expression) !== null) {
      const next = rows.filter((row) => row.value !== expression);
      onOutput(next.length ? next : null);
    } else {
      onOutput([...rows, { name: collisionFreeKey(suggestKey(fieldName), stepName, taken), value: expression }]);
    }
  };
  const isObject = value !== null && typeof value === "object" && !Array.isArray(value);
  return html`<section class="sub sample" aria-label="Sample response">
    <div class="sub-h"><span class="lbl">Sample response</span></div>
    <p class="hint">
      A response body like the participant's, to capture output from. Saved beside the definition in
      .designer/, never in the definition itself.
    </p>
    <textarea
      class="in mono body-text"
      aria-label=${`${label} sample response`}
      placeholder='{"payment_id": "p-1", "amount": 100}'
      value=${text ?? ""}
      onInput=${(event) => onText(event.currentTarget.value === "" ? null : event.currentTarget.value)}
    />
    ${error && html`<p class="hint" role="note">${error}</p>`}
    ${value !== null &&
    !isObject &&
    html`<p class="hint" role="note">
      The body is not a JSON object, so only the whole body ($body) can be captured.
    </p>`}
    ${text &&
    html`<div class="capture-row">
      <code class="mono">$body</code>
      <span class="hint">the whole body, as text</span>
      <${CaptureButton}
        expression=${BODY}
        label="the whole body, as text"
        capturedAs=${capturedAs(BODY)}
        onToggle=${() => toggle(BODY, "body")}
      />
    </div>`}
    ${isObject &&
    html`<ul class="tree" aria-label=${`${label} sample fields`}>
      ${responseTree(value).map((node) => html`<${Node} node=${node} capturedAs=${capturedAs} onToggle=${toggle} />`)}
    </ul>`}
  </section>`;
}

function Node({ node, capturedAs, onToggle }) {
  const leaf = node.children.length === 0;
  return html`<li>
    <div class="capture-row">
      <code class="mono">${node.name}</code>
      ${leaf && html`<span class="mono hint preview">${preview(node.value)}</span>`}
      ${node.expression
        ? html`<${CaptureButton}
            expression=${node.expression}
            label=${node.expression}
            capturedAs=${capturedAs(node.expression)}
            onToggle=${() => onToggle(node.expression, node.name)}
          />`
        : html`<span class="hint" role="note">${node.reason}</span>`}
    </div>
    ${!leaf &&
    html`<ul>
      ${node.children.map((child) => html`<${Node} node=${child} capturedAs=${capturedAs} onToggle=${onToggle} />`)}
    </ul>`}
  </li>`;
}

function CaptureButton({ expression, label, capturedAs, onToggle }) {
  return html`<button
    class=${"btn small" + (capturedAs !== null ? " captured" : "")}
    type="button"
    aria-pressed=${capturedAs !== null}
    aria-label=${`Capture ${label}`}
    title=${expression}
    onClick=${onToggle}
  >
    ${capturedAs !== null ? `Captured as ${capturedAs}` : "Capture"}
  </button>`;
}

function preview(value) {
  const text = JSON.stringify(value);
  return text.length > 40 ? `${text.slice(0, 39)}…` : text;
}
