import { useRef } from "preact/hooks";
import { html } from "../html.js";
import { parseSample, sampleHas, unavailableKeys } from "../context.js";
import { FORWARD_PHASES, METHODS, newCall, withField } from "../model.js";
import { FieldsEditor } from "./body.js";
import { Extras, NumberField, Rows, TextField } from "./fields.js";
import { icons } from "./icons.js";
import { SampleResponse } from "./sample.js";

const PHASE_TEXT = {
  execution: "the forward call",
  compensation: "undoes the execution if the saga fails",
  reservation: "reserves what the step needs",
  confirmation: "confirms the reservation once every step has reserved",
  cancellation: "releases the reservation if the saga fails",
};
const CONTENT_TYPES = ["application/json", "text/plain", "application/x-www-form-urlencoded", "application/xml"];

const title = (phase) => phase.charAt(0).toUpperCase() + phase.slice(1);

/**
 * One phase's HTTP call, or a button to add it when the file does not have it. The call that takes
 * the step's sample response (sample and onSample set) shows it, to capture output from. A call
 * that can never run under the recovery strategy is dimmed with the reason, and stays editable,
 * because the file still requires it.
 */
export function CallEditor({ stepName, phase, call, onChange, keys, neverRuns, sample, onSample }) {
  const label = `${stepName} ${phase}`;
  const note = neverRuns && html`<p class="hint never" role="note">${neverRuns}</p>`;
  if (!call) {
    return html`<section class=${"call missing " + phase + (neverRuns ? " dim" : "")} aria-label=${title(phase)}>
      <h3>${title(phase)} <small>${PHASE_TEXT[phase]}</small></h3>
      ${note}
      <p class="hint">This step has no ${phase} call.</p>
      <button class="btn small" type="button" onClick=${() => onChange(newCall())}>
        Add the ${phase} call
      </button>
    </section>`;
  }
  const set = (key, value) => onChange(withField(call, key, value));
  const forward = FORWARD_PHASES.has(phase);
  const unavailable = unavailableKeys(call, keys);
  const own = new Set(keys.filter((key) => key.own).map((key) => key.key));
  const parsed = sample === undefined ? null : parseSample(sample).value;
  return html`<section class=${"call " + phase + (neverRuns ? " dim" : "")} aria-label=${title(phase)}>
    <h3>${title(phase)} <small>${PHASE_TEXT[phase]}</small></h3>
    ${note}
    ${unavailable.length > 0 &&
    html`<p class="err-text" role="note">
      ${icons.error}
      ${unavailable.map((key, i) => html`${i ? ", " : ""}<code class="mono">${"${" + key + "}"}</code>`)}:
      ${unavailable.every((key) => own.has(key))
        ? " this step's own output, which a compensation cannot read."
        : " not available here. Add the key to the sample input, or capture it in an earlier step."}
    </p>`}
    <div class="route-row">
      <label class="field">
        <span>Method</span>
        <select
          class="in mono"
          aria-label=${`${label} method`}
          value=${call.method ?? ""}
          onChange=${(event) => set("method", event.currentTarget.value)}
        >
          ${call.method !== null && !METHODS.includes(call.method) &&
          html`<option value=${call.method}>${call.method} (not a method the engine sends)</option>`}
          ${METHODS.map((method) => html`<option value=${method}>${method}</option>`)}
        </select>
      </label>
      <${PathField} label=${label} value=${call.path} keys=${keys} onChange=${(path) => set("path", path)} />
    </div>
    <${Rows}
      title="Query"
      rows=${call.query}
      nameLabel="name"
      valueLabel="value"
      namePlaceholder="page"
      valuePlaceholder="$\{pageNumber}"
      onChange=${(rows) => set("query", rows)}
    />
    <${Body} call=${call} label=${label} keys=${keys} onChange=${onChange} />
    ${forward &&
    html`<${Rows}
      title="Output"
      rows=${call.output}
      nameLabel="context key"
      valueLabel="expression"
      namePlaceholder="paymentId"
      valuePlaceholder="$.payment_id"
      hint="Each row puts a value from the response body into the saga context under its key: $.field, $.a.b, or $body for the whole body as text."
      flag=${(row) =>
        sampleHas(parsed, row.value) === false ? "Missing from the sample response, or null in it: the step fails when the response is like that." : null}
      onChange=${(rows) => set("output", rows)}
    />`}
    ${sample !== undefined &&
    html`<${SampleResponse}
      text=${sample}
      call=${call}
      stepName=${stepName}
      label=${label}
      keys=${keys}
      onText=${onSample}
      onOutput=${(rows) => set("output", rows)}
    />`}
    <details class="more" open=${call.contentType !== null || call.async !== null || call.callbackTimeoutMillis !== null}>
      <summary>More options</summary>
      <div class="grid2">
        <${TextField}
          label="Content type"
          value=${call.contentType}
          nullable
          mono
          placeholder="application/json"
          list="content-types"
          onChange=${(value) => set("contentType", value)}
        />
        ${forward &&
        html`<label class="toggle">
          <input
            type="checkbox"
            checked=${call.async === true}
            onChange=${(event) => set("async", event.currentTarget.checked ? true : null)}
          />
          Async: the participant reports the result later through a callback
        </label>`}
        ${forward &&
        call.async === true &&
        html`<${NumberField}
          label="Callback timeout (ms)"
          value=${call.callbackTimeoutMillis}
          placeholder="the saga timeout"
          onChange=${(value) => set("callbackTimeoutMillis", value)}
        />`}
      </div>
      <datalist id="content-types">
        ${CONTENT_TYPES.map((type) => html`<option value=${type} />`)}
      </datalist>
    </details>
    <${Extras} extras=${call.extras} where=${`the ${phase} call`} onChange=${(extras) => set("extras", extras)} />
  </section>`;
}

/** The path, with a chip per available key that inserts ${key} where the cursor is. */
function PathField({ label, value, keys, onChange }) {
  const input = useRef(null);
  // The step's own outputs, listed for a compensation or a cancellation, cannot be read there.
  const usable = keys.filter((key) => !key.own);
  const insert = (key) => {
    const element = input.current;
    const text = value ?? "";
    const start = element?.selectionStart ?? text.length;
    const end = element?.selectionEnd ?? start;
    const token = "${" + key + "}";
    onChange(text.slice(0, start) + token + text.slice(end));
    requestAnimationFrame(() => {
      element?.focus();
      element?.setSelectionRange(start + token.length, start + token.length);
    });
  };
  return html`<div class="field path">
    <${TextField}
      label="Path"
      inputRef=${input}
      value=${value}
      mono
      placeholder="/orders/$\{orderId}"
      onChange=${onChange}
    />
    ${usable.length > 0 &&
    html`<div class="chips" aria-label=${`${label}: keys to insert`}>
      ${usable.map(
        (key) => html`<button class="tok" type="button" title=${`from ${key.from}`} onClick=${() => insert(key.key)}>
          ${"${" + key.key + "}"}
        </button>`,
      )}
    </div>`}
  </div>`;
}

/** The body: none, a JSON body built from fields, or a text body. */
function Body({ call, label, keys, onChange }) {
  const mode = call.jsonBody !== null ? "json" : call.stringBody !== null ? "text" : "none";
  const both = call.jsonBody !== null && call.stringBody !== null;
  // A file can hold both bodies, which the parser refuses; choosing one keeps that body as it is.
  const choose = (next) => {
    if (next === mode && !both) {
      return;
    }
    const jsonBody = next === "json" ? (call.jsonBody ?? { kind: "object", fields: [] }) : null;
    const stringBody = next === "text" ? (call.stringBody ?? "") : null;
    onChange(withField(withField(call, "jsonBody", jsonBody), "stringBody", stringBody));
  };
  return html`<div class="sub">
    <div class="sub-h">
      <span class="lbl">Body</span>
      <div class="seg" role="group" aria-label=${`${label} body`}>
        ${[
          ["none", "No body"],
          ["json", "JSON body"],
          ["text", "Text body"],
        ].map(
          ([value, text]) => html`<button type="button" aria-pressed=${mode === value} onClick=${() => choose(value)}>
            ${text}
          </button>`,
        )}
      </div>
    </div>
    ${call.jsonBody !== null &&
    html`<${FieldsEditor}
      fields=${call.jsonBody.fields}
      keys=${keys}
      label=${`${label} body`}
      onChange=${(fields) => onChange(withField(call, "jsonBody", { ...call.jsonBody, fields }))}
    />`}
    ${call.stringBody !== null &&
    html`<textarea
      class="in mono body-text"
      aria-label=${`${label} text body`}
      placeholder="order=$\{orderId}"
      value=${call.stringBody}
      onInput=${(event) => onChange(withField(call, "stringBody", event.currentTarget.value))}
    />`}
    ${both && html`<p class="hint">The file has both a JSON body and a text body; keep one.</p>`}
  </div>`;
}
