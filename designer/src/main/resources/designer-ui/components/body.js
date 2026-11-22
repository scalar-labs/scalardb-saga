import { html } from "../html.js";
import { convertValue } from "../model.js";
import { icons } from "./icons.js";

const KINDS = [
  ["context", "From the context"],
  ["fixed", "Fixed value"],
  ["template", "Text with keys"],
  ["object", "Object"],
  ["list", "List"],
];
const TYPES = ["string", "number", "boolean", "null"];

/** The fields of an object body value, each a name and a value of any kind. */
export function FieldsEditor({ fields, onChange, keys, label, depth = 0 }) {
  const set = (index, field) => onChange(fields.map((f, i) => (i === index ? field : f)));
  return html`<div class=${"fields" + (depth ? " nested" : "")}>
    ${fields.map(
      (field, index) => html`<div class="body-row">
        <input
          class="in mono"
          aria-label=${`${label} field ${index + 1} name`}
          placeholder="field"
          value=${field.name}
          onInput=${(event) => set(index, { ...field, name: event.currentTarget.value })}
        />
        <${ValueEditor}
          value=${field.value}
          keys=${keys}
          label=${`${label} field ${index + 1}`}
          depth=${depth}
          onChange=${(value) => set(index, { ...field, value })}
        />
        <button
          class="icon-btn danger"
          type="button"
          aria-label=${`Remove ${label} field ${index + 1}`}
          onClick=${() => onChange(fields.filter((_, i) => i !== index))}
        >
          ${icons.close}
        </button>
      </div>`,
    )}
    <button
      class="btn ghost small"
      type="button"
      onClick=${() => onChange([...fields, { name: "", value: { kind: "context", key: "" } }])}
    >
      ${icons.plus} Add field
    </button>
  </div>`;
}

/**
 * One body value: a key from the context, a fixed value, text with keys, or a nested object or
 * list. A context key exported as a whole "${key}" keeps the value's type in the JSON body.
 */
export function ValueEditor({ value, onChange, keys, label, depth }) {
  const listId = `${label.replaceAll(" ", "-")}-keys`;
  return html`<div class="val">
    <select
      class="in"
      aria-label=${`${label} kind`}
      value=${value.kind}
      onChange=${(event) => onChange(convertValue(value, event.currentTarget.value))}
    >
      ${KINDS.map(([kind, text]) => html`<option value=${kind}>${text}</option>`)}
    </select>
    ${value.kind === "context" &&
    html`<input
        class="in mono"
        aria-label=${`${label} context key`}
        placeholder="orderId"
        list=${listId}
        value=${value.key}
        onInput=${(event) => onChange({ ...value, key: event.currentTarget.value })}
      />
      <datalist id=${listId}>
        ${keys.map((key) => html`<option value=${key.key}>${key.from}</option>`)}
      </datalist>`}
    ${value.kind === "template" &&
    html`<input
      class="in mono"
      aria-label=${`${label} text`}
      placeholder="order-$\{orderId}"
      value=${value.text}
      onInput=${(event) => onChange({ ...value, text: event.currentTarget.value })}
    />`}
    ${value.kind === "fixed" && html`<${FixedValue} value=${value} onChange=${onChange} label=${label} />`}
    ${value.kind === "object" &&
    html`<${FieldsEditor}
      fields=${value.fields}
      keys=${keys}
      label=${label}
      depth=${depth + 1}
      onChange=${(fields) => onChange({ ...value, fields })}
    />`}
    ${value.kind === "list" &&
    html`<${ItemsEditor}
      items=${value.items}
      keys=${keys}
      label=${label}
      depth=${depth + 1}
      onChange=${(items) => onChange({ ...value, items })}
    />`}
  </div>`;
}

function FixedValue({ value, onChange, label }) {
  return html`<div class="fixed">
    <select
      class="in"
      aria-label=${`${label} type`}
      value=${value.type}
      onChange=${(event) => {
        const type = event.currentTarget.value;
        const text = type === "null" ? "null" : type === "boolean" ? "true" : value.type === "null" || value.type === "boolean" ? "" : value.text;
        onChange({ ...value, type, text });
      }}
    >
      ${TYPES.map((type) => html`<option value=${type}>${type}</option>`)}
    </select>
    ${value.type === "boolean" &&
    html`<select
      class="in mono"
      aria-label=${`${label} value`}
      value=${value.text}
      onChange=${(event) => onChange({ ...value, text: event.currentTarget.value })}
    >
      <option value="true">true</option>
      <option value="false">false</option>
      ${value.text !== "true" && value.text !== "false" && html`<option value=${value.text}>${value.text}</option>`}
    </select>`}
    ${(value.type === "string" || value.type === "number") &&
    html`<input
      class="in mono"
      aria-label=${`${label} value`}
      inputmode=${value.type === "number" ? "decimal" : undefined}
      placeholder=${value.type === "number" ? "42" : "JPY"}
      value=${value.text}
      onInput=${(event) => onChange({ ...value, text: event.currentTarget.value })}
    />`}
  </div>`;
}

function ItemsEditor({ items, onChange, keys, label, depth }) {
  const set = (index, item) => onChange(items.map((it, i) => (i === index ? item : it)));
  return html`<div class="fields nested">
    ${items.map(
      (item, index) => html`<div class="body-row item">
        <span class="hint">${index + 1}</span>
        <${ValueEditor}
          value=${item}
          keys=${keys}
          label=${`${label} item ${index + 1}`}
          depth=${depth}
          onChange=${(next) => set(index, next)}
        />
        <button
          class="icon-btn danger"
          type="button"
          aria-label=${`Remove ${label} item ${index + 1}`}
          onClick=${() => onChange(items.filter((_, i) => i !== index))}
        >
          ${icons.close}
        </button>
      </div>`,
    )}
    <button
      class="btn ghost small"
      type="button"
      onClick=${() => onChange([...items, { kind: "context", key: "" }])}
    >
      ${icons.plus} Add item
    </button>
  </div>`;
}
