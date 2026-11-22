import { html } from "../html.js";
import { numberOrNull } from "../model.js";
import { icons } from "./icons.js";

/** A labelled text field; an empty value becomes null when nullable is set. */
export function TextField({ label, value, onChange, placeholder, mono, nullable, list, inputRef }) {
  return html`<label class="field">
    <span>${label}</span>
    <input
      ref=${inputRef}
      class=${"in" + (mono ? " mono" : "")}
      value=${value ?? ""}
      placeholder=${placeholder}
      list=${list}
      onInput=${(event) => {
        const text = event.currentTarget.value;
        onChange(nullable && text === "" ? null : text);
      }}
    />
  </label>`;
}

/**
 * A number field. The text is kept as typed and sent as text: the Java side writes it back
 * exactly, and reports anything that is not a number as a problem.
 */
export function NumberField({ label, value, onChange, placeholder }) {
  return html`<label class="field">
    <span>${label}</span>
    <input
      class="in mono"
      inputmode="decimal"
      value=${value ?? ""}
      placeholder=${placeholder}
      onInput=${(event) => onChange(numberOrNull(event.currentTarget.value))}
    />
  </label>`;
}

/** Name and value rows, such as a call's query or output. Null rows mean the field is absent. */
export function Rows({ title, rows, onChange, nameLabel, valueLabel, namePlaceholder, valuePlaceholder, hint }) {
  const list = rows || [];
  const set = (index, key, text) =>
    onChange(list.map((row, i) => (i === index ? { ...row, [key]: text } : row)));
  const remove = (index) => {
    const next = list.filter((_, i) => i !== index);
    onChange(next.length ? next : null);
  };
  return html`<div class="sub">
    <div class="sub-h">
      <span class="lbl">${title}</span>
      <button
        class="btn ghost small"
        type="button"
        onClick=${() => onChange([...list, { name: "", value: "" }])}
      >
        ${icons.plus} Add
      </button>
    </div>
    ${hint && html`<p class="hint">${hint}</p>`}
    ${list.map(
      (row, index) => html`<div class="kv">
        <input
          class="in mono"
          aria-label=${`${title} ${index + 1}: ${nameLabel}`}
          placeholder=${namePlaceholder}
          value=${row.name}
          onInput=${(event) => set(index, "name", event.currentTarget.value)}
        />
        <input
          class="in mono"
          aria-label=${`${title} ${index + 1}: ${valueLabel}`}
          placeholder=${valuePlaceholder}
          value=${row.value}
          onInput=${(event) => set(index, "value", event.currentTarget.value)}
        />
        <button
          class="icon-btn danger"
          type="button"
          aria-label=${`Remove ${title} ${index + 1}`}
          onClick=${() => remove(index)}
        >
          ${icons.close}
        </button>
      </div>`,
    )}
  </div>`;
}

/**
 * Fields the form cannot edit: unknown fields, and known ones holding a value of the wrong type.
 * They are kept as they were in the file; the parser rejects unknown ones, so each can be removed.
 */
export function Extras({ extras, onChange, where }) {
  if (!extras || !extras.length) {
    return null;
  }
  return html`<div class="extras" role="group" aria-label=${`Fields the form does not edit in ${where}`}>
    <p class="hint">
      Kept as they are in the file. The form cannot edit them; remove any the definition should not
      have.
    </p>
    ${extras.map(
      (extra, index) => html`<div class="extra">
        <code class="mono">${extra.name}: ${extra.json}</code>
        <button
          class="btn small"
          type="button"
          onClick=${() => onChange(extras.filter((_, i) => i !== index))}
        >
          Remove ${extra.name}
        </button>
      </div>`,
    )}
  </div>`;
}

/** A list of problems, each with an icon and text, never colour alone. */
export function Problems({ problems }) {
  if (!problems.length) {
    return null;
  }
  return html`<ul class="problems">
    ${problems.map(
      (problem) => html`<li class="problem">
        ${icons.error}<span>${problem.message}</span>
      </li>`,
    )}
  </ul>`;
}
