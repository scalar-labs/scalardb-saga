import { useState } from "preact/hooks";
import { html } from "../html.js";
import { isReadOnly, phasesFor } from "../model.js";
import { icons } from "./icons.js";

/**
 * The step list: select, reorder by dragging or with the up and down buttons, and add. A card shows
 * the step's first call and how many problems it has. Cards are divs with a button role, not
 * buttons, because Firefox does not start a drag on a button.
 */
export function StepList({ model, selectedId, problemsByStep, onSelect, onMove, onAdd }) {
  const [dragging, setDragging] = useState(null);
  const [over, setOver] = useState(null);
  const steps = model.steps ?? [];
  const last = steps.length - 1;
  return html`<section class="panel" aria-label="Steps">
    <div class="panel-h"><h2 class="lbl">Steps</h2></div>
    <ol class="steps">
      ${steps.map((step, index) => {
        const count = problemsByStep.get(step.id)?.length ?? 0;
        const name = step.name || (isReadOnly(step) ? "(not editable)" : "(no name)");
        return html`<li
          key=${step.id}
          class=${"card-row" + (over === index && dragging !== null && dragging !== index ? " drop" : "")}
          onDragOver=${(event) => {
            if (dragging !== null) {
              event.preventDefault();
              setOver(index);
            }
          }}
          onDrop=${(event) => {
            event.preventDefault();
            if (dragging !== null) {
              onMove(dragging, index);
            }
            setDragging(null);
            setOver(null);
          }}
        >
          <div
            class=${"card" + (step.id === selectedId ? " sel" : "") + (dragging === index ? " dragging" : "")}
            role="button"
            tabindex="0"
            draggable="true"
            aria-current=${step.id === selectedId ? "true" : undefined}
            onDragStart=${(event) => {
              event.dataTransfer.effectAllowed = "move";
              event.dataTransfer.setData("text/plain", step.id);
              setDragging(index);
            }}
            onDragEnd=${() => {
              setDragging(null);
              setOver(null);
            }}
            onClick=${() => onSelect(step.id)}
            onKeyDown=${(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelect(step.id);
              }
            }}
          >
            <span class="num">${index + 1}</span>
            <span class="t">${name}</span>
            <span class="svc">${step.service || ""}</span>
            <span class="route">
              ${route(model, step)}
              ${count > 0 &&
              html`<span class="chip err st">${icons.error} ${count} ${count === 1 ? "problem" : "problems"}</span>`}
            </span>
          </div>
          <div class="move">
            <button
              class="icon-btn"
              type="button"
              aria-label=${`Move ${name} up`}
              disabled=${index === 0}
              onClick=${() => onMove(index, index - 1)}
            >
              ${icons.up}
            </button>
            <button
              class="icon-btn"
              type="button"
              aria-label=${`Move ${name} down`}
              disabled=${index === last}
              onClick=${() => onMove(index, index + 1)}
            >
              ${icons.down}
            </button>
          </div>
        </li>`;
      })}
    </ol>
    ${model.steps === null
      ? html`<p class="hint pad">
          The file's "steps" is not a list, so it is kept as it is under Definition. Adding a step
          replaces it with a list.
        </p>`
      : steps.length === 0 && html`<p class="hint pad">No steps yet. A saga needs at least one.</p>`}
    <div class="pad">
      <button class="btn add-step" type="button" onClick=${onAdd}>${icons.plus} Add a step</button>
    </div>
  </section>`;
}

function route(model, step) {
  if ("raw" in step) {
    return "not an object";
  }
  if (step.stepClass) {
    return "class step";
  }
  const call = step.phases[phasesFor(model.mode)[0]];
  return call ? `${call.method ?? ""} ${call.path ?? ""}` : "";
}
