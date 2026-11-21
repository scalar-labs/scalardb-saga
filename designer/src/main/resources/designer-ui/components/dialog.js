import { useEffect, useRef } from "preact/hooks";
import { html } from "../html.js";
import { icons } from "./icons.js";

/**
 * A modal <dialog>: the browser traps focus inside it, closes it on Escape, and returns focus to
 * where it was. Mount it only while it is open.
 */
export function Dialog({ title, onClose, children, footer, wide }) {
  const ref = useRef(null);
  useEffect(() => {
    const dialog = ref.current;
    dialog.showModal();
    return () => dialog.open && dialog.close();
  }, []);
  return html`<dialog
    ref=${ref}
    class=${wide ? "wide" : ""}
    aria-label=${title}
    onCancel=${(event) => {
      event.preventDefault();
      onClose();
    }}
  >
    <div class="dlg-h">
      <h2>${title}</h2>
      <button class="icon-btn" type="button" aria-label="Close" onClick=${onClose}>
        ${icons.close}
      </button>
    </div>
    <div class="dlg-b">${children}</div>
    ${footer && html`<div class="dlg-f">${footer}</div>`}
  </dialog>`;
}
