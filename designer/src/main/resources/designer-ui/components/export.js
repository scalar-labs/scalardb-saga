import { useEffect, useState } from "preact/hooks";
import { api, saveText } from "../api.js";
import { html } from "../html.js";
import { Dialog } from "./dialog.js";

/** Downloads: the definition in either format, its service-file templates, or both as a zip. */
export function ExportDialog({ model, definitionText, problemCount, checkState, defaultFormat, onClose, onError }) {
  const [format, setFormat] = useState(defaultFormat);
  const [templates, setTemplates] = useState(null);
  useEffect(() => {
    api.serviceTemplates(model).then((result) => setTemplates(result.templates), onError);
  }, []);
  const blocked = problemCount > 0 || checkState !== "checked";
  const download = (path) => api.download(path, { model, format }).catch(onError);
  return html`<${Dialog}
    title="Export"
    wide
    onClose=${onClose}
    footer=${html`<button class="btn" type="button" onClick=${onClose}>Close</button>`}
  >
    <div class="sub-h">
      <div class="seg" role="group" aria-label="Format">
        ${["JSON", "YAML"].map(
          (value) => html`<button type="button" aria-pressed=${format === value} onClick=${() => setFormat(value)}>
            ${value}
          </button>`,
        )}
      </div>
      <div class="top-actions">
        <button class="btn primary" type="button" disabled=${blocked} onClick=${() => download("/api/export")}>
          Download the definition
        </button>
        <button class="btn" type="button" disabled=${blocked} onClick=${() => download("/api/bundle")}>
          Download with service templates (zip)
        </button>
      </div>
    </div>
    ${checkState === "checking" && html`<p class="hint" role="status">Checking the definition…</p>`}
    ${checkState === "failed" &&
    html`<p class="err-text" role="alert">
      The definition could not be checked, so it is not offered for download. Close this and check
      it again.
    </p>`}
    ${checkState === "checked" &&
    problemCount > 0 &&
    html`<p class="err-text" role="alert">
      Fix the ${problemCount === 1 ? "problem" : `${problemCount} problems`} first: a definition the
      server would refuse is not offered for download.
    </p>`}
    <p class="hint">
      Sample responses and the sample input are designer-only data, so they are not part of the file.
    </p>
    <h3 class="lbl">Preview (JSON)</h3>
    <pre class="code" aria-label="Definition preview">${definitionText}</pre>
    <h3 class="lbl">Service files</h3>
    ${templates === null && html`<p class="hint">Loading…</p>`}
    ${templates?.length === 0 && html`<p class="hint">The definition names no service yet.</p>`}
    ${templates?.map(
      (template) => html`<details class="template">
        <summary>
          <code class="mono">${template.fileName}</code>
          ${template.exists && html`<span class="chip ok">already in the services folder</span>`}
          ${!template.usable && html`<span class="chip warn">rename the service to use it as a file name</span>`}
        </summary>
        <pre class="code">${template.text}</pre>
        <button class="btn small" type="button" disabled=${!template.usable} onClick=${() => saveText(template.text, template.fileName)}>
          Download ${template.fileName}
        </button>
      </details>`,
    )}
  <//>`;
}
