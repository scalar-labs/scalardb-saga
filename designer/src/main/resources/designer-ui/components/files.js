import { useRef, useState } from "preact/hooks";
import { html } from "../html.js";
import { Dialog } from "./dialog.js";
import { icons } from "./icons.js";

const FILE_NAME = /^[A-Za-z0-9_][A-Za-z0-9._-]{0,199}\.(json|yaml|yml)$/i;

/** The workspace's definition files, with New and Import. */
export function FilesView({ workspace, onOpen, onNew, onImport, error }) {
  const upload = useRef(null);
  const [creating, setCreating] = useState(false);
  return html`<main class="files" aria-label="Definition files">
    <div class="files-h">
      <div>
        <h1>Definition files</h1>
        <p class="hint mono">${workspace?.workspace ?? ""}</p>
      </div>
      <div class="top-actions">
        <button class="btn primary" type="button" onClick=${() => setCreating(true)}>
          ${icons.plus} New definition
        </button>
        <button class="btn" type="button" onClick=${() => upload.current.click()}>Import a file…</button>
        <input
          ref=${upload}
          type="file"
          hidden
          aria-label="Import a definition file"
          accept=".json,.yaml,.yml"
          onChange=${async (event) => {
            const file = event.currentTarget.files[0];
            event.currentTarget.value = "";
            if (file) {
              onImport(file.name, await file.text());
            }
          }}
        />
      </div>
    </div>
    ${error && html`<p class="banner err" role="alert">${error}</p>`}
    ${workspace &&
    html`<table class="file-table">
      <thead>
        <tr>
          <th scope="col">File</th>
          <th scope="col">Saga</th>
          <th scope="col">Version</th>
          <th scope="col"><span class="sr">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        ${workspace.files.map(
          (file) => html`<tr>
            <td class="mono">${file.file}</td>
            ${file.error
              ? html`<td colspan="2" class="err-text">${icons.error} ${file.error}</td>`
              : html`<td>${file.name ?? ""}</td>
                  <td class="mono">${file.version ?? ""}</td>`}
            <td class="right">
              <button class="btn small" type="button" aria-label=${`Open ${file.file}`} onClick=${() => onOpen(file.file)}>
                Open
              </button>
            </td>
          </tr>`,
        )}
      </tbody>
    </table>`}
    ${workspace && workspace.files.length === 0 && html`<p class="hint">No definition files here yet.</p>`}
    ${workspace?.uneditable?.length > 0 &&
    html`<p class="hint">
      The server also loads ${workspace.uneditable.join(", ")}, but the designer edits only files named
      with letters, digits, ".", "_" and "-". Rename ${workspace.uneditable.length === 1 ? "it" : "them"}
      to edit here.
    </p>`}
    ${creating &&
    html`<${NewDialog}
      existing=${new Set(workspace?.files.map((file) => file.file) ?? [])}
      onClose=${() => setCreating(false)}
      onCreate=${(name, fileName) => {
        setCreating(false);
        onNew(name, fileName);
      }}
    />`}
  </main>`;
}

function NewDialog({ existing, onClose, onCreate }) {
  const [name, setName] = useState("");
  const [format, setFormat] = useState("json");
  const fileName = `${name.trim()}.${format}`;
  const problem = !name.trim()
    ? "Name the saga."
    : !FILE_NAME.test(fileName)
      ? "Use letters, digits, \".\", \"_\" and \"-\", so the name also works as a file name."
      : existing.has(fileName)
        ? `${fileName} already exists. Open it instead, or choose another name.`
        : null;
  const create = () => !problem && onCreate(name.trim(), fileName);
  return html`<${Dialog}
    title="New definition"
    onClose=${onClose}
    footer=${html`<button class="btn" type="button" onClick=${onClose}>Cancel</button>
      <button class="btn primary" type="button" disabled=${problem !== null} onClick=${create}>Create</button>`}
  >
    <form
      class="grid"
      onSubmit=${(event) => {
        event.preventDefault();
        create();
      }}
    >
      <label class="field">
        <span>Saga name</span>
        <input class="in" autofocus value=${name} placeholder="order-saga" onInput=${(event) => setName(event.currentTarget.value)} />
      </label>
      <div class="field">
        <span>Format</span>
        <div class="seg" role="group" aria-label="Format">
          ${["json", "yaml"].map(
            (value) => html`<button type="button" aria-pressed=${format === value} onClick=${() => setFormat(value)}>
              ${value.toUpperCase()}
            </button>`,
          )}
        </div>
      </div>
      <p class="hint">Saved as <code class="mono">${fileName}</code> in the workspace.</p>
      ${name.trim() && problem && html`<p class="err-text" role="alert">${problem}</p>`}
    </form>
  <//>`;
}
