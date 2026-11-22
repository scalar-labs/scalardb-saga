// The designer's page: the file list, and the editor of one open definition.
import { render } from "preact";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "./api.js";
import { DefinitionPanel } from "./components/definition.js";
import { Dialog } from "./components/dialog.js";
import { ExportDialog } from "./components/export.js";
import { FilesView } from "./components/files.js";
import { icons } from "./components/icons.js";
import { StepEditor } from "./components/step.js";
import { StepList } from "./components/steps.js";
import { keysFor } from "./context.js";
import { html } from "./html.js";
import { duplicateStep, moveStep, newDefinition, newStep, withField } from "./model.js";

const VALIDATE_DELAY_MS = 300;

// Each opened document gets a new session number. An answer or an Undo that belongs to an earlier
// session, such as a save still in flight when the user opened another file, is dropped.
let lastSession = 0;
const TOAST_MS = 6000;

// Unsaved edits are kept in this browser, per workspace and file, so a closed tab or a reload loses
// nothing. They are a personal convenience, so they never go into the workspace.
const draftKey = (workspace, file) => `saga-designer:draft:${workspace}:${file}`;

function readDraft(workspace, file) {
  try {
    const text = localStorage.getItem(draftKey(workspace, file));
    return text ? JSON.parse(text) : null;
  } catch {
    return null;
  }
}

/**
 * The definitions that exist only as drafts in this browser: new or imported ones never saved to
 * the workspace, newest first.
 */
function browserOnlyDrafts(workspace) {
  const prefix = draftKey(workspace.workspace, "");
  const inWorkspace = new Set(workspace.files.map((file) => file.file));
  const drafts = [];
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      const file = key?.startsWith(prefix) ? key.slice(prefix.length) : null;
      if (file && !inWorkspace.has(file)) {
        const draft = readDraft(workspace.workspace, file);
        if (draft?.isNew && draft.model) {
          drafts.push({ file, name: draft.model.name, version: draft.model.version, savedAt: draft.savedAt });
        }
      }
    }
  } catch {
    // Storage is blocked: there are no drafts to list.
  }
  return drafts.sort((a, b) => b.savedAt - a.savedAt);
}

function writeDraft(workspace, file, draft) {
  try {
    if (draft) {
      localStorage.setItem(draftKey(workspace, file), JSON.stringify(draft));
    } else {
      localStorage.removeItem(draftKey(workspace, file));
    }
  } catch {
    // Storage is full or blocked: the draft is a convenience, so editing goes on without it.
  }
}

function App() {
  const [workspace, setWorkspace] = useState(null);
  const [filesError, setFilesError] = useState(null);
  const [doc, setDoc] = useState(null);
  const [selectedId, setSelectedId] = useState(null);
  const [validation, setValidation] = useState({ problems: [], definition: "" });
  const [dialog, setDialog] = useState(null);
  const [toast, setToast] = useState(null);
  const [saving, setSaving] = useState(false);
  // Bumped to show the file list again after a draft is discarded, or to check the model again.
  const [, setDraftsChanged] = useState(0);
  const [recheck, setRecheck] = useState(0);
  const revision = useRef(0);
  const session = useRef(0);

  const refreshWorkspace = useCallback(
    () =>
      api.workspace().then(
        (result) => {
          setWorkspace(result);
          setFilesError(null);
        },
        (error) => setFilesError(error.message),
      ),
    [],
  );
  useEffect(() => {
    refreshWorkspace();
  }, []);

  const notify = useCallback((text, action) => setToast({ text, action, at: Date.now() }), []);
  useEffect(() => {
    if (!toast) {
      return undefined;
    }
    const timer = setTimeout(() => setToast(null), TOAST_MS);
    return () => clearTimeout(timer);
  }, [toast]);

  const modelJson = doc && JSON.stringify(doc.model);
  const dirty = doc !== null && modelJson !== doc.openedJson;

  // Validation runs a moment after the last edit. Every edit takes a new revision at once, and an
  // answer for any other revision is dropped, so an answer for an earlier model is never shown,
  // even one that arrives before the next request is sent. The results also record the model
  // they describe, so the page can tell whether they still apply to what is on screen.
  useEffect(() => {
    if (!doc) {
      return undefined;
    }
    const current = ++revision.current;
    const sentJson = modelJson;
    const timer = setTimeout(() => {
      api.validate(current, doc.model, doc.isNew ? undefined : doc.file).then(
        (result) => {
          if (result.revision === revision.current) {
            setValidation({
              problems: result.problems,
              definition: result.definition,
              sameVersion: result.sameVersion,
              modelJson: sentJson,
            });
          }
        },
        (error) => {
          if (current === revision.current) {
            setValidation((previous) => ({ ...previous, failed: error.message }));
            notify(`Validation failed: ${error.message}`);
          }
        },
      );
    }, VALIDATE_DELAY_MS);
    return () => clearTimeout(timer);
  }, [doc?.model, recheck]);
  const checkFailed = Boolean(validation.failed);
  const checked = !checkFailed && doc !== null && validation.modelJson === modelJson;

  // A draft is written once the model differs from what was loaded, and removed only by a save that
  // reaches the file or by "Discard them": closing the restore dialog, or editing back to the
  // loaded state, keeps it. A new or imported definition is stored at once, since the browser is
  // the only place it exists until it is saved; the file list offers it again. Nothing is written
  // while the restore question is open, so the stored draft is still there whatever the answer. A
  // layout effect, so the draft is stored before the edit is even painted, and a tab closed right
  // after typing loses nothing. It also runs when a save answers: a save removes the draft, and
  // edits made while it was on its way are stored again.
  const restorePending = dialog?.type === "restore";
  useLayoutEffect(() => {
    if (doc && workspace && !restorePending && (doc.isNew || modelJson !== doc.loadedJson)) {
      writeDraft(workspace.workspace, doc.file, {
        model: doc.model,
        designerData: doc.designerData,
        isNew: doc.isNew,
        format: doc.format,
        warnings: doc.warnings,
        savedAt: Date.now(),
      });
    }
  }, [doc?.model, doc?.loadedJson, doc?.isNew, restorePending]);

  /** Shows the file list, leaving the open document behind. */
  const close = () => {
    session.current = ++lastSession;
    revision.current++;
    setDoc(null);
    setDialog(null);
    setToast(null);
  };

  const load = (next) => {
    session.current = ++lastSession;
    // Drops any validation answer still on its way for the previous document.
    revision.current++;
    const loadedJson = JSON.stringify(next.model);
    // A new or imported file is unsaved from the start, so it counts as changed until saved.
    const openedJson = next.isNew ? "" : loadedJson;
    setDoc({ ...next, session: session.current, loadedJson, openedJson });
    setValidation({ problems: [], definition: "", sameVersion: false });
    setSelectedId(next.model.steps?.[0]?.id ?? null);
    setToast(null);
    const draft = workspace && readDraft(workspace.workspace, next.file);
    setDialog(draft && JSON.stringify(draft.model) !== loadedJson ? { type: "restore", draft } : null);
  };

  const openFile = async (file) => {
    try {
      const result = await api.open(file);
      load({
        file,
        format: result.format,
        model: result.model,
        designerData: result.designerData,
        baseSha: result.sha256,
        warnings: result.warnings,
        isNew: false,
      });
    } catch (error) {
      close();
      setFilesError(error.message);
    }
  };

  const openDraft = (file) => {
    const draft = workspace && readDraft(workspace.workspace, file);
    if (!draft) {
      setDraftsChanged((count) => count + 1);
      return;
    }
    load({
      file,
      format: draft.format ?? (file.endsWith(".json") ? "JSON" : "YAML"),
      model: draft.model,
      designerData: draft.designerData ?? { sampleInput: null, steps: {} },
      baseSha: null,
      warnings: draft.warnings ?? [],
      isNew: true,
    });
  };

  const discardDraft = (file) => {
    const draft = readDraft(workspace.workspace, file);
    writeDraft(workspace.workspace, file, null);
    setDraftsChanged((count) => count + 1);
    notify(`Discarded ${file}.`, {
      label: "Undo",
      run: () => {
        writeDraft(workspace.workspace, file, draft);
        setDraftsChanged((count) => count + 1);
      },
    });
  };

  const newFile = (name, fileName) =>
    load({
      file: fileName,
      format: fileName.endsWith(".json") ? "JSON" : "YAML",
      model: newDefinition(name),
      designerData: { sampleInput: null, steps: {} },
      baseSha: null,
      warnings: [],
      isNew: true,
    });

  const importFile = async (fileName, text) => {
    try {
      const result = await api.importFile(fileName, text);
      load({
        file: fileName,
        format: result.format,
        model: result.model,
        designerData: result.designerData,
        baseSha: null,
        warnings: result.warnings,
        isNew: true,
      });
      if (workspace?.files.some((file) => file.file === fileName)) {
        notify(`${fileName} already exists in the workspace; saving asks before replacing it.`);
      }
    } catch (error) {
      setFilesError(error.message);
    }
  };

  /** Applies an edit to a copy of the model, if the document of that session is still open. */
  const edit = (change, forSession = session.current) =>
    setDoc((current) => {
      if (!current || current.session !== forSession) {
        return current;
      }
      const model = structuredClone(current.model);
      change(model);
      return { ...current, model };
    });

  const save = async (base = doc.baseSha) => {
    const sent = doc.model;
    const file = doc.file;
    const savedSession = doc.session;
    // The answer belongs to this document only: if another one was opened meanwhile, it is dropped.
    const stillOpen = () => session.current === savedSession;
    setSaving(true);
    try {
      const result = await api.save(file, sent, doc.designerData, base);
      if (!stillOpen()) {
        refreshWorkspace();
        return;
      }
      const clean = result.problems.length === 0;
      setDoc((current) =>
        current?.session !== savedSession
          ? current
          : {
              ...current,
              baseSha: result.sha256 ?? current.baseSha,
              isNew: result.sha256 ? false : current.isNew,
              openedJson: clean ? JSON.stringify(sent) : current.openedJson,
              loadedJson: clean ? JSON.stringify(sent) : current.loadedJson,
            },
      );
      if (clean && workspace) {
        writeDraft(workspace.workspace, file, null);
      }
      const notes = [];
      if (result.definitionWritten) {
        notes.push(`Saved ${file}.`);
      } else if (result.unchanged) {
        notes.push("No changes to the definition.");
      } else {
        const count = result.problems.length;
        notes.push(`Not saved: fix the ${count === 1 ? "problem" : `${count} problems`} first. The file on disk is unchanged.`);
      }
      if (result.designerDataError) {
        notes.push(`Samples not saved: ${result.designerDataError}`);
      }
      if (result.designerDataDeferred?.length) {
        notes.push(`Samples of ${result.designerDataDeferred.join(", ")} are saved once the definition is.`);
      }
      notify(notes.join(" "));
      refreshWorkspace();
    } catch (error) {
      if (!stillOpen()) {
        return;
      }
      if (error.status === 409) {
        setDialog({ type: "conflict", currentSha: error.body?.currentSha256 ?? null });
      } else {
        notify(`Save failed: ${error.message}. Your edits are kept in this browser.`);
      }
    } finally {
      setSaving(false);
    }
  };

  useEffect(() => {
    const onKey = (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "s") {
        event.preventDefault();
        if (doc && !saving) {
          save();
        }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  const problemsByStep = useMemo(() => {
    const map = new Map();
    for (const problem of validation.problems) {
      if (problem.stepId) {
        map.set(problem.stepId, [...(map.get(problem.stepId) ?? []), problem]);
      }
    }
    return map;
  }, [validation.problems]);

  if (!doc) {
    return html`<${FilesView}
        workspace=${workspace}
        drafts=${workspace ? browserOnlyDrafts(workspace) : []}
        error=${filesError}
        onOpen=${openFile}
        onOpenDraft=${openDraft}
        onDiscardDraft=${discardDraft}
        onNew=${newFile}
        onImport=${importFile}
      />
      ${toast && html`<${Toast} toast=${toast} onClose=${() => setToast(null)} />`}`;
  }

  const model = doc.model;
  const steps = model.steps ?? [];
  const stepIds = new Set(steps.map((step) => step.id));
  const definitionProblems = validation.problems.filter((problem) => !problem.stepId || !stepIds.has(problem.stepId));
  const index = steps.findIndex((step) => step.id === selectedId);
  const count = validation.problems.length;

  const showFirstProblem = () => {
    const step = steps.find((s) => problemsByStep.has(s.id));
    if (step) {
      setSelectedId(step.id);
    } else {
      document.querySelector("[aria-label=Definition]")?.scrollIntoView();
    }
  };

  const addStep = () => {
    const added = newStep(model);
    edit((m) => {
      // A "steps" the file held as something other than a list is replaced by the new list.
      Object.assign(m, withField(m, "steps", m.steps ?? []));
      m.steps.splice(index < 0 ? m.steps.length : index + 1, 0, added);
    });
    setSelectedId(added.id);
  };

  const deleteStep = () => {
    const removed = steps[index];
    const at = index;
    const deletedIn = session.current;
    edit((m) => m.steps.splice(at, 1));
    const next = steps[at + 1] ?? steps[at - 1];
    setSelectedId(next?.id ?? null);
    notify(`Deleted ${removed.name || "the step"}.`, {
      label: "Undo",
      run: () => {
        edit((m) => m.steps.splice(Math.min(at, m.steps.length), 0, removed), deletedIn);
        setSelectedId(removed.id);
      },
    });
  };

  return html`<div class="app">
    <header class="top">
      <div class="brand">${icons.logo}<span>Saga Designer</span></div>
      <div class="def-id">
        <span class="file mono" title=${doc.isNew ? "Not saved yet" : "Saved in the workspace"}>
          ${doc.file}${dirty ? html`<span class="dirty" aria-label="unsaved changes"> ●</span>` : ""}
        </span>
        <input class="in name" aria-label="Saga name" value=${model.name ?? ""} onInput=${(event) => {
            const name = event.currentTarget.value;
            edit((m) => Object.assign(m, withField(m, "name", name)));
          }} />
        <input class="in ver mono" aria-label="Version" placeholder="version" value=${model.version ?? ""} onInput=${(event) => {
            // An emptied version leaves the field out again, as in a file that never had one.
            const version = event.currentTarget.value || null;
            edit((m) => Object.assign(m, withField(m, "version", version)));
          }} />
        <span class="chip neutral" title="The definition's mode">${model.mode ?? "SAGA"}</span>
        ${checkFailed
          ? html`<button
              type="button"
              class="chip err"
              title=${validation.failed}
              aria-label="The check failed; check again"
              onClick=${() => {
                setValidation((previous) => ({ ...previous, failed: null }));
                setRecheck((n) => n + 1);
              }}
            >
              ${icons.error} Check failed
            </button>`
          : !checked
            ? html`<span class="chip neutral" role="status">Checking…</span>`
            : html`<button
                type="button"
                class=${"chip " + (count ? "err" : "ok")}
                aria-label=${count ? `${count} problems; show the first` : "No problems"}
                onClick=${showFirstProblem}
              >
                ${count ? html`${icons.error} ${count} ${count === 1 ? "problem" : "problems"}` : html`${icons.ok} Valid`}
              </button>`}
      </div>
      <div class="top-actions">
        <button
          class="btn"
          type="button"
          onClick=${() => {
            close();
            refreshWorkspace();
            if (doc.isNew) {
              notify(`${doc.file} is kept in this browser until you save it; it is listed under "Not saved yet".`);
            } else if (dirty) {
              notify("Your unsaved changes are kept in this browser; open the file again to restore them.");
            }
          }}
        >
          Files
        </button>
        <button class="btn" type="button" onClick=${() => setDialog({ type: "export" })}>Export</button>
        <button class="btn primary" type="button" disabled=${saving} onClick=${() => save()}>
          ${saving ? "Saving…" : "Save"}
        </button>
      </div>
    </header>
    ${checked && validation.sameVersion &&
    html`<div class="banner warn" role="status">
      <p>
        The content changed but the version did not. A server that has registered this version
        refuses changed content and rejects the whole reload: change the version to one the server
        has never seen.
      </p>
    </div>`}
    ${doc.warnings?.length > 0 &&
    html`<div class="banner warn" role="status">${doc.warnings.map((warning) => html`<p>${warning}</p>`)}</div>`}
    <div class="main">
      <aside class="rail">
        <${StepList}
          model=${model}
          selectedId=${selectedId}
          problemsByStep=${problemsByStep}
          onSelect=${setSelectedId}
          onMove=${(from, to) => edit((m) => moveStep(m, from, to))}
          onAdd=${addStep}
        />
        <${DefinitionPanel}
          model=${model}
          problems=${definitionProblems}
          onChange=${(next) => edit((m) => Object.assign(m, next))}
        />
      </aside>
      <div class="editor-wrap">
        ${index >= 0
          ? html`<${StepEditor}
              key=${selectedId}
              model=${model}
              index=${index}
              problems=${problemsByStep.get(selectedId) ?? []}
              keysFor=${(phase) => keysFor(model, index, phase)}
              services=${workspace?.services ?? []}
              onChange=${(step) => edit((m) => { m.steps[index] = step; })}
              onDelete=${deleteStep}
              onDuplicate=${() => {
                const copy = duplicateStep(structuredClone(model), index);
                edit((m) => m.steps.splice(index + 1, 0, copy));
                setSelectedId(copy.id);
              }}
            />`
          : html`<section class="editor empty-editor"><p class="hint">Add a step to start, or select one on the left.</p></section>`}
      </div>
    </div>
    ${dialog?.type === "export" &&
    html`<${ExportDialog}
      model=${model}
      definitionText=${checked ? validation.definition : ""}
      problemCount=${count}
      checkState=${checkFailed ? "failed" : checked ? "checked" : "checking"}
      defaultFormat=${doc.format}
      onClose=${() => setDialog(null)}
      onError=${(error) => notify(error.message)}
    />`}
    ${dialog?.type === "conflict" &&
    html`<${Dialog}
      title="The file changed on disk"
      onClose=${() => setDialog(null)}
      footer=${html`<button class="btn" type="button" onClick=${() => openFile(doc.file)}>
          Reload from disk
        </button>
        <button class="btn primary" type="button" onClick=${() => { setDialog(null); save(dialog.currentSha); }}>
          Overwrite
        </button>`}
    >
      <p>
        ${doc.file} changed since you opened it, for example through git or another editor.
        ${dialog.currentSha === null ? " It has since been deleted." : ""}
      </p>
      <p class="hint">
        Reload from disk shows the file as it is now; your edits stay in this browser and are offered
        back. Overwrite replaces the file with your version.
      </p>
    <//>`}
    ${dialog?.type === "restore" &&
    html`<${Dialog}
      title="Restore unsaved changes?"
      onClose=${() => setDialog(null)}
      footer=${html`<button class="btn" type="button" onClick=${() => { writeDraft(workspace.workspace, doc.file, null); setDialog(null); }}>
          Discard them
        </button>
        <button
          class="btn primary"
          type="button"
          onClick=${() => {
            const draft = dialog.draft;
            setDoc((current) => ({ ...current, model: draft.model, designerData: draft.designerData ?? current.designerData }));
            setSelectedId(draft.model.steps?.[0]?.id ?? null);
            setDialog(null);
          }}
        >
          Restore
        </button>`}
    >
      <p>This browser has changes to ${doc.file} from ${new Date(dialog.draft.savedAt).toLocaleString()} that were never saved.</p>
    <//>`}
    ${toast && html`<${Toast} toast=${toast} onClose=${() => setToast(null)} />`}
  </div>`;
}

function Toast({ toast, onClose }) {
  return html`<div class="toast" role="status">
    <span>${toast.text}</span>
    ${toast.action &&
    html`<button
      class="btn small"
      type="button"
      onClick=${() => {
        toast.action.run();
        onClose();
      }}
    >
      ${toast.action.label}
    </button>`}
  </div>`;
}

// Preact renders next to existing children, so the loading note goes first.
const root = document.getElementById("app");
root.replaceChildren();
render(html`<${App} />`, root);
