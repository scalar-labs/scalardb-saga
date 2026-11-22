// The designer's local HTTP API. Every call is same-origin; a request that changes something is
// sent as JSON, which the server's request guard requires.

/** An error response from the API, with the HTTP status and the parsed JSON body. */
export class ApiError extends Error {
  constructor(status, body) {
    super(body?.error || `The designer answered ${status}`);
    this.status = status;
    this.body = body;
  }
}

async function call(method, path, body) {
  const init = { method, headers: {} };
  if (body !== undefined) {
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  const response = await fetch(path, init);
  if (!response.ok) {
    let error = null;
    try {
      error = await response.json();
    } catch {
      // not JSON: the status alone says what happened
    }
    throw new ApiError(response.status, error);
  }
  return response;
}

const json = async (method, path, body) => (await call(method, path, body)).json();
const filePath = (file) => "/api/files/" + encodeURIComponent(file);

export const api = {
  workspace: () => json("GET", "/api/workspace"),
  open: (file) => json("GET", filePath(file)),
  importFile: (fileName, text) => json("POST", "/api/import", { fileName, text }),
  validate: (revision, model, file) => json("POST", "/api/validate", { revision, model, file }),
  save: (file, model, designerData, baseSha256) =>
    json("PUT", filePath(file), { model, designerData, baseSha256 }),
  serviceTemplates: (model) => json("POST", "/api/service-templates", { model }),

  /** Fetches a file the API offers as a download, and hands it to the browser to save. */
  async download(path, body) {
    const response = await call("POST", path, body);
    const disposition = response.headers.get("Content-Disposition") || "";
    const name = /filename="([^"]+)"/.exec(disposition)?.[1] || "download";
    saveBlob(await response.blob(), name);
    return name;
  },
};

/** Saves data built in the page, such as a service template, as a file. */
export function saveText(text, name) {
  saveBlob(new Blob([text], { type: "text/plain" }), name);
}

function saveBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
