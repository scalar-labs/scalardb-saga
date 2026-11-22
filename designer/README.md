# ScalarDB Saga designer

`scalardb-saga-designer` is a local process that serves a browser editor for the declarative saga
definitions in one folder. It reads and writes the files in that folder, checks every edit with the
same parser the server uses, and writes a definition file only when the server would accept it. For
how to use the editor, see [Design a Definition](../guides/design-a-definition.md).

The designer is not published. Run it from a checkout of this repository.

## Running it

```bash
./gradlew :designer:run --args="--workspace getting-started/conf/definitions --open"
```

The editor is then at `http://127.0.0.1:7070`.

| Option | What it does | Default |
| --- | --- | --- |
| `--workspace DIR` | The folder of definition files to edit. | The current folder |
| `--services DIR` | The server's folder of service files, read only. The editor offers the service names it finds, and export skips the templates of services that already have a file. | None |
| `--host ADDRESS` | The address to listen on. | `127.0.0.1` |
| `--allowed-host NAME` | A further host name the browser may use to reach the designer, such as a VM's host name. Repeatable. | None |
| `--port PORT` | The port to listen on. `0` picks a free one. | `7070` |
| `--open` | Opens the editor in the default browser. | Off |

## What it writes

- **Definition files** in the workspace, as `.json`, `.yaml` or `.yml`, the same files a server reads
  from its `definitions_path`. A definition with problems is never written, because one invalid file
  stops a server from applying any definition change. Writes are atomic: a server watching the folder
  never sees a half-written file.
- **Designer data** in `.designer/<definition file name>`: the sample saga input and each step's
  sample response. The server skips names that start with `.`, so the folder is safe to mount as
  `definitions_path` as it is. Commit `.designer/` with the definitions, so that a teammate opening
  them gets the same samples.
- **Unsaved edits** stay in the browser's storage until they are saved, per workspace and file. They
  never reach the workspace.

## Security

The designer has no authentication. Anyone who can send it a request can change the files in the
workspace, so it accepts requests only from this machine by default:

- It listens on `127.0.0.1`. With `--host` set to another address, it logs a warning at startup.
- It refuses a request whose `Host` header names anything but a loopback name, the `--host` address,
  this machine's own addresses (on a wildcard `--host`), or an `--allowed-host` name. This stops a
  web page from reaching it through DNS rebinding.
- It refuses a request that changes anything unless it is JSON and its `Origin` is absent or the
  designer's own, so another site open in the same browser cannot send one.
- It reads and writes only files directly inside the workspace, never through a path or a symbolic
  link that leads outside it.
- The page loads nothing from the network, under a content security policy of `default-src 'self'`.

## Development

- The UI is plain ES modules under `src/main/resources/designer-ui`, built on Preact and htm through
  an import map, with no npm and no build step. Edit a file and reload the page.
- `./gradlew :designer:uiTest` runs the browser tests (Playwright, Chromium). `check` compiles and
  analyses them but does not run them.

## Third-party JavaScript

The page uses [Preact](https://preactjs.com) (MIT) and [htm](https://github.com/developit/htm)
(Apache-2.0), vendored under `src/main/resources/designer-ui/vendor` with their licenses. The
license report does not see them and Dependabot cannot bump them;
[vendor/README.md](src/main/resources/designer-ui/vendor/README.md) lists the versions and how to
update them.
