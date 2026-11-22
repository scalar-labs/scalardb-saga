# Design a Definition

This page explains how to build and edit saga definitions with the ScalarDB Saga designer, a local
editor that runs in your browser. The designer writes the same JSON and YAML files described in
[Model Your Workflow](model-your-workflow.md), checks each edit with the parser the server uses,
and writes a file only when the server would accept it. It uses the definitions of the
[getting started guide](../getting-started/README.md) as its example.

## Start the designer

From a checkout of this repository, point the designer at a folder of definition files:

```bash
./gradlew :designer:run --args="--workspace getting-started/conf/definitions --open"
```

`--open` opens `http://127.0.0.1:7070` in your browser. The folder is called the workspace. Add
`--services` with the server's services folder to have the editor suggest the service names it
already knows. For every option, and for how the designer keeps the workspace safe, see the
[designer README](../designer/README.md).

The designer listens only on this machine and has no sign-in, so run it on your own computer, not on
a shared server.

## Open or create a definition

The first page lists the definition files in the workspace. From there:

- **Open** a file to edit it. A file the parser cannot read is listed with the reason.
- **New definition** asks for the saga name and the format, JSON or YAML, and creates
  `<name>.json` or `<name>.yaml` once you save it.
- **Import a file** opens a definition from elsewhere on your computer. Saving it writes it into the
  workspace.

A new or imported definition exists only in your browser until you save it. It is listed under
**Not saved yet**, where you can open it again or discard it.

## Add and order the steps

The left side of the editor lists the steps in the order they run.

- **Add a step** inserts a step after the selected one, with an unused name and an empty call for
  each phase.
- **Duplicate** and **Delete** act on the selected step. Deleting offers **Undo** for a few seconds.
- Reorder steps by dragging their cards, or with the up and down buttons beside each card.

Select a step to edit it on the right: its name, its service, a call for each phase, and its retry
policy and timeout. A card shows the step's first call and how many problems it has.

## Give the saga a sample input

The **Sample input** panel holds the saga's input as a JSON object, such as
`{"orderId": "o-1", "amount": 100}`. Its keys are in the context of every step, so the editor
offers them wherever a call reads the context. When you open a file, any key that a call reads but
no step captures is added to the sample input for you, with a null value to fill in.

The sample input is the designer's own data. It is never part of the definition and is never sent to
a server.

## Read values from the context

Each step shows **Context here**: the keys it can read, each with where it comes from, either the
input or the earlier step that captures it. The editor works this out in the order the engine runs
the calls, so in `TCC` mode a confirmation can read the outputs of every reservation.

- **In a JSON body,** each field takes one of three kinds of value. **From the context** picks a key
  from a list and writes `"${key}"`, which keeps the value's type, so a number stays a number. A
  **fixed value** is written as it is, such as `JPY` or `42`. **Text with keys** mixes text and keys,
  such as `order-${orderId}`, and is always a string. Picking a key for an unnamed field names the
  field after the key.
- **In a path,** click a key below the field to insert `${key}` where the cursor is.
- **In a compensation or a cancellation,** the step's own outputs are listed but cannot be picked,
  because the server rejects a compensation that reads them.

A key that stops being available, for example because you moved a step above the one that captures
it, stays where it is and is marked **Not available here**, so you can see and fix it.

## Capture output from a sample response

The first call of each step, the execution in `SAGA` mode or the reservation in `TCC` mode, has a
**Sample response**: paste a response body like the participant's, and the editor shows its fields
as a tree.

- **Capture** on a field adds an output row that reads it, such as `$.payment_id`, under a key in
  camelCase, such as `paymentId`. When another key here already has that name, the step's name goes
  in front: `amount` captured in `charge` becomes `chargeAmount`. Rename the key in the output row
  if you like. Capture the field again to remove the row.
- **The whole body, as text,** captures `$body`, which also works for a response that is not JSON.
- Fields the engine cannot read are shown with the reason: items of a list, and fields whose name
  contains a dot.
- An output row whose field is missing from the sample, or is null there, is flagged: a response like
  that fails the step.

You can also type output rows by hand when you have no sample. The samples are saved beside the
definition, in `.designer/`, and are never part of the definition.

## Choose what happens when a step fails

In `SAGA` mode, the **When a step fails** panel sets the recovery strategy:

| Strategy | When a step fails |
| --- | --- |
| `BACKWARD` (default) | Every completed step is compensated. |
| `FORWARD` | Nothing is compensated; recovery retries the failed step until it succeeds. |
| `MIXED` | A failure at or before the pivot compensates; a failure after it is retried forward. |

Choosing `MIXED` marks the second-to-last step as the pivot. To move it, select another step and
check **Pivot**. A **Point of no return** line in the step list sits below the pivot, and the steps
after it are shaded. Switching to another strategy removes the pivot mark.

A compensation that can never run, after the pivot or under `FORWARD`, is dimmed with the reason.
It stays editable, because the file still requires it. For how to order steps around the pivot, see
[Order the steps](model-your-workflow.md#order-the-steps).

## Switch between SAGA and TCC

The **Mode** list in the top bar switches the definition between `SAGA` and `TCC`. Before it
switches, the designer says what changes and what is dropped:

- To `TCC`: each execution becomes a reservation and each compensation a cancellation. Each step
  then needs a confirmation call. The recovery strategy and the pivot are dropped, because `TCC`
  has neither.
- To `SAGA`: each reservation becomes an execution and each cancellation a compensation. The
  confirmation calls are dropped.

## Fix problems

The editor checks the definition a moment after each edit, with the server's own parser. The top
bar shows **Valid** or the number of problems; click it to go to the first one. Problems appear on
the step they belong to, and problems of the whole definition appear in the **Definition** panel.

Fields the form cannot edit, such as an unknown field in an imported file, are kept and listed
where they are, each with a button to remove it.

## Save

**Save** writes the definition and its samples.

- **A definition with problems is not written.** The file on disk stays as it was, so a server that
  reads the folder never sees an invalid file. If the file already exists, the samples of the steps
  it has are saved.
- **An unchanged definition is not rewritten,** so opening and saving a file never reformats it.
  A file the designer does write gets its standard layout, and a YAML file loses its comments; the
  editor warns about this when you open a YAML file that has comments.
- **Unsaved edits are kept in your browser.** If you close the tab or open another file, opening the
  file again offers to restore them.
- **If the file changed on disk** since you opened it, for example through `git pull`, the designer
  asks whether to reload it or overwrite it with your version.

A server refuses a definition whose content changed under a version it has already registered, and
then applies no definition change at all. When the content differs from the file as you opened it
but the version is the same, the editor warns you. If the version ends in a number, **Bump version**
raises it, such as `1.9` to `1.10`, and puts the cursor in the version field so you can check it.
The designer cannot see which versions a server has registered, so pick one that no server has
seen.

## Export

**Export** downloads the definition, in JSON or YAML, without saving it to the workspace. It also
offers a template service file for each service the definition calls, such as
`payment.properties`, for the server's services folder. **Download with service templates (zip)**
gets them all at once. Export is available only when the definition has no problems.

## Next steps

- [Model Your Workflow](model-your-workflow.md): what each field of a definition means, and how to
  order and compensate steps.
- [ScalarDB Saga Configurations](configurations.md): register the definitions with a server, and
  configure its services.
