package com.scalar.db.saga.designer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import com.scalar.db.saga.designer.ServiceTemplates.Template;
import com.scalar.db.saga.designer.Workspace.FileContent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * What each API route does, apart from HTTP: every method takes and returns JSON trees, so the
 * behaviour is tested without a server. The request and response shapes are documented on each
 * method; the editor model is documented on {@link EditorModel}.
 */
final class DesignerApi {

  private final Workspace workspace;
  private final @Nullable Path servicesDir;

  DesignerApi(Workspace workspace, @Nullable Path servicesDir) {
    this.workspace = workspace;
    this.servicesDir = servicesDir;
  }

  /**
   * {@code GET /api/workspace}: the definition files, each with its saga name and version when it
   * can be read; {@code uneditable}, the files the server loads but whose names the designer does
   * not accept; and the existing service names when {@code --services} is set.
   */
  ObjectNode workspace() throws IOException {
    ObjectNode result = object();
    result.put("workspace", workspace.root().toString());
    ArrayNode files = result.putArray("files");
    for (String fileName : workspace.list()) {
      ObjectNode file = files.addObject();
      file.put("file", fileName);
      Format format = Format.ofFileName(fileName);
      file.put("format", format.name());
      FileContent content;
      try {
        content = workspace.readDefinition(fileName);
      } catch (BadRequestException e) {
        file.put("error", e.reason());
        continue;
      }
      if (content == null) {
        continue; // removed since the listing
      }
      try {
        JsonNode root = DefinitionFormats.read(content.text(), format);
        copyIfText(root, file, "name");
        copyIfText(root, file, "version");
      } catch (JsonProcessingException e) {
        file.put("error", e.getOriginalMessage());
      }
    }
    ArrayNode uneditable = result.putArray("uneditable");
    workspace.listUneditable().forEach(uneditable::add);
    if (servicesDir != null) {
      ArrayNode services = result.putArray("services");
      existingServices().forEach(services::add);
    } else {
      result.putNull("services");
    }
    return result;
  }

  /**
   * {@code GET /api/files/{file}}: {@code {file, format, model, designerData, sha256, warnings}}.
   *
   * @throws BadRequestException with 404 if there is no such file, or 400 if it is not well-formed
   */
  ObjectNode open(String fileName) throws IOException {
    FileContent content = workspace.readDefinition(fileName);
    if (content == null) {
      throw new BadRequestException(404, "No definition file '" + fileName + "'");
    }
    ObjectNode result = load(fileName, content.text());
    result.put("sha256", content.sha256());
    // Unreadable designer data must not lock the definition away: open it with empty data and say
    // why. A save then replaces a damaged file, and leaves one from a newer designer alone.
    try {
      FileContent data = workspace.readDesignerData(fileName);
      if (data != null) {
        result.set(
            "designerData",
            DesignerData.read(data.text(), Format.ofFileName(fileName), result.get("model")));
      }
    } catch (BadRequestException e) {
      ((ArrayNode) result.get("warnings"))
          .add("The designer's saved samples for this file are not shown: " + e.reason());
    } catch (IOException e) {
      ((ArrayNode) result.get("warnings"))
          .add("The designer's saved samples for this file could not be read: " + e.getMessage());
    }
    return result;
  }

  /**
   * {@code POST /api/import} with {@code {fileName, text}}: opens an uploaded file without saving
   * it. Same response as {@link #open}, with a null {@code sha256}, since there is no file on disk.
   */
  ObjectNode importFile(JsonNode request) {
    String fileName = text(request, "fileName");
    if (!Workspace.isDefinitionFileName(fileName)) {
      throw new BadRequestException(
          "Name the file like a definition file: letters, digits, '.', '_' or '-', ending in"
              + " .json, .yaml or .yml");
    }
    ObjectNode result = load(fileName, text(request, "text"));
    result.putNull("sha256");
    return result;
  }

  /**
   * {@code POST /api/validate} with {@code {revision, model, file?}}: {@code {revision, problems,
   * definition, sameVersion}}. {@code definition} is the JSON the model exports to; {@code
   * sameVersion} is true when {@code file} exists, its content differs from the model's, and its
   * version does not, which a server that registered that version would refuse.
   */
  ObjectNode validate(JsonNode request) throws IOException {
    JsonNode model = required(request, "model");
    Analysis analysis = analyze(model);
    ObjectNode result = object();
    result.set("revision", request.path("revision"));
    result.set("problems", problems(analysis.problems()));
    result.put("definition", DefinitionFormats.write(analysis.definition(), Format.JSON));
    boolean sameVersion = false;
    JsonNode file = request.get("file");
    if (file != null && file.isTextual()) {
      JsonNode current = normalized(currentTree(file.asText()));
      sameVersion =
          current != null
              && !DefinitionFormats.identical(current, analysis.definition())
              && DefinitionFormats.identical(
                  current.path("version"), analysis.definition().path("version"));
    }
    result.put("sameVersion", sameVersion);
    return result;
  }

  /**
   * {@code PUT /api/files/{file}} with {@code {model, designerData, baseSha256}}. Writes the
   * definition only if it has no problems, and not at all if it is unchanged; writes the designer
   * data whenever the definition file exists. Response: {@code {definitionWritten, unchanged,
   * designerDataWritten, designerDataError, designerDataDeferred, sha256, problems}}, where {@code
   * designerDataDeferred} names the steps whose samples wait for the definition to be saved, since
   * the file does not have those steps yet.
   *
   * <p>Saves run one at a time, so that two editor tabs saving the same file at once cannot both
   * pass the SHA check: the second one sees the first one's write and gets a conflict.
   *
   * @throws Workspace.ConflictException if the file changed since {@code baseSha256} was read
   */
  synchronized ObjectNode save(String fileName, JsonNode request) throws IOException {
    JsonNode model = required(request, "model");
    FileContent current = workspace.readDefinition(fileName);
    Format format = Format.ofFileName(fileName);
    JsonNode base = request.get("baseSha256");
    String baseSha256 = base == null || base.isNull() ? null : base.asText();
    // Checked on every save, not only when the definition is written: the response's sha256
    // becomes the client's next base, so handing back a SHA the client never read would let its
    // next save overwrite a change made outside the designer.
    Workspace.requireUnchanged(current, baseSha256);
    Analysis analysis = analyze(model);
    JsonNode currentTree = current == null ? null : parseOrNull(current.text(), format);
    JsonNode normalizedTree = normalized(currentTree);
    boolean unchanged =
        normalizedTree != null
            && DefinitionFormats.identical(normalizedTree, analysis.definition());

    ObjectNode result = object();
    boolean written = false;
    String sha256 = current == null ? null : current.sha256();
    if (!unchanged && analysis.problems().isEmpty()) {
      sha256 =
          workspace.writeDefinition(
              fileName, DefinitionFormats.write(analysis.definition(), format), baseSha256);
      written = true;
    }
    result.put("definitionWritten", written);
    result.put("unchanged", unchanged);

    // From here on the definition is saved, so nothing may fail the request: a problem with the
    // designer data is reported next to the new sha256, which the client needs for its next save.
    boolean dataWritten = false;
    ArrayNode deferred = DefinitionFormats.json().createArrayNode();
    JsonNode data = request.get("designerData");
    result.putNull("designerDataError");
    if (sha256 != null && data != null && data.isObject()) {
      try {
        FileContent existing = workspace.readDesignerData(fileName);
        // When the model did not reach the file, the samples stay keyed to the names on disk.
        List<String> savedNames = written || unchanged ? null : stepNames(currentTree);
        DesignerData.Written out =
            DesignerData.write(
                data, model, format, existing == null ? null : existing.text(), savedNames);
        out.deferred().forEach(deferred::add);
        if (existing == null || !existing.text().equals(out.text())) {
          workspace.writeDesignerData(fileName, out.text());
          dataWritten = true;
        }
      } catch (BadRequestException e) {
        result.put("designerDataError", e.reason());
      } catch (IOException e) {
        result.put("designerDataError", "The designer data could not be saved: " + e.getMessage());
      }
    }
    result.put("designerDataWritten", dataWritten);
    result.set("designerDataDeferred", deferred);
    result.put("sha256", sha256);
    result.set("problems", problems(analysis.problems()));
    return result;
  }

  /** The step names a definition file holds, in order; none if it cannot be read. */
  private static List<String> stepNames(@Nullable JsonNode definition) {
    List<String> names = new ArrayList<>();
    if (definition != null) {
      for (JsonNode step : definition.path("steps")) {
        JsonNode name = step.get("name");
        if (name != null && name.isTextual()) {
          names.add(name.asText());
        }
      }
    }
    return names;
  }

  /**
   * A file's tree as the editor would export it, for comparing with the model's export. The editor
   * writes known fields in a fixed order, so a file that lists {@code version} before {@code name}
   * must not count as changed; the order of body fields and query rows, and the scale of decimals,
   * still count, since the editor keeps them.
   */
  private static @Nullable JsonNode normalized(@Nullable JsonNode tree) {
    if (tree == null || !tree.isObject()) {
      return tree;
    }
    try {
      return EditorModel.toDefinition(EditorModel.fromDefinition(tree)).definition();
    } catch (BadRequestException e) {
      return tree; // a model the editor cannot export back: compare the file as it is
    }
  }

  /** An export download: the file name to offer and its content. */
  static final class Download {
    private final String fileName;
    private final String contentType;
    private final byte[] content;

    Download(String fileName, String contentType, byte[] content) {
      this.fileName = fileName;
      this.contentType = contentType;
      this.content = content.clone();
    }

    String fileName() {
      return fileName;
    }

    String contentType() {
      return contentType;
    }

    byte[] content() {
      return content.clone();
    }
  }

  /**
   * {@code POST /api/export} with {@code {model, format}}: the definition as a file.
   *
   * @throws BadRequestException with 422 if the definition has problems: a file that would be
   *     refused is not offered for download
   */
  Download export(JsonNode request) {
    Format format = format(request);
    Analysis analysis = validDefinition(request);
    String text = DefinitionFormats.write(analysis.definition(), format);
    return new Download(
        downloadName(analysis.definition(), format),
        format == Format.JSON ? "application/json" : "application/yaml",
        text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  /**
   * {@code POST /api/service-templates} with {@code {model}}: {@code {templates: [{service,
   * fileName, text, usable, exists}]}}, where {@code exists} says {@code --services} already has
   * that service's file.
   */
  ObjectNode serviceTemplates(JsonNode request) throws IOException {
    Analysis analysis = analyze(required(request, "model"));
    Set<String> existing = existingServices();
    ObjectNode result = object();
    ArrayNode templates = result.putArray("templates");
    for (Template template : templates(analysis.definition())) {
      ObjectNode item = templates.addObject();
      item.put("service", template.service());
      item.put("fileName", template.fileName());
      item.put("text", template.text());
      item.put("usable", template.usable());
      item.put("exists", existing.contains(template.service()));
    }
    return result;
  }

  /**
   * {@code POST /api/bundle} with {@code {model, format}}: a zip of the definition and the
   * templates of every service that {@code --services} does not already have.
   *
   * @throws BadRequestException with 422 if the definition has problems
   */
  Download bundle(JsonNode request) throws IOException {
    Format format = format(request);
    Analysis analysis = validDefinition(request);
    Set<String> existing = existingServices();
    List<Template> missing = new ArrayList<>();
    for (Template template : templates(analysis.definition())) {
      if (!existing.contains(template.service())) {
        missing.add(template);
      }
    }
    String definitionFile = downloadName(analysis.definition(), format);
    byte[] zip =
        ServiceTemplates.bundle(
            definitionFile, DefinitionFormats.write(analysis.definition(), format), missing);
    String base = definitionFile.substring(0, definitionFile.lastIndexOf('.'));
    return new Download(base + ".zip", "application/zip", zip);
  }

  // ---------------------------------------------------------------------------------------------

  /** A model's definition tree and every problem with it: the form's first, then the parser's. */
  private record Analysis(ObjectNode definition, List<Problem> problems) {}

  private static Analysis analyze(JsonNode model) {
    EditorModel.Export export = EditorModel.toDefinition(model);
    List<Problem> problems = new ArrayList<>(export.problems());
    problems.addAll(DefinitionValidator.validate(export.definition(), EditorModel.stepIds(model)));
    return new Analysis(export.definition(), problems);
  }

  private static Analysis validDefinition(JsonNode request) {
    Analysis analysis = analyze(required(request, "model"));
    if (!analysis.problems().isEmpty()) {
      throw new BadRequestException(
          422, "Fix the definition's problems first: " + analysis.problems().get(0).message());
    }
    return analysis;
  }

  private static ObjectNode load(String fileName, String text) {
    Format format = Format.ofFileName(fileName);
    JsonNode root;
    try {
      root = DefinitionFormats.read(text, format);
    } catch (JsonProcessingException e) {
      throw new BadRequestException(
          "'" + fileName + "' is not valid " + format + ": " + e.getOriginalMessage());
    }
    ObjectNode model = EditorModel.fromDefinition(root);
    ObjectNode result = object();
    result.put("file", fileName);
    result.put("format", format.name());
    result.set("model", model);
    result.set("designerData", DesignerData.empty());
    ArrayNode warnings = result.putArray("warnings");
    if (format == Format.YAML) {
      if (text.contains("#")) {
        warnings.add("Comments in this file are lost when you save it.");
      }
      JsonNode version = root.get("version");
      if (version != null && version.isNumber()) {
        warnings.add(
            "The version "
                + version.asText()
                + " is a number in this file, so a value such as 1.10 is read as 1.1. Write it as"
                + " a string to keep it exact.");
      }
    }
    return result;
  }

  private @Nullable JsonNode currentTree(String fileName) throws IOException {
    FileContent current = workspace.readDefinition(fileName);
    return current == null ? null : parseOrNull(current.text(), Format.ofFileName(fileName));
  }

  private static @Nullable JsonNode parseOrNull(String text, Format format) {
    try {
      return DefinitionFormats.read(text, format);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  private List<Template> templates(JsonNode definition) {
    String name = definition.path("name").asText("this saga");
    List<Template> templates = new ArrayList<>();
    for (String service : ServiceTemplates.services(definition)) {
      templates.add(ServiceTemplates.template(service, name));
    }
    return templates;
  }

  private Set<String> existingServices() throws IOException {
    Set<String> names = new TreeSet<>();
    if (servicesDir == null) {
      return names;
    }
    try (Stream<Path> entries = Files.list(servicesDir)) {
      for (Path entry : (Iterable<Path>) entries::iterator) {
        String name = String.valueOf(entry.getFileName());
        if (name.endsWith(".properties") && !name.startsWith(".") && Files.isRegularFile(entry)) {
          names.add(name.substring(0, name.length() - ".properties".length()));
        }
      }
    }
    return names;
  }

  /** The file name a download gets: the saga's name if it is a usable file name. */
  private static String downloadName(JsonNode definition, Format format) {
    String candidate = definition.path("name").asText("") + format.extension();
    return Workspace.isDefinitionFileName(candidate)
        ? candidate
        : "definition" + format.extension();
  }

  private static Format format(JsonNode request) {
    String format = request.path("format").asText("JSON").toUpperCase(Locale.ROOT);
    try {
      return Format.valueOf(format);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Unknown format '" + format + "'");
    }
  }

  private static ArrayNode problems(List<Problem> problems) {
    ArrayNode array = DefinitionFormats.json().createArrayNode();
    for (Problem problem : problems) {
      ObjectNode item = array.addObject();
      item.put("code", problem.code());
      item.put("stepId", problem.stepId());
      item.put("message", problem.message());
    }
    return array;
  }

  private static JsonNode required(JsonNode request, String field) {
    JsonNode value = request.get(field);
    if (value == null || !value.isObject()) {
      throw new BadRequestException("'" + field + "' must be an object");
    }
    return value;
  }

  private static String text(JsonNode request, String field) {
    JsonNode value = request.get(field);
    if (value == null || !value.isTextual()) {
      throw new BadRequestException("'" + field + "' must be a string");
    }
    return value.asText();
  }

  private static void copyIfText(JsonNode from, ObjectNode to, String field) {
    JsonNode value = from.get(field);
    if (value != null && value.isTextual()) {
      to.put(field, value.asText());
    }
  }

  private static ObjectNode object() {
    return DefinitionFormats.json().createObjectNode();
  }
}
