package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DesignerApiTest {

  private static final String VALID =
      """
      {
        "name": "order",
        "version": "1.0",
        "steps": [{
          "name": "charge",
          "service": "payment",
          "execution": {"path": "/charges", "output": {"paymentId": "$.id"}},
          "compensation": {"path": "/charges/${orderId}/refund"}
        }]
      }
      """;

  @TempDir Path dir;
  @TempDir Path servicesDir;
  private DesignerApi api;

  @BeforeEach
  void setUp() throws IOException {
    api = new DesignerApi(new Workspace(dir), null);
  }

  private static ObjectNode json(String text) throws Exception {
    return (ObjectNode) DefinitionFormats.read(text, Format.JSON);
  }

  private ObjectNode saveRequest(JsonNode model, JsonNode data, String baseSha) {
    ObjectNode request = newFileRequest(model, data);
    request.put("baseSha256", baseSha);
    return request;
  }

  /** A save of a file that does not exist yet, so there is no base to compare against. */
  private ObjectNode newFileRequest(JsonNode model, JsonNode data) {
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.set("model", model);
    request.set("designerData", data);
    request.putNull("baseSha256");
    return request;
  }

  private ObjectNode modelRequest(JsonNode model) {
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.set("model", model);
    return request;
  }

  // ---- workspace

  @Test
  void workspace_filesGiven_listsEachWithNameVersionOrError() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    Files.writeString(dir.resolve("b.yaml"), "name: b\nversion: 2\n");
    Files.writeString(dir.resolve("c.json"), "{oops");

    // Act
    ObjectNode result = api.workspace();

    // Assert
    JsonNode files = result.get("files");
    assertThat(files).hasSize(3);
    assertThat(files.get(0).toString())
        .isEqualTo(
            "{\"file\":\"a.json\",\"format\":\"JSON\",\"name\":\"order\",\"version\":\"1.0\"}");
    assertThat(files.get(1).has("version")).isFalse();
    assertThat(files.get(2).has("error")).isTrue();
    assertThat(result.get("services").isNull()).isTrue();
  }

  @Test
  void workspace_fileThatIsNotUtf8_reportsItAndListsTheOthers() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    Files.write(dir.resolve("b.yaml"), new byte[] {'n', ':', ' ', (byte) 0xE9});

    // Act
    ObjectNode result = api.workspace();

    // Assert
    JsonNode files = result.get("files");
    assertThat(files).hasSize(2);
    assertThat(files.get(0).get("name").asText()).isEqualTo("order");
    assertThat(files.get(1).get("error").asText()).contains("UTF-8");
  }

  @Test
  void workspace_withServicesFolder_listsServiceNames() throws Exception {
    // Arrange
    Files.writeString(servicesDir.resolve("payment.properties"), "base_url=http://p");
    Files.writeString(servicesDir.resolve(".hidden.properties"), "");
    Files.writeString(servicesDir.resolve("notes.txt"), "");
    DesignerApi withServices = new DesignerApi(new Workspace(dir), servicesDir);

    // Act Assert
    assertThat(withServices.workspace().get("services").toString()).isEqualTo("[\"payment\"]");
  }

  // ---- open and import

  @Test
  void open_existingFile_returnsModelShaAndEmptyDesignerData() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);

    // Act
    ObjectNode result = api.open("a.json");

    // Assert
    assertThat(result.get("format").asText()).isEqualTo("JSON");
    assertThat(result.get("model").get("name").asText()).isEqualTo("order");
    assertThat(result.get("sha256").asText()).isEqualTo(Workspace.sha256(VALID));
    assertThat(result.get("designerData")).isEqualTo(DesignerData.empty());
    assertThat(result.get("warnings")).isEmpty();
  }

  @Test
  void open_withSavedDesignerData_returnsItKeyedByStepId() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    Files.createDirectory(dir.resolve(".designer"));
    Files.writeString(
        dir.resolve(".designer/a.json"),
        "{\"formatVersion\": 1, \"steps\": {\"charge\": {\"sampleResponse\": \"{}\"}}}");

    // Act
    ObjectNode result = api.open("a.json");

    // Assert
    assertThat(result.get("designerData").get("steps").get("s1").get("sampleResponse").asText())
        .isEqualTo("{}");
  }

  @Test
  void open_yamlWithCommentsAndANumericVersion_warnsAboutBoth() throws Exception {
    // Arrange
    Files.writeString(
        dir.resolve("a.yaml"), "# order saga\nname: order\nversion: 1.10\nsteps: []\n");

    // Act
    JsonNode warnings = api.open("a.yaml").get("warnings");

    // Assert
    assertThat(warnings).hasSize(2);
    assertThat(warnings.get(1).asText()).contains("1.10");
  }

  @Test
  void open_withDamagedDesignerData_opensTheDefinitionWithAWarning() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    Files.createDirectory(dir.resolve(".designer"));
    Files.writeString(dir.resolve(".designer/a.json"), "{\"formatVersion\": 1, \"steps\": {");

    // Act
    ObjectNode result = api.open("a.json");

    // Assert
    assertThat(result.get("model").get("name").asText()).isEqualTo("order");
    assertThat(result.get("designerData")).isEqualTo(DesignerData.empty());
    assertThat(result.get("warnings")).hasSize(1);
  }

  @Test
  void open_fileThatIsNotUtf8_throwsBadRequest() throws Exception {
    // Arrange
    Files.write(dir.resolve("a.yaml"), new byte[] {'n', ':', ' ', (byte) 0xE9});

    // Act Assert
    assertThatThrownBy(() -> api.open("a.yaml")).isInstanceOf(BadRequestException.class);
  }

  @Test
  void open_missingFile_throwsNotFound() {
    // Act Assert
    assertThatThrownBy(() -> api.open("a.json"))
        .isInstanceOfSatisfying(
            BadRequestException.class, e -> assertThat(e.status()).isEqualTo(404));
  }

  @Test
  void open_malformedFile_throwsBadRequest() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), "{oops");

    // Act Assert
    assertThatThrownBy(() -> api.open("a.json"))
        .isInstanceOfSatisfying(
            BadRequestException.class, e -> assertThat(e.status()).isEqualTo(400));
  }

  @Test
  void importFile_uploadedTextGiven_returnsModelWithNoSha() throws Exception {
    // Arrange
    ObjectNode request = DefinitionFormats.json().createObjectNode();
    request.put("fileName", "up.yaml");
    request.put("text", "name: up\nsteps: []\n");

    // Act
    ObjectNode result = api.importFile(request);

    // Assert
    assertThat(result.get("model").get("name").asText()).isEqualTo("up");
    assertThat(result.get("sha256").isNull()).isTrue();
    assertThat(Files.exists(dir.resolve("up.yaml"))).isFalse();
  }

  @Test
  void importFile_unusableFileName_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode request = json("{\"fileName\": \"../up.json\", \"text\": \"{}\"}");

    // Act Assert
    assertThatThrownBy(() -> api.importFile(request)).isInstanceOf(BadRequestException.class);
  }

  @Test
  void importFile_missingText_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode request = json("{\"fileName\": \"up.json\"}");

    // Act Assert
    assertThatThrownBy(() -> api.importFile(request)).isInstanceOf(BadRequestException.class);
  }

  // ---- validate

  @Test
  void validate_validModel_returnsNoProblemsTheDefinitionTextAndTheRevision() throws Exception {
    // Arrange
    ObjectNode request = modelRequest(EditorModel.fromDefinition(json(VALID)));
    request.put("revision", 7);

    // Act
    ObjectNode result = api.validate(request);

    // Assert
    assertThat(result.get("revision").asInt()).isEqualTo(7);
    assertThat(result.get("problems")).isEmpty();
    assertThat(json(result.get("definition").asText())).isEqualTo(json(VALID));
    assertThat(result.get("sameVersion").asBoolean()).isFalse();
  }

  @Test
  void validate_invalidModel_returnsProblemsWithStepIds() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    ((ObjectNode) model.get("steps").get(0).get("phases")).remove("compensation");

    // Act
    JsonNode problems = api.validate(modelRequest(model)).get("problems");

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).get("stepId").asText()).isEqualTo("s1");
    assertThat(problems.get(0).get("code").asText()).isEqualTo("INVALID_STEP_DEFINITION");
  }

  @Test
  void validate_changedContentUnderTheSameVersion_flagsSameVersion() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    model.put("timeoutMillis", "5000");
    ObjectNode request = modelRequest(model);
    request.put("file", "a.json");

    // Act Assert
    assertThat(api.validate(request).get("sameVersion").asBoolean()).isTrue();
  }

  @Test
  void validate_changedContentUnderANewVersion_doesNotFlagSameVersion() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    model.put("timeoutMillis", "5000");
    model.put("version", "1.1");
    ObjectNode request = modelRequest(model);
    request.put("file", "a.json");

    // Act Assert
    assertThat(api.validate(request).get("sameVersion").asBoolean()).isFalse();
  }

  @Test
  void validate_unchangedContent_doesNotFlagSameVersion() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode request = modelRequest(EditorModel.fromDefinition(json(VALID)));
    request.put("file", "a.json");

    // Act Assert
    assertThat(api.validate(request).get("sameVersion").asBoolean()).isFalse();
  }

  @Test
  void validate_missingModel_throwsBadRequest() throws Exception {
    // Act Assert
    assertThatThrownBy(() -> api.validate(json("{}"))).isInstanceOf(BadRequestException.class);
  }

  // ---- save

  @Test
  void save_unchangedModel_writesNothing() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));

    // Act
    ObjectNode result =
        api.save("a.json", saveRequest(model, DesignerData.empty(), Workspace.sha256(VALID)));

    // Assert
    assertThat(result.get("unchanged").asBoolean()).isTrue();
    assertThat(result.get("definitionWritten").asBoolean()).isFalse();
    assertThat(Files.readString(dir.resolve("a.json"))).isEqualTo(VALID);
  }

  @Test
  void save_validChange_writesTheDefinitionAndTheDesignerData() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    model.put("version", "1.1");
    ObjectNode data =
        json("{\"sampleInput\": \"{}\", \"steps\": {\"s1\": {\"sampleResponse\": \"{}\"}}}");

    // Act
    ObjectNode result = api.save("a.json", saveRequest(model, data, Workspace.sha256(VALID)));

    // Assert
    assertThat(result.get("definitionWritten").asBoolean()).isTrue();
    assertThat(result.get("designerDataWritten").asBoolean()).isTrue();
    String written = Files.readString(dir.resolve("a.json"));
    assertThat(result.get("sha256").asText()).isEqualTo(Workspace.sha256(written));
    assertThat(json(written).get("version").asText()).isEqualTo("1.1");
    assertThat(Files.readString(dir.resolve(".designer/a.json"))).contains("\"charge\"");
  }

  @Test
  void save_invalidChange_writesOnlyTheDesignerDataAndReturnsTheProblems() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    ((ObjectNode) model.get("steps").get(0).get("phases")).remove("compensation");
    ObjectNode data = json("{\"steps\": {\"s1\": {\"sampleResponse\": \"{}\"}}}");

    // Act
    ObjectNode result = api.save("a.json", saveRequest(model, data, Workspace.sha256(VALID)));

    // Assert
    assertThat(result.get("definitionWritten").asBoolean()).isFalse();
    assertThat(result.get("problems")).hasSize(1);
    assertThat(Files.readString(dir.resolve("a.json"))).isEqualTo(VALID);
    assertThat(result.get("designerDataWritten").asBoolean()).isTrue();
  }

  @Test
  void save_decimalScaleChanged_writesTheChange() throws Exception {
    // Arrange
    String text =
        """
        {"name": "order", "version": "1", "steps": [{"name": "charge", "service": "payment",
          "execution": {"path": "/charges", "jsonBody": {"amount": 100.5}},
          "compensation": {"path": "/refunds"}}]}
        """;
    Files.writeString(dir.resolve("a.json"), text);
    ObjectNode model = EditorModel.fromDefinition(json(text));
    ObjectNode amount =
        (ObjectNode)
            model
                .get("steps")
                .get(0)
                .get("phases")
                .get("execution")
                .get("jsonBody")
                .get("fields")
                .get(0)
                .get("value");
    amount.put("text", "100.50");

    // Act
    ObjectNode result =
        api.save("a.json", saveRequest(model, DesignerData.empty(), Workspace.sha256(text)));

    // Assert
    assertThat(result.get("unchanged").asBoolean()).isFalse();
    assertThat(result.get("definitionWritten").asBoolean()).isTrue();
    assertThat(Files.readString(dir.resolve("a.json"))).contains("100.50");
  }

  @Test
  void save_queryRowsReordered_writesTheChange() throws Exception {
    // Arrange
    String text =
        """
        {"name": "order", "version": "1", "steps": [{"name": "charge", "service": "payment",
          "execution": {"path": "/charges", "query": {"a": "1", "b": "2"}},
          "compensation": {"path": "/refunds"}}]}
        """;
    Files.writeString(dir.resolve("a.json"), text);
    ObjectNode model = EditorModel.fromDefinition(json(text));
    ArrayNode rows =
        (ArrayNode) model.get("steps").get(0).get("phases").get("execution").get("query");
    rows.add(rows.remove(0));

    // Act
    ObjectNode result =
        api.save("a.json", saveRequest(model, DesignerData.empty(), Workspace.sha256(text)));

    // Assert
    assertThat(result.get("definitionWritten").asBoolean()).isTrue();
    String written = Files.readString(dir.resolve("a.json"));
    assertThat(written.indexOf("\"b\"")).isLessThan(written.indexOf("\"a\""));
  }

  @Test
  void save_invalidModelAfterAnOutsideChange_throwsConflictAndWritesNothing() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    String staleSha = Workspace.sha256(VALID);
    Files.writeString(dir.resolve("a.json"), VALID.replace("1.0", "1.1"));
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    ((ObjectNode) model.get("steps").get(0).get("phases")).remove("compensation");
    ObjectNode data = json("{\"steps\": {\"s1\": {\"sampleResponse\": \"{}\"}}}");

    // Act Assert
    assertThatThrownBy(() -> api.save("a.json", saveRequest(model, data, staleSha)))
        .isInstanceOf(Workspace.ConflictException.class);
    assertThat(dir.resolve(".designer")).doesNotExist();
  }

  @Test
  void save_unchangedModelAfterAnOutsideChange_throwsConflict() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    String staleSha = Workspace.sha256(VALID);
    String changed = VALID.replace("\"1.0\"", "\"1.0\" ");
    Files.writeString(dir.resolve("a.json"), changed);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));

    // Act Assert
    assertThatThrownBy(() -> api.save("a.json", saveRequest(model, DesignerData.empty(), staleSha)))
        .isInstanceOf(Workspace.ConflictException.class);
  }

  @Test
  void save_fileNameThatIsNotADefinitionName_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode request =
        newFileRequest(EditorModel.fromDefinition(json(VALID)), DesignerData.empty());

    // Act Assert
    assertThatThrownBy(() -> api.save("notes.txt", request))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void save_newValidFile_createsIt() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json(VALID));

    // Act
    ObjectNode result = api.save("new.yaml", newFileRequest(model, DesignerData.empty()));

    // Assert
    assertThat(result.get("definitionWritten").asBoolean()).isTrue();
    assertThat(Files.readString(dir.resolve("new.yaml"))).startsWith("name: order\n");
  }

  @Test
  void save_newInvalidFile_writesNothingAtAll() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json("{\"name\": \"x\", \"steps\": []}"));
    ObjectNode data = json("{\"sampleInput\": \"{}\", \"steps\": {}}");

    // Act
    ObjectNode result = api.save("new.json", newFileRequest(model, data));

    // Assert
    assertThat(result.get("definitionWritten").asBoolean()).isFalse();
    assertThat(result.get("designerDataWritten").asBoolean()).isFalse();
    assertThat(result.get("sha256").isNull()).isTrue();
    try (var entries = Files.list(dir)) {
      assertThat(entries).isEmpty();
    }
  }

  @Test
  void save_fileChangedOnDisk_throwsConflictAndKeepsTheFile() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    model.put("version", "1.1");

    // Act Assert
    assertThatThrownBy(
            () ->
                api.save(
                    "a.json",
                    saveRequest(model, DesignerData.empty(), Workspace.sha256("older content"))))
        .isInstanceOf(Workspace.ConflictException.class);
    assertThat(Files.readString(dir.resolve("a.json"))).isEqualTo(VALID);
  }

  @Test
  void save_duplicateStepNames_reportsADesignerDataError() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    ObjectNode copy = model.get("steps").get(0).deepCopy();
    copy.put("id", "s2");
    ((com.fasterxml.jackson.databind.node.ArrayNode) model.get("steps")).add(copy);

    // Act
    ObjectNode result =
        api.save("a.json", saveRequest(model, DesignerData.empty(), Workspace.sha256(VALID)));

    // Assert
    assertThat(result.get("definitionWritten").asBoolean()).isFalse();
    assertThat(result.get("designerDataError").asText()).contains("unique name");
  }

  @Test
  void save_sameDesignerDataTwice_writesItOnce() throws Exception {
    // Arrange
    Files.writeString(dir.resolve("a.json"), VALID);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    ObjectNode data = json("{\"sampleInput\": \"{}\", \"steps\": {}}");
    api.save("a.json", saveRequest(model, data, Workspace.sha256(VALID)));

    // Act
    ObjectNode result = api.save("a.json", saveRequest(model, data, Workspace.sha256(VALID)));

    // Assert
    assertThat(result.get("designerDataWritten").asBoolean()).isFalse();
  }

  // ---- export, templates, bundle

  @Test
  void export_validModel_returnsTheFileNamedAfterTheSaga() throws Exception {
    // Arrange
    ObjectNode request = modelRequest(EditorModel.fromDefinition(json(VALID)));
    request.put("format", "yaml");

    // Act
    DesignerApi.Download download = api.export(request);

    // Assert
    assertThat(download.fileName()).isEqualTo("order.yaml");
    assertThat(download.contentType()).isEqualTo("application/yaml");
    assertThat(new String(download.content(), StandardCharsets.UTF_8)).startsWith("name: order\n");
  }

  @Test
  void export_sagaNameUnusableAsAFileName_fallsBackToAGenericName() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    model.put("name", "order saga");

    // Act Assert
    assertThat(api.export(modelRequest(model)).fileName()).isEqualTo("definition.json");
  }

  @Test
  void export_invalidModel_throwsUnprocessable() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json("{\"name\": \"x\", \"steps\": []}"));

    // Act Assert
    assertThatThrownBy(() -> api.export(modelRequest(model)))
        .isInstanceOfSatisfying(
            BadRequestException.class, e -> assertThat(e.status()).isEqualTo(422));
  }

  @Test
  void export_unknownFormat_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode request = modelRequest(EditorModel.fromDefinition(json(VALID)));
    request.put("format", "toml");

    // Act Assert
    assertThatThrownBy(() -> api.export(request))
        .isInstanceOfSatisfying(
            BadRequestException.class, e -> assertThat(e.status()).isEqualTo(400));
  }

  @Test
  void serviceTemplates_withServicesFolder_marksExistingServices() throws Exception {
    // Arrange
    Files.writeString(servicesDir.resolve("payment.properties"), "base_url=http://p");
    DesignerApi withServices = new DesignerApi(new Workspace(dir), servicesDir);
    ObjectNode model = EditorModel.fromDefinition(json(VALID));
    ObjectNode second = model.get("steps").get(0).deepCopy();
    second.put("id", "s2").put("name", "reserve").put("service", "inventory");
    ((com.fasterxml.jackson.databind.node.ArrayNode) model.get("steps")).add(second);

    // Act
    JsonNode templates = withServices.serviceTemplates(modelRequest(model)).get("templates");

    // Assert
    assertThat(templates).hasSize(2);
    assertThat(templates.get(0).get("service").asText()).isEqualTo("payment");
    assertThat(templates.get(0).get("exists").asBoolean()).isTrue();
    assertThat(templates.get(1).get("exists").asBoolean()).isFalse();
    assertThat(templates.get(1).get("text").asText()).contains("base_url=");
  }

  @Test
  void bundle_withServicesFolder_zipsOnlyMissingServiceTemplates() throws Exception {
    // Arrange
    Files.writeString(servicesDir.resolve("payment.properties"), "base_url=http://p");
    DesignerApi withServices = new DesignerApi(new Workspace(dir), servicesDir);

    // Act
    DesignerApi.Download zip =
        withServices.bundle(modelRequest(EditorModel.fromDefinition(json(VALID))));

    // Assert
    assertThat(zip.fileName()).isEqualTo("order.zip");
    assertThat(entries(zip.content())).containsExactly("definitions/order.json");
  }

  @Test
  void bundle_withoutServicesFolder_zipsEveryServiceTemplate() throws Exception {
    // Act
    DesignerApi.Download zip = api.bundle(modelRequest(EditorModel.fromDefinition(json(VALID))));

    // Assert
    assertThat(entries(zip.content()))
        .containsExactly("definitions/order.json", "services/payment.properties");
  }

  private static List<String> entries(byte[] zip) throws IOException {
    List<String> names = new ArrayList<>();
    try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zip))) {
      for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
        names.add(entry.getName());
      }
    }
    return names;
  }
}
