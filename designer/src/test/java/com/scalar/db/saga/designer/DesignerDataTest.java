package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.util.List;
import org.junit.jupiter.api.Test;

class DesignerDataTest {

  private static JsonNode model(String json) throws Exception {
    return EditorModel.fromDefinition(DefinitionFormats.read(json, Format.JSON));
  }

  private static ObjectNode uiData(String json) throws Exception {
    return (ObjectNode) DefinitionFormats.read(json, Format.JSON);
  }

  @Test
  void write_uiDataKeyedByIdGiven_writesAFileKeyedByStepNameInDefinitionOrder() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": [{\"name\": \"b\"}, {\"name\": \"a\"}]}");
    ObjectNode data =
        uiData(
            "{\"sampleInput\": \"{\\\"orderId\\\": 1}\", \"steps\": {"
                + "\"s2\": {\"sampleResponse\": \"{}\"}, \"s1\": {\"sampleResponse\": \"[]\"},"
                + "\"gone\": {\"sampleResponse\": \"x\"}}}");

    // Act
    String text = DesignerData.write(data, model, Format.JSON, null);

    // Assert
    assertThat(text)
        .isEqualTo(
            """
            {
              "formatVersion" : 1,
              "sampleInput" : "{\\"orderId\\": 1}",
              "steps" : {
                "b" : {
                  "sampleResponse" : "[]"
                },
                "a" : {
                  "sampleResponse" : "{}"
                }
              }
            }
            """);
  }

  @Test
  void read_fileKeyedByNameGiven_returnsUiDataKeyedByIdAndDropsUnknownSteps() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}]}");
    String file =
        """
        formatVersion: 1
        sampleInput: '{}'
        steps:
          a:
            sampleResponse: 'x'
          removed:
            sampleResponse: 'y'
        """;

    // Act
    ObjectNode data = DesignerData.read(file, Format.YAML, model);

    // Assert
    assertThat(data.toString())
        .isEqualTo("{\"sampleInput\":\"{}\",\"steps\":{\"s1\":{\"sampleResponse\":\"x\"}}}");
  }

  @Test
  void writeThenRead_renamedStep_keepsItsSampleUnderTheNewName() throws Exception {
    // Arrange
    ObjectNode model = (ObjectNode) model("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}]}");
    ObjectNode data = uiData("{\"steps\": {\"s1\": {\"sampleResponse\": \"{}\"}}}");
    ((ObjectNode) model.get("steps").get(0)).put("name", "renamed");

    // Act
    String text = DesignerData.write(data, model, Format.YAML, null);
    ObjectNode back = DesignerData.read(text, Format.YAML, model);

    // Assert
    assertThat(text).contains("renamed");
    assertThat(back.get("steps").get("s1").get("sampleResponse").asText()).isEqualTo("{}");
  }

  @Test
  void write_duplicateStepNames_throwsBadRequest() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}, {\"name\": \"a\"}]}");

    // Act Assert
    assertThatThrownBy(() -> DesignerData.write(DesignerData.empty(), model, Format.JSON, null))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void write_blankStepName_throwsBadRequest() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": [{\"name\": \" \"}]}");

    // Act Assert
    assertThatThrownBy(() -> DesignerData.write(DesignerData.empty(), model, Format.JSON, null))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void write_stepThatIsNotAnObject_isSkipped() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": [7, {\"name\": \"a\"}]}");

    // Act
    String text = DesignerData.write(DesignerData.empty(), model, Format.JSON, null);

    // Assert
    assertThat(text).contains("\"steps\" : { }");
  }

  @Test
  void write_existingFileFromANewerDesigner_throwsBadRequest() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": []}");

    // Act Assert
    assertThatThrownBy(
            () ->
                DesignerData.write(
                    DesignerData.empty(), model, Format.JSON, "{\"formatVersion\": 2}"))
        .isInstanceOfSatisfying(
            BadRequestException.class, e -> assertThat(e.status()).isEqualTo(422));
  }

  @Test
  void write_damagedExistingFile_isReplaced() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": []}");

    // Act
    String text = DesignerData.write(DesignerData.empty(), model, Format.JSON, "{oops");

    // Assert
    assertThat(text).contains("\"formatVersion\" : 1");
  }

  @Test
  void read_fileFromANewerDesigner_throwsBadRequest() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": []}");

    // Act Assert
    assertThatThrownBy(() -> DesignerData.read("{\"formatVersion\": 9}", Format.JSON, model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void read_malformedFile_throwsBadRequest() throws Exception {
    // Arrange
    JsonNode model = model("{\"name\": \"n\", \"steps\": []}");

    // Act Assert
    assertThatThrownBy(() -> DesignerData.read("{oops", Format.JSON, model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void empty_always_hasANullInputAndNoSteps() {
    // Act Assert
    assertThat(DesignerData.empty().toString()).isEqualTo("{\"sampleInput\":null,\"steps\":{}}");
  }

  @Test
  void write_withSavedNames_keepsUnsavedRenamesKeyedToTheNamesOnDisk() throws Exception {
    // Arrange
    JsonNode model =
        model(
            "{\"name\": \"n\", \"steps\": [{\"name\": \"pay\"}, {\"name\": \"ship\"},"
                + " {\"name\": \"new\"}]}");
    ObjectNode data =
        uiData(
            "{\"steps\": {\"s1\": {\"sampleResponse\": \"P\"}, \"s2\": {\"sampleResponse\":"
                + " \"S\"}, \"s3\": {\"sampleResponse\": \"N\"}}}");
    String existing =
        "{\"formatVersion\": 1, \"steps\": {\"charge\": {\"sampleResponse\": \"C\"},"
            + " \"ship\": {\"sampleResponse\": \"old\"}}}";

    // Act
    DesignerData.Written written =
        DesignerData.write(data, model, Format.JSON, existing, List.of("charge", "ship"));

    // Assert
    JsonNode steps = DefinitionFormats.read(written.text(), Format.JSON).get("steps");
    assertThat(steps.get("charge").get("sampleResponse").asText()).isEqualTo("C");
    assertThat(steps.get("ship").get("sampleResponse").asText()).isEqualTo("S");
    assertThat(steps.has("pay")).isFalse();
    assertThat(steps.has("new")).isFalse();
    assertThat(written.deferred()).containsExactly("pay", "new");
  }
}
