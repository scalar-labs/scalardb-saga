package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class EditorModelTest {

  private static JsonNode json(String text) throws Exception {
    return DefinitionFormats.read(text, Format.JSON);
  }

  private static ObjectNode roundTrip(JsonNode definition) {
    EditorModel.Export export = EditorModel.toDefinition(EditorModel.fromDefinition(definition));
    assertThat(export.problems()).isEmpty();
    return export.definition();
  }

  static Stream<Path> fixtures() {
    return Fixtures.definitionFiles().stream();
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void fromDefinitionThenToDefinition_repositoryFileGiven_givesTheSameTree(Path file)
      throws Exception {
    // Arrange
    JsonNode original =
        DefinitionFormats.read(
            Files.readString(file), Format.ofFileName(String.valueOf(file.getFileName())));

    // Act
    ObjectNode exported = roundTrip(original);

    // Assert
    assertThat(exported).isEqualTo(original);
  }

  @Test
  void fromDefinition_declarativeStepGiven_buildsTheDocumentedModel() throws Exception {
    // Arrange
    JsonNode definition =
        json(
            """
            {"name": "order", "version": "1.0", "steps": [{
              "name": "charge", "service": "payment", "pivot": true,
              "execution": {"method": "POST", "path": "/charges/${orderId}",
                "query": {"currency": "${currency}"},
                "jsonBody": {"orderId": "${orderId}", "note": "order-${orderId}", "amount": 100.50,
                  "rush": false, "gift": null, "lines": [{"sku": "A"}]},
                "output": {"paymentId": "$.payment_id"}},
              "compensation": {"method": "DELETE", "path": "/charges/${orderId}"}}]}
            """);

    // Act
    ObjectNode model = EditorModel.fromDefinition(definition);

    // Assert
    assertThat(model.get("name").asText()).isEqualTo("order");
    assertThat(model.get("mode").isNull()).isTrue();
    JsonNode step = model.get("steps").get(0);
    assertThat(step.get("id").asText()).isEqualTo("s1");
    assertThat(step.get("pivot").asBoolean()).isTrue();
    JsonNode execution = step.get("phases").get("execution");
    assertThat(execution.get("query").get(0).get("value").asText()).isEqualTo("${currency}");
    assertThat(execution.get("output").get(0).get("name").asText()).isEqualTo("paymentId");
    JsonNode fields = execution.get("jsonBody").get("fields");
    assertThat(fields.get(0).get("value").toString())
        .isEqualTo("{\"kind\":\"context\",\"key\":\"orderId\"}");
    assertThat(fields.get(1).get("value").toString())
        .isEqualTo("{\"kind\":\"template\",\"text\":\"order-${orderId}\"}");
    assertThat(fields.get(2).get("value").toString())
        .isEqualTo("{\"kind\":\"fixed\",\"type\":\"number\",\"text\":\"100.50\"}");
    assertThat(fields.get(3).get("value").get("type").asText()).isEqualTo("boolean");
    assertThat(fields.get(4).get("value").get("type").asText()).isEqualTo("null");
    assertThat(fields.get(5).get("value").get("kind").asText()).isEqualTo("list");
    assertThat(step.get("phases").has("reservation")).isFalse();
  }

  @Test
  void fromDefinition_unknownAndMistypedFieldsGiven_keepsThemAsExtrasInOrder() throws Exception {
    // Arrange
    JsonNode definition =
        json(
            """
            {"name": "order", "colour": "blue", "version": 2, "timeoutMillis": "30000",
             "steps": [{"name": "a", "pivot": "yes", "execution": {"path": "/a", "query": {"n": 1}}},
                       "not a step"]}
            """);

    // Act
    ObjectNode model = EditorModel.fromDefinition(definition);

    // Assert
    assertThat(model.get("extras").toString())
        .isEqualTo(
            "[{\"name\":\"colour\",\"json\":\"\\\"blue\\\"\"},{\"name\":\"version\",\"json\":\"2\"},"
                + "{\"name\":\"timeoutMillis\",\"json\":\"\\\"30000\\\"\"}]");
    assertThat(model.get("version").isNull()).isTrue();
    JsonNode step = model.get("steps").get(0);
    assertThat(step.get("extras").get(0).get("name").asText()).isEqualTo("pivot");
    assertThat(step.get("phases").get("execution").get("extras").get(0).get("name").asText())
        .isEqualTo("query");
    assertThat(model.get("steps").get(1).get("raw").asText()).isEqualTo("\"not a step\"");
  }

  @Test
  void toDefinition_unknownAndMistypedFieldsKept_writesThemBackUnchanged() throws Exception {
    // Arrange
    JsonNode definition =
        json(
            """
            {"name": "order", "colour": "blue", "version": 2, "timeoutMillis": "30000",
             "steps": [{"name": "a", "pivot": "yes", "execution": {"path": "/a", "query": {"n": 1}}},
                       "not a step", 7]}
            """);

    // Act
    ObjectNode exported = roundTrip(definition);

    // Assert
    assertThat(exported).isEqualTo(definition);
  }

  @Test
  void toDefinition_numbersGiven_keepsTheirExactText() throws Exception {
    // Arrange
    JsonNode definition =
        json(
            """
            {"name": "n", "timeoutMillis": 12345678901234567890,
             "steps": [{"name": "a", "execution": {"path": "/a",
               "jsonBody": {"amount": 100.50, "big": 98765432109876543210.000}}}]}
            """);

    // Act
    String written = DefinitionFormats.write(roundTrip(definition), Format.JSON);

    // Assert
    assertThat(written)
        .contains("12345678901234567890")
        .contains("100.50")
        .contains("98765432109876543210.000");
  }

  @Test
  void toDefinition_smallAndLargeDecimalsGiven_writesThemInPlainNotation() throws Exception {
    // Arrange
    JsonNode definition =
        json(
            """
            {"name": "n", "steps": [{"name": "a", "execution": {"path": "/a",
              "jsonBody": {"fee": 0.00000001, "rate": 1.50E-7}}}]}
            """);

    // Act
    ObjectNode model = EditorModel.fromDefinition(definition);
    String json = DefinitionFormats.write(roundTrip(definition), Format.JSON);
    String yaml = DefinitionFormats.write(roundTrip(definition), Format.YAML);

    // Assert
    JsonNode fee =
        model.get("steps").get(0).get("phases").get("execution").get("jsonBody").get("fields");
    assertThat(fee.get(0).get("value").get("text").asText()).isEqualTo("0.00000001");
    assertThat(json).contains("0.00000001").contains("0.000000150").doesNotContain("E-");
    assertThat(yaml).contains("0.00000001").contains("0.000000150").doesNotContain("E-");
  }

  @ParameterizedTest
  @ValueSource(strings = {"null", "\"x\"", "{}"})
  void fromDefinition_stepsThatIsNotAListGiven_keepsItAsAnExtraThatExportsBack(String steps)
      throws Exception {
    // Arrange
    JsonNode definition = json("{\"name\": \"n\", \"steps\": " + steps + "}");

    // Act
    ObjectNode model = EditorModel.fromDefinition(definition);
    EditorModel.Export export = EditorModel.toDefinition(model);

    // Assert
    assertThat(model.get("steps").isNull()).isTrue();
    assertThat(export.definition()).isEqualTo(definition);
  }

  @Test
  void fromDefinition_noStepsGiven_startsAnEmptyStepList() throws Exception {
    // Act
    ObjectNode model = EditorModel.fromDefinition(json("{\"name\": \"n\"}"));

    // Assert
    assertThat(model.get("steps").isArray()).isTrue();
    assertThat(model.get("steps")).isEmpty();
  }

  @Test
  void toDefinition_fieldsInAnyOrder_writesTheReferenceOrderThenExtras() throws Exception {
    // Arrange
    JsonNode definition =
        json(
            """
            {"zzz": 1, "steps": [], "mode": "SAGA", "name": "n", "version": "1"}
            """);

    // Act
    ObjectNode exported = roundTrip(definition);

    // Assert
    assertThat(exported.toString())
        .isEqualTo("{\"name\":\"n\",\"version\":\"1\",\"mode\":\"SAGA\",\"steps\":[],\"zzz\":1}");
  }

  @Test
  void toDefinition_notANumberInANumberField_reportsAFormProblemOnTheStep() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(json("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}]}"));
    ((ObjectNode) model.get("steps").get(0)).put("timeoutMillis", "soon");

    // Act
    EditorModel.Export export = EditorModel.toDefinition(model);

    // Assert
    assertThat(export.problems()).hasSize(1);
    assertThat(export.problems().get(0).code()).isEqualTo(Problem.FORM);
    assertThat(export.problems().get(0).stepId()).isEqualTo("s1");
    assertThat(export.definition().get("steps").get(0).get("timeoutMillis").asText())
        .isEqualTo("soon");
  }

  @Test
  void toDefinition_badFixedBooleanAndDuplicateNames_reportsFormProblems() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(
            json(
                """
                {"name": "n", "steps": [{"name": "a", "execution": {"path": "/a",
                  "jsonBody": {"x": true}, "query": {"q": "1"}}}]}
                """));
    JsonNode execution = model.get("steps").get(0).get("phases").get("execution");
    ((ObjectNode) execution.get("jsonBody").get("fields").get(0).get("value")).put("text", "yes");
    ((com.fasterxml.jackson.databind.node.ArrayNode) execution.get("query"))
        .addObject()
        .put("name", "q")
        .put("value", "2");

    // Act
    EditorModel.Export export = EditorModel.toDefinition(model);

    // Assert
    assertThat(export.problems()).extracting(Problem::code).containsOnly(Problem.FORM);
    assertThat(export.problems()).hasSize(2);
  }

  @Test
  void toDefinition_editedBodyValuesGiven_writesEachKind() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(
            json(
                """
                {"name": "n", "steps": [{"name": "a", "execution": {"path": "/a",
                  "jsonBody": {"k": "x"}}}]}
                """));
    ObjectNode value =
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
    value.removeAll();
    value.put("kind", "context").put("key", "orderId");

    // Act
    EditorModel.Export export = EditorModel.toDefinition(model);

    // Assert
    assertThat(export.definition().get("steps").get(0).get("execution").get("jsonBody").toString())
        .isEqualTo("{\"k\":\"${orderId}\"}");
  }

  @Test
  void fromDefinition_notAnObjectGiven_throwsBadRequest() throws Exception {
    // Arrange
    JsonNode array = json("[1, 2]");

    // Act Assert
    assertThatThrownBy(() -> EditorModel.fromDefinition(array))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void toDefinition_stepWithoutId_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(json("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}]}"));
    ((ObjectNode) model.get("steps").get(0)).remove("id");

    // Act Assert
    assertThatThrownBy(() -> EditorModel.toDefinition(model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void toDefinition_duplicateStepIds_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(
            json("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}, {\"name\": \"b\"}]}"));
    ((ObjectNode) model.get("steps").get(1)).put("id", "s1");

    // Act Assert
    assertThatThrownBy(() -> EditorModel.toDefinition(model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void toDefinition_extraNamedLikeAnEditedField_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json("{\"name\": \"n\", \"version\": 2}"));
    model.put("version", "3");

    // Act Assert
    assertThatThrownBy(() -> EditorModel.toDefinition(model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void toDefinition_extraThatIsNotJson_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json("{\"name\": \"n\", \"x\": 1}"));
    ((ObjectNode) model.get("extras").get(0)).put("json", "{oops");

    // Act Assert
    assertThatThrownBy(() -> EditorModel.toDefinition(model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void toDefinition_unknownBodyKind_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(
            json(
                """
                {"name": "n", "steps": [{"name": "a", "execution": {"path": "/a",
                  "jsonBody": {"k": "x"}}}]}
                """));
    ((ObjectNode)
            model
                .get("steps")
                .get(0)
                .get("phases")
                .get("execution")
                .get("jsonBody")
                .get("fields")
                .get(0)
                .get("value"))
        .put("kind", "magic");

    // Act Assert
    assertThatThrownBy(() -> EditorModel.toDefinition(model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void toDefinition_textFieldHoldingANumber_throwsBadRequest() throws Exception {
    // Arrange
    ObjectNode model = EditorModel.fromDefinition(json("{\"name\": \"n\"}"));
    model.put("version", 3);

    // Act Assert
    assertThatThrownBy(() -> EditorModel.toDefinition(model))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void stepIds_modelGiven_returnsIdsInOrder() throws Exception {
    // Arrange
    ObjectNode model =
        EditorModel.fromDefinition(
            json("{\"name\": \"n\", \"steps\": [{\"name\": \"a\"}, 1, {\"name\": \"c\"}]}"));

    // Act Assert
    assertThat(EditorModel.stepIds(model)).containsExactly("s1", "s2", "s3");
  }
}
