package com.scalar.db.saga.designer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.saga.designer.DefinitionFormats.Format;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DefinitionValidatorTest {

  private static List<Problem> validate(String json) throws Exception {
    ObjectNode model = EditorModel.fromDefinition(DefinitionFormats.read(json, Format.JSON));
    EditorModel.Export export = EditorModel.toDefinition(model);
    return DefinitionValidator.validate(export.definition(), EditorModel.stepIds(model));
  }

  private static String step(String name, String extra) {
    return "{\"name\": \""
        + name
        + "\", \"service\": \"svc\", \"execution\": {\"path\": \"/"
        + name
        + "\"}, \"compensation\": {\"path\": \"/undo-"
        + name
        + "\"}"
        + extra
        + "}";
  }

  static Stream<Path> fixtures() {
    return Fixtures.definitionFiles().stream();
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void validate_repositoryFileGiven_findsNoProblem(Path file) throws Exception {
    // Arrange
    JsonNode tree =
        DefinitionFormats.read(
            Files.readString(file), Format.ofFileName(String.valueOf(file.getFileName())));
    ObjectNode model = EditorModel.fromDefinition(tree);

    // Act
    List<Problem> problems =
        DefinitionValidator.validate(
            EditorModel.toDefinition(model).definition(), EditorModel.stepIds(model));

    // Assert
    assertThat(problems).isEmpty();
  }

  @Test
  void validate_stepLocalProblemsInTwoSteps_placesEachOnItsStep() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"steps\": ["
            + step("a", "")
            + ","
            + "{\"name\": \"b\", \"service\": \"svc\", \"execution\": {\"method\": \"GET\","
            + " \"path\": \"/b\", \"jsonBody\": {\"x\": 1}}, \"compensation\": {\"path\": \"/u\"}},"
            + "{\"name\": \"c\", \"service\": \"svc\", \"execution\": {\"path\": \"/c\"}}"
            + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).extracting(Problem::stepId).containsExactly("s2", "s3");
    assertThat(problems).extracting(Problem::code).containsOnly("INVALID_STEP_DEFINITION");
  }

  @Test
  void validate_compensationReadsItsOwnOutput_placesTheProblemOnThatStep() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"steps\": ["
            + step("a", "")
            + ","
            + "{\"name\": \"b\", \"service\": \"svc\", \"execution\": {\"path\": \"/b\","
            + " \"output\": {\"bId\": \"$.id\"}}, \"compensation\": {\"path\": \"/b/${bId}\"}}"
            + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isEqualTo("s2");
  }

  @Test
  void validate_duplicateStepNames_reportsOneDefinitionLevelProblem() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"steps\": [" + step("a", "") + "," + step("a", "") + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isNull();
  }

  @Test
  void validate_mixedWithPivotOnTheLastStep_reportsOneDefinitionLevelProblem() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"recoveryStrategy\": \"MIXED\", \"steps\": ["
            + step("a", "")
            + ","
            + step("b", "")
            + ","
            + step("c", ", \"pivot\": true")
            + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isNull();
  }

  @Test
  void validate_mixedWithPivotInTheMiddle_findsNoProblem() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"recoveryStrategy\": \"MIXED\", \"steps\": ["
            + step("a", "")
            + ","
            + step("b", ", \"pivot\": true")
            + ","
            + step("c", "")
            + "]}";

    // Act Assert
    assertThat(validate(definition)).isEmpty();
  }

  @Test
  void validate_invalidMode_reportsItOnceNotOnEveryStep() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"mode\": \"SAGAS\", \"steps\": ["
            + step("a", "")
            + ","
            + step("b", "")
            + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isNull();
  }

  @Test
  void validate_tccStepWithAPivot_placesTheProblemOnThatStep() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"mode\": \"TCC\", \"steps\": [{\"name\": \"a\", \"service\": \"s\","
            + " \"pivot\": true, \"reservation\": {\"path\": \"/r\"}, \"confirmation\":"
            + " {\"path\": \"/c\"}, \"cancellation\": {\"path\": \"/x\"}}]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).isNotEmpty();
    assertThat(problems.get(0).stepId()).isEqualTo("s1");
  }

  @Test
  void validate_unknownStepField_placesTheProblemOnThatStep() throws Exception {
    // Arrange
    String definition =
        "{\"name\": \"n\", \"steps\": ["
            + step("a", "")
            + ","
            + step("b", ", \"retries\": 3")
            + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isEqualTo("s2");
    assertThat(problems.get(0).message()).contains("retries");
  }

  @Test
  void validate_missingName_reportsADefinitionLevelProblem() throws Exception {
    // Arrange
    String definition = "{\"steps\": [" + step("a", "") + "]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isNull();
    assertThat(problems.get(0).code()).isEqualTo("INVALID_DEFINITION");
  }

  @Test
  void validate_stepThatIsNotAnObject_reportsADefinitionLevelProblem() throws Exception {
    // Arrange
    String definition = "{\"name\": \"n\", \"steps\": [" + step("a", "") + ", 7]}";

    // Act
    List<Problem> problems = validate(definition);

    // Assert
    assertThat(problems).hasSize(1);
    assertThat(problems.get(0).stepId()).isNull();
  }
}
