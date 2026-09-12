package com.scalar.db.saga.definition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.definition.SagaDefinition.SagaMode;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaErrorCode;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SagaDefinitionParserTest {

  // =========================================================================
  // File and resource parsing — error-code classification
  // =========================================================================

  @Nested
  class FileAndResourceParsing {

    @TempDir Path dir;

    @Test
    void parseFile_malformedJsonFileGiven_throwsMalformedDefinitionWithFilePath() throws Exception {
      // Arrange — a file that exists and opens fine but is not valid JSON. Jackson's parse
      // failure extends IOException, so without a dedicated arm it would be misreported as an
      // unreadable source.
      Path file = dir.resolve("bad.json");
      Files.writeString(file, "{ this is not valid json");

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseFile(file))
          .isInstanceOfSatisfying(
              SagaDefinitionException.class,
              e -> {
                assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.MALFORMED_DEFINITION);
                assertThat(e.getMetadata()).containsEntry("source", file.toString());
              });
    }

    @Test
    void parseFile_nonexistentPathGiven_throwsUnreadableSource() {
      // Act & Assert — a genuine I/O failure (no such file) stays UNREADABLE_DEFINITION_SOURCE
      Path missing = dir.resolve("missing.json");
      assertThatThrownBy(() -> SagaDefinitionParser.parseFile(missing))
          .isInstanceOfSatisfying(
              SagaDefinitionException.class,
              e ->
                  assertThat(e.getErrorCode())
                      .isEqualTo(SagaErrorCode.UNREADABLE_DEFINITION_SOURCE));
    }

    @Test
    void parseResource_malformedJsonResourceGiven_throwsMalformedDefinitionWithResourcePath() {
      // Act & Assert — same classification on the classpath-resource path
      assertThatThrownBy(() -> SagaDefinitionParser.parseResource("malformed-definition.json"))
          .isInstanceOfSatisfying(
              SagaDefinitionException.class,
              e -> {
                assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.MALFORMED_DEFINITION);
                assertThat(e.getMetadata()).containsEntry("source", "malformed-definition.json");
              });
    }
  }

  // =========================================================================
  // JSON parsing
  // =========================================================================

  @Nested
  class JsonParsing {

    @Test
    void parseJson_validDefinitionGiven_parsesAllFields() {
      // Arrange
      String json =
          """
          {
            "name": "transferMoney",
            "mode": "SAGA",
            "version": "2.0",
            "recoveryStrategy": "BACKWARD",
            "timeoutMillis": 30000,
            "defaultRetryPolicy": {
              "maxAttempts": 5,
              "initialIntervalMillis": 500,
              "backoffMultiplier": 2.0,
              "maxIntervalMillis": 10000
            },
            "steps": [
              {
                "name": "debit",
                "stepClass": "com.example.DebitStep",
                "timeoutMillis": 5000,
                "retryPolicy": { "maxAttempts": 3 }
              },
              {
                "name": "credit",
                "stepClass": "com.example.CreditStep"
              }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert
      assertThat(def.getName()).isEqualTo("transferMoney");
      assertThat(def.getMode()).isEqualTo(SagaMode.SAGA);
      assertThat(def.getVersion()).isEqualTo("2.0");
      assertThat(def.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.BACKWARD);
      assertThat(def.getTimeoutMillis()).isEqualTo(30000);
      assertThat(def.getDefaultRetryPolicy()).isNotNull();
      assertThat(def.getDefaultRetryPolicy())
          .satisfies(policy -> assertThat(policy.getMaxAttempts()).isEqualTo(5));
      assertThat(def.getSteps()).hasSize(2);

      StepDefinition step0 = def.getSteps().get(0);
      assertThat(step0.getName()).isEqualTo("debit");
      assertThat(((SagaDefinition.ClassStep) step0).getStepClass())
          .isEqualTo("com.example.DebitStep");
      assertThat(step0.getTimeoutMillis()).isEqualTo(5000);
      assertThat(step0.getRetryPolicy()).isNotNull();
      assertThat(step0.getRetryPolicy())
          .satisfies(policy -> assertThat(policy.getMaxAttempts()).isEqualTo(3));

      StepDefinition step1 = def.getSteps().get(1);
      assertThat(step1.getName()).isEqualTo("credit");
      assertThat(((SagaDefinition.ClassStep) step1).getStepClass())
          .isEqualTo("com.example.CreditStep");
    }

    @Test
    void parseJson_minimalDefinitionGiven_usesDefaults() {
      // Arrange
      String json =
          """
          {
            "name": "minimal",
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1" }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert — defaults applied
      assertThat(def.getName()).isEqualTo("minimal");
      assertThat(def.getMode()).isEqualTo(SagaMode.SAGA);
      assertThat(def.getVersion()).isEqualTo("1.0");
      assertThat(def.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.BACKWARD);
      assertThat(def.getTimeoutMillis()).isEqualTo(0);
      assertThat(def.getDefaultRetryPolicy()).isNull();
      assertThat(def.getSteps()).hasSize(1);
    }

    @Test
    void parseJson_serviceStepGiven_createsServiceStep() {
      // Arrange
      String json =
          """
          {
            "name": "svcSaga",
            "steps": [
              {
                "name": "debit",
                "service": "account-service",
                "execution": { "path": "/debit" },
                "compensation": { "path": "/reverse" }
              }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert
      SagaDefinition.ServiceStep step = (SagaDefinition.ServiceStep) def.getSteps().get(0);
      assertThat(step.getService()).isEqualTo("account-service");
      assertThat(step.getPhases().keySet())
          .containsExactlyInAnyOrder(
              SagaDefinition.ServiceStep.Phase.EXECUTION,
              SagaDefinition.ServiceStep.Phase.COMPENSATION);
    }

    @Test
    void parseJson_stringBodyWithContentTypeGiven_parsesCallSpec() {
      // Arrange
      String json =
          """
          {
            "name": "svcSaga",
            "steps": [
              {
                "name": "notify",
                "service": "account-service",
                "execution": {
                  "path": "/notify",
                  "stringBody": "user=${userName}",
                  "contentType": "text/plain"
                },
                "compensation": { "path": "/noop" }
              }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert
      SagaDefinition.ServiceStep step = (SagaDefinition.ServiceStep) def.getSteps().get(0);
      HttpCall exec =
          (HttpCall) step.getPhase(SagaDefinition.ServiceStep.Phase.EXECUTION).orElseThrow();
      assertThat(exec.getStringBody()).isEqualTo("user=${userName}");
      assertThat(exec.getContentType()).isEqualTo("text/plain");
    }

    @Test
    void parseJson_stepWithoutAnyKind_throwsSagaDefinitionException() {
      // Arrange
      String json =
          """
          {
            "name": "bad",
            "steps": [ { "name": "s1" } ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_serviceWithoutPhases_throwsSagaDefinitionException() {
      // Arrange — a service step with no phases is invalid
      String json =
          """
          {
            "name": "bad",
            "steps": [ { "name": "s1", "service": "account-service" } ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_unknownStepField_throwsException() {
      // Arrange — an unrecognized field on a step is rejected
      String json =
          """
          {
            "name": "bad",
            "steps": [ { "name": "s1", "stepClass": "com.example.Step1", "bogus": "x" } ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_stepClassAndService_throwsSagaDefinitionException() {
      // Arrange — a step defining both a class and a service step is a conflict
      String json =
          """
          {
            "name": "bad",
            "steps": [
              {
                "name": "s1",
                "stepClass": "com.example.DebitStep",
                "service": "account-service",
                "execution": { "path": "/debit" },
                "compensation": { "path": "/reverse" }
              }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_nullOptionalFieldsGiven_usesDefaults() {
      // Arrange
      String json =
          """
          {
            "name": "nullFields",
            "version": null,
            "timeoutMillis": null,
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1", "timeoutMillis": null, "pivot": null }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert — null fields treated as absent, defaults applied
      assertThat(def.getVersion()).isEqualTo("1.0");
      assertThat(def.getTimeoutMillis()).isEqualTo(0);
      assertThat(def.getSteps().get(0).getTimeoutMillis()).isEqualTo(0);
    }

    @Test
    void parseJson_tccModeGiven_parsesCorrectly() {
      // Arrange
      String json =
          """
          {
            "name": "tccSaga",
            "mode": "TCC",
            "steps": [
              { "name": "t1", "stepClass": "com.example.TccStep1" },
              { "name": "t2", "stepClass": "com.example.TccStep2" }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert
      assertThat(def.getMode()).isEqualTo(SagaMode.TCC);
      assertThat(def.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.PREDEFINED);
    }

    @Test
    void parseJson_tccWithRecoveryStrategy_throwsSagaDefinitionException() {
      // Arrange — a TCC definition must not specify recoveryStrategy; recovery is predefined.
      String json =
          """
          {
            "name": "tccSaga",
            "mode": "TCC",
            "recoveryStrategy": "BACKWARD",
            "steps": [
              { "name": "t1", "stepClass": "com.example.TccStep1" }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_tccWithPivotStep_throwsSagaDefinitionException() {
      // Arrange — a TCC step must not specify pivot; recovery is predefined, so the pivot is fixed
      // at the last try step.
      String json =
          """
          {
            "name": "tccSaga",
            "mode": "TCC",
            "steps": [
              { "name": "t1", "stepClass": "com.example.TccStep1", "pivot": true }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_mixedStrategyGiven_parsesCorrectly() {
      // Arrange
      String json =
          """
          {
            "name": "mixedSaga",
            "mode": "SAGA",
            "recoveryStrategy": "MIXED",
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1" },
              { "name": "pivot", "stepClass": "com.example.PivotStep", "pivot": true },
              { "name": "s3", "stepClass": "com.example.Step3" }
            ]
          }
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseJson(json);

      // Assert
      assertThat(def.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.MIXED);
      assertThat(def.getPivotIndex()).isEqualTo(1);
    }
  }

  // =========================================================================
  // YAML parsing
  // =========================================================================

  @Nested
  class YamlParsing {

    @Test
    void parseYaml_validDefinitionGiven_parsesAllFields() {
      // Arrange
      String yaml =
          """
          # Transfer money saga
          name: transferMoney
          mode: SAGA
          version: "2.0"
          recoveryStrategy: BACKWARD
          timeoutMillis: 30000
          defaultRetryPolicy:
            maxAttempts: 5
            initialIntervalMillis: 500
            backoffMultiplier: 2.0
            maxIntervalMillis: 10000
          steps:
            - name: debit
              stepClass: com.example.DebitStep
              timeoutMillis: 5000
              retryPolicy:
                maxAttempts: 3
            - name: credit
              stepClass: com.example.CreditStep
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseYaml(yaml);

      // Assert
      assertThat(def.getName()).isEqualTo("transferMoney");
      assertThat(def.getMode()).isEqualTo(SagaMode.SAGA);
      assertThat(def.getVersion()).isEqualTo("2.0");
      assertThat(def.getRecoveryStrategy()).isEqualTo(RecoveryStrategy.BACKWARD);
      assertThat(def.getTimeoutMillis()).isEqualTo(30000);
      assertThat(def.getDefaultRetryPolicy()).isNotNull();
      assertThat(def.getDefaultRetryPolicy())
          .satisfies(policy -> assertThat(policy.getMaxAttempts()).isEqualTo(5));
      assertThat(def.getSteps()).hasSize(2);

      StepDefinition step0 = def.getSteps().get(0);
      assertThat(step0.getName()).isEqualTo("debit");
      assertThat(((SagaDefinition.ClassStep) step0).getStepClass())
          .isEqualTo("com.example.DebitStep");
      assertThat(step0.getTimeoutMillis()).isEqualTo(5000);
      assertThat(step0.getRetryPolicy()).isNotNull();
      assertThat(step0.getRetryPolicy())
          .satisfies(policy -> assertThat(policy.getMaxAttempts()).isEqualTo(3));

      StepDefinition step1 = def.getSteps().get(1);
      assertThat(step1.getName()).isEqualTo("credit");
      assertThat(((SagaDefinition.ClassStep) step1).getStepClass())
          .isEqualTo("com.example.CreditStep");
    }

    @Test
    void parseYaml_stringBodyWithContentTypeGiven_parsesCallSpec() {
      // Arrange
      String yaml =
          """
          name: svcSaga
          steps:
            - name: notify
              service: account-service
              execution:
                path: /notify
                stringBody: "user=${userName}"
                contentType: text/plain
              compensation:
                path: /noop
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseYaml(yaml);

      // Assert
      SagaDefinition.ServiceStep step = (SagaDefinition.ServiceStep) def.getSteps().get(0);
      HttpCall exec =
          (HttpCall) step.getPhase(SagaDefinition.ServiceStep.Phase.EXECUTION).orElseThrow();
      assertThat(exec.getStringBody()).isEqualTo("user=${userName}");
      assertThat(exec.getContentType()).isEqualTo("text/plain");
    }

    @Test
    void parseYaml_commentsIgnored_parsesSuccessfully() {
      // Arrange
      String yaml =
          """
          # This is a comment
          name: simple
          # Another comment
          steps:
            # Step comment
            - name: s1
              stepClass: com.example.Step1
          """;

      // Act
      SagaDefinition def = SagaDefinitionParser.parseYaml(yaml);

      // Assert
      assertThat(def.getName()).isEqualTo("simple");
    }
  }

  // =========================================================================
  // Resource parsing
  // =========================================================================

  @Nested
  class ResourceParsing {

    @Test
    void parseResource_jsonFileGiven_parsesSuccessfully() {
      // Act
      SagaDefinition def = SagaDefinitionParser.parseResource("sagas/transfer.json");

      // Assert
      assertThat(def.getName()).isEqualTo("transferMoney");
      assertThat(def.getSteps()).hasSize(2);
    }

    @Test
    void parseResource_yamlFileGiven_parsesSuccessfully() {
      // Act
      SagaDefinition def = SagaDefinitionParser.parseResource("sagas/transfer.yaml");

      // Assert
      assertThat(def.getName()).isEqualTo("transferMoney");
      assertThat(def.getSteps()).hasSize(2);
    }

    @Test
    void parseResource_minimalJsonGiven_parsesSuccessfully() {
      // Act
      SagaDefinition def = SagaDefinitionParser.parseResource("sagas/minimal.json");

      // Assert
      assertThat(def.getName()).isEqualTo("minimal");
      assertThat(def.getMode()).isEqualTo(SagaMode.SAGA);
    }

    @Test
    void parseResource_notFound_throwsException() {
      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseResource("sagas/nonexistent.json"))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseResource_unsupportedExtensionGiven_throwsException() {
      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseResource("sagas/transfer.txt"))
          .isInstanceOf(SagaDefinitionException.class);
    }
  }

  // =========================================================================
  // Validation errors
  // =========================================================================

  @Nested
  class ValidationErrors {

    @Test
    void parseJson_missingName_throwsException() {
      // Arrange
      String json =
          """
          {
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1" }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_emptySteps_throwsException() {
      // Arrange
      String json =
          """
          {
            "name": "empty",
            "steps": []
          }
          """;

      // Act & Assert — SagaDefinition.build() validates non-empty steps
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_missingSteps_throwsException() {
      // Arrange
      String json =
          """
          { "name": "noSteps" }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_missingStepClass_throwsException() {
      // Arrange
      String json =
          """
          {
            "name": "bad",
            "steps": [
              { "name": "s1" }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_unknownTopLevelField_throwsException() {
      // Arrange
      String json =
          """
          {
            "name": "bad",
            "unknownField": "value",
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1" }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_unknownStepField_throwsException() {
      // Arrange
      String json =
          """
          {
            "name": "bad",
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1", "extra": true }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_invalidMode_throwsException() {
      // Arrange
      String json =
          """
          {
            "name": "bad",
            "mode": "INVALID",
            "steps": [
              { "name": "s1", "stepClass": "com.example.Step1" }
            ]
          }
          """;

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }

    @Test
    void parseJson_invalidJson_throwsException() {
      // Arrange
      String json = "not valid json {{{";

      // Act & Assert
      assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
          .isInstanceOf(SagaDefinitionException.class);
    }
  }
}
