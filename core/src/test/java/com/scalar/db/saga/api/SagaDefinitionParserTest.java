package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SagaDefinitionParserTest {

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
      assertThat(step0.getStepClass()).isEqualTo("com.example.DebitStep");
      assertThat(step0.getTimeoutMillis()).isEqualTo(5000);
      assertThat(step0.getRetryPolicy()).isNotNull();
      assertThat(step0.getRetryPolicy())
          .satisfies(policy -> assertThat(policy.getMaxAttempts()).isEqualTo(3));

      StepDefinition step1 = def.getSteps().get(1);
      assertThat(step1.getName()).isEqualTo("credit");
      assertThat(step1.getStepClass()).isEqualTo("com.example.CreditStep");
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
      assertThat(step0.getStepClass()).isEqualTo("com.example.DebitStep");
      assertThat(step0.getTimeoutMillis()).isEqualTo(5000);
      assertThat(step0.getRetryPolicy()).isNotNull();
      assertThat(step0.getRetryPolicy())
          .satisfies(policy -> assertThat(policy.getMaxAttempts()).isEqualTo(3));

      StepDefinition step1 = def.getSteps().get(1);
      assertThat(step1.getName()).isEqualTo("credit");
      assertThat(step1.getStepClass()).isEqualTo("com.example.CreditStep");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("not found");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("name");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("steps");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("stepClass");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("Unknown field")
          .hasMessageContaining("unknownField");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("Unknown field")
          .hasMessageContaining("extra");
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
          .isInstanceOf(SagaDefinitionException.class)
          .hasMessageContaining("INVALID");
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
