package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinition.ServiceStep;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.SagaDefinitionException;
import org.junit.jupiter.api.Test;

class SagaDefinitionParserDeclarativeTest {

  @Test
  void parseJson_sagaDeclarativeStepGiven_parsesDeclarativeStep() {
    // Arrange
    String json =
        "{\"name\":\"transfer\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"account-service\","
            + "\"execution\":{\"method\":\"POST\",\"path\":\"/debit\","
            + "\"jsonBody\":{\"amount\":\"${amount}\"},\"output\":{\"debitId\":\"$.debit_id\"}},"
            + "\"compensation\":{\"path\":\"/reverse\",\"jsonBody\":{\"id\":\"${debitId}\"}}}]}";

    // Act
    SagaDefinition definition = SagaDefinitionParser.parseJson(json);

    // Assert
    ServiceStep step = (ServiceStep) definition.getSteps().get(0);
    assertThat(step.getName()).isEqualTo("debit");
    assertThat(step.getService()).isEqualTo("account-service");
    assertThat(step.getTransport()).isEqualTo(CallSpec.Transport.HTTP);
    assertThat(step.isTcc()).isFalse();
    HttpCall execution = (HttpCall) step.getPhase(Phase.EXECUTION).orElseThrow();
    assertThat(execution.getMethod()).isEqualTo(HttpMethod.POST);
    assertThat(execution.getPath()).isEqualTo("/debit");
    assertThat(execution.getJsonBody()).containsEntry("amount", "${amount}");
    assertThat(execution.getOutput()).containsEntry("debitId", "$.debit_id");
  }

  @Test
  void parseJson_tccDeclarativeStepGiven_parsesTccDeclarativeStep() {
    // Arrange
    String json =
        "{\"name\":\"reserveSeats\",\"mode\":\"TCC\",\"steps\":["
            + "{\"name\":\"seat\",\"service\":\"booking-service\","
            + "\"reservation\":{\"path\":\"/reserve\"},"
            + "\"confirmation\":{\"path\":\"/confirm\"},"
            + "\"cancellation\":{\"path\":\"/cancel\"}}]}";

    // Act
    SagaDefinition definition = SagaDefinitionParser.parseJson(json);

    // Assert
    ServiceStep step = (ServiceStep) definition.getSteps().get(0);
    assertThat(step.isTcc()).isTrue();
    assertThat(step.getPhases().keySet())
        .containsExactlyInAnyOrder(Phase.RESERVATION, Phase.CONFIRMATION, Phase.CANCELLATION);
  }

  @Test
  void parseYaml_sagaDeclarativeStepGiven_parsesDeclarativeStep() {
    // Arrange
    String yaml =
        """
        name: transfer
        mode: SAGA
        steps:
          - name: debit
            service: account-service
            execution:
              method: GET
              path: /users
              query:
                id: ${userId}
              output:
                name: $.name
            compensation:
              path: /noop
        """;

    // Act
    SagaDefinition definition = SagaDefinitionParser.parseYaml(yaml);

    // Assert
    ServiceStep step = (ServiceStep) definition.getSteps().get(0);
    HttpCall execution = (HttpCall) step.getPhase(Phase.EXECUTION).orElseThrow();
    assertThat(execution.getMethod()).isEqualTo(HttpMethod.GET);
    assertThat(execution.getQuery()).containsEntry("id", "${userId}");
  }

  @Test
  void parseJson_grpcTransportGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\",\"transport\":\"GRPC\","
            + "\"execution\":{\"path\":\"/debit\"},\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_mixStepClassAndPhaseGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"stepClass\":\"com.example.S\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\"},\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_mixSagaAndTccPhasesGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\"},\"reservation\":{\"path\":\"/reserve\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_sagaPhasesInTccMode_throwsException() {
    // Arrange — a TCC definition whose service step declares only SAGA phases.
    String json =
        "{\"name\":\"t\",\"mode\":\"TCC\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\"},\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_tccPhasesInSagaMode_throwsException() {
    // Arrange — a SAGA definition whose service step declares only TCC phases.
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"seat\",\"service\":\"svc\","
            + "\"reservation\":{\"path\":\"/reserve\"},\"confirmation\":{\"path\":\"/confirm\"},"
            + "\"cancellation\":{\"path\":\"/cancel\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_missingCompensationGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_missingCancellationInTccGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"TCC\",\"steps\":["
            + "{\"name\":\"seat\",\"service\":\"svc\","
            + "\"reservation\":{\"path\":\"/reserve\"},\"confirmation\":{\"path\":\"/confirm\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_missingServiceGiven_throwsException() {
    // Arrange — phases present but no 'service' hits the missing-service step-kind error
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"execution\":{\"path\":\"/debit\"},"
            + "\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class)
        .hasMessageContaining(
            "must define either 'stepClass' or a declarative service step ('service' + phases)");
  }

  @Test
  void parseJson_serviceWithoutPhasesGiven_throwsException() {
    // Arrange — a service step without any phases is rejected
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\"}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class)
        .hasMessageContaining("must define both 'execution' and 'compensation'");
  }

  @Test
  void parseJson_missingPathGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"method\":\"POST\"},\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_getWithRequestBodyGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"fetch\",\"service\":\"svc\","
            + "\"execution\":{\"method\":\"GET\",\"path\":\"/u\",\"jsonBody\":{\"a\":\"${b}\"}},"
            + "\"compensation\":{\"path\":\"/noop\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_invalidMethodGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"method\":\"FETCH\",\"path\":\"/debit\"},"
            + "\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_unknownStepFieldGiven_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\",\"bogus\":true,"
            + "\"execution\":{\"path\":\"/debit\"},\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_queryNotObjectGiven_throwsException() {
    // Arrange — a non-object 'query' must not be silently dropped
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\",\"query\":\"amount=5\"},"
            + "\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_jsonBodyNotObjectGiven_throwsException() {
    // Arrange — a non-object 'jsonBody' (array) must not be silently dropped
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\",\"jsonBody\":[\"a\",\"b\"]},"
            + "\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_outputNotObjectGiven_throwsException() {
    // Arrange — a non-object 'output' must not be silently dropped
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\",\"output\":\"$.debit_id\"},"
            + "\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void parseJson_unknownCallSpecFieldGiven_throwsException() {
    // Arrange — a misspelled 'method' ("methd") inside a call spec must not be silently ignored
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"execution\":{\"path\":\"/debit\",\"methd\":\"DELETE\"},"
            + "\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionParser.parseJson(json))
        .isInstanceOf(SagaDefinitionException.class);
  }
}
