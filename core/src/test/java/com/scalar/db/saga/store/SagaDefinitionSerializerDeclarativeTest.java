package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.saga.api.HttpMethod;
import com.scalar.db.saga.definition.CallSpec.Transport;
import com.scalar.db.saga.definition.HttpCall;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SagaDefinitionSerializerDeclarativeTest {

  private SagaDefinitionSerializer serializer;

  @BeforeEach
  void setUp() {
    serializer = new SagaDefinitionSerializer(new ObjectMapper());
  }

  private static HttpCall call(String path) {
    return HttpCall.newBuilder(path).build();
  }

  private ServiceStep firstStep(SagaDefinition definition) {
    return (ServiceStep) definition.getSteps().get(0);
  }

  @Test
  void serializeAndDeserialize_sagaDeclarativeStep_roundTripsCorrectly() {
    // Arrange
    HttpCall execution =
        HttpCall.newBuilder("/debit")
            .method(HttpMethod.POST)
            .jsonBody(Map.of("amount", "${amount}"))
            .output(Map.of("debitId", "$.debit_id"))
            .build();
    HttpCall compensation =
        HttpCall.newBuilder("/debit/reverse").jsonBody(Map.of("amount", "${amount}")).build();
    SagaDefinition original =
        SagaDefinition.newBuilder("transfer")
            .saga()
            .serviceStep("debit", "account-service")
            .execution(execution)
            .compensation(compensation)
            .add()
            .build();

    // Act
    SagaDefinition restored = serializer.deserialize(serializer.serialize(original));

    // Assert
    assertThat(firstStep(restored)).isEqualTo(firstStep(original));
  }

  @Test
  void parseThenSerializeAndDeserialize_declarativeStep_agreesAcrossBothPaths() {
    // Arrange — a definition authored as JSON, exercising every CallSpec field (method, path,
    // query, jsonBody, output) through the user-facing parser and the store (de)serializer, which
    // now share one CallSpecCodec. This guards against the two parsing paths drifting.
    String json =
        """
        {
          "name": "transfer",
          "mode": "SAGA",
          "steps": [
            {
              "name": "debit",
              "service": "account-service",
              "transport": "HTTP",
              "execution": {
                "method": "POST",
                "path": "/accounts/${id}/debit",
                "query": {"trace": "${traceId}"},
                "jsonBody": {"amount": "${amount}"},
                "output": {"debitId": "$.debit_id"}
              },
              "compensation": {
                "method": "POST",
                "path": "/accounts/${id}/credit",
                "jsonBody": {"id": "${id}"}
              }
            }
          ]
        }
        """;

    // Act — parse (api path), then round-trip through the store (store path)
    SagaDefinition parsed = SagaDefinitionParser.parseJson(json);
    SagaDefinition restored = serializer.deserialize(serializer.serialize(parsed));

    // Assert — both paths produce the identical declarative step
    assertThat(firstStep(restored)).isEqualTo(firstStep(parsed));
  }

  @Test
  void serializeAndDeserialize_tccDeclarativeStep_roundTripsCorrectly() {
    // Arrange
    SagaDefinition original =
        SagaDefinition.newBuilder("reserveSeats")
            .tcc()
            .serviceStep("seat", "booking-service")
            .reservation(call("/reserve"))
            .confirmation(call("/confirm"))
            .cancellation(call("/cancel"))
            .add()
            .build();

    // Act
    SagaDefinition restored = serializer.deserialize(serializer.serialize(original));

    // Assert
    ServiceStep step = firstStep(restored);
    assertThat(step).isEqualTo(firstStep(original));
    assertThat(step.isTcc()).isTrue();
    assertThat(step.getPhases().keySet())
        .containsExactlyInAnyOrder(Phase.RESERVATION, Phase.CONFIRMATION, Phase.CANCELLATION);
  }

  @Test
  void serialize_tccDefinition_omitsRecoveryStrategyAndPivot() {
    // Arrange — a TCC definition has neither an explicit recoveryStrategy nor a pivot step; the
    // serializer must omit both so the round-trip survives (deserialize rejects them for TCC).
    SagaDefinition original =
        SagaDefinition.newBuilder("reserveSeats")
            .tcc()
            .serviceStep("seat", "booking-service")
            .reservation(call("/reserve"))
            .confirmation(call("/confirm"))
            .cancellation(call("/cancel"))
            .add()
            .build();

    // Act
    String json = serializer.serialize(original);
    SagaDefinition restored = serializer.deserialize(json);

    // Assert — serialized form drops recoveryStrategy/pivot, and the round-trip is faithful.
    assertThat(json).doesNotContain("recoveryStrategy").doesNotContain("pivot");
    assertThat(restored).isEqualTo(original);
  }

  @Test
  void serializeAndDeserialize_getDeclarativeStepWithQuery_roundTripsCorrectly() {
    // Arrange
    HttpCall execution =
        HttpCall.newBuilder("/users")
            .method(HttpMethod.GET)
            .query(Map.of("id", "${userId}"))
            .output(Map.of("name", "$.profile.name"))
            .build();
    SagaDefinition original =
        SagaDefinition.newBuilder("lookup")
            .saga()
            .serviceStep("fetchUser", "user-service")
            .execution(execution)
            .compensation(call("/noop"))
            .add()
            .build();

    // Act
    SagaDefinition restored = serializer.deserialize(serializer.serialize(original));

    // Assert
    ServiceStep step = firstStep(restored);
    assertThat(step).isEqualTo(firstStep(original));
    HttpCall restoredExecution = (HttpCall) step.getPhase(Phase.EXECUTION).orElseThrow();
    assertThat(restoredExecution.getMethod()).isEqualTo(HttpMethod.GET);
    assertThat(restoredExecution.getQuery()).containsEntry("id", "${userId}");
  }

  @Test
  void serialize_declarativeStep_emitsTransportAndPhaseKeys() {
    // Arrange
    SagaDefinition definition =
        SagaDefinition.newBuilder("transfer")
            .saga()
            .serviceStep("debit", "account-service")
            .execution(call("/debit"))
            .compensation(call("/reverse"))
            .add()
            .build();

    // Act
    String json = serializer.serialize(definition);

    // Assert — a declarative step emits service, transport, and phase keys, but NOT 'operation'
    // (the 'operation' field is removed).
    assertThat(json)
        .contains("\"service\":\"account-service\"")
        .doesNotContain("\"operation\"")
        .contains("\"transport\":\"HTTP\"")
        .contains("\"execution\"")
        .contains("\"compensation\"");
  }

  @Test
  void deserialize_transportAbsent_defaultsToHttp() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"timeoutMillis\":0,\"pivot\":false,"
            + "\"execution\":{\"method\":\"POST\",\"path\":\"/debit\"},"
            + "\"compensation\":{\"method\":\"POST\",\"path\":\"/reverse\"}}]}";

    // Act
    SagaDefinition restored = serializer.deserialize(json);

    // Assert
    assertThat(firstStep(restored).getTransport()).isEqualTo(Transport.HTTP);
  }

  @Test
  void deserialize_grpcTransport_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\",\"transport\":\"GRPC\","
            + "\"timeoutMillis\":0,\"pivot\":false,"
            + "\"execution\":{\"path\":\"/debit\"},\"compensation\":{\"path\":\"/reverse\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_mixedSagaAndTccPhases_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"timeoutMillis\":0,\"pivot\":false,"
            + "\"execution\":{\"path\":\"/debit\"},\"reservation\":{\"path\":\"/reserve\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_tccWithRecoveryStrategy_throwsException() {
    // Arrange — a stored TCC record must not carry 'recoveryStrategy'; the deserializer rejects it,
    // mirroring the parser (recovery is predefined for TCC).
    String json =
        "{\"name\":\"t\",\"mode\":\"TCC\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"seat\",\"service\":\"svc\",\"timeoutMillis\":0,"
            + "\"reservation\":{\"path\":\"/reserve\"},\"confirmation\":{\"path\":\"/confirm\"},"
            + "\"cancellation\":{\"path\":\"/cancel\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_tccStepWithPivot_throwsException() {
    // Arrange — a stored TCC step must not carry 'pivot'; the deserializer rejects it, mirroring
    // the
    // parser (recovery is predefined, so the pivot is fixed at the last try step).
    String json =
        "{\"name\":\"t\",\"mode\":\"TCC\",\"version\":\"1.0\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"seat\",\"service\":\"svc\",\"timeoutMillis\":0,\"pivot\":true,"
            + "\"reservation\":{\"path\":\"/reserve\"},\"confirmation\":{\"path\":\"/confirm\"},"
            + "\"cancellation\":{\"path\":\"/cancel\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_getWithRequestBody_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"fetch\",\"service\":\"svc\","
            + "\"timeoutMillis\":0,\"pivot\":false,"
            + "\"execution\":{\"method\":\"GET\",\"path\":\"/u\",\"jsonBody\":{\"a\":\"${b}\"}},"
            + "\"compensation\":{\"path\":\"/noop\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_missingCompensation_throwsException() {
    // Arrange
    String json =
        "{\"name\":\"t\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,\"steps\":["
            + "{\"name\":\"debit\",\"service\":\"svc\","
            + "\"timeoutMillis\":0,\"pivot\":false,"
            + "\"execution\":{\"path\":\"/debit\"}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }
}
