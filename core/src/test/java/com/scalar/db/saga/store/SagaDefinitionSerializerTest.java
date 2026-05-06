package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.exception.SagaPersistenceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SagaDefinitionSerializerTest {

  private SagaDefinitionSerializer serializer;

  @BeforeEach
  void setUp() {
    serializer = new SagaDefinitionSerializer(new ObjectMapper());
  }

  @Test
  void serializeAndDeserialize_withAllFields_roundTripsCorrectly() {
    // Arrange
    SagaDefinition original =
        SagaDefinition.newBuilder("test-saga", SagaMode.SAGA)
            .version("2.0")
            .recoveryStrategy(RecoveryStrategy.BACKWARD)
            .timeoutMillis(30000)
            .defaultRetryPolicy(
                RetryPolicy.newBuilder()
                    .maxAttempts(5)
                    .initialIntervalMillis(500)
                    .backoffMultiplier(2.0)
                    .maxIntervalMillis(10000)
                    .build())
            .step("step1", "com.example.Step1")
            .timeoutMillis(5000)
            .retryPolicy(
                RetryPolicy.newBuilder()
                    .maxAttempts(3)
                    .initialIntervalMillis(1000)
                    .backoffMultiplier(1.5)
                    .maxIntervalMillis(5000)
                    .build())
            .add()
            .step("step2", "com.example.Step2")
            .timeoutMillis(10000)
            .add()
            .build();

    // Act
    String json = serializer.serialize(original);
    SagaDefinition deserialized = serializer.deserialize(json);

    // Assert
    assertThat(deserialized).isEqualTo(original);
  }

  @Test
  void serializeAndDeserialize_withNoRetryPolicies_roundTripsCorrectly() {
    // Arrange
    SagaDefinition original =
        SagaDefinition.newBuilder("simple-saga", SagaMode.SAGA)
            .step("step1", "com.example.Step1")
            .add()
            .build();

    // Act
    String json = serializer.serialize(original);
    SagaDefinition deserialized = serializer.deserialize(json);

    // Assert
    assertThat(deserialized).isEqualTo(original);
  }

  @Test
  void deserialize_missingRootField_throwsSagaPersistenceException() {
    // Arrange
    String json = "{\"name\":\"test\",\"version\":\"1.0\"}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_missingStepField_throwsSagaPersistenceException() {
    // Arrange
    String json =
        "{\"name\":\"test\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,"
            + "\"steps\":[{\"name\":\"s1\"}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_invalidEnumValue_throwsSagaPersistenceException() {
    // Arrange
    String json =
        "{\"name\":\"test\",\"mode\":\"INVALID\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,"
            + "\"steps\":[]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_missingRetryPolicyField_throwsSagaPersistenceException() {
    // Arrange — retryPolicy node is present but missing required fields
    String json =
        "{\"name\":\"test\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,"
            + "\"steps\":[{\"name\":\"s1\",\"stepClass\":\"com.example.S1\","
            + "\"timeoutMillis\":1000,\"pivot\":false,"
            + "\"retryPolicy\":{\"maxAttempts\":3}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_malformedJson_throwsSagaPersistenceException() {
    // Arrange
    String json = "not valid json";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void deserialize_invalidRetryPolicyValues_throwsSagaPersistenceException() {
    // Arrange — maxAttempts=0 is invalid, triggers IllegalArgumentException from builder
    String json =
        "{\"name\":\"test\",\"mode\":\"SAGA\",\"version\":\"1.0\","
            + "\"recoveryStrategy\":\"BACKWARD\",\"timeoutMillis\":0,"
            + "\"steps\":[{\"name\":\"s1\",\"stepClass\":\"com.example.S1\","
            + "\"timeoutMillis\":1000,\"pivot\":false,"
            + "\"retryPolicy\":{\"maxAttempts\":0,\"initialIntervalMillis\":100,"
            + "\"backoffMultiplier\":2.0,\"maxIntervalMillis\":1000}}]}";

    // Act & Assert
    assertThatThrownBy(() -> serializer.deserialize(json))
        .isInstanceOf(SagaPersistenceException.class);
  }
}
