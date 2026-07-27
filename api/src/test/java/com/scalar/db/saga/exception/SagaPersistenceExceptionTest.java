package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaPersistenceExceptionTest {

  @Test
  void storeUnavailable_causeGiven_setsRetryableCodeAndFixedMessage() {
    // Arrange
    RuntimeException cause = new RuntimeException("db error");

    // Act
    SagaPersistenceException e = SagaPersistenceException.storeUnavailable(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-20002: Underlying store is temporarily unavailable");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isTrue();
  }

  @Test
  void serializationFailed_causeGiven_setsNonRetryableCodeAndFixedMessage() {
    // Arrange
    RuntimeException cause = new RuntimeException("bad json");

    // Act
    SagaPersistenceException e = SagaPersistenceException.serializationFailed(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-30001: Failed to serialize event payload");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isFalse();
  }

  @Test
  void deserializationFailed_causeGiven_setsNonRetryableCodeAndFixedMessage() {
    // Arrange
    RuntimeException cause = new RuntimeException("parse error");

    // Act
    SagaPersistenceException e = SagaPersistenceException.deserializationFailed(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED);
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-30002: Failed to deserialize event payload or definition");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isFalse();
  }

  @SuppressWarnings("NullAway")
  @Test
  void storeUnavailable_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaPersistenceException.storeUnavailable(null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void serializationFailed_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaPersistenceException.serializationFailed(null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void deserializationFailed_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaPersistenceException.deserializationFailed(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaPersistenceException.class);
  }
}
