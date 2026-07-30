package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
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

  @Test
  void fromWire_persistenceCodeGiven_reconstructsCauseFreeWithItsRetryVerdict() {
    // Act — no cause crosses the wire, so the reconstructed exception carries none
    SagaPersistenceException e =
        SagaPersistenceException.fromWire(
            SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE, Collections.emptyMap());

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
    assertThat(e.isRetryable()).isTrue();
    assertThat(e.getCause()).isNull();
  }

  @Test
  void fromWire_unrelatedCodeGiven_throwsIllegalState() {
    // Only ExceptionRegistry calls fromWire, so an unrelated code is a registry wiring bug rather
    // than caller error — and IllegalStateException escapes the registry's IllegalArgumentException
    // | NullPointerException catch, so it is not misreported as UNRECOGNIZED_SERVER_ERROR.
    assertThatThrownBy(
            () ->
                SagaPersistenceException.fromWire(
                    SagaErrorCode.SAGA_NOT_FOUND, Collections.emptyMap()))
        .isInstanceOf(IllegalStateException.class);
  }
}
