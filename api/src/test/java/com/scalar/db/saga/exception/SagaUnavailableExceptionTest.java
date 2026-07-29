package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaUnavailableExceptionTest {

  @Test
  void constructor_noArgsGiven_carriesServiceUnavailableCode() {
    // Arrange & Act
    SagaUnavailableException e = new SagaUnavailableException();

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SERVICE_UNAVAILABLE);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getCause()).isNull();
  }

  @Test
  void constructor_causeGiven_carriesServiceUnavailableCodeAndCause() {
    // Arrange
    RuntimeException cause = new RuntimeException("connect failed");

    // Act
    SagaUnavailableException e = new SagaUnavailableException(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SERVICE_UNAVAILABLE);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaUnavailableException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void fromWire_serviceUnavailableGiven_carriesServiceUnavailableCode() {
    // Arrange & Act
    SagaUnavailableException e =
        SagaUnavailableException.fromWire(SagaErrorCode.SERVICE_UNAVAILABLE);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SERVICE_UNAVAILABLE);
    assertThat(e.getMetadata()).isEmpty();
  }

  @Test
  void fromWire_persistenceStoreUnavailableGiven_carriesPersistenceStoreUnavailableCode() {
    // Arrange & Act — fromWire is the only path that yields a non-default code; the constructors
    // always produce SERVICE_UNAVAILABLE.
    SagaUnavailableException e =
        SagaUnavailableException.fromWire(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
    assertThat(e.getMetadata()).isEmpty();
  }

  @Test
  void fromWire_unrelatedCodeGiven_throwsIllegalArgumentException() {
    // Arrange & Act & Assert — SagaUnavailableException does not carry SAGA_NOT_FOUND.
    assertThatThrownBy(() -> SagaUnavailableException.fromWire(SagaErrorCode.SAGA_NOT_FOUND))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void fromWire_nullCodeGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaUnavailableException.fromWire(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isSagaRuntimeException() {
    // Assert — remote-relevant exception, part of the saga exception family.
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaUnavailableException.class);
  }
}
