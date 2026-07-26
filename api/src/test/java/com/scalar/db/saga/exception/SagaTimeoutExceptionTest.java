package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaTimeoutExceptionTest {

  @Test
  void constructor_noArgsGiven_setsCodeAndFixedMessage() {
    // Arrange & Act
    SagaTimeoutException e = new SagaTimeoutException();

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.REQUEST_TIMEOUT);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-40002: The request to the saga server timed out");
    assertThat(e.getCause()).isNull();
  }

  @Test
  void constructor_causeGiven_setsCauseAndFixedMessage() {
    // Arrange
    RuntimeException cause = new RuntimeException("interrupted");

    // Act
    SagaTimeoutException e = new SagaTimeoutException(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.REQUEST_TIMEOUT);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-40002: The request to the saga server timed out");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaTimeoutException((Throwable) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaTimeoutException.class);
  }
}
