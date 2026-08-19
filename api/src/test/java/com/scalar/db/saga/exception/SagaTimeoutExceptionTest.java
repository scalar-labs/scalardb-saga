package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaTimeoutExceptionTest {

  @Test
  void awaitExpired_always_carriesAwaitTimeoutCode() {
    // Arrange & Act — the saga keeps running; only the caller's wait budget expired
    SagaTimeoutException e = SagaTimeoutException.awaitExpired();

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_AWAIT_TIMEOUT);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-40005: The wait for the saga to finish timed out");
    assertThat(e.getCause()).isNull();
  }

  @Test
  void requestTimedOut_causeGiven_carriesRequestTimeoutCodeAndCause() {
    // Arrange
    RuntimeException cause = new RuntimeException("deadline exceeded");

    // Act
    SagaTimeoutException e = SagaTimeoutException.requestTimedOut(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.REQUEST_TIMEOUT);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-40002: The request to the saga server timed out");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void requestTimedOut_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaTimeoutException.requestTimedOut(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaTimeoutException.class);
  }
}
