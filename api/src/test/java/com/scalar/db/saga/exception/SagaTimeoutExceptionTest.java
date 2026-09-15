package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import org.junit.jupiter.api.Test;

class SagaTimeoutExceptionTest {

  @Test
  void awaitExpired_sagaIdGiven_carriesAwaitTimeoutCodeAndThatId() {
    // Arrange & Act — the saga keeps running; only the caller's wait budget expired
    SagaTimeoutException e = SagaTimeoutException.awaitExpired("s-1");

    // Assert — the id is on the exception, in its metadata, and in the rendered message: the code's
    // remediation says to poll the saga by its ID, so the exception has to carry one.
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_AWAIT_TIMEOUT);
    assertThat(e.getSagaId()).isEqualTo("s-1");
    assertThat(e.getMetadata()).containsExactly(entry("saga_id", "s-1"));
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-40005: The wait for the saga to finish timed out [saga_id=s-1]");
    assertThat(e.getCause()).isNull();
  }

  @SuppressWarnings("NullAway")
  @Test
  void awaitExpired_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaTimeoutException.awaitExpired(null))
        .isInstanceOf(NullPointerException.class);
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
    // No saga in view on this path: it is mapped from a bare transport status, so there is no id to
    // hand back and the accessor says so rather than inventing one.
    assertThat(e.getSagaId()).isNull();
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
