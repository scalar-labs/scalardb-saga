package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaUnauthenticatedExceptionTest {

  @Test
  void constructor_noArgsGiven_carriesAuthUnauthenticatedCode() {
    // Arrange & Act
    SagaUnauthenticatedException e = new SagaUnauthenticatedException();

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.AUTH_UNAUTHENTICATED);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getCause()).isNull();
  }

  @Test
  void constructor_causeGiven_carriesCodeAndCause() {
    // Arrange
    RuntimeException cause = new RuntimeException("underlying");

    // Act
    SagaUnauthenticatedException e = new SagaUnauthenticatedException(cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.AUTH_UNAUTHENTICATED);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaUnauthenticatedException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isSagaRuntimeException() {
    // Assert
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaUnauthenticatedException.class);
  }
}
