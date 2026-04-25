package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaTimeoutExceptionTest {

  @Test
  void constructor_messageGiven_setsMessage() {
    // Arrange & Act
    SagaTimeoutException e = new SagaTimeoutException("saga deadline exceeded");

    // Assert
    assertThat(e.getMessage()).isEqualTo("saga deadline exceeded");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaTimeoutException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_messageAndCauseGiven_setsBoth() {
    // Arrange
    RuntimeException cause = new RuntimeException("interrupted");

    // Act
    SagaTimeoutException e = new SagaTimeoutException("saga deadline exceeded", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("saga deadline exceeded");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaTimeoutException("timeout", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaTimeoutException.class);
  }
}
