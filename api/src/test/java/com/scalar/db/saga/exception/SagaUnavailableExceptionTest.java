package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaUnavailableExceptionTest {

  @Test
  void constructor_messageGiven_setsMessage() {
    // Arrange & Act
    SagaUnavailableException e = new SagaUnavailableException("service unavailable");

    // Assert
    assertThat(e.getMessage()).isEqualTo("service unavailable");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaUnavailableException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_messageAndCauseGiven_setsBoth() {
    // Arrange
    RuntimeException cause = new RuntimeException("connect failed");

    // Act
    SagaUnavailableException e = new SagaUnavailableException("service unavailable", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("service unavailable");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaUnavailableException("service unavailable", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isSagaRuntimeException() {
    // Assert — remote-relevant exception, part of the saga exception family.
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaUnavailableException.class);
  }
}
