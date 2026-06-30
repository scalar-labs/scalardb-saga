package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaRuntimeExceptionTest {

  @Test
  void constructor_messageGiven_setsMessage() {
    // Arrange & Act
    SagaRuntimeException e = new SagaRuntimeException("boom");

    // Assert
    assertThat(e.getMessage()).isEqualTo("boom");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaRuntimeException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_messageAndCauseGiven_setsBoth() {
    // Arrange
    RuntimeException cause = new RuntimeException("inner");

    // Act
    SagaRuntimeException e = new SagaRuntimeException("boom", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("boom");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaRuntimeException("boom", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaRuntimeException.class);
  }

  @Test
  void classHierarchy_sagaExceptions_extendSagaRuntimeException() {
    // Assert — the unchecked saga exceptions are re-parented onto SagaRuntimeException so callers
    // can catch any saga failure uniformly.
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaPersistenceException.class);
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaNotFoundException.class);
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaTimeoutException.class);
  }
}
