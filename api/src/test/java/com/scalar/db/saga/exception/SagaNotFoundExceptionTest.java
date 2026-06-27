package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaNotFoundExceptionTest {

  @Test
  void constructor_sagaIdGiven_setsFieldAndMessage() {
    // Arrange & Act
    SagaNotFoundException e = new SagaNotFoundException("saga-123");

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-123");
    assertThat(e.getMessage()).isEqualTo("Saga not found: saga-123");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaNotFoundException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaNotFoundException.class);
  }
}
