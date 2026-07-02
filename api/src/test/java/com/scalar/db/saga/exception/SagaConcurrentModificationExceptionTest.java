package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaConcurrentModificationExceptionTest {

  @Test
  void constructor_sagaIdGiven_setsFieldAndMessage() {
    // Arrange & Act
    SagaConcurrentModificationException e = new SagaConcurrentModificationException("saga-456");

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-456");
    assertThat(e.getMessage()).isEqualTo("Saga is being processed by another replica: saga-456");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaConcurrentModificationException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_sagaIdAndCauseGiven_setsBoth() {
    // Arrange
    RuntimeException cause = new RuntimeException("version mismatch");

    // Act
    SagaConcurrentModificationException e =
        new SagaConcurrentModificationException("saga-789", cause);

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-789");
    assertThat(e.getMessage()).isEqualTo("Saga is being processed by another replica: saga-789");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaConcurrentModificationException("saga-456", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaConcurrentModificationException.class);
  }
}
