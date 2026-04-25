package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaPersistenceExceptionTest {

  @Test
  void constructor_messageAndCauseGiven_setsBoth() {
    // Arrange
    RuntimeException cause = new RuntimeException("db error");

    // Act
    SagaPersistenceException e = new SagaPersistenceException("store write failed", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("store write failed");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaPersistenceException(null, new RuntimeException()))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaPersistenceException("message", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaPersistenceException.class);
  }
}
