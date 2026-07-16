package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaPersistenceExceptionTest {

  @Test
  void retryable_messageAndCauseGiven_setsBothAndIsRetryable() {
    // Arrange
    RuntimeException cause = new RuntimeException("db error");

    // Act
    SagaPersistenceException e = SagaPersistenceException.retryable("store write failed", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("store write failed");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isTrue();
  }

  @Test
  void nonRetryable_messageAndCauseGiven_setsBothAndIsNotRetryable() {
    // Arrange
    RuntimeException cause = new RuntimeException("bad json");

    // Act
    SagaPersistenceException e =
        SagaPersistenceException.nonRetryable("serialization failed", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("serialization failed");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isFalse();
  }

  @SuppressWarnings("NullAway")
  @Test
  void retryable_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaPersistenceException.retryable(null, new RuntimeException()))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void nonRetryable_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaPersistenceException.nonRetryable("message", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaPersistenceException.class);
  }
}
