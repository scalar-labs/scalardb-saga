package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StepTimeoutExceptionTest {

  @Test
  void constructor_messageGiven_setsMessageAndNonRetryable() {
    // Arrange & Act
    StepTimeoutException e = new StepTimeoutException("step timed out after 5s");

    // Assert
    assertThat(e.getMessage()).isEqualTo("step timed out after 5s");
    assertThat(e.isRetryable()).isFalse();
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepTimeoutException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_messageAndCauseGiven_setsBothAndNonRetryable() {
    // Arrange
    RuntimeException cause = new RuntimeException("underlying timeout");

    // Act
    StepTimeoutException e = new StepTimeoutException("step timed out", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("step timed out");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isFalse();
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepTimeoutException("timeout", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_extendsStepExecutionException() {
    // Assert
    assertThat(StepExecutionException.class).isAssignableFrom(StepTimeoutException.class);
  }
}
