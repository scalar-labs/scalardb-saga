package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StepExecutionExceptionTest {

  @Test
  void constructor_messageOnlyGiven_defaultsToRetryable() {
    // Arrange & Act
    StepExecutionException e = new StepExecutionException("timeout");

    // Assert
    assertThat(e.getMessage()).isEqualTo("timeout");
    assertThat(e.isRetryable()).isTrue();
  }

  @Test
  void constructor_causeOnlyGiven_defaultsToRetryable() {
    // Arrange
    RuntimeException cause = new RuntimeException("network error");

    // Act
    StepExecutionException e = new StepExecutionException(cause);

    // Assert
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isTrue();
  }

  @Test
  void constructor_messageAndRetryableGiven_setsRetryableFlag() {
    // Arrange & Act
    StepExecutionException e = new StepExecutionException("bad request", false);

    // Assert
    assertThat(e.getMessage()).isEqualTo("bad request");
    assertThat(e.isRetryable()).isFalse();
  }

  @Test
  void constructor_causeAndRetryableGiven_setsRetryableFlag() {
    // Arrange
    RuntimeException cause = new RuntimeException("permanent failure");

    // Act
    StepExecutionException e = new StepExecutionException(cause, false);

    // Assert
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isFalse();
  }

  @Test
  void constructor_messageCauseAndRetryableGiven_setsAllFields() {
    // Arrange
    RuntimeException cause = new RuntimeException("underlying");

    // Act
    StepExecutionException e = new StepExecutionException("wrapper", cause, true);

    // Assert
    assertThat(e.getMessage()).isEqualTo("wrapper");
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.isRetryable()).isTrue();
  }

  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepExecutionException((String) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepExecutionException((Throwable) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isCheckedExceptionNotRuntime() {
    // Assert — StepExecutionException extends Exception, not RuntimeException
    assertThat(Exception.class.isAssignableFrom(StepExecutionException.class)).isTrue();
    assertThat(RuntimeException.class.isAssignableFrom(StepExecutionException.class)).isFalse();
  }
}
