package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StepCompensationExceptionTest {

  @Test
  void constructor_messageOnlyGiven_setsDefaultFields() {
    // Arrange & Act
    StepCompensationException e = new StepCompensationException("failed");

    // Assert
    assertThat(e.getMessage()).isEqualTo("failed");
    assertThat(e.getStepName()).isNull();
    assertThat(e.getStepIndex()).isEqualTo(-1);
  }

  @Test
  void constructor_causeOnlyGiven_setsDefaultFields() {
    // Arrange
    RuntimeException cause = new RuntimeException("io error");

    // Act
    StepCompensationException e = new StepCompensationException(cause);

    // Assert
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.getStepName()).isNull();
    assertThat(e.getStepIndex()).isEqualTo(-1);
  }

  @Test
  void constructor_stepInfoGiven_setsAllFields() {
    // Arrange
    RuntimeException cause = new RuntimeException("db error");

    // Act
    StepCompensationException e = new StepCompensationException("refund", 2, cause);

    // Assert
    assertThat(e.getStepName()).isEqualTo("refund");
    assertThat(e.getStepIndex()).isEqualTo(2);
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.getMessage()).contains("refund").contains("2");
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert — compensation exceptions are unchecked
    assertThat(RuntimeException.class).isAssignableFrom(StepCompensationException.class);
  }
}
