package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StepCompensationExceptionTest {

  @Test
  void constructor_messageOnlyGiven_setsDefaultsAndNullCode() {
    // Arrange & Act
    StepCompensationException e = new StepCompensationException("failed");

    // Assert — user-thrown form leaves errorCode null and metadata empty
    assertThat(e.getMessage()).isEqualTo("failed");
    assertThat(e.getStepName()).isNull();
    assertThat(e.getStepIndex()).isEqualTo(-1);
    assertThat(e.getErrorCode()).isNull();
    assertThat(e.getMetadata()).isEmpty();
  }

  @Test
  void constructor_causeOnlyGiven_setsDefaultsAndNullCode() {
    // Arrange
    RuntimeException cause = new RuntimeException("io error");

    // Act
    StepCompensationException e = new StepCompensationException(cause);

    // Assert
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.getStepName()).isNull();
    assertThat(e.getStepIndex()).isEqualTo(-1);
    assertThat(e.getErrorCode()).isNull();
    assertThat(e.getMetadata()).isEmpty();
  }

  @Test
  void constructor_stepInfoGiven_setsCompensationFailedCodeAndMetadata() {
    // Arrange
    RuntimeException cause = new RuntimeException("db error");

    // Act
    StepCompensationException e = new StepCompensationException("refund", 2, cause);

    // Assert
    assertThat(e.getStepName()).isEqualTo("refund");
    assertThat(e.getStepIndex()).isEqualTo(2);
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.COMPENSATION_FAILED);
    assertThat(e.getMetadata())
        .containsEntry("step_name", "refund")
        .containsEntry("step_index", "2")
        .hasSize(2);
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-30005: Compensation of step failed [step_name=refund, step_index=2]");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepCompensationException((String) null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepCompensationException((Throwable) null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullStepNameGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepCompensationException(null, 0, new RuntimeException()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new StepCompensationException("step", -1, new RuntimeException()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert — compensation exceptions are unchecked and outside the SagaRuntimeException tree
    assertThat(RuntimeException.class).isAssignableFrom(StepCompensationException.class);
  }
}
