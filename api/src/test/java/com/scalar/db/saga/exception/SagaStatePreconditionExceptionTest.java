package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaStatePreconditionExceptionTest {

  @Test
  void constructor_sagaIdCodeAndMessageGiven_setsAllFields() {
    // Act
    SagaStatePreconditionException e =
        new SagaStatePreconditionException(
            "saga-1",
            SagaStatePreconditionException.Code.SAGA_WRONG_STATE,
            "Saga is RUNNING, expected ESCALATED");

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-1");
    assertThat(e.getCode()).isEqualTo(SagaStatePreconditionException.Code.SAGA_WRONG_STATE);
    assertThat(e.getMessage()).isEqualTo("Saga is RUNNING, expected ESCALATED");
  }

  @Test
  void constructor_parkedCodeGiven_setsParkedCode() {
    // Act
    SagaStatePreconditionException e =
        new SagaStatePreconditionException(
            "saga-2", SagaStatePreconditionException.Code.SAGA_PARKED, "parked awaiting callback");

    // Assert
    assertThat(e.getCode()).isEqualTo(SagaStatePreconditionException.Code.SAGA_PARKED);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaIdGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                new SagaStatePreconditionException(
                    null, SagaStatePreconditionException.Code.SAGA_WRONG_STATE, "m"))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCodeGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new SagaStatePreconditionException("saga-1", null, "m"))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaStatePreconditionException.class);
  }
}
