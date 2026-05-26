package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinitionId;
import org.junit.jupiter.api.Test;

class SagaDefinitionNotFoundExceptionTest {

  @Test
  void constructor_sagaNameGiven_setsFieldAndMessage() {
    // Arrange & Act
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException("order-saga");

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getMessage()).isEqualTo("No saga definition registered for: order-saga");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaNameGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionNotFoundException((String) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_sagaDefinitionIdGiven_setsFieldAndMessage() {
    // Arrange & Act
    SagaDefinitionId id = new SagaDefinitionId("order-saga", "2.0");
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException(id);

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getMessage()).isEqualTo("No saga definition registered for: order-saga (v2.0)");
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaDefinitionNotFoundException.class);
  }
}
