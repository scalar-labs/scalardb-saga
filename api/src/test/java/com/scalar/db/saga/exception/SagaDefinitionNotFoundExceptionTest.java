package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.SagaDefinitionId;
import org.junit.jupiter.api.Test;

class SagaDefinitionNotFoundExceptionTest {

  @Test
  void constructor_sagaNameGiven_setsFieldAndMessage() {
    // Arrange & Act
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException("order-saga");

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getVersion()).isNull();
    assertThat(e.getMessage()).isEqualTo("No saga definition registered for: order-saga");
  }

  @Test
  void constructor_sagaNameAndVersionGiven_setsFieldsAndMessage() {
    // Arrange & Act
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException("order-saga", "2.0");

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getVersion()).isEqualTo("2.0");
    assertThat(e.getMessage()).isEqualTo("No saga definition registered for: order-saga (v2.0)");
  }

  @Test
  void constructor_sagaDefinitionIdGiven_setsFieldsAndMessage() {
    // Arrange & Act
    SagaDefinitionId id = new SagaDefinitionId("order-saga", "2.0");
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException(id);

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getVersion()).isEqualTo("2.0");
    assertThat(e.getMessage()).isEqualTo("No saga definition registered for: order-saga (v2.0)");
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaDefinitionNotFoundException.class);
  }
}
