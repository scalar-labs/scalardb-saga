package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SagaDefinitionExceptionTest {

  @Test
  void constructor_messageOnlyGiven_setsMessage() {
    // Arrange & Act
    SagaDefinitionException e = new SagaDefinitionException("invalid definition");

    // Assert
    assertThat(e.getMessage()).isEqualTo("invalid definition");
    assertThat(e.getCause()).isNull();
  }

  @Test
  void constructor_messageAndCauseGiven_setsBoth() {
    // Arrange
    RuntimeException cause = new RuntimeException("root cause");

    // Act
    SagaDefinitionException e = new SagaDefinitionException("invalid definition", cause);

    // Assert
    assertThat(e.getMessage()).isEqualTo("invalid definition");
    assertThat(e.getCause()).isSameAs(cause);
  }

  @Test
  void constructor_nullMessageGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_nullMessageWithCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionException(null, new RuntimeException()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionException("message", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaDefinitionException.class);
  }
}
