package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class SagaNotFoundExceptionTest {

  @Test
  void constructor_sagaIdGiven_setsFieldAndCodeAndMetadataAndMessage() {
    // Arrange & Act
    SagaNotFoundException e = new SagaNotFoundException("saga-123");

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-123");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_NOT_FOUND);
    assertThat(e.getMetadata()).containsEntry("saga_id", "saga-123").hasSize(1);
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-10201: Saga not found [saga_id=saga-123]");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaNotFoundException(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void fromWire_metadataWithSagaIdGiven_reconstructsTheSameCodeAndId() {
    // Act
    SagaNotFoundException e =
        SagaNotFoundException.fromWire(Collections.singletonMap("saga_id", "saga-123"));

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-123");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_NOT_FOUND);
    assertThat(e.getMetadata()).containsEntry("saga_id", "saga-123").hasSize(1);
  }

  @Test
  void fromWire_metadataMissingSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert — the registry catches this and degrades, so a peer that omits the
    // id never yields a typed exception with none
    assertThatThrownBy(() -> SagaNotFoundException.fromWire(Collections.emptyMap()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaNotFoundException.class);
  }
}
