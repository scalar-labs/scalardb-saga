package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaStatePreconditionExceptionTest {

  @Test
  void wrongState_allFieldsGiven_setsCodeAndMetadataAndMessage() {
    // Arrange & Act
    SagaStatePreconditionException e =
        SagaStatePreconditionException.wrongState("saga-1", "RUNNING", "force-complete");

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-1");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
    assertThat(e.getMetadata())
        .containsEntry("saga_id", "saga-1")
        .containsEntry("current_state", "RUNNING")
        .containsEntry("requested_operation", "force-complete")
        .hasSize(3);
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-11200: Operation not allowed in the saga's current state"
                + " [saga_id=saga-1, current_state=RUNNING, requested_operation=force-complete]");
  }

  @Test
  void parked_sagaIdGiven_setsCodeAndMetadataAndMessage() {
    // Arrange & Act
    SagaStatePreconditionException e = SagaStatePreconditionException.parked("saga-2");

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-2");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_PARKED);
    assertThat(e.getMetadata()).containsEntry("saga_id", "saga-2").hasSize(1);
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-11201: Saga is parked and cannot be resumed automatically [saga_id=saga-2]");
  }

  @Test
  void fromWire_wrongStateCodeAndFullMetadataGiven_reconstructs() {
    // Arrange — the client SDK receives the code + metadata over the wire and reconstructs
    Map<String, String> wire = new LinkedHashMap<>();
    wire.put("saga_id", "s-x");
    wire.put("current_state", "COMPENSATING");
    wire.put("requested_operation", "resume");

    // Act
    SagaStatePreconditionException e =
        SagaStatePreconditionException.fromWire(SagaErrorCode.SAGA_WRONG_STATE, wire);

    // Assert
    assertThat(e.getSagaId()).isEqualTo("s-x");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
    assertThat(e.getMetadata()).hasSize(3);
  }

  @Test
  void fromWire_parkedCodeAndSagaIdMetadataGiven_reconstructs() {
    // Arrange & Act
    SagaStatePreconditionException e =
        SagaStatePreconditionException.fromWire(
            SagaErrorCode.SAGA_PARKED, Collections.singletonMap("saga_id", "s-y"));

    // Assert
    assertThat(e.getSagaId()).isEqualTo("s-y");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_PARKED);
  }

  @Test
  void fromWire_unrelatedCodeGiven_throwsIllegalArgument() {
    // Arrange & Act & Assert — guards against a wire misroute of an unrelated code
    assertThatThrownBy(
            () ->
                SagaStatePreconditionException.fromWire(
                    SagaErrorCode.SAGA_NOT_FOUND, Collections.singletonMap("saga_id", "s-z")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void wrongState_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(
            () -> SagaStatePreconditionException.wrongState(null, "RUNNING", "force-complete"))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void parked_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> SagaStatePreconditionException.parked(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaStatePreconditionException.class);
  }
}
