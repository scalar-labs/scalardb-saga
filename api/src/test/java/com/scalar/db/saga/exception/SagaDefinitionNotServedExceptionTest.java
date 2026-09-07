package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SagaDefinitionNotServedExceptionTest {

  @Test
  void of_sagaNameGiven_carriesTheNameAndCode() {
    // Arrange & Act
    SagaDefinitionNotServedException e = SagaDefinitionNotServedException.of("order-saga");

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_NOT_SERVED);
    assertThat(e.getMetadata()).containsEntry("saga_name", "order-saga").hasSize(1);
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-10403: Saga definition is registered but not served by this daemon"
                + " [saga_name=order-saga]");
  }

  @Test
  @SuppressWarnings("NullAway") // deliberately passing null: the guard is what is under test
  void of_nullSagaNameGiven_throwsNullPointer() {
    // Act & Assert
    assertThatThrownBy(() -> SagaDefinitionNotServedException.of(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void tryReconstruct_wireMetadataGiven_returnsTheTypedException() {
    // The client SDK rebuilds this from the wire, so the registry entry has to round-trip it.
    // Arrange & Act
    Optional<SagaRuntimeException> reconstructed =
        ExceptionRegistry.tryReconstruct(
            SagaErrorCode.SAGA_DEFINITION_NOT_SERVED.code(), Map.of("saga_name", "order-saga"));

    // Assert
    assertThat(reconstructed)
        .get()
        .isInstanceOf(SagaDefinitionNotServedException.class)
        .extracting(e -> ((SagaDefinitionNotServedException) e).getSagaName())
        .isEqualTo("order-saga");
  }

  @Test
  void tryReconstruct_declaredKeyMissing_degradesToEmpty() {
    // Protocol drift: a declared key absent from the wire is not something to guess at, so the
    // reconstruction degrades to untyped rather than inventing a saga name.
    // Act
    Optional<SagaRuntimeException> reconstructed =
        ExceptionRegistry.tryReconstruct(SagaErrorCode.SAGA_DEFINITION_NOT_SERVED.code(), Map.of());

    // Assert
    assertThat(reconstructed).isEmpty();
  }
}
