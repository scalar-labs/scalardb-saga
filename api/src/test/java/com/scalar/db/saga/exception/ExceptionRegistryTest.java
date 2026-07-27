package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExceptionRegistryTest {

  @Test
  void reconstruct_knownCodeWithValidMetadata_producesTypedException() {
    // Arrange & Act
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(
            SagaErrorCode.SAGA_NOT_FOUND.code(), Collections.singletonMap("saga_id", "s-1"));

    // Assert — typed exception with the same code and metadata a server-side thrower produced
    assertThat(e).isInstanceOf(SagaNotFoundException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_NOT_FOUND);
    assertThat(e.getMetadata()).containsEntry("saga_id", "s-1");
  }

  @Test
  void reconstruct_definitionCode_producesSagaDefinitionException() {
    // Arrange
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_name", "transfer");
    metadata.put("detail", "duplicate step name 'debit'");

    // Act
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(SagaErrorCode.INVALID_DEFINITION.code(), metadata);

    // Assert
    assertThat(e).isInstanceOf(SagaDefinitionException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_DEFINITION);
  }

  @Test
  void reconstruct_preconditionCode_producesStatePreconditionException() {
    // Arrange
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("current_state", "RUNNING");
    metadata.put("requested_operation", "recover");

    // Act
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(SagaErrorCode.SAGA_WRONG_STATE.code(), metadata);

    // Assert
    assertThat(e).isInstanceOf(SagaStatePreconditionException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
  }

  @Test
  void reconstruct_authCode_producesAuthException() {
    // Arrange & Act
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(SagaErrorCode.UNAUTHENTICATED.code(), Collections.emptyMap());

    // Assert
    assertThat(e).isInstanceOf(SagaUnauthenticatedException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNAUTHENTICATED);
  }

  @Test
  void reconstruct_unavailableCode_producesSagaUnavailableException() {
    // Arrange & Act
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(
            SagaErrorCode.SERVICE_UNAVAILABLE.code(), Collections.emptyMap());

    // Assert
    assertThat(e).isInstanceOf(SagaUnavailableException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SERVICE_UNAVAILABLE);
  }

  @Test
  void reconstruct_codelessCode_producesRawSagaRuntimeException() {
    // Arrange & Act — INTERNAL_ERROR has no dedicated exception type
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(SagaErrorCode.INTERNAL_ERROR.code(), Collections.emptyMap());

    // Assert — raw SagaRuntimeException carrying the code
    assertThat(e).isExactlyInstanceOf(SagaRuntimeException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INTERNAL_ERROR);
  }

  @Test
  void reconstruct_unknownCode_degradesToUnrecognizedServerError() {
    // Arrange & Act — a code this client SDK doesn't know
    SagaRuntimeException e = ExceptionRegistry.reconstruct("DB-SAGA-99999", Collections.emptyMap());

    // Assert — degrades to UNRECOGNIZED_SERVER_ERROR carrying the raw wire code
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR);
    assertThat(e.getMetadata()).containsEntry("server_value", "DB-SAGA-99999");
  }

  @Test
  void reconstruct_knownCodeButMissingMetadata_degradesToUnrecognizedServerError() {
    // Arrange & Act — SAGA_NOT_FOUND requires "saga_id" but the wire lacks it
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(SagaErrorCode.SAGA_NOT_FOUND.code(), Collections.emptyMap());

    // Assert — protocol drift; degrade rather than shim a partial exception
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR);
    assertThat(e.getMetadata()).containsEntry("server_value", SagaErrorCode.SAGA_NOT_FOUND.code());
  }

  @Test
  void reconstruct_knownCodeButWrongMetadataShape_degradesToUnrecognizedServerError() {
    // Arrange & Act — SAGA_WRONG_STATE requires three keys; supply only one
    SagaRuntimeException e =
        ExceptionRegistry.reconstruct(
            SagaErrorCode.SAGA_WRONG_STATE.code(), Collections.singletonMap("saga_id", "s-1"));

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR);
  }
}
