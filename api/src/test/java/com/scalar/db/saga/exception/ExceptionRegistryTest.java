package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExceptionRegistryTest {

  @Test
  void tryReconstruct_everyCode_roundTripsToItsOwnCode() {
    // Every enum constant must have a registry entry that preserves its code. Without this, a code
    // can be added to the enum and silently fail to reconstruct, or — as
    // PERSISTENCE_STORE_UNAVAILABLE did — be reconstructed as an exception hardcoding a *different*
    // code, so the caller sees DB-SAGA-20003 for a DB-SAGA-20002 failure.
    for (SagaErrorCode code : SagaErrorCode.values()) {
      Map<String, String> metadata = new LinkedHashMap<>();
      for (String key : code.schema().requiredKeys()) {
        metadata.put(key, "x");
      }

      SagaRuntimeException reconstructed = reconstruct(code.code(), metadata);

      assertThat(reconstructed.getErrorCode())
          .as("reconstructed code of %s", code.name())
          .isEqualTo(code);
    }
  }

  @Test
  void tryReconstruct_persistenceCodes_produceSagaPersistenceExceptionWithItsRetryVerdict() {
    // Arrange & Act — the transient code and one permanent code
    SagaRuntimeException transientFailure =
        reconstruct(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE.code(), Collections.emptyMap());
    SagaRuntimeException permanent =
        reconstruct(
            SagaErrorCode.PERSISTENCE_DESERIALIZATION_FAILED.code(), Collections.emptyMap());

    // Assert — a remote caller gets the same type and the same isRetryable() an embedded one does
    assertThat(transientFailure)
        .isInstanceOf(SagaPersistenceException.class)
        .extracting(e -> ((SagaPersistenceException) e).isRetryable())
        .isEqualTo(true);
    assertThat(permanent)
        .isInstanceOf(SagaPersistenceException.class)
        .extracting(e -> ((SagaPersistenceException) e).isRetryable())
        .isEqualTo(false);
  }

  @Test
  void tryReconstruct_knownCodeWithValidMetadata_producesTypedException() {
    // Arrange & Act
    SagaRuntimeException e =
        reconstruct(
            SagaErrorCode.SAGA_NOT_FOUND.code(), Collections.singletonMap("saga_id", "s-1"));

    // Assert — typed exception with the same code and metadata a server-side thrower produced
    assertThat(e).isInstanceOf(SagaNotFoundException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_NOT_FOUND);
    assertThat(e.getMetadata()).containsEntry("saga_id", "s-1");
  }

  @Test
  void tryReconstruct_invalidArgumentCode_keepsTheCodeDistinctFromInvalidRequest() {
    // Arrange — the two bad-input codes are deliberately separate: INVALID_REQUEST is the wire
    // message failing validation at the daemon edge, INVALID_ARGUMENT a caller value the engine
    // rejected. A remote client must not collapse one into the other.
    Map<String, String> argumentMeta = Collections.singletonMap("detail", "malformed page token");
    Map<String, String> requestMeta = Collections.singletonMap("detail", "'name' is required");

    // Act
    SagaRuntimeException argument =
        reconstruct(SagaErrorCode.INVALID_ARGUMENT.code(), argumentMeta);
    SagaRuntimeException request = reconstruct(SagaErrorCode.INVALID_REQUEST.code(), requestMeta);

    // Assert — each round-trips to its own code, carrying its own detail
    assertThat(argument.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_ARGUMENT);
    assertThat(argument.getMetadata()).containsEntry("detail", "malformed page token");
    assertThat(request.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_REQUEST);
    assertThat(request.getMetadata()).containsEntry("detail", "'name' is required");
  }

  @Test
  void tryReconstruct_definitionCode_producesSagaDefinitionException() {
    // Arrange
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_name", "transfer");
    metadata.put("detail", "duplicate step name 'debit'");

    // Act
    SagaRuntimeException e = reconstruct(SagaErrorCode.INVALID_DEFINITION.code(), metadata);

    // Assert
    assertThat(e).isInstanceOf(SagaDefinitionException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INVALID_DEFINITION);
  }

  @Test
  void tryReconstruct_preconditionCode_producesStatePreconditionException() {
    // Arrange
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("current_state", "RUNNING");
    metadata.put("requested_operation", "recover");

    // Act
    SagaRuntimeException e = reconstruct(SagaErrorCode.SAGA_WRONG_STATE.code(), metadata);

    // Assert
    assertThat(e).isInstanceOf(SagaStatePreconditionException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
  }

  @Test
  void tryReconstruct_authCode_producesAuthException() {
    // Arrange & Act
    SagaRuntimeException e =
        reconstruct(SagaErrorCode.UNAUTHENTICATED.code(), Collections.emptyMap());

    // Assert
    assertThat(e).isInstanceOf(SagaUnauthenticatedException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNAUTHENTICATED);
  }

  @Test
  void tryReconstruct_unavailableCode_producesSagaUnavailableException() {
    // Arrange & Act
    SagaRuntimeException e =
        reconstruct(SagaErrorCode.SERVICE_UNAVAILABLE.code(), Collections.emptyMap());

    // Assert
    assertThat(e).isInstanceOf(SagaUnavailableException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SERVICE_UNAVAILABLE);
  }

  @Test
  void tryReconstruct_persistenceStoreUnavailableCode_producesSagaPersistenceException() {
    // Arrange & Act — the code must survive, and so must the type: a transient store failure is
    // what the engine threw, so a remote caller sees SagaPersistenceException with isRetryable()
    // working, not a SagaUnavailableException substituted for it.
    SagaRuntimeException e =
        reconstruct(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE.code(), Collections.emptyMap());

    // Assert
    assertThat(e).isInstanceOf(SagaPersistenceException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
    assertThat(((SagaPersistenceException) e).isRetryable()).isTrue();
  }

  @Test
  void tryReconstruct_codelessCode_producesRawSagaRuntimeException() {
    // Arrange & Act — INTERNAL_ERROR has no dedicated exception type
    SagaRuntimeException e =
        reconstruct(SagaErrorCode.INTERNAL_ERROR.code(), Collections.emptyMap());

    // Assert — raw SagaRuntimeException carrying the code
    assertThat(e).isExactlyInstanceOf(SagaRuntimeException.class);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INTERNAL_ERROR);
  }

  @Test
  void tryReconstruct_knownCodeButWrongMetadataShape_returnsEmpty() {
    // Arrange & Act — SAGA_WRONG_STATE requires three keys; supply only one
    assertThat(
            ExceptionRegistry.tryReconstruct(
                SagaErrorCode.SAGA_WRONG_STATE.code(), Collections.singletonMap("saga_id", "s-1")))
        .isEmpty();
  }

  @Test
  void tryReconstruct_unknownCode_returnsEmpty() {
    // Arrange & Act — a code this client SDK doesn't know (server newer than client). Empty, not
    // UNRECOGNIZED_SERVER_ERROR: the caller may hold a better classification (the gRPC status),
    // and substituting a CLIENT_ERROR here is what used to flip a retryable failure into "upgrade
    // the SDK".
    assertThat(ExceptionRegistry.tryReconstruct("DB-SAGA-99999", Collections.emptyMap())).isEmpty();
  }

  @Test
  void tryReconstruct_knownCodeButMissingMetadata_returnsEmpty() {
    // Arrange & Act — SAGA_NOT_FOUND requires "saga_id" but the wire lacks it
    assertThat(
            ExceptionRegistry.tryReconstruct(
                SagaErrorCode.SAGA_NOT_FOUND.code(), Collections.emptyMap()))
        .isEmpty();
  }

  @Test
  void tryReconstruct_unrecognizedServerErrorCode_returnsIt() {
    // Arrange & Act — a genuine DB-SAGA-49999 sent by the server is a registered code, not a
    // degradation, so it round-trips with the server's own metadata.
    SagaRuntimeException e =
        ExceptionRegistry.tryReconstruct(
                SagaErrorCode.UNRECOGNIZED_SERVER_ERROR.code(),
                Collections.singletonMap("server_value", "SOME_ENUM_VALUE"))
            .orElseThrow(() -> new AssertionError("expected a reconstruction"));

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR);
    assertThat(e.getMetadata()).containsEntry("server_value", "SOME_ENUM_VALUE");
  }

  /**
   * Drives {@link ExceptionRegistry#tryReconstruct}, failing the test if the registry cannot
   * resolve the code — the callers here all expect a successful reconstruction.
   */
  private static SagaRuntimeException reconstruct(String wireCode, Map<String, String> metadata) {
    return ExceptionRegistry.tryReconstruct(wireCode, metadata)
        .orElseThrow(() -> new AssertionError("expected a reconstruction for " + wireCode));
  }
}
