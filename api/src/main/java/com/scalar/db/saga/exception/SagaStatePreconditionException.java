package com.scalar.db.saga.exception;

import java.util.Map;
import java.util.Objects;

/**
 * Thrown when an operation is rejected because the saga is in a state that the operation does not
 * accept — a <b>static wrong-state</b> rejection, as opposed to a lost optimistic-concurrency race
 * (which is a {@link SagaConcurrentModificationException}). The concept is general and not limited
 * to the Admin API; that is simply where every current thrower happens to live.
 *
 * <p>Examples: {@code forceComplete} on a saga that is not {@code ESCALATED}; {@code recoverSaga}
 * on an {@code ESCALATED} saga; any mutation on a {@code WAITING} (async-parked) or terminal saga.
 * The daemon maps this to <b>HTTP 422 / gRPC FAILED_PRECONDITION</b>.
 *
 * <p>The machine-readable {@link #getErrorCode()} distinguishes {@link
 * SagaErrorCode#SAGA_WRONG_STATE} from {@link SagaErrorCode#SAGA_PARKED} so a client can switch on
 * the failure without parsing the message. Use the static factories {@link #wrongState} or {@link
 * #parked} to construct.
 */
public class SagaStatePreconditionException extends SagaRuntimeException {

  private final String sagaId;

  private SagaStatePreconditionException(SagaErrorCode code, Map<String, String> metadata) {
    super(code, metadata);
    this.sagaId =
        Objects.requireNonNull(metadata.get("saga_id"), "metadata.saga_id must not be null");
  }

  /**
   * The saga is in a status the operation does not accept — carries {@link
   * SagaErrorCode#SAGA_WRONG_STATE}.
   *
   * @param sagaId the saga instance id
   * @param currentState the saga's current status name (e.g. {@code "RUNNING"})
   * @param requestedOperation a short operation label (e.g. {@code "force-complete"})
   */
  public static SagaStatePreconditionException wrongState(
      String sagaId, String currentState, String requestedOperation) {
    return new SagaStatePreconditionException(
        SagaErrorCode.SAGA_WRONG_STATE,
        ErrorMetadata.of(
            "saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null"),
            "current_state", Objects.requireNonNull(currentState, "currentState must not be null"),
            "requested_operation",
                Objects.requireNonNull(requestedOperation, "requestedOperation must not be null")));
  }

  /**
   * The saga is {@code WAITING} on an async callback and resolves via callback or timeout — carries
   * {@link SagaErrorCode#SAGA_PARKED}.
   */
  public static SagaStatePreconditionException parked(String sagaId) {
    return new SagaStatePreconditionException(
        SagaErrorCode.SAGA_PARKED,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
  }

  /**
   * Reconstructs the exception from a wire-received {@link SagaErrorCode} and metadata, for use by
   * the client SDK when it decodes an {@code ErrorInfo} from the daemon. The code must be one this
   * exception represents ({@link SagaErrorCode#SAGA_WRONG_STATE} or {@link
   * SagaErrorCode#SAGA_PARKED}).
   *
   * <p>Package-private: {@link ExceptionRegistry} is the only caller, so a code this type does not
   * represent is a registry wiring bug rather than caller error, and throws {@link
   * IllegalStateException}. That is deliberately outside the {@code IllegalArgumentException |
   * NullPointerException} the registry catches for genuine wire-metadata drift, so a wiring bug
   * surfaces as itself instead of as {@code UNRECOGNIZED_SERVER_ERROR}.
   */
  static SagaStatePreconditionException fromWire(SagaErrorCode code, Map<String, String> metadata) {
    if (code != SagaErrorCode.SAGA_WRONG_STATE && code != SagaErrorCode.SAGA_PARKED) {
      throw new IllegalStateException("SagaStatePreconditionException does not carry code " + code);
    }
    return new SagaStatePreconditionException(code, metadata);
  }

  public String getSagaId() {
    return sagaId;
  }
}
