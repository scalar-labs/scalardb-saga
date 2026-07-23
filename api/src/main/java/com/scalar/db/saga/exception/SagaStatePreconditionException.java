package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when an operation is rejected because the saga is in a state that the operation does not
 * accept — a <b>static wrong-state</b> rejection, as opposed to a lost optimistic-concurrency race
 * (which is a {@link SagaConcurrentModificationException}). The concept is general and not limited
 * to the Admin API; that is simply where every current thrower happens to live.
 *
 * <p>Examples: {@code forceComplete} on a saga that is not {@code ESCALATED}; {@code recoverSaga}
 * on an {@code ESCALATED} saga; any mutation on a {@code WAITING} (async-parked) or terminal saga.
 * The daemon maps this to <b>HTTP 422 / gRPC FAILED_PRECONDITION</b>. The machine-readable {@link
 * #getCode() code} lets a client distinguish the reason without parsing the message.
 */
public class SagaStatePreconditionException extends SagaRuntimeException {

  /** Machine-readable reason for the precondition failure. */
  public enum Code {
    /** The saga is not in a status this operation accepts. */
    SAGA_WRONG_STATE,
    /** The saga is {@code WAITING} on an async callback; it resolves via callback or timeout. */
    SAGA_PARKED
  }

  private final String sagaId;
  private final Code code;

  public SagaStatePreconditionException(String sagaId, Code code, String message) {
    super(message);
    this.sagaId = Objects.requireNonNull(sagaId, "sagaId must not be null");
    this.code = Objects.requireNonNull(code, "code must not be null");
  }

  public String getSagaId() {
    return sagaId;
  }

  public Code getCode() {
    return code;
  }
}
