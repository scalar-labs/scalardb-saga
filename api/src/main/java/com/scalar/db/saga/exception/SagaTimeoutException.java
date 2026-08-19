package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a saga-level wait expires client-side. Carries one of two codes, chosen by the
 * factory, because the two situations demand opposite reactions:
 *
 * <ul>
 *   <li>{@link SagaErrorCode#REQUEST_TIMEOUT} — the request itself did not complete (a gRPC {@code
 *       DEADLINE_EXCEEDED}); retry it or raise the deadline. Construct via {@link
 *       #requestTimedOut(Throwable)}.
 *   <li>{@link SagaErrorCode#SAGA_AWAIT_TIMEOUT} — every request succeeded and the saga keeps
 *       running; only the caller's wait-for-terminal budget expired. Poll the saga by ID rather
 *       than re-sending anything. Construct via {@link #awaitExpired()}.
 * </ul>
 *
 * <p>This is an unchecked exception in a separate hierarchy from {@link StepTimeoutException}
 * because saga-level and step-level timeouts are semantically different.
 */
public class SagaTimeoutException extends SagaRuntimeException {

  private SagaTimeoutException(SagaErrorCode code) {
    super(code, ErrorMetadata.of());
  }

  private SagaTimeoutException(SagaErrorCode code, Throwable cause) {
    super(code, ErrorMetadata.of(), Objects.requireNonNull(cause, "cause must not be null"));
  }

  /** The request itself did not complete before its deadline; the transport status is the cause. */
  public static SagaTimeoutException requestTimedOut(Throwable cause) {
    return new SagaTimeoutException(SagaErrorCode.REQUEST_TIMEOUT, cause);
  }

  /**
   * The saga did not reach a terminal state within the client-side wait bound — the saga keeps
   * running and nothing failed, so the caller should poll it by ID, not re-send the request.
   */
  public static SagaTimeoutException awaitExpired() {
    return new SagaTimeoutException(SagaErrorCode.SAGA_AWAIT_TIMEOUT);
  }
}
