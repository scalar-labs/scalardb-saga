package com.scalar.db.saga.exception;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

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
 *       than re-sending anything. Construct via {@link #awaitExpired(String)}.
 * </ul>
 *
 * <p>Only the await flavour carries a saga ID, and it always does: a blocking {@code start} that
 * mints the ID itself has not returned it yet, so this exception is the caller's only copy of the
 * handle to work that is still running. A request timeout is mapped from a bare transport status
 * with no saga in view, so {@link #getSagaId()} is null there.
 *
 * <p>This is an unchecked exception in a separate hierarchy from {@link StepTimeoutException}
 * because saga-level and step-level timeouts are semantically different.
 */
public class SagaTimeoutException extends SagaRuntimeException {

  private final @Nullable String sagaId;

  private SagaTimeoutException(String sagaId) {
    super(
        SagaErrorCode.SAGA_AWAIT_TIMEOUT,
        ErrorMetadata.of("saga_id", Objects.requireNonNull(sagaId, "sagaId must not be null")));
    this.sagaId = sagaId;
  }

  private SagaTimeoutException(SagaErrorCode code, Throwable cause) {
    super(code, ErrorMetadata.of(), Objects.requireNonNull(cause, "cause must not be null"));
    this.sagaId = null;
  }

  /** The request itself did not complete before its deadline; the transport status is the cause. */
  public static SagaTimeoutException requestTimedOut(Throwable cause) {
    return new SagaTimeoutException(SagaErrorCode.REQUEST_TIMEOUT, cause);
  }

  /**
   * The saga did not reach a terminal state within the client-side wait bound — the saga keeps
   * running and nothing failed, so the caller should poll {@code sagaId}, not re-send the request.
   *
   * @param sagaId the saga still running server-side; the handle the caller needs to poll it
   */
  public static SagaTimeoutException awaitExpired(String sagaId) {
    return new SagaTimeoutException(sagaId);
  }

  /**
   * The saga the expired wait was waiting for, or null when this is a {@link
   * SagaErrorCode#REQUEST_TIMEOUT}. Non-null for {@link SagaErrorCode#SAGA_AWAIT_TIMEOUT}, where it
   * is what the code's remediation tells the caller to poll.
   */
  public @Nullable String getSagaId() {
    return sagaId;
  }
}
