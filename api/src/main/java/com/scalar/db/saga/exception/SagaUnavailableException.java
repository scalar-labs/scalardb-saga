package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when the saga service is temporarily unavailable — a transient condition that should be
 * retried with backoff. Always carries {@link SagaErrorCode#SERVICE_UNAVAILABLE}.
 *
 * <p>This is a remote-relevant exception: the embedded {@code SagaOrchestrator} never throws it. A
 * remote client raises it for a gRPC {@code UNAVAILABLE} status that carried no {@code ErrorInfo}
 * to reconstruct from, which covers a server too busy or unreachable to answer and a pure
 * client-side connectivity failure where the request never arrived. In either case the request did
 * not definitively complete and is safe to retry.
 *
 * <p>A transient <i>store</i> failure is {@link SagaPersistenceException} with {@link
 * SagaErrorCode#PERSISTENCE_STORE_UNAVAILABLE}, not this type — the daemon attaches that code and
 * the client reconstructs the exception the engine actually threw. So this is not the only
 * retryable exception a remote caller sees; key on {@link
 * SagaErrorCode.Category#RETRYABLE_SERVER_ERROR} to cover both.
 */
public class SagaUnavailableException extends SagaRuntimeException {

  public SagaUnavailableException() {
    super(SagaErrorCode.SERVICE_UNAVAILABLE, ErrorMetadata.of());
  }

  public SagaUnavailableException(Throwable cause) {
    super(
        SagaErrorCode.SERVICE_UNAVAILABLE,
        ErrorMetadata.of(),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
