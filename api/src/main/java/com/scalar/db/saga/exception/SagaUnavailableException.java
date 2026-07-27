package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when the saga service is temporarily unavailable — a transient condition that should be
 * retried with backoff.
 *
 * <p>This is a remote-relevant exception: the embedded {@code SagaOrchestrator} never throws it. A
 * remote client maps a gRPC {@code UNAVAILABLE} status to it, which covers both a server-side
 * availability/persistence failure surfaced as {@code UNAVAILABLE} and a pure client-side
 * connectivity failure (the request never reached the server). In either case the request did not
 * definitively complete and is safe to retry.
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
