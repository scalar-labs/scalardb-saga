package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when the request carries no valid credential, so the caller could not be authenticated.
 *
 * <p>This is a remote-relevant exception: the embedded {@code SagaOrchestrator} never throws it,
 * because authentication is a daemon concern. A remote client maps a gRPC {@code UNAUTHENTICATED}
 * status to it (the gRPC analogue of the daemon's REST {@code 401}). Retrying without changing the
 * credential will not succeed; the caller needs to present a valid one.
 */
public class SagaUnauthenticatedException extends SagaRuntimeException {

  /** The request presented no valid credential, with no transport status to attach as the cause. */
  public SagaUnauthenticatedException() {
    super(SagaErrorCode.UNAUTHENTICATED, ErrorMetadata.of());
  }

  /**
   * The request presented no valid credential.
   *
   * @param cause the transport status that reported it
   */
  public SagaUnauthenticatedException(Throwable cause) {
    super(
        SagaErrorCode.UNAUTHENTICATED,
        ErrorMetadata.of(),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
