package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when the caller is authenticated but lacks the role required for the operation.
 *
 * <p>This is a remote-relevant exception: the embedded {@code SagaOrchestrator} never throws it,
 * because authorization is a daemon concern. A remote client maps a gRPC {@code PERMISSION_DENIED}
 * status to it (the gRPC analogue of the daemon's REST {@code 403}). Retrying with the same
 * credential will not succeed; the caller needs a credential that carries the required role.
 */
public class SagaPermissionDeniedException extends SagaRuntimeException {

  public SagaPermissionDeniedException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }

  public SagaPermissionDeniedException(String message, Throwable cause) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
  }
}
