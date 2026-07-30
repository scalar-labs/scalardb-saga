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
 *
 * <p>Carries one of two RETRYABLE_SERVER_ERROR codes: {@link SagaErrorCode#SERVICE_UNAVAILABLE}
 * (the default — the saga service or an upstream dependency is transiently unavailable) or {@link
 * SagaErrorCode#PERSISTENCE_STORE_UNAVAILABLE} (specifically the underlying store). The default
 * constructors produce the general {@code SERVICE_UNAVAILABLE}; {@link #fromWire(SagaErrorCode)} is
 * the client SDK's reconstruction path that preserves whichever specific code the daemon sent.
 */
public class SagaUnavailableException extends SagaRuntimeException {

  public SagaUnavailableException() {
    this(SagaErrorCode.SERVICE_UNAVAILABLE);
  }

  public SagaUnavailableException(Throwable cause) {
    this(SagaErrorCode.SERVICE_UNAVAILABLE, cause);
  }

  private SagaUnavailableException(SagaErrorCode code) {
    super(code, ErrorMetadata.of());
  }

  private SagaUnavailableException(SagaErrorCode code, Throwable cause) {
    super(code, ErrorMetadata.of(), Objects.requireNonNull(cause, "cause must not be null"));
  }

  /**
   * Reconstructs from a wire-received code, for use by the client SDK when it decodes the daemon's
   * response. The code must be a RETRYABLE_SERVER_ERROR with a schemaless shape (currently {@link
   * SagaErrorCode#SERVICE_UNAVAILABLE} or {@link SagaErrorCode#PERSISTENCE_STORE_UNAVAILABLE});
   * other codes throw {@link IllegalArgumentException}. This is the only path that preserves a
   * non-default code on the reconstructed exception; the default constructors always produce {@code
   * SERVICE_UNAVAILABLE}.
   */
  public static SagaUnavailableException fromWire(SagaErrorCode code) {
    Objects.requireNonNull(code, "code must not be null");
    if (code != SagaErrorCode.SERVICE_UNAVAILABLE
        && code != SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE) {
      throw new IllegalArgumentException("SagaUnavailableException does not carry code " + code);
    }
    return new SagaUnavailableException(code);
  }
}
