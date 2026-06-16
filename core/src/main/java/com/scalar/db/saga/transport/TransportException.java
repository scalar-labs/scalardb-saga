package com.scalar.db.saga.transport;

/**
 * Thrown by a {@link TransportAdapter} when a declarative call fails. Carries a {@code retryable}
 * flag the engine uses to decide whether to retry (forward path) — transport errors and the
 * status-derived/overridden classification map onto it; a definition or contract error (missing
 * {@code ${key}}, malformed URL, unexpected response) is non-retryable.
 */
public final class TransportException extends Exception {

  private final boolean retryable;

  public TransportException(String message, boolean retryable) {
    super(message);
    this.retryable = retryable;
  }

  public TransportException(String message, Throwable cause, boolean retryable) {
    super(message, cause);
    this.retryable = retryable;
  }

  /** Whether the failure is transient and the call may be retried. */
  public boolean isRetryable() {
    return retryable;
  }
}
