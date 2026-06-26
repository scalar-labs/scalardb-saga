package com.scalar.db.saga.transport;

/**
 * Thrown by a {@link TransportAdapter} when a declarative call fails. Carries a {@code retryable}
 * flag the engine uses to decide whether to retry (forward path) — transport errors and the
 * status-derived/overridden classification map onto it; a definition or contract error (missing
 * {@code ${key}}, malformed URL, unexpected response) is non-retryable.
 */
public final class TransportException extends Exception {

  private final boolean retryable;
  private final boolean knownNotCommitted;

  public TransportException(String message, boolean retryable) {
    this(message, retryable, false);
  }

  public TransportException(String message, boolean retryable, boolean knownNotCommitted) {
    super(message);
    this.retryable = retryable;
    this.knownNotCommitted = knownNotCommitted;
  }

  public TransportException(String message, Throwable cause, boolean retryable) {
    this(message, cause, retryable, false);
  }

  public TransportException(
      String message, Throwable cause, boolean retryable, boolean knownNotCommitted) {
    super(message, cause);
    this.retryable = retryable;
    this.knownNotCommitted = knownNotCommitted;
  }

  /** Whether the failure is transient and the call may be retried. */
  public boolean isRetryable() {
    return retryable;
  }

  /**
   * Whether the framework can prove the call's side effect did <b>not</b> commit — the call failed
   * before reaching the participant, or is proven not to have reached it. Default {@code false} —
   * an unproven failure is treated as possibly committed so the engine compensates the failed step.
   */
  public boolean knownNotCommitted() {
    return knownNotCommitted;
  }
}
