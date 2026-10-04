package com.scalar.db.saga.transport;

/**
 * Thrown by a {@link TransportAdapter} when a declarative call fails. Carries a {@code retryable}
 * flag the engine uses to decide whether to retry (forward path) — transport errors and the
 * status-derived/overridden classification map onto it; a definition or contract error (missing
 * {@code ${key}}, malformed URL, unexpected response) is non-retryable.
 */
public final class TransportException extends Exception {

  /** Whether the failure is transient and the call may be retried. */
  private final boolean retryable;

  /** Whether the call is proven not to have reached the participant. */
  private final boolean knownNotCommitted;

  /**
   * A failure described by {@code message} alone, treated as possibly committed: typically a
   * contract error found after the call, such as a response missing a declared output.
   *
   * @param message what failed
   * @param retryable whether the call may be retried
   */
  public TransportException(String message, boolean retryable) {
    this(message, retryable, false);
  }

  /**
   * A failure described by {@code message} alone, with the commit knowledge stated. A definition
   * error found before the call is sent, such as an unresolvable {@code ${key}}, is non-retryable
   * and known not to have committed; a service with no registered endpoint is retryable and known
   * not to have committed.
   *
   * @param message what failed
   * @param retryable whether the call may be retried
   * @param knownNotCommitted {@code true} when the call is proven not to have reached the
   *     participant
   */
  public TransportException(String message, boolean retryable, boolean knownNotCommitted) {
    super(message);
    this.retryable = retryable;
    this.knownNotCommitted = knownNotCommitted;
  }

  /**
   * A failure caused by {@code cause}, treated as possibly committed.
   *
   * @param message what failed
   * @param cause the underlying failure
   * @param retryable whether the call may be retried
   */
  public TransportException(String message, Throwable cause, boolean retryable) {
    this(message, cause, retryable, false);
  }

  /**
   * A failure caused by {@code cause}, with the commit knowledge stated; this is how the HTTP
   * adapter wraps an {@link HttpCallException}, carrying over both of its flags.
   *
   * @param message what failed
   * @param cause the underlying failure
   * @param retryable whether the call may be retried
   * @param knownNotCommitted {@code true} when the call is proven not to have reached the
   *     participant
   */
  public TransportException(
      String message, Throwable cause, boolean retryable, boolean knownNotCommitted) {
    super(message, cause);
    this.retryable = retryable;
    this.knownNotCommitted = knownNotCommitted;
  }

  /**
   * Whether the failure is transient and the call may be retried.
   *
   * @return {@code true} if the call may be retried
   */
  public boolean isRetryable() {
    return retryable;
  }

  /**
   * Whether the framework can prove the call's side effect did <b>not</b> commit — the call failed
   * before reaching the participant, or is proven not to have reached it. Default {@code false} —
   * an unproven failure is treated as possibly committed so the engine compensates the failed step.
   *
   * @return {@code true} only when the side effect is proven not to have committed
   */
  public boolean knownNotCommitted() {
    return knownNotCommitted;
  }
}
