package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when the saga store layer encounters a failure (e.g., a database write error).
 *
 * <p>Carries a {@link #isRetryable()} flag that separates a <em>transient</em> failure — a store
 * outage or a retry-exhausted transaction, where retrying the operation may succeed — from a
 * <em>permanent</em> one — a serialization or parse failure, where retrying always fails the same
 * way. The daemon's error mappers use it to choose a retryable ({@code UNAVAILABLE} / {@code 503})
 * versus a non-retryable ({@code INTERNAL} / {@code 500}) wire code, so a permanent failure is not
 * retried futilely and is not misreported to the caller as a transient outage.
 *
 * <p>Construct via {@link #retryable(String, Throwable)} or {@link #nonRetryable(String,
 * Throwable)}.
 */
public class SagaPersistenceException extends SagaRuntimeException {

  private final boolean retryable;

  private SagaPersistenceException(String message, Throwable cause, boolean retryable) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.retryable = retryable;
  }

  /**
   * Creates an exception for a <em>transient</em> failure — a store outage or a retry-exhausted
   * transaction — that may succeed if the operation is retried.
   *
   * @param message the detail message
   * @param cause the underlying cause
   * @return a retryable persistence exception
   */
  public static SagaPersistenceException retryable(String message, Throwable cause) {
    return new SagaPersistenceException(message, cause, true);
  }

  /**
   * Creates an exception for a <em>permanent</em> failure — a serialization or parse error — that
   * always fails the same way, so the operation must not be retried.
   *
   * @param message the detail message
   * @param cause the underlying cause
   * @return a non-retryable persistence exception
   */
  public static SagaPersistenceException nonRetryable(String message, Throwable cause) {
    return new SagaPersistenceException(message, cause, false);
  }

  /**
   * Whether retrying the failed operation may succeed (a transient failure) rather than fail
   * identically (a permanent failure).
   *
   * @return {@code true} for a transient failure, {@code false} for a permanent one
   */
  public boolean isRetryable() {
    return retryable;
  }
}
