package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a step's forward action ({@code execute} or {@code reserve}) fails.
 *
 * <p>The {@code retryable} flag signals whether the engine should retry the step or begin
 * compensation. The default is {@code true} (retryable) because transient failures are the common
 * case.
 */
public class StepExecutionException extends Exception {

  private final boolean retryable;

  public StepExecutionException(String message) {
    this(message, true);
  }

  public StepExecutionException(Throwable cause) {
    this(cause, true);
  }

  public StepExecutionException(String message, boolean retryable) {
    super(Objects.requireNonNull(message, "message must not be null"));
    this.retryable = retryable;
  }

  public StepExecutionException(Throwable cause, boolean retryable) {
    super(Objects.requireNonNull(cause, "cause must not be null"));
    this.retryable = retryable;
  }

  public StepExecutionException(String message, Throwable cause, boolean retryable) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.retryable = retryable;
  }

  public boolean isRetryable() {
    return retryable;
  }
}
