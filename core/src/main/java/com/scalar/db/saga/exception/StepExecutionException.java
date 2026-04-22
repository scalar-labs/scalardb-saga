package com.scalar.db.saga.exception;

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
    super(message);
    this.retryable = retryable;
  }

  public StepExecutionException(Throwable cause, boolean retryable) {
    super(cause);
    this.retryable = retryable;
  }

  public StepExecutionException(String message, Throwable cause, boolean retryable) {
    super(message, cause);
    this.retryable = retryable;
  }

  public boolean isRetryable() {
    return retryable;
  }
}
