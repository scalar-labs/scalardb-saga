package com.scalar.db.saga.exception;

import java.util.Objects;

/**
 * Thrown when a step's forward action ({@code execute} or {@code reserve}) fails.
 *
 * <p>The {@code retryable} flag signals whether the engine should retry the step or begin
 * compensation. The default is {@code true} (retryable) because transient failures are the common
 * case.
 *
 * <p>The {@code knownNotCommitted} flag signals whether the framework can <em>prove</em> the step's
 * side effect did not commit. The default is {@code false} (unknown), so an ordinary failure is
 * treated as possibly committed and the engine compensates the failed step too; only a proven
 * non-delivery sets it {@code true} to skip the failed step. The safe value rides the Java default.
 */
public class StepExecutionException extends Exception {

  /** Whether the engine should retry the step rather than begin compensation. */
  private final boolean retryable;

  /** Whether the step's side effect is proven not to have committed. */
  private final boolean knownNotCommitted;

  /**
   * A retryable failure described by {@code message}; the same as {@code
   * StepExecutionException(message, true)}.
   *
   * @param message what went wrong
   */
  public StepExecutionException(String message) {
    this(message, true);
  }

  /**
   * A retryable failure caused by {@code cause}; the same as {@code StepExecutionException(cause,
   * true)}.
   *
   * @param cause the failure the step's forward action hit
   */
  public StepExecutionException(Throwable cause) {
    this(cause, true);
  }

  /**
   * A failure described by {@code message}, retryable or not as the step decides.
   *
   * @param message what went wrong
   * @param retryable {@code true} to have the engine retry the step, {@code false} to begin
   *     compensation
   */
  public StepExecutionException(String message, boolean retryable) {
    super(Objects.requireNonNull(message, "message must not be null"));
    this.retryable = retryable;
    this.knownNotCommitted = false;
  }

  /**
   * A failure caused by {@code cause}, retryable or not as the step decides. The side effect is
   * treated as possibly committed; see {@link #knownNotCommitted()}.
   *
   * @param cause the failure the step's forward action hit
   * @param retryable {@code true} to have the engine retry the step, {@code false} to begin
   *     compensation
   */
  public StepExecutionException(Throwable cause, boolean retryable) {
    this(cause, retryable, false);
  }

  /**
   * A failure caused by {@code cause}, with the retry decision and commit knowledge both stated.
   * This is the one constructor that can mark the side effect as proven not committed.
   *
   * @param cause the failure the step's forward action hit
   * @param retryable {@code true} to have the engine retry the step, {@code false} to begin
   *     compensation
   * @param knownNotCommitted {@code true} only when the framework can prove the step's side effect
   *     did not commit (e.g. a failure before the request was sent, or one proven never to have
   *     reached the participant). Defaults to {@code false} elsewhere — an unproven failure is
   *     treated as possibly committed. See {@link #knownNotCommitted()}.
   */
  public StepExecutionException(Throwable cause, boolean retryable, boolean knownNotCommitted) {
    super(Objects.requireNonNull(cause, "cause must not be null"));
    this.retryable = retryable;
    this.knownNotCommitted = knownNotCommitted;
  }

  /**
   * A failure described by {@code message} and caused by {@code cause}, retryable or not as the
   * step decides.
   *
   * @param message what went wrong
   * @param cause the failure the step's forward action hit
   * @param retryable {@code true} to have the engine retry the step, {@code false} to begin
   *     compensation
   */
  public StepExecutionException(String message, Throwable cause, boolean retryable) {
    super(
        Objects.requireNonNull(message, "message must not be null"),
        Objects.requireNonNull(cause, "cause must not be null"));
    this.retryable = retryable;
    this.knownNotCommitted = false;
  }

  /**
   * Whether the engine should retry the step rather than begin compensation.
   *
   * @return {@code true} if the step may be retried
   */
  public boolean isRetryable() {
    return retryable;
  }

  /**
   * Whether the framework can prove this step's side effect did <b>not</b> commit. When {@code
   * false} (the default for an ordinary failure), the engine must assume the side effect may have
   * committed and compensate the failed step as well; when {@code true} (a proven non-delivery —
   * e.g. a pre-send error or a connection that never reached the participant), the failed step is
   * skipped from compensation. The safe value rides the Java default, so a flag never set, lost in
   * deserialization, or forgotten on a new path fails safe (compensate the failed step).
   *
   * @return {@code true} only when the side effect is proven not to have committed
   */
  public boolean knownNotCommitted() {
    return knownNotCommitted;
  }
}
