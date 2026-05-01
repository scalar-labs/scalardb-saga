package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * A saga step with a forward action and a compensating action.
 *
 * <p><b>Lifecycle:</b> Steps are non-static, application-level singletons — a single instance is
 * shared across all concurrent saga executions. All methods <b>must</b> be thread-safe.
 *
 * <p><b>Idempotency:</b> Both {@code execute} and {@code compensate} must be idempotent. On crash
 * recovery a step whose action completed externally but whose result was not persisted will be
 * re-executed. Use a dedup key (e.g., {@code sagaId + stepName}) to detect and skip duplicates.
 *
 * <p><b>Error signaling:</b> The step decides whether a failure is retryable by throwing {@link
 * StepExecutionException} with the appropriate {@code retryable} flag. The engine never inspects
 * exception class names.
 */
public interface Step {

  /** Returns the unique name of this step within a saga definition. */
  String getName();

  /**
   * Executes the forward action. The returned result is merged into {@link SagaContext} for
   * subsequent steps.
   *
   * @param context the saga execution context
   * @return the step output
   * @throws StepExecutionException if the action fails (retryable or not)
   */
  StepResult execute(SagaContext context) throws StepExecutionException;

  /**
   * Undoes the forward action. Called during compensation.
   *
   * @param context the saga execution context
   * @throws StepCompensationException if compensation fails
   */
  void compensate(SagaContext context) throws StepCompensationException;
}
