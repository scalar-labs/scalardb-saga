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
   * <p><b>Must be idempotent and no-op-safe.</b> Compensation runs whenever a forward failure's
   * non-delivery is not proven — including for a step whose {@code execute} may not have committed,
   * or never ran (e.g. a pre-I/O validation failure). So it must undo the side effect if it
   * happened and otherwise do nothing ("undo X if X happened, else no-op"). Recovery also retries
   * compensation, so repeated calls must be safe.
   *
   * <p><b>Must not depend on this step's own {@code execute} output.</b> Because compensation may
   * run for a step whose {@code execute} failed, that output may not exist. Undo must be executable
   * from pre-execution / correlation data — the saga id, the step inputs, or a prior completed
   * step's output — e.g. "undo whatever was done under this saga id" rather than "undo the resource
   * id this step returned." (Referencing a <em>prior, completed</em> step's output is safe; that
   * output is durably available.)
   *
   * @param context the saga execution context
   * @throws StepCompensationException if compensation fails
   */
  void compensate(SagaContext context) throws StepCompensationException;
}
