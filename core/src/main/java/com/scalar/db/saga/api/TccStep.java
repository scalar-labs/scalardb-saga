package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * A TCC (Try-Confirm-Cancel) step with three-phase execution.
 *
 * <p>The engine internally adapts {@code TccStep} to {@link Step} via {@code TccReserveStep} and
 * {@code TccConfirmStep} so that both Saga and TCC run through the same pivot-based execution loop.
 *
 * <p><b>Lifecycle:</b> Same as {@link Step} — non-static, application-level singletons. All methods
 * <b>must</b> be thread-safe.
 *
 * <p><b>Idempotency:</b> All three methods must be idempotent — each may be called multiple times
 * on crash recovery.
 */
public interface TccStep {

  /** Returns the unique name of this step within a saga definition. */
  String getName();

  /**
   * Try phase: reserves resources or performs a tentative operation.
   *
   * @param context the saga execution context
   * @return the step output
   * @throws StepExecutionException if the reservation fails (retryable or not)
   */
  StepResult reserve(SagaContext context) throws StepExecutionException;

  /**
   * Confirm phase: makes the reservation permanent. Called only after all steps' {@code reserve}
   * succeed. Must eventually succeed — resources are reserved; confirmation should not permanently
   * fail.
   *
   * @param context the saga execution context
   * @throws StepExecutionException if confirmation fails
   */
  void confirm(SagaContext context) throws StepExecutionException;

  /**
   * Cancel phase: releases the reservation. Called when any step's {@code reserve} fails.
   *
   * @param context the saga execution context
   * @throws StepCompensationException if cancellation fails
   */
  void cancel(SagaContext context) throws StepCompensationException;
}
