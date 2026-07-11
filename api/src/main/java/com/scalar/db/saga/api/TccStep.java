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
   * @return the step output; {@link StepResult#pending()} if the confirmation was accepted for
   *     asynchronous completion (the participant completes it later via a callback)
   * @throws StepExecutionException if confirmation fails
   */
  StepResult confirm(SagaContext context) throws StepExecutionException;

  /**
   * Cancel phase: releases the reservation. Called when any step's {@code reserve} fails.
   *
   * <p><b>Must be idempotent and no-op-safe.</b> Cancellation runs whenever a reserve failure's
   * non-delivery is not proven — including for a reservation that may not have committed, or never
   * ran. So it must release the reservation if it was made and otherwise do nothing. Recovery also
   * retries cancellation, so repeated calls must be safe.
   *
   * <p><b>Must not depend on this step's own {@code reserve} output.</b> Because cancellation may
   * run for a step whose {@code reserve} failed, that output may not exist. Cancellation must be
   * executable from pre-execution / correlation data — the saga id, the step inputs, or a prior
   * completed step's output — e.g. "release whatever was reserved under this saga id" rather than
   * "release the reservation id this step returned."
   *
   * @param context the saga execution context
   * @throws StepCompensationException if cancellation fails
   */
  void cancel(SagaContext context) throws StepCompensationException;
}
