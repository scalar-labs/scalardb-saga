package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * Wraps a {@link TccStep} for the Reserve (Try) phase.
 *
 * <p>Forward action delegates to {@link TccStep#reserve}, compensation delegates to {@link
 * TccStep#cancel}. This adapter allows TCC steps to run through the same pivot-based execution loop
 * as regular Saga steps.
 */
class TccReserveStep implements Step {

  private final TccStep tccStep;
  private final String name;

  TccReserveStep(TccStep tccStep) {
    this.tccStep = tccStep;
    this.name = tccStep.getName() + ".reserve";
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    return tccStep.reserve(context);
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    tccStep.cancel(context);
  }
}
