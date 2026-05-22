package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * Wraps a {@link TccStep} for the Confirm phase.
 *
 * <p>Forward action delegates to {@link TccStep#confirm}. Confirm steps are always after the pivot
 * boundary, so they are retriable and never compensated; calling {@link #compensate} throws {@link
 * UnsupportedOperationException}.
 */
class TccConfirmStep implements Step {

  private final TccStep tccStep;

  TccConfirmStep(TccStep tccStep) {
    this.tccStep = tccStep;
  }

  @Override
  public String getName() {
    return tccStep.getName() + ".confirm";
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    tccStep.confirm(context);
    return StepResult.empty();
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    throw new UnsupportedOperationException(
        "TCC confirm steps are after the pivot and must not be compensated");
  }
}
