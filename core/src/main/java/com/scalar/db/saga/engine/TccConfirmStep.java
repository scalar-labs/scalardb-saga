package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.TccStepNaming;
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
  private final String name;

  TccConfirmStep(TccStep tccStep) {
    this.tccStep = tccStep;
    this.name = tccStep.getName() + TccStepNaming.CONFIRM_SUFFIX;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    return tccStep.confirm(context);
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    throw new UnsupportedOperationException(
        "TCC confirm steps are after the pivot and must not be compensated");
  }
}
