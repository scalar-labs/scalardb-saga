package com.scalar.db.saga.daemon;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * A code step whose forward action always fails (non-retryable) — used to trigger compensation of
 * the preceding steps.
 */
public final class FailingStep implements Step {

  @Override
  public String getName() {
    return "failing";
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    throw new StepExecutionException("forced forward failure", false);
  }

  @Override
  public void compensate(SagaContext context) {
    // no-op
  }
}
