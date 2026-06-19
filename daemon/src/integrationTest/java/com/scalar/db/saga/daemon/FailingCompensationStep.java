package com.scalar.db.saga.daemon;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;

/**
 * A code step that succeeds forward but always fails to compensate — used to leave a saga in {@code
 * COMPENSATING} (compensation incomplete, recovery will retry).
 */
public final class FailingCompensationStep implements Step {

  @Override
  public String getName() {
    return "failing-compensation";
  }

  @Override
  public StepResult execute(SagaContext context) {
    return StepResult.empty();
  }

  @Override
  public void compensate(SagaContext context) {
    throw new StepCompensationException("forced compensation failure");
  }
}
