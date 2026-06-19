package com.scalar.db.saga.daemon;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;

/**
 * A no-op code step (public no-arg constructor) used to exercise the saga lifecycle endpoints
 * end-to-end without standing up an external participant. Resolved by the engine's default
 * reflective resolver from a declarative definition's {@code stepClass}.
 */
public final class NoopStep implements Step {

  @Override
  public String getName() {
    return "noop";
  }

  @Override
  public StepResult execute(SagaContext context) {
    return StepResult.empty();
  }

  @Override
  public void compensate(SagaContext context) {
    // no-op
  }
}
