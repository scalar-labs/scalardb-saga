package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.CallSpec;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.Objects;

/**
 * Realizes a SAGA-mode declarative {@code ServiceStep} (Layer 2b) as a plain {@link Step}: the
 * {@code execute} and {@code compensate} {@link CallSpec}s are performed through a {@link
 * TransportAdapter}. A {@link TransportException} becomes a {@link StepExecutionException}
 * (carrying the retryable flag) on the forward path, or a {@link StepCompensationException} on
 * compensation (never retried inline). The TCC counterpart is {@link DeclarativeBindingTccStep}.
 */
final class DeclarativeBindingStep implements Step {

  private final String name;
  private final TransportAdapter transport;
  private final CallSpec execute;
  private final CallSpec compensate;

  DeclarativeBindingStep(String name, TransportAdapter transport, Map<Phase, CallSpec> phases) {
    this.name = name;
    this.transport = transport;
    this.execute =
        Objects.requireNonNull(phases.get(Phase.EXECUTION), "execution call spec must not be null");
    this.compensate =
        Objects.requireNonNull(
            phases.get(Phase.COMPENSATION), "compensation call spec must not be null");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    try {
      return StepResult.of(transport.call(execute, context, name));
    } catch (TransportException e) {
      throw new StepExecutionException(e, e.isRetryable(), e.knownNotCommitted());
    }
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    try {
      transport.call(compensate, context, name);
    } catch (TransportException e) {
      throw new StepCompensationException(e);
    }
  }
}
