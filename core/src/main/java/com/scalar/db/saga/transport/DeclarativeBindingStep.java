package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.Objects;

/**
 * Realizes a SAGA-mode declarative {@code ServiceStep} (Layer 2b) as a plain {@link Step}: the
 * {@code execute} and {@code compensate} {@link CallSpec}s are performed through the {@link
 * TransportAdapter} its service name resolves to at each call — late binding, so the step (cached
 * in the engine's plan cache) survives configuration swaps and each call lands on the endpoint
 * registered at that moment. A {@link TransportException} (from the resolution or the call) becomes
 * a {@link StepExecutionException} (carrying the retryable and known-not-committed flags) on the
 * forward path, or a {@link StepCompensationException} on compensation (never retried inline). The
 * TCC counterpart is {@link DeclarativeBindingTccStep}.
 */
final class DeclarativeBindingStep implements Step {

  private final String name;
  private final TransportResolver resolver;
  private final String service;
  private final CallSpec execute;
  private final CallSpec compensate;

  DeclarativeBindingStep(
      String name, TransportResolver resolver, String service, Map<Phase, CallSpec> phases) {
    this.name = name;
    this.resolver = resolver;
    this.service = service;
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
      return resolver.resolve(service).call(execute, context, name);
    } catch (TransportException e) {
      throw new StepExecutionException(e, e.isRetryable(), e.knownNotCommitted());
    }
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    try {
      resolver.resolve(service).call(compensate, context, name);
    } catch (TransportException e) {
      throw new StepCompensationException(e);
    }
  }
}
