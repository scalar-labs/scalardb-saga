package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * Adapts a {@code ServiceStep} {@code (service, operation)} to the {@link Step} interface so the
 * engine dispatches it like any other step. Wraps the engine's {@link SagaContext} in a {@code
 * ServiceCallContext} that adds the step name (for the {@code X-Saga-Step} header).
 */
final class ServiceInvokerStep implements Step {

  private final ServiceInvokerRegistry registry;
  private final String stepName;
  private final String service;
  private final String operation;

  ServiceInvokerStep(
      ServiceInvokerRegistry registry, String stepName, String service, String operation) {
    this.registry = registry;
    this.stepName = stepName;
    this.service = service;
    this.operation = operation;
  }

  @Override
  public String getName() {
    return stepName;
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    return registry.execute(
        service, operation, new DelegatingServiceCallContext(stepName, context));
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    registry.compensate(service, operation, new DelegatingServiceCallContext(stepName, context));
  }
}
