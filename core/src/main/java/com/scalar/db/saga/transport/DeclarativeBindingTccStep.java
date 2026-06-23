package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.CallSpec;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.Objects;

/**
 * Realizes a TCC-mode declarative {@code ServiceStep} (Layer 2b) as a {@link TccStep}: the {@code
 * reserve}, {@code confirm}, and {@code cancel} {@link CallSpec}s are performed through a {@link
 * TransportAdapter}. A {@link TransportException} becomes a {@link StepExecutionException}
 * (carrying the retryable flag) for reserve/confirm, or a {@link StepCompensationException} for
 * cancel. The SAGA counterpart is {@link DeclarativeBindingStep}.
 */
final class DeclarativeBindingTccStep implements TccStep {

  private final String name;
  private final TransportAdapter transport;
  private final CallSpec reserve;
  private final CallSpec confirm;
  private final CallSpec cancel;

  DeclarativeBindingTccStep(String name, TransportAdapter transport, Map<Phase, CallSpec> phases) {
    this.name = name;
    this.transport = transport;
    this.reserve =
        Objects.requireNonNull(
            phases.get(Phase.RESERVATION), "reservation call spec must not be null");
    this.confirm =
        Objects.requireNonNull(
            phases.get(Phase.CONFIRMATION), "confirmation call spec must not be null");
    this.cancel =
        Objects.requireNonNull(
            phases.get(Phase.CANCELLATION), "cancellation call spec must not be null");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult reserve(SagaContext context) throws StepExecutionException {
    try {
      return StepResult.of(transport.call(reserve, context, name));
    } catch (TransportException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public void confirm(SagaContext context) throws StepExecutionException {
    try {
      transport.call(confirm, context, name);
    } catch (TransportException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public void cancel(SagaContext context) throws StepCompensationException {
    try {
      transport.call(cancel, context, name);
    } catch (TransportException e) {
      throw new StepCompensationException(e);
    }
  }
}
