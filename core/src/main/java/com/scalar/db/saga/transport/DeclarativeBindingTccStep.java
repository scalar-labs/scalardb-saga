package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.CallSpec;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.definition.TccStepNaming;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Map;
import java.util.Objects;

/**
 * Realizes a TCC-mode declarative {@code ServiceStep} (Layer 2b) as a {@link TccStep}: the {@code
 * reserve}, {@code confirm}, and {@code cancel} {@link CallSpec}s are performed through the {@link
 * TransportAdapter} its service name resolves to at each phase call — late binding, so a
 * configuration swap between phases means reserve may land on the old endpoint and confirm/cancel
 * on its replacement (endpoint changes must stay backward-compatible for in-flight sagas). A {@link
 * TransportException} (from the resolution or the call) becomes a {@link StepExecutionException}
 * (carrying the retryable and known-not-committed flags) for reserve/confirm, or a {@link
 * StepCompensationException} for cancel. The SAGA counterpart is {@link DeclarativeBindingStep}.
 */
final class DeclarativeBindingTccStep implements TccStep {

  private final String name;
  private final TransportResolver resolver;
  private final String service;
  private final CallSpec reserve;
  private final CallSpec confirm;
  private final CallSpec cancel;

  DeclarativeBindingTccStep(
      String name, TransportResolver resolver, String service, Map<Phase, CallSpec> phases) {
    this.name = name;
    this.resolver = resolver;
    this.service = service;
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

  // Each phase calls out under its phase-qualified step name (e.g. seat.reserve). This must match
  // the name the engine's TccReserveStep/TccConfirmStep wrappers record in the STEP_PENDING marker,
  // so an async step's callback URL (keyed on the name passed here) resolves to the parked step.

  @Override
  public StepResult reserve(SagaContext context) throws StepExecutionException {
    return perform(reserve, context, TccStepNaming.RESERVE_SUFFIX);
  }

  @Override
  public StepResult confirm(SagaContext context) throws StepExecutionException {
    return perform(confirm, context, TccStepNaming.CONFIRM_SUFFIX);
  }

  /**
   * Performs a forward-phase call through one shared exception mapping, so reserve and confirm
   * cannot drift apart in how they carry the retryable and known-not-committed flags. Cancel stays
   * separate: it maps to {@link StepCompensationException}.
   */
  private StepResult perform(CallSpec spec, SagaContext context, String suffix)
      throws StepExecutionException {
    try {
      return resolver.resolve(service).call(spec, context, name + suffix);
    } catch (TransportException e) {
      throw new StepExecutionException(e, e.isRetryable(), e.knownNotCommitted());
    }
  }

  @Override
  public void cancel(SagaContext context) throws StepCompensationException {
    try {
      resolver.resolve(service).call(cancel, context, name + TccStepNaming.CANCEL_SUFFIX);
    } catch (TransportException e) {
      throw new StepCompensationException(e);
    }
  }
}
