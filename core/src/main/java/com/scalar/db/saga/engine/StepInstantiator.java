package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaDefinition.ClassStep;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.SagaDefinitionException;

/**
 * Turns a {@link StepDefinition} into a {@code Step}/{@code TccStep} instance, dispatching on the
 * sealed step kind:
 *
 * <ul>
 *   <li>{@link ClassStep} → resolved by class name via {@link StepResolver} (Layer 1)
 *   <li>{@link ServiceStep} → wrapped in a {@code ServiceInvokerStep} (SAGA) or {@code
 *       ServiceInvokerTccStep} (TCC) (Layer 2)
 * </ul>
 *
 * <p>This concentrates all step-kind branching in one place so {@link SagaEngine}'s execution and
 * compensation loops stay a single {@code Step.execute}/{@code Step.compensate} dispatch path.
 */
final class StepInstantiator {

  private final StepResolver stepResolver;
  private final ServiceInvokerRegistry serviceInvokerRegistry;

  StepInstantiator(StepResolver stepResolver, ServiceInvokerRegistry serviceInvokerRegistry) {
    this.stepResolver = stepResolver;
    this.serviceInvokerRegistry = serviceInvokerRegistry;
  }

  /**
   * Instantiates the step and verifies it is an instance of {@code expectedType} ({@code Step} for
   * the SAGA path, {@code TccStep} for the TCC path).
   *
   * @throws SagaDefinitionException if the step cannot be instantiated or is not of the expected
   *     type
   */
  <T> T instantiate(StepDefinition stepDef, Class<T> expectedType) {
    return switch (stepDef) {
      case ClassStep classStep -> resolveClassStep(classStep, expectedType);
      case ServiceStep serviceStep -> resolveServiceStep(serviceStep, expectedType);
    };
  }

  private <T> T resolveServiceStep(ServiceStep stepDef, Class<T> expectedType) {
    String name = stepDef.getName();
    String service = stepDef.getService();
    String operation = stepDef.getOperation();

    // Resolve the service step fully at registration — the analog of resolving and type-checking a
    // class step — so wiring errors fail fast here instead of mid-saga. The required phases depend
    // on the saga's mode:
    //   - SAGA: both execute and compensate are required. Compensation is mandatory regardless of
    //     pivot position (e.g. a step after a MIXED pivot, which is never compensated) — it is part
    //     of the operation's nature, the analog of a Step class implementing compensate(), and the
    //     same operation may be compensated in another saga.
    //   - TCC: reserve, confirm, and cancel are all required — every reserved step must be
    //     confirmable on success and cancellable on abort, the analog of a TccStep implementing all
    //     three phases.
    if (!serviceInvokerRegistry.contains(service)) {
      throw new SagaDefinitionException(
          "Service step '" + name + "' references unregistered service '" + service + "'");
    }

    if (expectedType == TccStep.class) {
      requirePhase(
          serviceInvokerRegistry.supportsReserve(service, operation),
          name,
          operation,
          service,
          "reservation");
      requirePhase(
          serviceInvokerRegistry.supportsConfirm(service, operation),
          name,
          operation,
          service,
          "confirmation");
      requirePhase(
          serviceInvokerRegistry.supportsCancel(service, operation),
          name,
          operation,
          service,
          "cancellation");
      return expectedType.cast(serviceInvokerRegistry.toTccStep(name, service, operation));
    }

    requirePhase(
        serviceInvokerRegistry.supportsExecute(service, operation),
        name,
        operation,
        service,
        "execution");
    requirePhase(
        serviceInvokerRegistry.supportsCompensate(service, operation),
        name,
        operation,
        service,
        "compensation");
    return expectedType.cast(serviceInvokerRegistry.toStep(name, service, operation));
  }

  private static void requirePhase(
      boolean supported, String stepName, String operation, String service, String phase) {
    if (!supported) {
      throw new SagaDefinitionException(
          "Service step '"
              + stepName
              + "' references operation '"
              + operation
              + "' which has no "
              + phase
              + " on service '"
              + service
              + "'");
    }
  }

  private <T> T resolveClassStep(ClassStep stepDef, Class<T> expectedType) {
    Object resolved = stepResolver.resolve(stepDef.getName(), stepDef.getStepClass());
    if (expectedType.isInstance(resolved)) {
      return expectedType.cast(resolved);
    }
    throw new SagaDefinitionException(
        "Step '"
            + stepDef.getName()
            + "' (class "
            + stepDef.getStepClass()
            + ") does not implement "
            + expectedType.getName()
            + ". Found: "
            + resolved.getClass().getName());
  }
}
