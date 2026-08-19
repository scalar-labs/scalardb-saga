package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.SagaDefinition.ClassStep;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.engine.StepResolver.ResolutionContext;
import com.scalar.db.saga.exception.SagaDefinitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a {@link StepDefinition} into a {@code Step}/{@code TccStep} instance, dispatching on the
 * sealed step kind:
 *
 * <ul>
 *   <li>{@link ClassStep} → resolved by class name via {@link StepResolver} (Layer 1)
 *   <li>{@link ServiceStep} → wrapped in a declarative adapter backed by a {@code TransportAdapter}
 *       (Layer 2b)
 * </ul>
 *
 * <p>This concentrates all step-kind branching in one place so {@link SagaEngine}'s execution and
 * compensation loops stay a single {@code Step.execute}/{@code Step.compensate} dispatch path.
 *
 * <p>It owns the HTTP endpoint registry (which holds HTTP clients) and releases it via {@link
 * #close()}, called from {@link SagaEngine#shutdown()} once in-flight sagas drain.
 */
final class StepInstantiator {

  private static final Logger logger = LoggerFactory.getLogger(StepInstantiator.class);

  private final StepResolver stepResolver;
  private final HttpEndpointRegistry httpEndpointRegistry;
  private final ResolutionContext resolutionContext;

  StepInstantiator(StepResolver stepResolver, HttpEndpointRegistry httpEndpointRegistry) {
    this.stepResolver = stepResolver;
    this.httpEndpointRegistry = httpEndpointRegistry;
    // The ResolutionContext is the narrow SagaHttpClient lookup handed to the StepResolver; the
    // default ReflectiveStepResolver uses it to inject @Named SagaHttpClient and a custom resolver
    // may use it to construct its own steps with policy-enforcing clients.
    this.resolutionContext = httpEndpointRegistry;
  }

  /**
   * Closes the HTTP endpoint registry this instantiator owns, releasing its HTTP clients.
   * Best-effort: a failure is logged. Called once in-flight sagas have drained (see {@link
   * SagaEngine#shutdown()}).
   */
  void close() {
    try {
      httpEndpointRegistry.close();
    } catch (RuntimeException e) {
      logger.warn("Failed to close HTTP endpoint registry", e);
    }
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
    // Fail fast at registration (the analog of resolving a class step) if the service has no
    // registered transport endpoint. The SAGA/TCC phase set was already validated against the
    // saga's mode at definition build time, so the adapter is selected purely by expectedType.
    if (!httpEndpointRegistry.contains(service)) {
      throw SagaDefinitionException.declarativeStepInvalid(
          name, "references unregistered HTTP endpoint '" + service + "'");
    }
    Object resolved =
        expectedType == TccStep.class
            ? httpEndpointRegistry.toTccStep(name, service, stepDef.getPhases())
            : httpEndpointRegistry.toStep(name, service, stepDef.getPhases());
    if (expectedType.isInstance(resolved)) {
      return expectedType.cast(resolved);
    }
    throw SagaDefinitionException.declarativeStepInvalid(
        name,
        "service '"
            + service
            + "' does not produce "
            + expectedType.getName()
            + "; found "
            + resolved.getClass().getName());
  }

  private <T> T resolveClassStep(ClassStep stepDef, Class<T> expectedType) {
    Object resolved =
        stepResolver.resolve(stepDef.getName(), stepDef.getStepClass(), resolutionContext);
    if (expectedType.isInstance(resolved)) {
      return expectedType.cast(resolved);
    }
    throw SagaDefinitionException.stepClassInvalid(
        stepDef.getStepClass(),
        "does not implement "
            + expectedType.getName()
            + "; found "
            + resolved.getClass().getName());
  }
}
