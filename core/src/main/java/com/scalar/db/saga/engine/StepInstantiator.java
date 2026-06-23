package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaDefinition.ClassStep;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.api.StepResolver.ResolutionContext;
import com.scalar.db.saga.exception.SagaDefinitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a {@link StepDefinition} into a {@code Step}/{@code TccStep} instance: a {@link ClassStep}
 * is resolved by class name via {@link StepResolver} (Layer 1).
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
    };
  }

  private <T> T resolveClassStep(ClassStep stepDef, Class<T> expectedType) {
    Object resolved =
        stepResolver.resolve(stepDef.getName(), stepDef.getStepClass(), resolutionContext);
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
