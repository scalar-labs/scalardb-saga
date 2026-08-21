package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaHttpClient;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.SagaDefinition.ClassStep;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.engine.StepResolver.ResolutionContext;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.transport.HttpEndpointManager;
import com.scalar.db.saga.transport.HttpEndpointRegistrar;
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
 * <p>It is also the engine-side {@link ResolutionContext}: the narrow {@link SagaHttpClient} lookup
 * handed to the {@link StepResolver} for code steps. A client obtained here is pinned to the
 * endpoint current at that moment and is NOT rebound by a later configuration swap (unlike
 * declarative steps, which re-resolve per call through the {@link HttpEndpointManager}).
 *
 * <p>It holds the endpoint manager that owns the HTTP clients and releases them via {@link
 * #close()}, called from {@link SagaEngine#shutdown()} once in-flight sagas drain.
 */
final class StepInstantiator implements ResolutionContext {

  private static final Logger logger = LoggerFactory.getLogger(StepInstantiator.class);

  private final StepResolver stepResolver;
  private final HttpEndpointManager endpointManager;

  StepInstantiator(StepResolver stepResolver, HttpEndpointManager endpointManager) {
    this.stepResolver = stepResolver;
    this.endpointManager = endpointManager;
  }

  /** The swap seam for configuration hot reload, surfaced up through the engine. */
  HttpEndpointRegistrar httpEndpointRegistrar() {
    return endpointManager;
  }

  /**
   * Closes the endpoint manager this instantiator holds, releasing its HTTP clients. Best-effort: a
   * failure is logged. Called once in-flight sagas have drained (see {@link
   * SagaEngine#shutdown()}).
   */
  void close() {
    try {
      endpointManager.close();
    } catch (RuntimeException e) {
      logger.warn("Failed to close HTTP endpoint manager", e);
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

  /**
   * Returns the {@link SagaHttpClient} for the endpoint currently named {@code name}, pinned to
   * that endpoint (a later swap does not rebind it).
   *
   * @throws SagaDefinitionException if no endpoint is currently registered under {@code name}
   */
  @Override
  public SagaHttpClient httpClient(String name) {
    SagaHttpClient client = endpointManager.sagaHttpClient(name);
    if (client == null) {
      throw SagaDefinitionException.httpEndpointLookupFailed(
          "no HTTP endpoint registered under name '" + name + "'");
    }
    return client;
  }

  /**
   * Returns the sole registered endpoint's {@link SagaHttpClient}, pinned like {@link
   * #httpClient(String)}.
   *
   * @throws SagaDefinitionException if zero endpoints are registered, or if more than one is
   *     registered (then {@code @Named}/{@link #httpClient(String)} must select one)
   */
  @Override
  public SagaHttpClient httpClient() {
    var names = endpointManager.names();
    if (names.isEmpty()) {
      throw SagaDefinitionException.httpEndpointLookupFailed(
          "no HTTP endpoint is registered; call httpEndpoint(name, baseUrl) on the builder");
    }
    if (names.size() > 1) {
      throw SagaDefinitionException.httpEndpointLookupFailed(
          "multiple HTTP endpoints are registered "
              + names
              + "; annotate the SagaHttpClient parameter with @Named(\"<endpoint>\") to select"
              + " one");
    }
    return httpClient(names.iterator().next());
  }

  private <T> T resolveServiceStep(ServiceStep stepDef, Class<T> expectedType) {
    String name = stepDef.getName();
    String service = stepDef.getService();
    // Fail fast at registration (the analog of resolving a class step) if the service has no
    // registered transport endpoint. The SAGA/TCC phase set was already validated against the
    // saga's mode at definition build time, so the adapter is selected purely by expectedType.
    if (!endpointManager.contains(service)) {
      throw SagaDefinitionException.declarativeStepInvalid(
          name, "references unregistered HTTP endpoint '" + service + "'");
    }
    Object resolved =
        expectedType == TccStep.class
            ? endpointManager.toTccStep(name, service, stepDef.getPhases())
            : endpointManager.toStep(name, service, stepDef.getPhases());
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
    Object resolved = stepResolver.resolve(stepDef.getName(), stepDef.getStepClass(), this);
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
