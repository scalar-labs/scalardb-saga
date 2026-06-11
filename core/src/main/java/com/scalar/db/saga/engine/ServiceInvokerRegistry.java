package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.ServiceInvoker;
import com.scalar.db.saga.api.ServiceInvokerFactory;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable lookup of {@link ServiceInvoker}s by service name, built from the invokers registered
 * on {@code SagaManager.Builder}. The engine turns a {@code ServiceStep} into a {@link Step} via
 * {@link #toStep} (SAGA) or a {@link TccStep} via {@link #toTccStep} (TCC); dispatch then flows
 * through the unchanged {@code Step.execute}/{@code Step.compensate} or {@code TccStep} path.
 */
public final class ServiceInvokerRegistry {

  private static final Logger logger = LoggerFactory.getLogger(ServiceInvokerRegistry.class);

  private final Map<String, ServiceInvoker> invokers;

  private ServiceInvokerRegistry(Map<String, ServiceInvoker> invokers) {
    this.invokers = Map.copyOf(invokers);
  }

  /** Creates a registry from a copy of {@code invokers} (service name → invoker). */
  public static ServiceInvokerRegistry of(Map<String, ServiceInvoker> invokers) {
    return new ServiceInvokerRegistry(invokers);
  }

  /**
   * Creates a registry by invoking each factory once (service name → factory). If any factory
   * fails, the invokers already created are closed before rethrowing, so a failed build leaks no
   * invoker resources.
   */
  public static ServiceInvokerRegistry create(Map<String, ServiceInvokerFactory> factories) {
    Map<String, ServiceInvoker> invokers = new HashMap<>();
    try {
      factories.forEach(
          (name, factory) ->
              invokers.put(
                  name,
                  Objects.requireNonNull(
                      factory.createServiceInvoker(),
                      "ServiceInvokerFactory returned null for service: " + name)));
    } catch (RuntimeException e) {
      closeQuietly(invokers.values());
      throw e;
    }
    return new ServiceInvokerRegistry(invokers);
  }

  /**
   * Closes every registered invoker, releasing any resources they hold (e.g. HTTP clients). Called
   * by the engine on shutdown. Best-effort: a failure to close one invoker is logged and does not
   * prevent the others from being closed.
   */
  public void close() {
    closeQuietly(invokers.values());
  }

  /**
   * Best-effort close of each invoker in {@code invokers}: a failure to close one is logged and
   * does not prevent the others from being closed. Shared by {@link #close()} and the partial-
   * failure cleanup in {@link #create(Map)}.
   */
  private static void closeQuietly(Collection<ServiceInvoker> invokers) {
    for (ServiceInvoker invoker : invokers) {
      try {
        invoker.close();
      } catch (RuntimeException e) {
        logger.warn("Failed to close service invoker", e);
      }
    }
  }

  /** Returns whether an invoker is registered for {@code service}. */
  public boolean contains(String service) {
    return invokers.containsKey(service);
  }

  /** Returns whether {@code service}'s invoker has a forward operation for {@code operation}. */
  public boolean supportsExecute(String service, String operation) {
    ServiceInvoker invoker = invokers.get(service);
    return invoker != null && invoker.supportsExecute(operation);
  }

  /**
   * Returns whether {@code service}'s invoker has a compensating operation for {@code operation}.
   */
  public boolean supportsCompensate(String service, String operation) {
    ServiceInvoker invoker = invokers.get(service);
    return invoker != null && invoker.supportsCompensate(operation);
  }

  /**
   * Returns whether {@code service}'s invoker has a reserve (TCC) operation for {@code operation}.
   */
  public boolean supportsReserve(String service, String operation) {
    ServiceInvoker invoker = invokers.get(service);
    return invoker != null && invoker.supportsReserve(operation);
  }

  /**
   * Returns whether {@code service}'s invoker has a confirm (TCC) operation for {@code operation}.
   */
  public boolean supportsConfirm(String service, String operation) {
    ServiceInvoker invoker = invokers.get(service);
    return invoker != null && invoker.supportsConfirm(operation);
  }

  /**
   * Returns whether {@code service}'s invoker has a cancel (TCC) operation for {@code operation}.
   */
  public boolean supportsCancel(String service, String operation) {
    ServiceInvoker invoker = invokers.get(service);
    return invoker != null && invoker.supportsCancel(operation);
  }

  /** Wraps {@code (service, operation)} as a {@link Step} (SAGA) named {@code stepName}. */
  public Step toStep(String stepName, String service, String operation) {
    return new ServiceInvokerStep(this, stepName, service, operation);
  }

  /** Wraps {@code (service, operation)} as a {@link TccStep} named {@code stepName}. */
  public TccStep toTccStep(String stepName, String service, String operation) {
    return new ServiceInvokerTccStep(this, stepName, service, operation);
  }

  StepResult execute(String service, String operation, ServiceCallContext context)
      throws StepExecutionException {
    ServiceInvoker invoker = invokers.get(service);
    if (invoker == null) {
      throw new StepExecutionException(
          "No service invoker registered for service: " + service, false);
    }
    return invoker.execute(operation, context);
  }

  void compensate(String service, String operation, ServiceCallContext context)
      throws StepCompensationException {
    ServiceInvoker invoker = invokers.get(service);
    if (invoker == null) {
      throw new StepCompensationException("No service invoker registered for service: " + service);
    }
    invoker.compensate(operation, context);
  }

  StepResult reserve(String service, String operation, ServiceCallContext context)
      throws StepExecutionException {
    ServiceInvoker invoker = invokers.get(service);
    if (invoker == null) {
      throw new StepExecutionException(
          "No service invoker registered for service: " + service, false);
    }
    return invoker.reserve(operation, context);
  }

  void confirm(String service, String operation, ServiceCallContext context)
      throws StepExecutionException {
    ServiceInvoker invoker = invokers.get(service);
    if (invoker == null) {
      throw new StepExecutionException(
          "No service invoker registered for service: " + service, false);
    }
    invoker.confirm(operation, context);
  }

  void cancel(String service, String operation, ServiceCallContext context)
      throws StepCompensationException {
    ServiceInvoker invoker = invokers.get(service);
    if (invoker == null) {
      throw new StepCompensationException("No service invoker registered for service: " + service);
    }
    invoker.cancel(operation, context);
  }
}
