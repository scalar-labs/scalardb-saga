package com.scalar.db.saga.api;

/**
 * The context passed to a {@link ServiceInvoker} for a single step invocation.
 *
 * <p>Extends {@link SagaContext} (so the invoker can read saga data via {@link #get} and the saga
 * id via {@link #getSagaId()}) and additionally exposes the {@linkplain #getStepName() saga step
 * name}. The step name is what an HTTP/gRPC invoker propagates as the {@code X-Saga-Step}
 * correlation value — it is distinct from the service {@code method} an invoker dispatches on,
 * because two different saga steps may call the same service method.
 */
public interface ServiceCallContext extends SagaContext {

  /**
   * Returns the saga step name this invocation is for (the step's name in the saga definition),
   * used as the {@code X-Saga-Step} correlation value.
   */
  String getStepName();
}
