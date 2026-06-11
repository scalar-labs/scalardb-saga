package com.scalar.db.saga.invoker;

import com.scalar.db.saga.api.ServiceCallContext;

/**
 * A compensating HTTP action registered on an {@link HttpServiceInvoker}. Receives an {@link
 * HttpCallContext} and the {@link ServiceCallContext}; the engine never reads compensation output.
 */
@FunctionalInterface
public interface HttpCompensation {

  void apply(HttpCallContext http, ServiceCallContext context) throws HttpCallException;
}
