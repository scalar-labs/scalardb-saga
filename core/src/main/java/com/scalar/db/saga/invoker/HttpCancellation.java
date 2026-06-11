package com.scalar.db.saga.invoker;

import com.scalar.db.saga.api.ServiceCallContext;

/**
 * The Cancel phase of a TCC HTTP operation registered on an {@link HttpServiceInvoker}. Receives an
 * {@link HttpCallContext} and the {@link ServiceCallContext}; the engine never reads cancel output.
 */
@FunctionalInterface
public interface HttpCancellation {

  void apply(HttpCallContext http, ServiceCallContext context) throws HttpCallException;
}
