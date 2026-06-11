package com.scalar.db.saga.invoker;

import com.scalar.db.saga.api.ServiceCallContext;

/**
 * The Confirm phase of a TCC HTTP operation registered on an {@link HttpServiceInvoker}. Receives
 * an {@link HttpCallContext} and the {@link ServiceCallContext}; the engine never reads confirm
 * output.
 */
@FunctionalInterface
public interface HttpConfirmation {

  void apply(HttpCallContext http, ServiceCallContext context) throws HttpCallException;
}
