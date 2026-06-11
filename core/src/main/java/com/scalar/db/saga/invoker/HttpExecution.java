package com.scalar.db.saga.invoker;

import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.StepResult;

/**
 * A forward HTTP execution registered on an {@link HttpServiceInvoker}. Receives an {@link
 * HttpCallContext} (to make the call) and the {@link ServiceCallContext} (to read saga data), and
 * returns the step output.
 */
@FunctionalInterface
public interface HttpExecution {

  StepResult apply(HttpCallContext http, ServiceCallContext context) throws HttpCallException;
}
