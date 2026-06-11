package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.ServiceCallContext;
import java.util.Optional;

/** A {@link ServiceCallContext} that adds the step name to a delegate {@link SagaContext}. */
final class DelegatingServiceCallContext implements ServiceCallContext {

  private final String stepName;
  private final SagaContext delegate;

  DelegatingServiceCallContext(String stepName, SagaContext delegate) {
    this.stepName = stepName;
    this.delegate = delegate;
  }

  @Override
  public String getStepName() {
    return stepName;
  }

  @Override
  public String getSagaId() {
    return delegate.getSagaId();
  }

  @Override
  public <T> Optional<T> get(String key, Class<T> type) {
    return delegate.get(key, type);
  }
}
