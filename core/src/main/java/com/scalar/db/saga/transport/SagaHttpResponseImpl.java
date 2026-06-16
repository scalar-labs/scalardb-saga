package com.scalar.db.saga.transport;

import com.scalar.db.saga.api.SagaHttpResponse;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Internal {@link SagaHttpResponse} backed by an {@link HttpCallResponse} from the shared {@link
 * HttpExchange}. Decode failures from the underlying response surface as a non-retryable {@link
 * StepExecutionException} so a code step's body-decode error is engine-recognized like any other
 * forward failure.
 */
final class SagaHttpResponseImpl implements SagaHttpResponse {

  private final HttpCallResponse delegate;

  SagaHttpResponseImpl(HttpCallResponse delegate) {
    this.delegate = delegate;
  }

  @Override
  public int status() {
    return delegate.status();
  }

  @Override
  public Map<String, List<String>> headers() {
    return delegate.headers();
  }

  @Override
  public Optional<String> header(String name) {
    return delegate.header(name);
  }

  @Override
  public Map<String, Object> bodyJsonObject() throws StepExecutionException {
    try {
      return delegate.bodyJsonObject();
    } catch (HttpCallException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public <T> T bodyJson(Class<T> type) throws StepExecutionException {
    try {
      return delegate.bodyJson(type);
    } catch (HttpCallException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public String bodyString() {
    return delegate.bodyString();
  }

  @Override
  public byte[] bodyBytes() {
    return delegate.bodyBytes();
  }
}
