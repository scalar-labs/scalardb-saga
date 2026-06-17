package com.scalar.db.saga.transport;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Thrown by an HTTP call ({@link HttpExchange}) when it fails. Carries a {@code retryable} flag
 * derived from the response status (or the {@code X-Saga-Retryable} override). The transport
 * adapter maps it to a {@code StepExecutionException} (forward) or {@code
 * StepCompensationException} (compensation).
 *
 * <p>On a non-2xx HTTP response the exception also carries the {@linkplain #response() response} so
 * a caller can inspect the error body/status. For transport, policy, encode, or decode failures
 * there is no response and {@link #response()} is empty.
 */
public final class HttpCallException extends Exception {

  private final boolean retryable;
  private final transient @Nullable HttpCallResponse response;

  public HttpCallException(String message, boolean retryable) {
    super(message);
    this.retryable = retryable;
    this.response = null;
  }

  public HttpCallException(String message, Throwable cause, boolean retryable) {
    super(message, cause);
    this.retryable = retryable;
    this.response = null;
  }

  HttpCallException(String message, boolean retryable, HttpCallResponse response) {
    super(message);
    this.retryable = retryable;
    this.response = response;
  }

  public boolean isRetryable() {
    return retryable;
  }

  /**
   * The HTTP response that caused this failure, present only for non-2xx responses. Empty for
   * transport, policy, encode, or decode failures (no response was received or parsed).
   */
  public Optional<HttpCallResponse> response() {
    return Optional.ofNullable(response);
  }
}
