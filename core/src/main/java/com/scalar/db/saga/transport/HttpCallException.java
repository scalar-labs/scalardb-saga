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

  /** Whether the failure is transient and the call may be retried. */
  private final boolean retryable;

  /** Whether the call is proven not to have reached the participant. */
  private final boolean knownNotCommitted;

  private final transient @Nullable HttpCallResponse response;

  /**
   * A failure described by {@code message} alone, with no cause and no response, such as a response
   * body over the policy's size limit. The side effect is treated as possibly committed.
   *
   * @param message what failed
   * @param retryable whether the call may be retried
   */
  public HttpCallException(String message, boolean retryable) {
    super(message);
    this.retryable = retryable;
    this.knownNotCommitted = false;
    this.response = null;
  }

  /**
   * A failure caused by {@code cause}, treated as possibly committed: a timeout, an interrupt, or a
   * response that could not be decoded.
   *
   * @param message what failed
   * @param cause the underlying failure
   * @param retryable whether the call may be retried
   */
  public HttpCallException(String message, Throwable cause, boolean retryable) {
    this(message, cause, retryable, false);
  }

  /**
   * A failure caused by {@code cause}, with the commit knowledge stated. The HTTP engine uses this
   * for failures proven to happen before the request reached the participant: a policy refusal, a
   * malformed URI or body, a retired or shut-down client, or a connection that was never made.
   *
   * @param message what failed
   * @param cause the underlying failure, or {@code null} when the message says it all
   * @param retryable whether the call may be retried
   * @param knownNotCommitted {@code true} when the call is proven not to have reached the
   *     participant
   */
  public HttpCallException(
      String message, @Nullable Throwable cause, boolean retryable, boolean knownNotCommitted) {
    super(message, cause);
    this.retryable = retryable;
    this.knownNotCommitted = knownNotCommitted;
    this.response = null;
  }

  HttpCallException(String message, boolean retryable, HttpCallResponse response) {
    super(message);
    this.retryable = retryable;
    this.knownNotCommitted = false;
    this.response = response;
  }

  /**
   * Whether the failure is transient and the call may be retried, as derived from the response
   * status or the {@code X-Saga-Retryable} override.
   *
   * @return {@code true} if the call may be retried
   */
  public boolean isRetryable() {
    return retryable;
  }

  /**
   * Whether the framework can prove this call's side effect did <b>not</b> commit — the call failed
   * before reaching the participant, or is proven not to have reached it (e.g. connection refused,
   * DNS failure, TLS handshake failure, or a pre-send error). Default {@code false}: an HTTP status
   * response (any 4xx/5xx) or an ambiguous I/O failure may have committed, so the failed step is
   * compensated.
   *
   * @return {@code true} only when the side effect is proven not to have committed
   */
  public boolean knownNotCommitted() {
    return knownNotCommitted;
  }

  /**
   * The HTTP response that caused this failure, present only for non-2xx responses. Empty for
   * transport, policy, encode, or decode failures (no response was received or parsed).
   *
   * @return the failing response, or empty when none was received or parsed
   */
  public Optional<HttpCallResponse> response() {
    return Optional.ofNullable(response);
  }
}
