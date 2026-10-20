package com.scalar.db.saga.transport;

import org.jspecify.annotations.Nullable;

/**
 * Supplies the URLs a participant calls back on for an async (parked) step: the callback URL to
 * complete it, and the failure URL to report that it failed. Injected into the outgoing request (as
 * the {@code X-Saga-Callback-Url} and {@code X-Saga-Failure-Url} headers) when a declarative step
 * is async-capable.
 *
 * <p>The core engine holds this as an opaque seam: it never sees a secret or computes a signature.
 * In daemon mode the implementation mints signed, per-step URLs (its externally-reachable base URL
 * plus an HMAC token over the saga id, step name, route, and issue time). Returns {@code null} when
 * async completion is not configured, so the engine simply omits the headers.
 */
public interface CallbackUrlProvider {

  /**
   * Returns the full callback URL (including any auth token) for completing {@code stepName} of
   * {@code sagaId}, or {@code null} if async completion is not configured.
   *
   * @param sagaId the saga whose step the callback completes
   * @param stepName the async step the callback completes
   * @return the URL to send as {@code X-Saga-Callback-Url}, or {@code null} to omit the header
   */
  @Nullable String callbackUrl(String sagaId, String stepName);

  /**
   * Returns the URL a participant calls to report that {@code stepName} of {@code sagaId} failed,
   * sent as {@code X-Saga-Failure-Url} alongside the callback URL, or {@code null} to omit it.
   * Defaults to {@code null}, for a provider with no failure route.
   *
   * @param sagaId the saga whose step the participant reports on
   * @param stepName the async step the participant reports on
   * @return the URL to send as {@code X-Saga-Failure-Url}, or {@code null} to omit the header
   */
  default @Nullable String failureUrl(String sagaId, String stepName) {
    return null;
  }
}
