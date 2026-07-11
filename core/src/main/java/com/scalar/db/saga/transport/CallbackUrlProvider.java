package com.scalar.db.saga.transport;

import org.jspecify.annotations.Nullable;

/**
 * Supplies the callback URL a participant should call to complete an async (parked) step. Injected
 * into the outgoing request (as the {@code X-Saga-Callback-Url} header) when a declarative step is
 * async-capable.
 *
 * <p>The core engine holds this as an opaque seam: it never sees a secret or computes a signature.
 * In daemon mode the implementation mints a signed, per-step URL (its externally-reachable base URL
 * plus an HMAC token over the saga id, step name, and issue time). Returns {@code null} when async
 * completion is not configured, so the engine simply omits the header.
 */
public interface CallbackUrlProvider {

  /**
   * Returns the full callback URL (including any auth token) for completing {@code stepName} of
   * {@code sagaId}, or {@code null} if async completion is not configured.
   */
  @Nullable String callbackUrl(String sagaId, String stepName);
}
