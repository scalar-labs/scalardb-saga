package com.scalar.db.saga.daemon.api;

import java.util.Objects;

/**
 * Thrown when an async-callback request carries a missing or invalid HMAC token. Mapped to HTTP
 * {@code 401} by {@link ErrorMapper}. The message is daemon-owned and used only for server-side
 * reasoning; the mapped response is deliberately generic and does not distinguish a missing token
 * from an invalid one.
 */
public final class CallbackAuthException extends RuntimeException {

  public CallbackAuthException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }
}
