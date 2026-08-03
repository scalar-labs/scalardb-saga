package com.scalar.db.saga.server.api;

import java.util.Objects;

/**
 * Thrown when an incoming request is malformed or missing a required field. Mapped to HTTP {@code
 * 400} by {@link ErrorMapper} (REST) and to {@code INVALID_ARGUMENT} by the gRPC error mapper; the
 * message is daemon-owned and safe to return to the caller.
 */
public final class InvalidRequestException extends RuntimeException {

  public InvalidRequestException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }
}
