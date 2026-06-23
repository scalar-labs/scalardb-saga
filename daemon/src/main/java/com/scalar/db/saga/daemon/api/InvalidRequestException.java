package com.scalar.db.saga.daemon.api;

import java.util.Objects;

/**
 * Thrown when an incoming REST request is malformed or missing a required field. Mapped to HTTP
 * {@code 400} by {@link ErrorMapper}; the message is daemon-owned and safe to return to the caller.
 */
class InvalidRequestException extends RuntimeException {

  InvalidRequestException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }
}
