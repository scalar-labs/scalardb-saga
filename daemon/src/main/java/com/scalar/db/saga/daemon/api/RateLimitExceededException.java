package com.scalar.db.saga.daemon.api;

/**
 * Thrown when a caller exceeds the configured saga-start rate limit. Maps to HTTP {@code 429 Too
 * Many Requests}.
 */
public final class RateLimitExceededException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RateLimitExceededException(String message) {
    super(message);
  }
}
