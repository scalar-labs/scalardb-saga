package com.scalar.db.saga.server.api;

import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaRuntimeException;
import java.util.Objects;

/**
 * Thrown when a caller exceeds the configured saga-start rate limit. Extends {@link
 * SagaRuntimeException} carrying {@link SagaErrorCode#RATE_LIMIT_EXCEEDED}; maps to HTTP {@code 429
 * Too Many Requests} via the mapper's per-type table.
 *
 * <p>The specific reason (which principal, which operation) is a daemon-internal detail — the wire
 * body carries only the fixed generic message. Callers pass the reason for server-side logging via
 * {@link #getInternalDetail()}.
 */
public final class RateLimitExceededException extends SagaRuntimeException {

  private static final long serialVersionUID = 1L;

  /** The server-side-only reason, never sent on the wire. */
  private final String internalDetail;

  /** The advisory wait until the caller's window resets, in milliseconds. */
  private final long retryAfterMillis;

  /**
   * Creates the exception; the REST rate-limit handler throws it when the shared {@link
   * RateLimiter} refuses a start.
   *
   * @param internalDetail which principal and operation were refused, for the server-side log only
   * @param retryAfterMillis the advisory wait until the caller's window resets, in milliseconds
   * @throws IllegalArgumentException if {@code retryAfterMillis} is not positive
   */
  public RateLimitExceededException(String internalDetail, long retryAfterMillis) {
    super(SagaErrorCode.RATE_LIMIT_EXCEEDED, ErrorMetadata.of());
    this.internalDetail = Objects.requireNonNull(internalDetail, "internalDetail must not be null");
    if (retryAfterMillis <= 0) {
      throw new IllegalArgumentException(
          "retryAfterMillis must be positive, got " + retryAfterMillis);
    }
    this.retryAfterMillis = retryAfterMillis;
  }

  /**
   * The advisory wait until the caller's window resets; the 429's Retry-After derives from it.
   *
   * @return the wait in milliseconds, always positive
   */
  public long getRetryAfterMillis() {
    return retryAfterMillis;
  }

  /**
   * The server-side-only reason (never sent on the wire); used by the daemon's log statement.
   *
   * @return the reason, never {@code null}
   */
  public String getInternalDetail() {
    return internalDetail;
  }
}
