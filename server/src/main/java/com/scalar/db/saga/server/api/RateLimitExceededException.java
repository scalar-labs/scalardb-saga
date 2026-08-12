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

  private final String internalDetail;

  public RateLimitExceededException(String internalDetail) {
    super(SagaErrorCode.RATE_LIMIT_EXCEEDED, ErrorMetadata.of());
    this.internalDetail = Objects.requireNonNull(internalDetail, "internalDetail must not be null");
  }

  /** The server-side-only reason (never sent on the wire); used by the daemon's log statement. */
  public String getInternalDetail() {
    return internalDetail;
  }
}
