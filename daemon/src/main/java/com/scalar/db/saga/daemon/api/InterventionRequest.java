package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.exception.SagaInvalidRequestException;
import org.jspecify.annotations.Nullable;

/**
 * Request body for a single-saga admin intervention ({@code recover}, {@code force-complete},
 * {@code reset}): the operator's {@code reason}, recorded on the audit trail. The operator identity
 * itself is never in the body — it is taken from the authenticated request, so a caller cannot
 * forge who acted.
 */
public record InterventionRequest(@Nullable String reason) {

  /**
   * Returns the reason, failing with {@link SagaInvalidRequestException} (mapped to {@code 400}) if
   * it is missing or blank. The engine sanitizes and length-checks it further; this is the edge
   * check that turns a missing reason into a clean {@code 400} rather than a downstream error.
   *
   * @return the reason
   */
  public String requireReason() {
    if (reason == null || reason.isBlank()) {
      throw new SagaInvalidRequestException("'reason' is required");
    }
    return reason;
  }
}
