package com.scalar.db.saga.server.api;

import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import org.jspecify.annotations.Nullable;

/**
 * Request body for a single-saga admin intervention ({@code recover}, {@code force-complete},
 * {@code reset}): the operator's {@code reason}, recorded on the audit trail. The operator identity
 * itself is never in the body — it is taken from the authenticated request, so a caller cannot
 * forge who acted.
 */
public record InterventionRequest(@Nullable String reason) {

  /**
   * Returns the reason, failing with {@link SagaIllegalArgumentException} (mapped to {@code 400})
   * if it is missing or blank. The engine sanitizes and length-checks it further; this is the edge
   * check that turns a missing reason into a clean {@code 400} rather than a downstream error.
   *
   * <p>INVALID_ARGUMENT, not INVALID_REQUEST: a blank reason is the enum's own worked example of a
   * rejected argument, the engine and the gRPC transport classify it that way (proto3 cannot even
   * distinguish an absent string from a blank one), and the admin javadoc declares
   * SagaIllegalArgumentException — the same mistake must carry the same code on every transport.
   *
   * @return the reason
   */
  public String requireReason() {
    if (reason == null || reason.isBlank()) {
      throw new SagaIllegalArgumentException("'reason' is required");
    }
    return reason;
  }
}
