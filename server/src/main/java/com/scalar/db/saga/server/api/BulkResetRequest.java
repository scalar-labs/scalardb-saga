package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import org.jspecify.annotations.Nullable;

/**
 * Request body for the bulk reset sweep ({@code POST /admin/reset-escalated}): the operator's
 * {@code reason} plus the window and paging that select which escalated sagas to sweep. The status
 * filter is not accepted — the sweep is defined as "escalated sagas", which the engine pins — so
 * only the {@code updatedAt} window and paging are exposed.
 */
public record BulkResetRequest(
    @Nullable String reason,
    @Nullable String updatedAfter,
    @Nullable String updatedBefore,
    @Nullable Integer pageSize,
    @Nullable String pageToken) {

  /**
   * Returns the reason, failing with {@link SagaIllegalArgumentException} (mapped to {@code 400})
   * if it is missing or blank. INVALID_ARGUMENT, not INVALID_REQUEST, for the same reason as {@link
   * InterventionRequest#requireReason}: every transport must classify a blank reason identically.
   *
   * @return the reason
   */
  public String requireReason() {
    if (reason == null || reason.isBlank()) {
      throw new SagaIllegalArgumentException("'reason' is required");
    }
    return reason;
  }

  /**
   * Builds the {@link SagaQuery} selecting the sweep window. The engine pins the status to
   * escalated and applies its own bounds; a malformed timestamp or out-of-range page size fails
   * with {@link SagaInvalidRequestException} ({@code 400}).
   *
   * @return the query
   */
  public SagaQuery toQuery() {
    SagaQuery.Builder builder = SagaQuery.newBuilder();
    if (updatedAfter != null) {
      builder.updatedAfter(RequestParsing.parseInstant(updatedAfter, "updatedAfter"));
    }
    if (updatedBefore != null) {
      builder.updatedBefore(RequestParsing.parseInstant(updatedBefore, "updatedBefore"));
    }
    if (pageSize != null) {
      builder.pageSize(pageSize); // out-of-range -> IllegalArgumentException -> 400
    }
    if (pageToken != null) {
      builder.pageToken(pageToken);
    }
    return builder.build(); // an empty window -> IllegalArgumentException -> 400
  }
}
