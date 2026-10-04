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
 *
 * @param reason why the operator is sweeping, recorded on each reset saga's audit trail; required,
 *     checked by {@link #requireReason()}
 * @param updatedAfter inclusive lower bound on {@code updatedAt}, an ISO-8601 instant, or absent
 *     for none
 * @param updatedBefore inclusive upper bound on {@code updatedAt}, an ISO-8601 instant, or absent
 *     for none
 * @param pageSize the number of sagas to sweep in this call, or absent for the query's default
 * @param pageToken the {@code nextPageToken} of the previous response, or absent to start the sweep
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
   * escalated and applies its own bounds. A malformed timestamp fails with {@link
   * SagaInvalidRequestException}; an out-of-range page size or an empty window fails with {@link
   * SagaIllegalArgumentException} naming the bound and the offending value. Both are {@code 400}.
   *
   * @return the query
   */
  public SagaQuery toQuery() {
    return RequestParsing.buildQuery(
        params -> {
          if (updatedAfter != null) {
            params.updatedAfter(RequestParsing.parseInstant(updatedAfter, "updatedAfter"));
          }
          if (updatedBefore != null) {
            params.updatedBefore(RequestParsing.parseInstant(updatedBefore, "updatedBefore"));
          }
          if (pageSize != null) {
            params.pageSize(pageSize);
          }
          if (pageToken != null) {
            params.pageToken(pageToken);
          }
        });
  }
}
