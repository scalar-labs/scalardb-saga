package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.api.SagaQuery;
import java.time.Instant;
import java.time.format.DateTimeParseException;
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
   * Returns the reason, failing with {@link InvalidRequestException} (mapped to {@code 400}) if it
   * is missing or blank.
   *
   * @return the reason
   */
  public String requireReason() {
    if (reason == null || reason.isBlank()) {
      throw new InvalidRequestException("'reason' is required");
    }
    return reason;
  }

  /**
   * Builds the {@link SagaQuery} selecting the sweep window. The engine pins the status to
   * escalated and applies its own bounds; a malformed timestamp or out-of-range page size fails
   * with {@link InvalidRequestException} ({@code 400}).
   *
   * @return the query
   */
  public SagaQuery toQuery() {
    SagaQuery.Builder builder = SagaQuery.newBuilder();
    if (updatedAfter != null) {
      builder.updatedAfter(parseInstant(updatedAfter, "updatedAfter"));
    }
    if (updatedBefore != null) {
      builder.updatedBefore(parseInstant(updatedBefore, "updatedBefore"));
    }
    if (pageSize != null) {
      builder.pageSize(pageSize); // out-of-range -> IllegalArgumentException -> 400
    }
    if (pageToken != null) {
      builder.pageToken(pageToken);
    }
    return builder.build(); // an empty window -> IllegalArgumentException -> 400
  }

  private static Instant parseInstant(String value, String field) {
    try {
      return Instant.parse(value);
    } catch (DateTimeParseException e) {
      throw new InvalidRequestException("'" + field + "' is not a valid ISO-8601 instant");
    }
  }
}
