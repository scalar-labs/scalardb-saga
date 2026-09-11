package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * Shared request-parameter helpers, turning a malformed value into a {@code 400}. Used by both
 * transports: REST parses strings out of the query string, gRPC reads typed proto fields, but both
 * feed the same api-level builders and owe the caller the same diagnosis.
 */
public final class RequestParsing {

  private RequestParsing() {}

  /**
   * Parses an ISO-8601 instant, throwing {@link SagaInvalidRequestException} (mapped to {@code
   * 400}) if {@code value} is not one.
   *
   * @param value the raw value
   * @param field the field name, for the error message
   * @return the parsed instant
   */
  static Instant parseInstant(String value, String field) {
    try {
      return Instant.parse(value);
    } catch (DateTimeParseException e) {
      throw new SagaInvalidRequestException("'" + field + "' is not a valid ISO-8601 instant");
    }
  }

  /**
   * The inputs a request selects a {@link SagaQuery} with. An assign-only mirror of {@link
   * SagaQuery.Builder}: every setter here stores and returns, and none of them validates.
   *
   * <p>That is the whole point of the type. {@link #buildQuery} converts the builder's stdlib
   * {@link IllegalArgumentException} into a typed one and echoes its wording to the caller, which
   * is only sound for wording the builder actually authored. Collecting the request's values first
   * keeps the caller's own code out of that conversion, so a rejection it raises for its own
   * reasons cannot be re-labelled as the builder's.
   */
  public static final class QueryParams {

    private @Nullable SagaStatus status;
    private @Nullable Instant updatedAfter;
    private @Nullable Instant updatedBefore;
    private @Nullable Integer pageSize;
    private @Nullable String pageToken;

    public QueryParams status(SagaStatus status) {
      this.status = status;
      return this;
    }

    public QueryParams updatedAfter(Instant updatedAfter) {
      this.updatedAfter = updatedAfter;
      return this;
    }

    public QueryParams updatedBefore(Instant updatedBefore) {
      this.updatedBefore = updatedBefore;
      return this;
    }

    public QueryParams pageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public QueryParams pageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }
  }

  /**
   * Collects a request's query parameters through {@code params}, then builds the {@link SagaQuery}
   * they select, converting the builder's stdlib {@link IllegalArgumentException} into {@link
   * SagaIllegalArgumentException}.
   *
   * <p>The conversion lives here, at the wire edge, rather than in {@code SagaQuery.Builder}: the
   * builder is api-module surface that an embedded caller uses directly, where a stdlib {@link
   * IllegalArgumentException} is the idiomatic rejection. Only a value that arrived over the wire
   * needs a {@link com.scalar.db.saga.exception.SagaErrorCode} attached to it.
   *
   * <p>Note where the try begins. {@code params} runs outside it, and the guarded block holds
   * nothing but builder calls, so the echo is of builder-authored wording and nothing else. The
   * block still spans the setters as well as {@code build()}, because the builder validates in both
   * places: {@code pageSize} throws from the setter, an empty {@code updatedAt} window from {@code
   * build()}. Anything a caller's own parsing throws — a typed {@link SagaInvalidRequestException},
   * or a stdlib {@link IllegalArgumentException} out of {@code Integer.parseInt} or {@code
   * Enum.valueOf} — propagates untouched, which for the latter means it reaches the error mapper's
   * catch-all and is logged there rather than being reported to the caller as their own fault.
   *
   * @param params populates the query inputs from the request
   * @return the built query
   */
  public static SagaQuery buildQuery(Consumer<QueryParams> params) {
    QueryParams collected = new QueryParams();
    params.accept(collected);
    try {
      SagaQuery.Builder builder = SagaQuery.newBuilder();
      if (collected.status != null) {
        builder.status(collected.status);
      }
      if (collected.updatedAfter != null) {
        builder.updatedAfter(collected.updatedAfter);
      }
      if (collected.updatedBefore != null) {
        builder.updatedBefore(collected.updatedBefore);
      }
      if (collected.pageSize != null) {
        builder.pageSize(collected.pageSize);
      }
      if (collected.pageToken != null) {
        builder.pageToken(collected.pageToken);
      }
      return builder.build();
    } catch (IllegalArgumentException e) {
      // The builder authors these messages itself, so echoing one is safe and is the whole point:
      // it names the bound and the offending value.
      throw new SagaIllegalArgumentException(
          e.getMessage() == null ? e.toString() : e.getMessage(), e);
    }
  }
}
