package com.scalar.db.saga.server.api;

import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.function.Consumer;

/**
 * Shared parsers for request parameters, turning a malformed value into a {@code 400}. Used by both
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
   * Applies {@code steps} to a fresh {@link SagaQuery.Builder} and builds it, converting the
   * builder's stdlib {@link IllegalArgumentException} into {@link SagaIllegalArgumentException}.
   *
   * <p>The conversion lives here, at the wire edge, rather than in {@code SagaQuery.Builder}: the
   * builder is api-module surface that an embedded caller uses directly, where a stdlib {@link
   * IllegalArgumentException} is the idiomatic rejection. Only a value that arrived over the wire
   * needs a {@link com.scalar.db.saga.exception.SagaErrorCode} attached to it.
   *
   * <p>The block covers the setters as well as {@code build()} because the builder validates in
   * both places: {@code pageSize} throws from the setter, an empty {@code updatedAt} window from
   * {@code build()}. Callers whose own parsing throws a typed exception ({@link
   * SagaInvalidRequestException}, say) are unaffected: those are not {@link
   * IllegalArgumentException}s and pass through untouched.
   *
   * @param steps populates the builder from the request
   * @return the built query
   */
  public static SagaQuery buildQuery(Consumer<SagaQuery.Builder> steps) {
    SagaQuery.Builder builder = SagaQuery.newBuilder();
    try {
      steps.accept(builder);
      return builder.build();
    } catch (IllegalArgumentException e) {
      // The builder authors these messages itself, so echoing one is safe and is the whole point:
      // it names the bound and the offending value.
      throw new SagaIllegalArgumentException(
          e.getMessage() == null ? e.toString() : e.getMessage(), e);
    }
  }
}
