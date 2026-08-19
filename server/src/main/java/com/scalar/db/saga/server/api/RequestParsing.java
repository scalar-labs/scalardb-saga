package com.scalar.db.saga.server.api;

import com.scalar.db.saga.exception.SagaInvalidRequestException;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/** Shared parsers for request parameters, turning a malformed value into a {@code 400}. */
final class RequestParsing {

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
}
