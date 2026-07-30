package com.scalar.db.saga.daemon.api;

import com.scalar.db.saga.exception.SagaInvalidRequestException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Request body for starting a saga: the definition {@code sagaName} and the initial {@code input}
 * for the saga context.
 */
public record StartSagaRequest(@Nullable String sagaName, @Nullable Map<String, Object> input) {

  /** Defensively copies {@code input} (null-tolerant) so the request is immutable. */
  public StartSagaRequest {
    input = (input == null) ? null : Collections.unmodifiableMap(new LinkedHashMap<>(input));
  }

  /**
   * Returns the saga definition name, failing with {@link SagaInvalidRequestException} (mapped to
   * {@code 400}) if it is missing or blank.
   *
   * @return the saga definition name
   */
  public String requireSagaName() {
    if (sagaName == null || sagaName.isBlank()) {
      throw new SagaInvalidRequestException("'sagaName' is required");
    }
    return sagaName;
  }

  /** Returns the input map, or an empty map if none was provided. */
  public Map<String, Object> inputOrEmpty() {
    return input == null ? Map.of() : input;
  }
}
