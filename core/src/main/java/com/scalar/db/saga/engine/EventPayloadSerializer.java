package com.scalar.db.saga.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Serializes/deserializes SagaEvent payloads.
 *
 * <p>This type is {@code public} solely for cross-package access within the module (the recovery
 * package reads the {@code knownNotCommitted} flag persisted on a STEP_FAILED payload); it is not
 * part of the user-facing API.
 */
public final class EventPayloadSerializer {

  private static final String TYPE = "type";
  private static final String MESSAGE = "message";
  private static final String KNOWN_NOT_COMMITTED = "knownNotCommitted";

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          // Defense in depth against polymorphic-deserialization gadgets (off by default in 2.x).
          .deactivateDefaultTyping();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private EventPayloadSerializer() {}

  /** Serializes a map to a JSON string. Returns {@code null} for {@code null} input. */
  static @Nullable String serialize(@Nullable Map<String, Object> data) {
    if (data == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new SagaPersistenceException("Failed to serialize event payload", e);
    }
  }

  /** Deserializes a JSON string to a map. Returns an empty map for {@code null} or empty input. */
  static Map<String, Object> deserializeMap(@Nullable String json) {
    if (json == null || json.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      return MAPPER.readValue(json, MAP_TYPE);
    } catch (JsonProcessingException e) {
      throw new SagaPersistenceException("Failed to deserialize event payload", e);
    }
  }

  /** Serializes an exception to error JSON with type, message, and the knownNotCommitted flag. */
  static String serializeError(Exception e, boolean knownNotCommitted) {
    Map<String, Object> error = new LinkedHashMap<>();
    error.put(TYPE, e.getClass().getName());
    error.put(MESSAGE, e.getMessage() != null ? e.getMessage() : "");
    error.put(KNOWN_NOT_COMMITTED, knownNotCommitted);
    try {
      return MAPPER.writeValueAsString(error);
    } catch (JsonProcessingException ex) {
      throw new SagaPersistenceException("Failed to serialize error payload", ex);
    }
  }

  /**
   * Whether a STEP_FAILED error {@code payload} marks the failure as known-not-committed. Defaults
   * to {@code false} — the safe value — for a {@code null}, legacy (pre-flag), or unparseable
   * payload, so recovery compensates the failed step unless non-delivery was positively recorded.
   */
  public static boolean isKnownNotCommitted(@Nullable String payload) {
    if (payload == null || payload.isEmpty()) {
      return false;
    }
    try {
      Object value = MAPPER.readValue(payload, MAP_TYPE).get(KNOWN_NOT_COMMITTED);
      return value instanceof Boolean b && b;
    } catch (JsonProcessingException e) {
      return false;
    }
  }
}
