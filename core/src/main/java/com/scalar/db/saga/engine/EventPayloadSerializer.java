package com.scalar.db.saga.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Collections;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/** Package-private utility for serializing/deserializing SagaEvent payloads. */
final class EventPayloadSerializer {

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

  /** Serializes an exception to error JSON with type and message. */
  static String serializeError(Exception e) {
    try {
      return MAPPER.writeValueAsString(
          Map.of(
              "type",
              e.getClass().getName(),
              "message",
              e.getMessage() != null ? e.getMessage() : ""));
    } catch (JsonProcessingException ex) {
      throw new SagaPersistenceException("Failed to serialize error payload", ex);
    }
  }
}
