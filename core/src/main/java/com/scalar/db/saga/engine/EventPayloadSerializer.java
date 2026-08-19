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

  /** Stands in for a step failure's error text when its payload cannot be decoded. */
  public static final String UNREADABLE_MESSAGE = "<unreadable payload>";

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
      throw SagaPersistenceException.serializationFailed(e);
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
      throw SagaPersistenceException.deserializationFailed(e);
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
      throw SagaPersistenceException.serializationFailed(ex);
    }
  }

  /**
   * The {@code message} field of a serialized error {@code payload} (see {@link #serializeError}),
   * or {@code null} if the payload is absent or carries no string message. Used to surface a step
   * failure's error text on the admin timeline without exposing the raw payload.
   *
   * <p>An unparseable payload yields {@link #UNREADABLE_MESSAGE} rather than propagating, so a
   * single bad payload degrades one timeline entry instead of failing the whole read. The
   * placeholder is distinct from {@code null} so an operator can tell a corrupt payload from one
   * that simply recorded no message.
   */
  public static @Nullable String errorMessage(@Nullable String payload) {
    if (payload == null || payload.isEmpty()) {
      return null;
    }
    Map<String, Object> map;
    try {
      map = MAPPER.readValue(payload, MAP_TYPE);
    } catch (JsonProcessingException e) {
      return UNREADABLE_MESSAGE;
    }
    if (map == null) {
      // A JSON null literal ("null") deserializes to a null map; it carries no message.
      return null;
    }
    return map.get(MESSAGE) instanceof String message ? message : null;
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
      Map<String, Object> map = MAPPER.readValue(payload, MAP_TYPE);
      if (map == null) {
        // A JSON null literal ("null") deserializes to a null map; treat it as the safe default.
        return false;
      }
      Object value = map.get(KNOWN_NOT_COMMITTED);
      return value instanceof Boolean b && b;
    } catch (JsonProcessingException e) {
      return false;
    }
  }
}
