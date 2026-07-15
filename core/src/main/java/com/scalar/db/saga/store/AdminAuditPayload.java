package com.scalar.db.saga.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaPersistenceException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Codec for the audit payload carried by the operator-intervention status events ({@link
 * EventType#SAGA_FORCE_COMPLETED}, {@link EventType#SAGA_RECOVERED}, {@link EventType#SAGA_RESET}).
 *
 * <p>The payload records who performed the intervention ({@code operator}), why ({@code reason}),
 * and the {@code status} the engine was driven to. The target status is persisted here — not just
 * derived from the event type, as it is for the engine's own status events — because a variable-
 * target intervention ({@code SAGA_RECOVERED}/{@code SAGA_RESET} may drive to {@code COMPENSATING}
 * or {@code RUNNING}) must be reconstructable from the event stream, where the row itself stores
 * only the event type.
 */
public final class AdminAuditPayload {

  private static final String OPERATOR = "operator";
  private static final String REASON = "reason";
  private static final String STATUS = "status";

  private static final ObjectMapper MAPPER = new ObjectMapper().deactivateDefaultTyping();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private AdminAuditPayload() {}

  /** Serializes the operator, reason, and target status into the audit payload JSON. */
  static String encode(String operator, String reason, SagaStatus target) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(OPERATOR, operator);
    map.put(REASON, reason);
    map.put(STATUS, target.getStatusCode());
    try {
      return MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new SagaPersistenceException("Failed to serialize admin audit payload", e);
    }
  }

  /**
   * Reconstructs the target status recorded in {@code payload}. Used to rebuild a variable-target
   * intervention event from the event stream.
   *
   * @throws SagaPersistenceException if the payload is missing or has no valid status code
   */
  static SagaStatus target(@Nullable String payload) {
    Object value = decode(payload).get(STATUS);
    if (!(value instanceof Number number)) {
      throw new SagaPersistenceException(
          "Admin audit payload has no status: " + payload,
          new IllegalStateException("missing status field"));
    }
    try {
      return SagaStatus.fromStatusCode(number.intValue());
    } catch (IllegalArgumentException e) {
      throw new SagaPersistenceException(
          "Admin audit payload has an unknown status: " + payload, e);
    }
  }

  /** The operator recorded in {@code payload}, or {@code null} if absent. */
  public static @Nullable String operator(@Nullable String payload) {
    return decode(payload).get(OPERATOR) instanceof String operator ? operator : null;
  }

  /** The reason recorded in {@code payload}, or {@code null} if absent. */
  public static @Nullable String reason(@Nullable String payload) {
    return decode(payload).get(REASON) instanceof String reason ? reason : null;
  }

  private static Map<String, Object> decode(@Nullable String payload) {
    if (payload == null || payload.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      Map<String, Object> map = MAPPER.readValue(payload, MAP_TYPE);
      return map == null ? Collections.emptyMap() : map;
    } catch (JsonProcessingException e) {
      throw new SagaPersistenceException("Failed to parse admin audit payload", e);
    }
  }
}
