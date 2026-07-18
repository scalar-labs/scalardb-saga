package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.exception.SagaPersistenceException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EventPayloadSerializerTest {

  @Test
  void serialize_mapGiven_returnsJsonString() {
    // Arrange
    Map<String, Object> data = Map.of("key", "value", "count", 42);

    // Act
    String json = EventPayloadSerializer.serialize(data);

    // Assert
    assertThat(json).isNotNull();
    assertThat(json).contains("\"key\"");
    assertThat(json).contains("\"value\"");
    assertThat(json).contains("\"count\"");
    assertThat(json).contains("42");
  }

  @Test
  void serialize_emptyMapGiven_returnsEmptyJson() {
    // Act
    String json = EventPayloadSerializer.serialize(Collections.emptyMap());

    // Assert
    assertThat(json).isEqualTo("{}");
  }

  @Test
  void serialize_nullGiven_returnsNull() {
    // Act
    String json = EventPayloadSerializer.serialize(null);

    // Assert
    assertThat(json).isNull();
  }

  @Test
  void deserializeMap_jsonGiven_returnsMap() {
    // Arrange
    String json = "{\"key\":\"value\",\"count\":42}";

    // Act
    Map<String, Object> result = EventPayloadSerializer.deserializeMap(json);

    // Assert
    assertThat(result).containsEntry("key", "value").containsEntry("count", 42);
  }

  @Test
  void deserializeMap_nullGiven_returnsEmptyMap() {
    // Act
    Map<String, Object> result = EventPayloadSerializer.deserializeMap(null);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  void deserializeMap_emptyStringGiven_returnsEmptyMap() {
    // Act
    Map<String, Object> result = EventPayloadSerializer.deserializeMap("");

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  void deserializeMap_malformedJsonGiven_throwsNonRetryablePersistenceException() {
    // Act & Assert — a malformed payload is a permanent (non-retryable) failure: retrying the
    // deserialize can never succeed, so it must not be surfaced to clients as a retryable error.
    assertThatThrownBy(() -> EventPayloadSerializer.deserializeMap("{not json"))
        .isInstanceOfSatisfying(
            SagaPersistenceException.class, e -> assertThat(e.isRetryable()).isFalse());
  }

  @Test
  void serializeError_exceptionGiven_returnsErrorJson() {
    // Arrange
    RuntimeException e = new RuntimeException("something went wrong");

    // Act
    String json = EventPayloadSerializer.serializeError(e, false);

    // Assert
    Map<String, Object> result = EventPayloadSerializer.deserializeMap(json);
    assertThat(result).containsEntry("type", "java.lang.RuntimeException");
    assertThat(result).containsEntry("message", "something went wrong");
    assertThat(EventPayloadSerializer.isKnownNotCommitted(json)).isFalse();
  }

  @Test
  void serializeError_knownNotCommittedTrue_roundTripsThroughIsKnownNotCommitted() {
    // Arrange
    RuntimeException e = new RuntimeException("refused");

    // Act
    String json = EventPayloadSerializer.serializeError(e, true);

    // Assert — the flag survives persistence so recovery can skip the failed step.
    assertThat(EventPayloadSerializer.isKnownNotCommitted(json)).isTrue();
    assertThat(EventPayloadSerializer.deserializeMap(json))
        .containsEntry("knownNotCommitted", true);
  }

  @Test
  void isKnownNotCommitted_nullOrLegacyOrMalformedPayload_isFalse() {
    // A null, legacy (pre-flag), or unparseable payload defaults to false — the safe value.
    assertThat(EventPayloadSerializer.isKnownNotCommitted(null)).isFalse();
    assertThat(EventPayloadSerializer.isKnownNotCommitted("")).isFalse();
    assertThat(EventPayloadSerializer.isKnownNotCommitted("{\"type\":\"x\",\"message\":\"y\"}"))
        .isFalse(); // legacy payload without the flag
    assertThat(EventPayloadSerializer.isKnownNotCommitted("not json")).isFalse();
    // A JSON null literal deserializes to a null map — must not NPE.
    assertThat(EventPayloadSerializer.isKnownNotCommitted("null")).isFalse();
  }

  @Test
  void errorMessage_serializedErrorGiven_returnsMessage() {
    // Arrange
    String json = EventPayloadSerializer.serializeError(new RuntimeException("refused"), false);

    // Act
    String message = EventPayloadSerializer.errorMessage(json);

    // Assert
    assertThat(message).isEqualTo("refused");
  }

  @Test
  void errorMessage_payloadWithoutStringMessageGiven_returnsNull() {
    // A payload that simply recorded no message is reported as absent, not as unreadable.
    assertThat(EventPayloadSerializer.errorMessage(null)).isNull();
    assertThat(EventPayloadSerializer.errorMessage("")).isNull();
    assertThat(EventPayloadSerializer.errorMessage("{\"type\":\"x\"}")).isNull();
    assertThat(EventPayloadSerializer.errorMessage("{\"message\":42}")).isNull();
    // A JSON null literal deserializes to a null map — must not NPE.
    assertThat(EventPayloadSerializer.errorMessage("null")).isNull();
  }

  @Test
  void errorMessage_unparseablePayloadGiven_returnsPlaceholderInsteadOfThrowing() {
    // An undecodable payload must degrade so one bad step failure cannot fail a whole timeline
    // read; the placeholder keeps it distinguishable from a payload carrying no message.
    assertThat(EventPayloadSerializer.errorMessage("not json"))
        .isEqualTo(EventPayloadSerializer.UNREADABLE_MESSAGE);
    assertThat(EventPayloadSerializer.errorMessage("{\"message\":\"truncated"))
        .isEqualTo(EventPayloadSerializer.UNREADABLE_MESSAGE);
  }

  @Test
  void roundTrip_mapSerializedAndDeserialized_equalsOriginal() {
    // Arrange
    Map<String, Object> original = Map.of("name", "test", "amount", 100, "active", true);

    // Act
    String json = EventPayloadSerializer.serialize(original);
    Map<String, Object> restored = EventPayloadSerializer.deserializeMap(json);

    // Assert
    assertThat(restored).isEqualTo(original);
  }

  @Test
  void roundTrip_bigDecimalGiven_preservesPrecision() {
    // Arrange
    Map<String, Object> original = Map.of("amount", new BigDecimal("100.50"));

    // Act
    String json = EventPayloadSerializer.serialize(original);
    Map<String, Object> restored = EventPayloadSerializer.deserializeMap(json);

    // Assert — deserialized as BigDecimal, not Double
    assertThat(restored.get("amount")).isInstanceOf(BigDecimal.class);
    assertThat(restored.get("amount")).isEqualTo(new BigDecimal("100.50"));
  }
}
