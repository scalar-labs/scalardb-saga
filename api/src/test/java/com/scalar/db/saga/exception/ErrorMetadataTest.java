package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ErrorMetadataTest {

  @Test
  void of_noArgs_returnsEmptyMap() {
    // Arrange & Act & Assert
    assertThat(ErrorMetadata.of()).isEmpty();
  }

  @Test
  void of_oneKeyValue_returnsSingleEntryMap() {
    // Arrange & Act
    Map<String, String> m = ErrorMetadata.of("saga_id", "s-1");

    // Assert
    assertThat(m).containsExactly(entry("saga_id", "s-1"));
  }

  @Test
  void of_twoKeyValues_preservesInsertionOrder() {
    // Arrange & Act — insertion order matters for readable debugger inspection; the schema drives
    // the wire/log order via SagaErrorCode.buildMessage, not this map.
    Map<String, String> m = ErrorMetadata.of("saga_name", "orders", "step_name", "debit");

    // Assert
    assertThat(m.keySet()).containsExactly("saga_name", "step_name");
    assertThat(m).containsEntry("saga_name", "orders").containsEntry("step_name", "debit");
  }

  @Test
  void of_threeKeyValues_preservesInsertionOrder() {
    // Arrange & Act
    Map<String, String> m =
        ErrorMetadata.of("saga_id", "s-1", "current_state", "RUNNING", "requested_operation", "x");

    // Assert
    assertThat(m.keySet()).containsExactly("saga_id", "current_state", "requested_operation");
  }

  @Test
  void of_returnedMap_isUnmodifiable() {
    // Arrange
    Map<String, String> m = ErrorMetadata.of("saga_id", "s-1");

    // Act & Assert
    assertThatThrownBy(() -> m.put("uninvited", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void of_returnedEmptyMap_isUnmodifiable() {
    // Arrange
    Map<String, String> m = ErrorMetadata.of();

    // Act & Assert
    assertThatThrownBy(() -> m.put("uninvited", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static Map.Entry<String, String> entry(String key, String value) {
    return new java.util.AbstractMap.SimpleImmutableEntry<>(key, value);
  }
}
