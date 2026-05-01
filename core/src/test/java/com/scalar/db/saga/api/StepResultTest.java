package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StepResultTest {

  @Test
  void of_singleKeyValueGiven_returnsResultWithEntry() {
    // Arrange & Act
    StepResult result = StepResult.of("orderId", "123");

    // Assert
    assertThat(result.isPending()).isFalse();
    assertThat(result.getOutput()).containsEntry("orderId", "123");
    assertThat(result.getOutput()).hasSize(1);
  }

  @Test
  void of_mapGiven_returnsResultWithAllEntries() {
    // Arrange
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("key1", "value1");
    data.put("key2", 42);

    // Act
    StepResult result = StepResult.of(data);

    // Assert
    assertThat(result.isPending()).isFalse();
    assertThat(result.getOutput()).containsEntry("key1", "value1");
    assertThat(result.getOutput()).containsEntry("key2", 42);
    assertThat(result.getOutput()).hasSize(2);
  }

  @Test
  void of_originalMapModifiedAfterCreation_doesNotAffectResult() {
    // Arrange
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("key1", "value1");

    // Act
    StepResult result = StepResult.of(data);
    data.put("key2", "value2");

    // Assert — modifying the original map should not affect the result
    assertThat(result.getOutput()).hasSize(1);
    assertThat(result.getOutput()).doesNotContainKey("key2");
  }

  @Test
  void of_mapGiven_returnsUnmodifiableOutput() {
    // Arrange
    Map<String, Object> data = Map.of("k", "v");
    StepResult result = StepResult.of(data);

    // Act & Assert
    assertThatThrownBy(() -> result.getOutput().put("new", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void of_mapWithNullKeyGiven_throwsNullPointerException() {
    // Arrange
    Map<String, Object> data = new HashMap<>();
    data.put(null, "value");

    // Act & Assert
    assertThatThrownBy(() -> StepResult.of(data)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void of_mapWithNullValueGiven_throwsNullPointerException() {
    // Arrange
    Map<String, Object> data = new HashMap<>();
    data.put("key", null);

    // Act & Assert
    assertThatThrownBy(() -> StepResult.of(data)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void empty_called_returnsNonPendingEmptyResult() {
    // Arrange & Act
    StepResult result = StepResult.empty();

    // Assert
    assertThat(result.isPending()).isFalse();
    assertThat(result.getOutput()).isEmpty();
  }

  @Test
  void empty_calledTwice_returnsSameInstance() {
    // Act & Assert
    assertThat(StepResult.empty()).isSameAs(StepResult.empty());
  }

  @Test
  void pending_called_returnsPendingResult() {
    // Arrange & Act
    StepResult result = StepResult.pending();

    // Assert
    assertThat(result.isPending()).isTrue();
    assertThat(result.getOutput()).isEmpty();
  }

  @Test
  void pending_calledTwice_returnsSameInstance() {
    // Act & Assert
    assertThat(StepResult.pending()).isSameAs(StepResult.pending());
  }

  @Test
  void equals_sameEntries_returnsTrue() {
    // Arrange
    StepResult a = StepResult.of("key", "value");
    StepResult b = StepResult.of("key", "value");

    // Act & Assert
    assertThat(a).isEqualTo(b);
  }

  @Test
  void equals_differentEntries_returnsFalse() {
    // Arrange
    StepResult a = StepResult.of("key", "value1");
    StepResult b = StepResult.of("key", "value2");

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_pendingVsNonPending_returnsFalse() {
    // Arrange
    StepResult pending = StepResult.pending();
    StepResult empty = StepResult.empty();

    // Act & Assert
    assertThat(pending).isNotEqualTo(empty);
  }

  @Test
  void equals_nullGiven_returnsFalse() {
    // Act & Assert
    assertThat(StepResult.empty()).isNotEqualTo(null);
  }

  @Test
  void hashCode_equalObjects_sameHashCode() {
    // Arrange
    StepResult a = StepResult.of("key", "value");
    StepResult b = StepResult.of("key", "value");

    // Act & Assert
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void toString_pendingResult_containsPending() {
    // Act & Assert
    assertThat(StepResult.pending().toString()).contains("PENDING");
  }

  @Test
  void toString_emptyResult_containsEmpty() {
    // Act & Assert
    assertThat(StepResult.empty().toString()).contains("EMPTY");
  }

  @Test
  void toString_resultWithOutput_containsOutput() {
    // Arrange
    StepResult result = StepResult.of("orderId", "123");

    // Act & Assert
    assertThat(result.toString()).contains("output=");
  }
}
