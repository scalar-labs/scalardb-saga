package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaTest {

  @Test
  void of_keysGiven_preservesInsertionOrder() {
    // Arrange & Act
    Schema schema = Schema.of("saga_id", "step_name", "step_index");

    // Assert
    assertThat(schema.requiredKeys()).containsExactly("saga_id", "step_name", "step_index");
  }

  @Test
  void of_noKeysGiven_returnsEmptySchema() {
    // Arrange & Act
    Schema schema = Schema.of();

    // Assert
    assertThat(schema.requiredKeys()).isEmpty();
  }

  @Test
  void none_called_returnsEmptySchema() {
    // Arrange & Act & Assert
    assertThat(Schema.none().requiredKeys()).isEmpty();
  }

  @SuppressWarnings("NullAway")
  @Test
  void of_nullKeyGiven_throwsIllegalArgument() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> Schema.of("saga_id", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void of_blankKeyGiven_throwsIllegalArgument() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> Schema.of("saga_id", "  "))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Schema.of("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void of_duplicateKeyGiven_throwsIllegalArgument() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> Schema.of("saga_id", "saga_id"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void requiredKeys_always_isUnmodifiable() {
    // Arrange
    Schema schema = Schema.of("saga_id");

    // Act & Assert
    assertThatThrownBy(() -> schema.requiredKeys().add("intruder"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void validate_matchingKeysAndValuesGiven_passes() {
    // Arrange
    Schema schema = Schema.of("saga_id", "step_name");
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("step_name", "debit");

    // Act & Assert — pick any code; validate reads only the key set + values
    assertThatCode(() -> schema.validate(SagaErrorCode.COMPENSATION_FAILED, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  void validate_missingKeyGiven_throwsIllegalArgument() {
    // Arrange
    Schema schema = Schema.of("saga_id", "step_name");
    Map<String, String> metadata = Collections.singletonMap("saga_id", "s-1");

    // Act & Assert
    assertThatThrownBy(() -> schema.validate(SagaErrorCode.COMPENSATION_FAILED, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validate_extraKeyGiven_throwsIllegalArgument() {
    // Arrange
    Schema schema = Schema.of("saga_id");
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("uninvited", "value");

    // Act & Assert
    assertThatThrownBy(() -> schema.validate(SagaErrorCode.SAGA_NOT_FOUND, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validate_nullValueGiven_throwsIllegalArgument() {
    // Arrange
    Schema schema = Schema.of("saga_id");
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_id", null);

    // Act & Assert
    assertThatThrownBy(() -> schema.validate(SagaErrorCode.SAGA_NOT_FOUND, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validate_emptySchemaAndEmptyMetadataGiven_passes() {
    // Arrange & Act & Assert
    assertThatCode(
            () -> Schema.none().validate(SagaErrorCode.INTERNAL_ERROR, Collections.emptyMap()))
        .doesNotThrowAnyException();
  }

  @Test
  void validate_emptySchemaButMetadataGiven_throwsIllegalArgument() {
    // Arrange
    Map<String, String> metadata = Collections.singletonMap("saga_id", "s-1");

    // Act & Assert
    assertThatThrownBy(() -> Schema.none().validate(SagaErrorCode.INTERNAL_ERROR, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
