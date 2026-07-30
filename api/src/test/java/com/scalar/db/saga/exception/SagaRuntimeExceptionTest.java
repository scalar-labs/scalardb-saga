package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaRuntimeExceptionTest {

  @Test
  void constructor_codeAndMetadataGiven_setsFieldsAndBuildsMessage() {
    // Arrange
    Map<String, String> metadata = Collections.singletonMap("saga_id", "s-1");

    // Act
    SagaRuntimeException e = new SagaRuntimeException(SagaErrorCode.SAGA_NOT_FOUND, metadata);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_NOT_FOUND);
    assertThat(e.getMetadata()).containsExactly(entry("saga_id", "s-1"));
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-10201: Saga not found [saga_id=s-1]");
  }

  @Test
  void constructor_codeAndMetadataAndCauseGiven_setsCauseToo() {
    // Arrange
    Map<String, String> metadata = Collections.emptyMap();
    RuntimeException cause = new RuntimeException("inner");

    // Act
    SagaRuntimeException e =
        new SagaRuntimeException(SagaErrorCode.INTERNAL_ERROR, metadata, cause);

    // Assert
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.INTERNAL_ERROR);
    assertThat(e.getMetadata()).isEmpty();
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-39999: Internal error");
  }

  @Test
  void getMetadata_codeCarryingConstructor_isUnmodifiable() {
    // Arrange
    SagaRuntimeException e =
        new SagaRuntimeException(
            SagaErrorCode.SAGA_NOT_FOUND, Collections.singletonMap("saga_id", "s-1"));

    // Act & Assert
    assertThatThrownBy(() -> e.getMetadata().put("uninvited", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getMetadata_codeCarryingConstructor_isolatedFromCallerMap() {
    // Arrange — the ctor's defensive copy means later mutation of the caller's map is invisible
    Map<String, String> caller = new HashMap<>();
    caller.put("saga_id", "s-1");
    SagaRuntimeException e = new SagaRuntimeException(SagaErrorCode.SAGA_NOT_FOUND, caller);

    // Act
    caller.put("saga_id", "changed");
    caller.put("uninvited", "value");

    // Assert
    assertThat(e.getMetadata()).containsExactly(entry("saga_id", "s-1"));
  }

  @Test
  void getMessage_codeCarryingConstructor_rendersInSchemaOrder() {
    // Arrange — insertion order deliberately reversed to prove ordering isn't from the map
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("detail", "duplicate step name 'debit'");
    metadata.put("saga_name", "orders");

    // Act
    SagaRuntimeException e = new SagaRuntimeException(SagaErrorCode.INVALID_DEFINITION, metadata);

    // Assert — saga_name comes first because that's the declared schema order
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-10003: Saga definition is invalid [saga_name=orders,"
                + " detail=duplicate step name 'debit']");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCodeGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaRuntimeException(null, Collections.emptyMap()))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullMetadataGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaRuntimeException(SagaErrorCode.INTERNAL_ERROR, null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_codeAndMetadataAndNullCauseGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(
            () ->
                new SagaRuntimeException(
                    SagaErrorCode.INTERNAL_ERROR, Collections.emptyMap(), (Throwable) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void constructor_missingRequiredKeyGiven_throwsIllegalArgument() {
    // Arrange — SAGA_NOT_FOUND requires saga_id
    // Act & Assert
    assertThatThrownBy(
            () -> new SagaRuntimeException(SagaErrorCode.SAGA_NOT_FOUND, Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_extraKeyGiven_throwsIllegalArgument() {
    // Arrange
    Map<String, String> metadata = new HashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("uninvited", "value");

    // Act & Assert
    assertThatThrownBy(() -> new SagaRuntimeException(SagaErrorCode.SAGA_NOT_FOUND, metadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaRuntimeException.class);
  }

  @Test
  void classHierarchy_sagaExceptions_extendSagaRuntimeException() {
    // Assert — the unchecked saga exceptions are re-parented onto SagaRuntimeException so callers
    // can catch any saga failure uniformly.
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaPersistenceException.class);
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaNotFoundException.class);
    assertThat(SagaRuntimeException.class).isAssignableFrom(SagaTimeoutException.class);
  }

  // --- Helpers ---------------------------------------------------------

  private static Map.Entry<String, String> entry(String key, String value) {
    return new java.util.AbstractMap.SimpleImmutableEntry<>(key, value);
  }
}
