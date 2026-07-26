package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinitionId;
import org.junit.jupiter.api.Test;

class SagaDefinitionNotFoundExceptionTest {

  @Test
  void constructor_sagaNameOnlyGiven_carriesNameOnlyCode() {
    // Arrange & Act
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException("order-saga");

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getVersion()).isNull();
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_NOT_FOUND);
    assertThat(e.getMetadata()).containsEntry("saga_name", "order-saga").hasSize(1);
    assertThat(e.getMessage())
        .isEqualTo("DB-SAGA-11001: Saga definition not found [saga_name=order-saga]");
  }

  @Test
  void constructor_sagaNameAndVersionGiven_carriesVersionedCode() {
    // Arrange & Act
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException("order-saga", "2.0");

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getVersion()).isEqualTo("2.0");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND);
    assertThat(e.getMetadata())
        .containsEntry("saga_name", "order-saga")
        .containsEntry("version", "2.0")
        .hasSize(2);
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-11002: Saga definition version not found"
                + " [saga_name=order-saga, version=2.0]");
  }

  @Test
  void constructor_sagaDefinitionIdGiven_carriesVersionedCode() {
    // Arrange & Act — the SagaDefinitionId ctor delegates to (name, version), so it always uses
    // the versioned code even though the caller passed an id
    SagaDefinitionId id = new SagaDefinitionId("order-saga", "2.0");
    SagaDefinitionNotFoundException e = new SagaDefinitionNotFoundException(id);

    // Assert
    assertThat(e.getSagaName()).isEqualTo("order-saga");
    assertThat(e.getVersion()).isEqualTo("2.0");
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND);
    assertThat(e.getMessage())
        .isEqualTo(
            "DB-SAGA-11002: Saga definition version not found"
                + " [saga_name=order-saga, version=2.0]");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaNameGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionNotFoundException((String) null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_sagaNameAndNullVersionGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaDefinitionNotFoundException("order-saga", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaDefinitionNotFoundException.class);
  }
}
