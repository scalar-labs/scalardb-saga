package com.scalar.db.saga.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SagaAlreadyExistsExceptionTest {

  private static SagaStateSnapshot createSnapshot(String sagaId) {
    Instant now = Instant.now();
    return new SagaStateSnapshot(
        sagaId, "order-saga", SagaStatus.RUNNING, "owner-1", "v1", now, now);
  }

  @Test
  void constructor_sagaIdAndSnapshotGiven_setsFields() {
    // Arrange
    SagaStateSnapshot snapshot = createSnapshot("saga-1");

    // Act
    SagaAlreadyExistsException e = new SagaAlreadyExistsException("saga-1", snapshot);

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-1");
    assertThat(e.getExisting()).isSameAs(snapshot);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_ALREADY_EXISTS);
    assertThat(e.getMetadata()).containsEntry("saga_id", "saga-1").hasSize(1);
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-10301: Saga already exists [saga_id=saga-1]");
    assertThat(e.getCause()).isNull();
  }

  @Test
  void constructor_sagaIdAndSnapshotAndCauseGiven_setsAllFields() {
    // Arrange
    SagaStateSnapshot snapshot = createSnapshot("saga-2");
    RuntimeException cause = new RuntimeException("conflict");

    // Act
    SagaAlreadyExistsException e = new SagaAlreadyExistsException("saga-2", snapshot, cause);

    // Assert
    assertThat(e.getSagaId()).isEqualTo("saga-2");
    assertThat(e.getExisting()).isSameAs(snapshot);
    assertThat(e.getCause()).isSameAs(cause);
    assertThat(e.getErrorCode()).isEqualTo(SagaErrorCode.SAGA_ALREADY_EXISTS);
    assertThat(e.getMetadata()).containsEntry("saga_id", "saga-2").hasSize(1);
    assertThat(e.getMessage()).isEqualTo("DB-SAGA-10301: Saga already exists [saga_id=saga-2]");
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSagaIdGiven_throwsNullPointerException() {
    // Arrange
    SagaStateSnapshot snapshot = createSnapshot("saga-3");

    // Act & Assert
    assertThatThrownBy(() -> new SagaAlreadyExistsException(null, snapshot))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullSnapshotGiven_throwsNullPointerException() {
    // Arrange & Act & Assert
    assertThatThrownBy(() -> new SagaAlreadyExistsException("saga-3", null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullCauseGiven_throwsNullPointerException() {
    // Arrange
    SagaStateSnapshot snapshot = createSnapshot("saga-5");

    // Act & Assert
    assertThatThrownBy(() -> new SagaAlreadyExistsException("saga-5", snapshot, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void classHierarchy_always_isRuntimeException() {
    // Assert
    assertThat(RuntimeException.class).isAssignableFrom(SagaAlreadyExistsException.class);
  }
}
