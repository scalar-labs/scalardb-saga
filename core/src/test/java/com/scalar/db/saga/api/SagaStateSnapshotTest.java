package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class SagaStateSnapshotTest {

  private static final String SAGA_ID = "saga-001";
  private static final String SAGA_NAME = "order-saga";
  private static final String OWNER_ID = "node-1";
  private static final int VERSION = 3;
  private static final String DEFINITION_VERSION = "1.0";
  private static final Instant CREATED_AT = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant UPDATED_AT = Instant.parse("2026-01-01T01:00:00Z");

  private SagaStateSnapshot createSnapshot(SagaStatus status) {
    return new SagaStateSnapshot(
        SAGA_ID, SAGA_NAME, status, OWNER_ID, VERSION, DEFINITION_VERSION, CREATED_AT, UPDATED_AT);
  }

  @Test
  void constructor_allFieldsProvided_setsAllFields() {
    // Arrange & Act
    SagaStateSnapshot snapshot = createSnapshot(SagaStatus.RUNNING);

    // Assert
    assertThat(snapshot.getSagaId()).isEqualTo(SAGA_ID);
    assertThat(snapshot.getSagaName()).isEqualTo(SAGA_NAME);
    assertThat(snapshot.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(snapshot.getOwnerId()).isEqualTo(OWNER_ID);
    assertThat(snapshot.getVersion()).isEqualTo(VERSION);
    assertThat(snapshot.getDefinitionVersion()).isEqualTo(DEFINITION_VERSION);
    assertThat(snapshot.getCreatedAt()).isEqualTo(CREATED_AT);
    assertThat(snapshot.getUpdatedAt()).isEqualTo(UPDATED_AT);
  }

  @Test
  void withTransition_called_updatesStatusAndTimestamp() {
    // Arrange
    SagaStateSnapshot original = createSnapshot(SagaStatus.RUNNING);
    Instant newUpdatedAt = Instant.parse("2026-01-01T02:00:00Z");

    // Act
    SagaStateSnapshot transitioned = original.withTransition(SagaStatus.COMPENSATING, newUpdatedAt);

    // Assert
    assertThat(transitioned.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(transitioned.getUpdatedAt()).isEqualTo(newUpdatedAt);
  }

  @Test
  void withTransition_called_preservesOtherFields() {
    // Arrange
    SagaStateSnapshot original = createSnapshot(SagaStatus.RUNNING);
    Instant newUpdatedAt = Instant.parse("2026-01-01T02:00:00Z");

    // Act
    SagaStateSnapshot transitioned = original.withTransition(SagaStatus.COMPLETED, newUpdatedAt);

    // Assert
    assertThat(transitioned.getSagaId()).isEqualTo(SAGA_ID);
    assertThat(transitioned.getSagaName()).isEqualTo(SAGA_NAME);
    assertThat(transitioned.getOwnerId()).isEqualTo(OWNER_ID);
    assertThat(transitioned.getDefinitionVersion()).isEqualTo(DEFINITION_VERSION);
    assertThat(transitioned.getCreatedAt()).isEqualTo(CREATED_AT);
  }

  @Test
  void withTransition_called_doesNotIncrementVersion() {
    // Arrange
    SagaStateSnapshot original = createSnapshot(SagaStatus.RUNNING);
    Instant newUpdatedAt = Instant.parse("2026-01-01T02:00:00Z");

    // Act
    SagaStateSnapshot transitioned = original.withTransition(SagaStatus.COMPLETED, newUpdatedAt);

    // Assert — version must remain unchanged; only SagaStore increments it
    assertThat(transitioned.getVersion()).isEqualTo(VERSION);
  }

  @Test
  void withTransition_called_doesNotMutateOriginal() {
    // Arrange
    SagaStateSnapshot original = createSnapshot(SagaStatus.RUNNING);
    Instant newUpdatedAt = Instant.parse("2026-01-01T02:00:00Z");

    // Act
    SagaStateSnapshot transitioned = original.withTransition(SagaStatus.ESCALATED, newUpdatedAt);

    // Assert — original remains unchanged, transitioned is a different instance
    assertThat(original.getStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(original.getUpdatedAt()).isEqualTo(UPDATED_AT);
    assertThat(transitioned).isNotSameAs(original);
  }

  @Test
  void equals_sameFields_returnsTrue() {
    // Arrange
    SagaStateSnapshot a = createSnapshot(SagaStatus.RUNNING);
    SagaStateSnapshot b = createSnapshot(SagaStatus.RUNNING);

    // Act & Assert
    assertThat(a).isEqualTo(b);
  }

  @Test
  void equals_differentStatus_returnsFalse() {
    // Arrange
    SagaStateSnapshot a = createSnapshot(SagaStatus.RUNNING);
    SagaStateSnapshot b = createSnapshot(SagaStatus.COMPLETED);

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_differentSagaId_returnsFalse() {
    // Arrange
    SagaStateSnapshot a = createSnapshot(SagaStatus.RUNNING);
    SagaStateSnapshot b =
        new SagaStateSnapshot(
            "different-id",
            SAGA_NAME,
            SagaStatus.RUNNING,
            OWNER_ID,
            VERSION,
            DEFINITION_VERSION,
            CREATED_AT,
            UPDATED_AT);

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_nullGiven_returnsFalse() {
    // Act & Assert
    assertThat(createSnapshot(SagaStatus.RUNNING)).isNotEqualTo(null);
  }

  @Test
  void hashCode_equalObjects_sameHashCode() {
    // Arrange
    SagaStateSnapshot a = createSnapshot(SagaStatus.RUNNING);
    SagaStateSnapshot b = createSnapshot(SagaStatus.RUNNING);

    // Act & Assert
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void toString_called_containsKeyFields() {
    // Arrange
    SagaStateSnapshot snapshot = createSnapshot(SagaStatus.RUNNING);

    // Act
    String result = snapshot.toString();

    // Assert
    assertThat(result).contains(SAGA_ID);
    assertThat(result).contains(SAGA_NAME);
    assertThat(result).contains("RUNNING");
  }
}
