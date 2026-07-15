package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaPersistenceException;
import org.junit.jupiter.api.Test;

class AdminAuditPayloadTest {

  @Test
  void encode_thenAccessors_roundTripOperatorReasonAndTarget() {
    // Act
    String payload = AdminAuditPayload.encode("alice", "downstream restored", SagaStatus.RUNNING);

    // Assert
    assertThat(AdminAuditPayload.operator(payload)).isEqualTo("alice");
    assertThat(AdminAuditPayload.reason(payload)).isEqualTo("downstream restored");
    assertThat(AdminAuditPayload.target(payload)).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void target_compensatingEncoded_returnsCompensating() {
    // Arrange
    String payload = AdminAuditPayload.encode("bob", "rolling back", SagaStatus.COMPENSATING);

    // Act & Assert
    assertThat(AdminAuditPayload.target(payload)).isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void operator_nullPayloadGiven_returnsNull() {
    // Act & Assert
    assertThat(AdminAuditPayload.operator(null)).isNull();
    assertThat(AdminAuditPayload.reason(null)).isNull();
  }

  @Test
  void operator_unparseablePayloadGiven_throwsSagaPersistenceException() {
    // Act & Assert — fail closed on a corrupt payload rather than silently returning null
    assertThatThrownBy(() -> AdminAuditPayload.operator("not json"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void target_payloadWithoutStatusGiven_throwsSagaPersistenceException() {
    // Act & Assert
    assertThatThrownBy(() -> AdminAuditPayload.target("{\"operator\":\"bob\"}"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  @Test
  void target_unknownStatusCodeGiven_throwsSagaPersistenceException() {
    // Act & Assert — a corrupt status code is storage corruption, not a bad request
    assertThatThrownBy(() -> AdminAuditPayload.target("{\"status\":99}"))
        .isInstanceOf(SagaPersistenceException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void target_nullPayloadGiven_throwsSagaPersistenceException() {
    // Act & Assert
    assertThatThrownBy(() -> AdminAuditPayload.target(null))
        .isInstanceOf(SagaPersistenceException.class);
  }
}
