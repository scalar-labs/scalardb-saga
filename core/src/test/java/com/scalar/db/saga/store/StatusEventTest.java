package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class StatusEventTest {

  @Test
  void started_payloadGiven_createsEventWithRunningStatus() {
    // Arrange
    String payload = "{\"amount\":100}";

    // Act
    StatusEvent event = StatusEvent.started(payload);

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_STARTED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(event.getPayload()).isEqualTo(payload);
    assertThat(event.getTimestamp()).isNull();
  }

  @Test
  void started_nullPayloadGiven_createsEventWithNullPayload() {
    // Act
    StatusEvent event = StatusEvent.started(null);

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_STARTED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(event.getPayload()).isNull();
  }

  @Test
  void confirming_called_createsEventWithConfirmingStatus() {
    // Act
    StatusEvent event = StatusEvent.confirming();

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_CONFIRMING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.CONFIRMING);
    assertThat(event.getPayload()).isNull();
  }

  @Test
  void compensating_called_createsEventWithCompensatingStatus() {
    // Act
    StatusEvent event = StatusEvent.compensating();

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_COMPENSATING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void completed_called_createsEventWithCompletedStatus() {
    // Act
    StatusEvent event = StatusEvent.completed();

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_COMPLETED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void compensated_called_createsEventWithCompensatedStatus() {
    // Act
    StatusEvent event = StatusEvent.compensated();

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_COMPENSATED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void escalated_reasonGiven_createsEventWithEscalatedStatus() {
    // Arrange
    String reason = "max retries exceeded";

    // Act
    StatusEvent event = StatusEvent.escalated(reason);

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_ESCALATED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(event.getPayload()).isEqualTo(reason);
  }

  // --- withTimestamp ---

  @Test
  void withTimestamp_instantGiven_returnsNewEventWithTimestamp() {
    // Arrange
    StatusEvent original = StatusEvent.started("{\"input\":1}");
    Instant timestamp = Instant.parse("2026-01-15T10:30:00Z");

    // Act
    StatusEvent withTs = original.withTimestamp(timestamp);

    // Assert
    assertThat(withTs.getTimestamp()).isEqualTo(timestamp);
    assertThat(withTs.getEventType()).isEqualTo(original.getEventType());
    assertThat(withTs.getPayload()).isEqualTo(original.getPayload());
    assertThat(withTs.getTargetStatus()).isEqualTo(original.getTargetStatus());
    assertThat(original.getTimestamp()).isNull();
    assertThat(withTs).isNotSameAs(original);
  }

  // --- Null checks ---

  @SuppressWarnings("NullAway")
  @Test
  void escalated_nullReasonGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> StatusEvent.escalated(null)).isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void withTimestamp_nullGiven_throwsNullPointerException() {
    // Arrange
    StatusEvent event = StatusEvent.completed();

    // Act & Assert
    assertThatThrownBy(
            () -> {
              @SuppressWarnings("unused")
              StatusEvent ignored = event.withTimestamp(null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  // --- equals / hashCode ---

  @Test
  void equals_sameFields_returnsTrue() {
    // Arrange
    StatusEvent a = StatusEvent.completed();
    StatusEvent b = StatusEvent.completed();

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void equals_differentEventType_returnsFalse() {
    // Arrange
    StatusEvent a = StatusEvent.completed();
    StatusEvent b = StatusEvent.compensated();

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_nullGiven_returnsFalse() {
    // Act & Assert
    assertThat(StatusEvent.completed()).isNotEqualTo(null);
  }

  // --- toString ---

  @Test
  void toString_called_containsEventTypeAndStatus() {
    // Arrange
    StatusEvent event = StatusEvent.started("{\"input\":1}");

    // Act
    String result = event.toString();

    // Assert
    assertThat(result).contains("SAGA_STARTED");
    assertThat(result).contains("RUNNING");
  }
}
