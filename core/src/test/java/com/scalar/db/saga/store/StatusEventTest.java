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

  // --- operator interventions (Admin API) ---

  @Test
  void forceCompleted_operatorAndReasonGiven_createsCompletedEventWithAudit() {
    // Act
    StatusEvent event = StatusEvent.forceCompleted("alice", "confirmed done downstream");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_FORCE_COMPLETED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(AdminAuditPayload.operator(event.getPayload())).isEqualTo("alice");
    assertThat(AdminAuditPayload.reason(event.getPayload())).isEqualTo("confirmed done downstream");
  }

  @Test
  void recovering_compensatingTargetGiven_createsRecoveringEventTargetingCompensating() {
    // Act
    StatusEvent event = StatusEvent.recovering(SagaStatus.COMPENSATING, "bob", "rolling back");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_RECOVERING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(AdminAuditPayload.operator(event.getPayload())).isEqualTo("bob");
    assertThat(AdminAuditPayload.reason(event.getPayload())).isEqualTo("rolling back");
  }

  @Test
  void recovering_runningTargetGiven_createsRecoveringEventTargetingRunning() {
    // Act
    StatusEvent event = StatusEvent.recovering(SagaStatus.RUNNING, "bob", "downstream restored");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_RECOVERING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void reset_compensatingTargetGiven_createsResetEventTargetingCompensating() {
    // Act
    StatusEvent event = StatusEvent.reset(SagaStatus.COMPENSATING, "carol", "un-escalating");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_RESET);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(AdminAuditPayload.reason(event.getPayload())).isEqualTo("un-escalating");
  }

  @Test
  void reset_runningTargetGiven_createsResetEventTargetingRunning() {
    // Act
    StatusEvent event = StatusEvent.reset(SagaStatus.RUNNING, "carol", "resume forward");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_RESET);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void recovering_terminalTargetGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> StatusEvent.recovering(SagaStatus.COMPLETED, "bob", "why"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reset_waitingTargetGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> StatusEvent.reset(SagaStatus.WAITING, "carol", "why"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void forceCompleted_nullOperatorGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> StatusEvent.forceCompleted(null, "reason"))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void reconstruct_targetAndPayloadGiven_buildsEventWithoutReEncoding() {
    // Arrange — the payload the store persisted for a recovering-to-COMPENSATING event
    String payload = AdminAuditPayload.encode("bob", "rolling back", SagaStatus.COMPENSATING);

    // Act
    StatusEvent event =
        StatusEvent.reconstruct(EventType.SAGA_RECOVERING, SagaStatus.COMPENSATING, payload);

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.SAGA_RECOVERING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(event.getPayload()).isEqualTo(payload);
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
