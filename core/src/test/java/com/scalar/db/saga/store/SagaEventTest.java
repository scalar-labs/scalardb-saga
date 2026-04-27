package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SagaEventTest {

  // --- Saga-level factory methods ---

  @Test
  void sagaStarted_payloadGiven_createsEventWithRunningStatus() {
    // Arrange
    String payload = "{\"amount\":100}";

    // Act
    SagaEvent event = SagaEvent.sagaStarted(payload);

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.SAGA_STARTED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(event.getPayload()).isEqualTo(payload);
    assertThat(event.getStepIndex()).isEqualTo(-1);
    assertThat(event.getStepName()).isNull();
    assertThat(event.getTimestamp()).isNull();
  }

  @Test
  void sagaConfirming_called_createsEventWithConfirmingStatus() {
    // Act
    SagaEvent event = SagaEvent.sagaConfirming();

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.SAGA_CONFIRMING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.CONFIRMING);
    assertThat(event.getStepIndex()).isEqualTo(-1);
    assertThat(event.getStepName()).isNull();
    assertThat(event.getPayload()).isNull();
  }

  @Test
  void sagaCompensating_called_createsEventWithCompensatingStatus() {
    // Act
    SagaEvent event = SagaEvent.sagaCompensating();

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.SAGA_COMPENSATING);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(event.getStepIndex()).isEqualTo(-1);
  }

  @Test
  void sagaCompleted_called_createsEventWithCompletedStatus() {
    // Act
    SagaEvent event = SagaEvent.sagaCompleted();

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.SAGA_COMPLETED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(event.getStepIndex()).isEqualTo(-1);
  }

  @Test
  void sagaCompensated_called_createsEventWithCompensatedStatus() {
    // Act
    SagaEvent event = SagaEvent.sagaCompensated();

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.SAGA_COMPENSATED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(event.getStepIndex()).isEqualTo(-1);
  }

  @Test
  void sagaEscalated_reasonGiven_createsEventWithEscalatedStatus() {
    // Arrange
    String reason = "max retries exceeded";

    // Act
    SagaEvent event = SagaEvent.sagaEscalated(reason);

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.SAGA_ESCALATED);
    assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(event.getPayload()).isEqualTo(reason);
    assertThat(event.getStepIndex()).isEqualTo(-1);
  }

  // --- Step-level factory methods ---

  @Test
  void stepCompleted_validStepGiven_createsEventWithStepInfo() {
    // Act
    SagaEvent event = SagaEvent.stepCompleted(2, "debit", "{\"result\":\"ok\"}");

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.STEP_COMPLETED);
    assertThat(event.getStepIndex()).isEqualTo(2);
    assertThat(event.getStepName()).isEqualTo("debit");
    assertThat(event.getPayload()).isEqualTo("{\"result\":\"ok\"}");
    assertThat(event.getTargetStatus()).isNull();
  }

  @Test
  void stepFailed_validStepGiven_createsEventWithStepInfo() {
    // Act
    SagaEvent event = SagaEvent.stepFailed(1, "credit", "{\"error\":\"timeout\"}");

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.STEP_FAILED);
    assertThat(event.getStepIndex()).isEqualTo(1);
    assertThat(event.getStepName()).isEqualTo("credit");
    assertThat(event.getPayload()).isEqualTo("{\"error\":\"timeout\"}");
    assertThat(event.getTargetStatus()).isNull();
  }

  @Test
  void stepCompensated_validStepGiven_createsEventWithStepInfo() {
    // Act
    SagaEvent event = SagaEvent.stepCompensated(0, "debit");

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.STEP_COMPENSATED);
    assertThat(event.getStepIndex()).isEqualTo(0);
    assertThat(event.getStepName()).isEqualTo("debit");
    assertThat(event.getPayload()).isNull();
    assertThat(event.getTargetStatus()).isNull();
  }

  @Test
  void stepCompensationFailed_validStepGiven_createsEventWithStepInfo() {
    // Act
    SagaEvent event = SagaEvent.stepCompensationFailed(3, "notify", "{\"error\":\"unreachable\"}");

    // Assert
    assertThat(event.getEventType()).isEqualTo(SagaEvent.STEP_COMPENSATION_FAILED);
    assertThat(event.getStepIndex()).isEqualTo(3);
    assertThat(event.getStepName()).isEqualTo("notify");
    assertThat(event.getPayload()).isEqualTo("{\"error\":\"unreachable\"}");
    assertThat(event.getTargetStatus()).isNull();
  }

  // --- withTimestamp ---

  @Test
  void withTimestamp_instantGiven_returnsNewEventWithTimestamp() {
    // Arrange
    SagaEvent original = SagaEvent.sagaStarted("{\"input\":1}");
    Instant timestamp = Instant.parse("2026-01-15T10:30:00Z");

    // Act
    SagaEvent withTs = original.withTimestamp(timestamp);

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
  void sagaEscalated_nullReasonGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.sagaEscalated(null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void stepCompleted_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepCompleted(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void stepCompleted_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepCompleted(-1, "debit", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void stepFailed_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepFailed(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void stepFailed_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepFailed(-1, "credit", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void stepCompensated_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepCompensated(0, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void stepCompensated_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepCompensated(-1, "debit"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void stepCompensationFailed_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepCompensationFailed(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void stepCompensationFailed_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaEvent.stepCompensationFailed(-1, "notify", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void withTimestamp_nullGiven_throwsNullPointerException() {
    // Arrange
    SagaEvent event = SagaEvent.sagaCompleted();

    // Act & Assert
    assertThatThrownBy(
            () -> {
              @SuppressWarnings("unused")
              SagaEvent ignored = event.withTimestamp(null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  // --- equals / hashCode ---

  @Test
  void equals_sameFields_returnsTrue() {
    // Arrange
    SagaEvent a = SagaEvent.stepCompleted(1, "debit", "{\"ok\":true}");
    SagaEvent b = SagaEvent.stepCompleted(1, "debit", "{\"ok\":true}");

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void equals_differentEventType_returnsFalse() {
    // Arrange
    SagaEvent a = SagaEvent.sagaCompleted();
    SagaEvent b = SagaEvent.sagaCompensated();

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_nullGiven_returnsFalse() {
    // Act & Assert
    assertThat(SagaEvent.sagaCompleted()).isNotEqualTo(null);
  }

  // --- toString ---

  @Test
  void toString_sagaLevelEvent_containsEventTypeAndStatus() {
    // Arrange
    SagaEvent event = SagaEvent.sagaStarted("{\"input\":1}");

    // Act
    String result = event.toString();

    // Assert
    assertThat(result).contains("SAGA_STARTED");
    assertThat(result).contains("RUNNING");
  }

  @Test
  void toString_stepLevelEvent_containsStepInfo() {
    // Arrange
    SagaEvent event = SagaEvent.stepCompleted(2, "debit", null);

    // Act
    String result = event.toString();

    // Assert
    assertThat(result).contains("STEP_COMPLETED");
    assertThat(result).contains("debit");
    assertThat(result).contains("2");
  }
}
