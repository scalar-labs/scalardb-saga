package com.scalar.db.saga.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class StepEventTest {

  @Test
  void completed_validStepGiven_createsEventWithStepInfo() {
    // Act
    StepEvent event = StepEvent.completed(2, "debit", "{\"result\":\"ok\"}");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.STEP_COMPLETED);
    assertThat(event.getStepIndex()).isEqualTo(2);
    assertThat(event.getStepName()).isEqualTo("debit");
    assertThat(event.getPayload()).isEqualTo("{\"result\":\"ok\"}");
  }

  @Test
  void failed_validStepGiven_createsEventWithStepInfo() {
    // Act
    StepEvent event = StepEvent.failed(1, "credit", "{\"error\":\"timeout\"}");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.STEP_FAILED);
    assertThat(event.getStepIndex()).isEqualTo(1);
    assertThat(event.getStepName()).isEqualTo("credit");
    assertThat(event.getPayload()).isEqualTo("{\"error\":\"timeout\"}");
  }

  @Test
  void compensated_validStepGiven_createsEventWithStepInfo() {
    // Act
    StepEvent event = StepEvent.compensated(0, "debit");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.STEP_COMPENSATED);
    assertThat(event.getStepIndex()).isEqualTo(0);
    assertThat(event.getStepName()).isEqualTo("debit");
    assertThat(event.getPayload()).isNull();
  }

  @Test
  void compensationFailed_validStepGiven_createsEventWithStepInfo() {
    // Act
    StepEvent event = StepEvent.compensationFailed(3, "notify", "{\"error\":\"unreachable\"}");

    // Assert
    assertThat(event.getEventType()).isEqualTo(EventType.STEP_COMPENSATION_FAILED);
    assertThat(event.getStepIndex()).isEqualTo(3);
    assertThat(event.getStepName()).isEqualTo("notify");
    assertThat(event.getPayload()).isEqualTo("{\"error\":\"unreachable\"}");
  }

  // --- withTimestamp ---

  @Test
  void withTimestamp_instantGiven_returnsNewEventWithTimestamp() {
    // Arrange
    StepEvent original = StepEvent.completed(1, "debit", "{\"ok\":true}");
    Instant timestamp = Instant.parse("2026-01-15T10:30:00Z");

    // Act
    StepEvent withTs = original.withTimestamp(timestamp);

    // Assert
    assertThat(withTs.getTimestamp()).isEqualTo(timestamp);
    assertThat(withTs.getEventType()).isEqualTo(original.getEventType());
    assertThat(withTs.getStepIndex()).isEqualTo(original.getStepIndex());
    assertThat(withTs.getStepName()).isEqualTo(original.getStepName());
    assertThat(withTs.getPayload()).isEqualTo(original.getPayload());
    assertThat(original.getTimestamp()).isNull();
    assertThat(withTs).isNotSameAs(original);
  }

  // --- Null checks ---

  @SuppressWarnings("NullAway")
  @Test
  void completed_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.completed(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void completed_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.completed(-1, "debit", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void failed_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.failed(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void failed_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.failed(-1, "credit", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void compensated_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.compensated(0, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void compensated_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.compensated(-1, "debit"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void compensationFailed_nullStepNameGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.compensationFailed(0, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void compensationFailed_negativeStepIndexGiven_throwsIllegalArgumentException() {
    // Act & Assert
    assertThatThrownBy(() -> StepEvent.compensationFailed(-1, "notify", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void withTimestamp_nullGiven_throwsNullPointerException() {
    // Arrange
    StepEvent event = StepEvent.completed(0, "debit", null);

    // Act & Assert
    assertThatThrownBy(
            () -> {
              @SuppressWarnings("unused")
              StepEvent ignored = event.withTimestamp(null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  // --- equals / hashCode ---

  @Test
  void equals_sameFields_returnsTrue() {
    // Arrange
    StepEvent a = StepEvent.completed(1, "debit", "{\"ok\":true}");
    StepEvent b = StepEvent.completed(1, "debit", "{\"ok\":true}");

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void equals_differentEventType_returnsFalse() {
    // Arrange
    StepEvent a = StepEvent.completed(0, "debit", null);
    StepEvent b = StepEvent.failed(0, "debit", null);

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equals_nullGiven_returnsFalse() {
    // Act & Assert
    assertThat(StepEvent.completed(0, "debit", null)).isNotEqualTo(null);
  }

  // --- toString ---

  @Test
  void toString_called_containsStepInfo() {
    // Arrange
    StepEvent event = StepEvent.completed(2, "debit", null);

    // Act
    String result = event.toString();

    // Assert
    assertThat(result).contains("STEP_COMPLETED");
    assertThat(result).contains("debit");
    assertThat(result).contains("2");
  }
}
