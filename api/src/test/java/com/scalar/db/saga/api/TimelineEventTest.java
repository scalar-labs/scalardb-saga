package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class TimelineEventTest {

  private static final Instant TS = Instant.parse("2026-07-13T10:00:00Z");

  @Test
  void constructor_stepEventFieldsGiven_exposesStepMetadata() {
    // Act
    TimelineEvent e =
        new TimelineEvent(TS, "STEP_FAILED", 2, "charge", null, "gateway timeout", null);

    // Assert
    assertThat(e.getTimestamp()).isEqualTo(TS);
    assertThat(e.getType()).isEqualTo("STEP_FAILED");
    assertThat(e.getStepIndex()).isEqualTo(2);
    assertThat(e.getStepName()).isEqualTo("charge");
    assertThat(e.getResultingStatus()).isNull();
    assertThat(e.getDetail()).isEqualTo("gateway timeout");
    assertThat(e.getOperator()).isNull();
  }

  @Test
  void constructor_interventionFieldsGiven_exposesStatusOperatorAndReason() {
    // Act
    TimelineEvent e =
        new TimelineEvent(
            TS, "SAGA_RECOVERED", null, null, SagaStatus.COMPENSATING, "rolling back", "alice");

    // Assert
    assertThat(e.getStepIndex()).isNull();
    assertThat(e.getResultingStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(e.getDetail()).isEqualTo("rolling back");
    assertThat(e.getOperator()).isEqualTo("alice");
  }

  @Test
  void equals_sameFields_returnsTrueAndHashMatches() {
    // Arrange
    TimelineEvent a =
        new TimelineEvent(TS, "SAGA_STARTED", null, null, SagaStatus.RUNNING, null, null);
    TimelineEvent b =
        new TimelineEvent(TS, "SAGA_STARTED", null, null, SagaStatus.RUNNING, null, null);

    // Act & Assert
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void equals_differentType_returnsFalse() {
    // Arrange
    TimelineEvent a = new TimelineEvent(TS, "STEP_COMPLETED", 0, "x", null, null, null);
    TimelineEvent b = new TimelineEvent(TS, "STEP_FAILED", 0, "x", null, null, null);

    // Act & Assert
    assertThat(a).isNotEqualTo(b);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullTimestampGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new TimelineEvent(null, "SAGA_STARTED", null, null, null, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void constructor_nullTypeGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new TimelineEvent(TS, null, null, null, null, null, null))
        .isInstanceOf(NullPointerException.class);
  }
}
