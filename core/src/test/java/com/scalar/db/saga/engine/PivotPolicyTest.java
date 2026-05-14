package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.StatusEvent;
import org.junit.jupiter.api.Test;

class PivotPolicyTest {

  @Test
  void constructor_validArgs_createsRecord() {
    // Arrange & Act
    PivotPolicy policy = new PivotPolicy(2, StatusEvent.confirming());

    // Assert
    assertThat(policy.index()).isEqualTo(2);
    assertThat(policy.crossingEvent())
        .isNotNull()
        .extracting(StatusEvent::getEventType)
        .isEqualTo(EventType.SAGA_CONFIRMING);
  }

  @Test
  void constructor_nullCrossingEvent_allowed() {
    // Arrange & Act
    PivotPolicy policy = new PivotPolicy(1, null);

    // Assert
    assertThat(policy.index()).isEqualTo(1);
    assertThat(policy.crossingEvent()).isNull();
  }

  @Test
  void constructor_negativeIndex_allowed() {
    // Arrange & Act
    PivotPolicy policy = new PivotPolicy(-1, null);

    // Assert
    assertThat(policy.index()).isEqualTo(-1);
  }
}
