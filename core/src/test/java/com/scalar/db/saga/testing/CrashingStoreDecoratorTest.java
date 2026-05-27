package com.scalar.db.saga.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StepEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CrashingStoreDecoratorTest {

  private SagaStore delegate;

  @BeforeEach
  void setUp() {
    delegate = mock(SagaStore.class);
  }

  @Test
  void recordStepEvent_stepCompletedAtCrashIndex_delegatesThenThrows() {
    // Arrange
    CrashingStoreDecorator decorator = new CrashingStoreDecorator(delegate, 0);
    StepEvent event = StepEvent.completed(0, "step0", null);

    // Act & Assert
    assertThatThrownBy(() -> decorator.recordStepEvent("saga-1", 1, event))
        .isInstanceOf(SimulatedCrashError.class);
    // Verify delegate was called BEFORE the crash
    verify(delegate).recordStepEvent("saga-1", 1, event);
  }

  @Test
  void recordStepEvent_stepCompletedAtDifferentIndex_delegatesWithoutCrash() {
    // Arrange
    CrashingStoreDecorator decorator = new CrashingStoreDecorator(delegate, 1);
    StepEvent event = StepEvent.completed(0, "step0", null);

    // Act
    decorator.recordStepEvent("saga-1", 1, event);

    // Assert
    verify(delegate).recordStepEvent("saga-1", 1, event);
  }

  @Test
  void recordStepEvent_stepFailed_delegatesWithoutCrash() {
    // Arrange
    CrashingStoreDecorator decorator = new CrashingStoreDecorator(delegate, 0);
    StepEvent event = StepEvent.failed(0, "step0", null);

    // Act
    decorator.recordStepEvent("saga-1", 1, event);

    // Assert — STEP_FAILED at crash index does NOT trigger crash (only STEP_COMPLETED does)
    verify(delegate).recordStepEvent("saga-1", 1, event);
  }

  @Test
  void getDelegate_returnsUnderlyingStore() {
    // Arrange
    CrashingStoreDecorator decorator = new CrashingStoreDecorator(delegate, 0);

    // Act & Assert
    assertThat(decorator.getDelegate()).isSameAs(delegate);
  }

  @Test
  void constructor_negativeStepIndex_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> new CrashingStoreDecorator(delegate, -1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
