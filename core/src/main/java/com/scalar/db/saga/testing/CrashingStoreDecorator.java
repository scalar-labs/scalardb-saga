package com.scalar.db.saga.testing;

import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StepEvent;
import net.jcip.annotations.ThreadSafe;

/**
 * A {@link SagaStore} decorator that simulates a process crash at a configured step boundary.
 *
 * <p>When {@link #recordStepEvent} is called with a {@code STEP_COMPLETED} event for the configured
 * step index, the decorator <b>first delegates</b> to the underlying store (so the event is
 * persisted), then throws {@link SimulatedCrashError}. This simulates the most critical recovery
 * path: event persisted, but engine state lost.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * CrashingStoreDecorator crashingStore = new CrashingStoreDecorator(realStore, 0);
 * // Saga execution crashes after step 0 completes
 * // Recovery picks up from the persisted events
 * }</pre>
 */
@ThreadSafe
public final class CrashingStoreDecorator extends ForwardingSagaStore {

  private final int crashAfterStepIndex;

  /**
   * Creates a decorator that crashes after the specified step completes.
   *
   * @param delegate the underlying store to delegate to
   * @param crashAfterStepIndex the step index whose completion triggers the crash
   */
  public CrashingStoreDecorator(SagaStore delegate, int crashAfterStepIndex) {
    super(delegate);
    if (crashAfterStepIndex < 0) {
      throw new IllegalArgumentException("crashAfterStepIndex must be >= 0");
    }
    this.crashAfterStepIndex = crashAfterStepIndex;
  }

  @Override
  public void recordStepEvent(String sagaId, int sequence, StepEvent event) {
    delegate().recordStepEvent(sagaId, sequence, event);

    if (event.getEventType() == EventType.STEP_COMPLETED
        && event.getStepIndex() == crashAfterStepIndex) {
      throw new SimulatedCrashError(
          "Simulated crash after step " + crashAfterStepIndex + " completed for saga " + sagaId);
    }
  }
}
