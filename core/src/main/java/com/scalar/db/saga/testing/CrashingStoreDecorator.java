package com.scalar.db.saga.testing;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

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
public final class CrashingStoreDecorator implements SagaStore {

  private final SagaStore delegate;
  private final int crashAfterStepIndex;

  /**
   * Creates a decorator that crashes after the specified step completes.
   *
   * @param delegate the underlying store to delegate to
   * @param crashAfterStepIndex the step index whose completion triggers the crash
   */
  public CrashingStoreDecorator(SagaStore delegate, int crashAfterStepIndex) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    if (crashAfterStepIndex < 0) {
      throw new IllegalArgumentException("crashAfterStepIndex must be >= 0");
    }
    this.crashAfterStepIndex = crashAfterStepIndex;
  }

  @Override
  public void recordStepEvent(String sagaId, int sequence, StepEvent event) {
    delegate.recordStepEvent(sagaId, sequence, event);

    if (event.getEventType() == EventType.STEP_COMPLETED
        && event.getStepIndex() == crashAfterStepIndex) {
      throw new SimulatedCrashError(
          "Simulated crash after step " + crashAfterStepIndex + " completed for saga " + sagaId);
    }
  }

  // --- All remaining methods delegate unchanged ---

  @Override
  public SagaStateSnapshot createSaga(
      @Nullable String sagaId,
      String sagaName,
      String ownerId,
      Map<String, Object> input,
      String definitionVersion) {
    return delegate.createSaga(sagaId, sagaName, ownerId, input, definitionVersion);
  }

  @Override
  public void registerDefinition(SagaDefinition definition) {
    delegate.registerDefinition(definition);
  }

  @Override
  public Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion) {
    return delegate.getDefinition(sagaName, definitionVersion);
  }

  @Override
  public Optional<SagaDefinition> getDefinition(String sagaName) {
    return delegate.getDefinition(sagaName);
  }

  @Override
  public SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current, int sequence, StatusEvent event) {
    return delegate.recordStatusEvent(current, sequence, event);
  }

  @Override
  public SagaStateSnapshot park(
      SagaStateSnapshot current,
      int sequence,
      StepEvent pendingEvent,
      @Nullable Instant parkedDeadline) {
    return delegate.park(current, sequence, pendingEvent, parkedDeadline);
  }

  @Override
  public SagaStateSnapshot resumeParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent completedEvent) {
    return delegate.resumeParkedStep(current, sequence, completedEvent);
  }

  @Override
  public SagaStateSnapshot timeoutParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent failedEvent, SagaStatus targetStatus) {
    return delegate.timeoutParkedStep(current, sequence, failedEvent, targetStatus);
  }

  @Override
  public List<SagaEvent> getEvents(String sagaId) {
    return delegate.getEvents(sagaId);
  }

  @Override
  public int getEventCount(String sagaId) {
    return delegate.getEventCount(sagaId);
  }

  @Override
  public Optional<SagaStateSnapshot> getStateSnapshot(String sagaId) {
    return delegate.getStateSnapshot(sagaId);
  }

  @Override
  public Recoverables findRecoverable(Instant threshold, @Nullable ScanCursor cursor) {
    return delegate.findRecoverable(threshold, cursor);
  }

  @Override
  public Optional<SagaStateSnapshot> claimForRecovery(SagaStateSnapshot saga, String newOwnerId) {
    return delegate.claimForRecovery(saga, newOwnerId);
  }

  @Override
  public void markForRecovery(String sagaId) {
    delegate.markForRecovery(sagaId);
  }

  @Override
  public OverdueParked findOverdueParkedSagas(Instant threshold, @Nullable ScanCursor cursor) {
    return delegate.findOverdueParkedSagas(threshold, cursor);
  }

  @Override
  public List<SagaStateSnapshot> findByStatusOlderThan(
      SagaStatus status, Instant threshold, int maxResults) {
    return delegate.findByStatusOlderThan(status, threshold, maxResults);
  }

  @Override
  public void deleteSaga(String sagaId) {
    delegate.deleteSaga(sagaId);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
