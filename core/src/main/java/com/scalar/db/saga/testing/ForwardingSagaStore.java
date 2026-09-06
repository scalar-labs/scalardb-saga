package com.scalar.db.saga.testing;

import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStateAndEvents;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * A {@link SagaStore} decorator base that forwards every method to a delegate. Test decorators
 * extend it and override only the methods whose behavior they alter (see {@link
 * CrashingStoreDecorator}). Centralizing the forwarding also keeps decorators honest as the store
 * interface evolves: a newly added {@code SagaStore} method only needs a forwarder here, instead of
 * silently vanishing from every hand-rolled decorator.
 */
public abstract class ForwardingSagaStore implements SagaStore {

  private final SagaStore delegate;

  @SuppressFBWarnings(
      value = "CT_CONSTRUCTOR_THROW",
      justification =
          "The only state is the caller-supplied delegate and the constructor performs no security"
              + " check, so a finalizer-captured partial instance grants the caller nothing they"
              + " did not already pass in")
  protected ForwardingSagaStore(SagaStore delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
  }

  /** The store every non-overridden call forwards to. */
  protected final SagaStore delegate() {
    return delegate;
  }

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
  public void recordStepEvent(String sagaId, int sequence, StepEvent event) {
    delegate.recordStepEvent(sagaId, sequence, event);
  }

  @Override
  public SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current,
      int sequence,
      StatusEvent event,
      String ownerId,
      @Nullable Instant stateUpdatedAt) {
    return delegate.recordStatusEvent(current, sequence, event, ownerId, stateUpdatedAt);
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
  public SagaStateSnapshot failParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent failedEvent, SagaStatus targetStatus) {
    return delegate.failParkedStep(current, sequence, failedEvent, targetStatus);
  }

  @Override
  public SagaStateSnapshot redriveParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent redriveEvent) {
    return delegate.redriveParkedStep(current, sequence, redriveEvent);
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
  public Optional<NewestEvent> getNewestEvent(String sagaId) {
    return delegate.getNewestEvent(sagaId);
  }

  @Override
  public Optional<SagaStateSnapshot> getStateSnapshot(String sagaId) {
    return delegate.getStateSnapshot(sagaId);
  }

  @Override
  public Optional<SagaStateAndEvents> getStateWithEvents(String sagaId, int maxEvents) {
    return delegate.getStateWithEvents(sagaId, maxEvents);
  }

  @Override
  public SagaPage<SagaStateSnapshot> listStateSnapshots(SagaQuery query) {
    return delegate.listStateSnapshots(query);
  }

  @Override
  public ScanCursor initialSweepCursor(String ownerId) {
    return delegate.initialSweepCursor(ownerId);
  }

  @Override
  public @Nullable ScanCursor advanceSweepCursor(ScanCursor cursor) {
    return delegate.advanceSweepCursor(cursor);
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
      SagaStatus status, Instant threshold, int maxResults, String ownerId, int rotation) {
    return delegate.findByStatusOlderThan(status, threshold, maxResults, ownerId, rotation);
  }

  @Override
  public boolean deleteSaga(String sagaId) {
    return delegate.deleteSaga(sagaId);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
