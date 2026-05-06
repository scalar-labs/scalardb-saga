package com.scalar.db.saga.store;

import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Persistence interface for saga operations.
 *
 * <p>Implementations must guarantee atomicity: {@link #createSaga} and {@link #recordTransition}
 * write to both the event stream and the state table in a single transaction.
 */
public interface SagaStore {

  // ---------------------------------------------------------------------------
  // Saga lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a new saga instance, writing both a {@link SagaEvent#SAGA_STARTED} event and an initial
   * {@code saga_state} row in one transaction.
   *
   * @param sagaId caller-supplied saga ID, or {@code null} to generate a UUID. When provided, the
   *     ID should be reasonably short (e.g., UUID format) and contain only printable characters.
   * @param sagaName the saga definition name
   * @param ownerId the owner (engine instance) creating this saga
   * @param input the saga input data
   * @param definitionVersion the version of the saga definition to use
   * @return the initial state snapshot
   */
  SagaStateSnapshot createSaga(
      @Nullable String sagaId,
      String sagaName,
      String ownerId,
      Map<String, Object> input,
      String definitionVersion);

  /**
   * Persists a saga definition. Called once per definition version at registration time.
   *
   * <p>Idempotent: registering the same {@code (sagaName, version)} with identical content is a
   * no-op. If the same {@code (sagaName, version)} is registered with different content, throws
   * {@link com.scalar.db.saga.exception.SagaDefinitionException} to fail fast on version conflicts.
   */
  void registerDefinition(SagaDefinition definition);

  /** Looks up a saga definition by name and version. */
  Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion);

  // ---------------------------------------------------------------------------
  // Events
  // ---------------------------------------------------------------------------

  /**
   * Appends a step-level event to the event stream (no state transition).
   *
   * <p>The event must be a step-level event (i.e., {@link SagaEvent#getTargetStatus()} is {@code
   * null} and {@link SagaEvent#getStepIndex()} is non-negative). Use {@link #recordTransition} for
   * saga-level events that change the saga status.
   *
   * @param sagaId the saga instance ID
   * @param sequence the event sequence number (tracked by the caller)
   * @param event the step-level event to append
   */
  void appendEvent(String sagaId, int sequence, SagaEvent event);

  /**
   * Appends a saga-level event and transitions the saga state atomically in one transaction.
   *
   * <p>The new status is derived from {@link SagaEvent#getTargetStatus()}. If the saga has been
   * modified since the given {@code current} snapshot was taken (e.g., by another replica), throws
   * {@link com.scalar.db.saga.exception.SagaConcurrentModificationException}.
   *
   * @param current the caller's view of the current state; the transition is rejected if it is
   *     stale
   * @param sequence the event sequence number
   * @param event the transition event (must have a non-null target status)
   * @return the post-transition state snapshot
   */
  SagaStateSnapshot recordTransition(SagaStateSnapshot current, int sequence, SagaEvent event);

  /** Returns all events for the given saga, ordered by sequence number. */
  List<SagaEvent> getEvents(String sagaId);

  /** Returns the event count for the given saga without deserializing full event payloads. */
  int getEventCount(String sagaId);

  // ---------------------------------------------------------------------------
  // Queries
  // ---------------------------------------------------------------------------

  /** Looks up the current state snapshot for the given saga. */
  Optional<SagaStateSnapshot> getStateSnapshot(String sagaId);

  // Admin query methods (listStateSnapshots, countByStatus, countBySagaName) will be added
  // in the Admin API phase once SagaQuery and SagaPage are defined.

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  /**
   * Finds sagas in {@link SagaStatus#RUNNING}, {@link SagaStatus#CONFIRMING}, or {@link
   * SagaStatus#COMPENSATING} status whose {@code updated_at} is older than the recovery timeout
   * threshold.
   *
   * <p>Each call returns a batch of results. Pass the cursor from the previous result to continue
   * scanning, or {@code null} to start from the beginning. A {@code null} cursor in the returned
   * {@link Recoverables} indicates that the scan is complete.
   *
   * @param recoveryTimeoutMillis the staleness threshold in milliseconds
   * @param cursor the cursor from a previous call, or {@code null} to start a new scan
   * @return recoverable sagas and a cursor for the next batch
   */
  Recoverables findRecoverable(long recoveryTimeoutMillis, @Nullable RecoverablesCursor cursor);

  /**
   * Attempts to claim a saga for recovery by updating its owner. Returns an empty {@link Optional}
   * if the saga has already been claimed by another recovery process (optimistic concurrency
   * check).
   *
   * @param saga the saga snapshot to claim
   * @param newOwnerId the new owner (recovery process) ID
   * @return the claimed snapshot, or empty if already claimed
   */
  Optional<SagaStateSnapshot> claimForRecovery(SagaStateSnapshot saga, String newOwnerId);

  /**
   * Marks a saga for immediate recovery by setting its {@code updated_at} to epoch 0, ensuring it
   * will be picked up by the next recovery scan.
   *
   * @param sagaId the saga instance ID
   */
  void markForRecovery(String sagaId);

  // ---------------------------------------------------------------------------
  // Data retention
  // ---------------------------------------------------------------------------

  /**
   * Deletes all events and state for a terminal saga (COMPLETED, COMPENSATED, or ESCALATED).
   *
   * @param sagaId the saga instance ID to delete
   */
  void deleteSaga(String sagaId);

  // ---------------------------------------------------------------------------
  // Nested types
  // ---------------------------------------------------------------------------

  /**
   * Result of {@link #findRecoverable}: a batch of recoverable saga snapshots and an optional
   * cursor for the next batch.
   *
   * @param sagas the recoverable saga snapshots in this batch
   * @param nextCursor cursor for the next batch, or {@code null} if the scan is complete
   */
  record Recoverables(List<SagaStateSnapshot> sagas, @Nullable RecoverablesCursor nextCursor) {

    /** Creates a new instance, defensively copying the sagas list. */
    public Recoverables {
      sagas = List.copyOf(sagas);
    }

    /** Returns {@code true} if there are more results to fetch. */
    public boolean hasMore() {
      return nextCursor != null;
    }
  }

  /**
   * Opaque cursor for paginating through recoverable sagas returned by {@link #findRecoverable}.
   * Implementations define the internal state (e.g., partition index, page token).
   */
  interface RecoverablesCursor {}
}
