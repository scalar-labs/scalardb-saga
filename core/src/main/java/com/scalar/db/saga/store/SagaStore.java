package com.scalar.db.saga.store;

import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Persistence interface for saga operations.
 *
 * <p>Implementations must guarantee atomicity: {@link #createSaga} and {@link #recordStatusEvent}
 * write to both the event stream and the state table in a single transaction.
 */
public interface SagaStore {

  // ---------------------------------------------------------------------------
  // Saga lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a new saga instance, writing both a {@link EventType#SAGA_STARTED} event and an initial
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

  /** Persists a saga definition. Called once per definition version at registration time. */
  void registerDefinition(SagaDefinition definition);

  /** Looks up a saga definition by name and version. */
  Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion);

  /**
   * Looks up the latest version of a saga definition by name. "Latest" is determined by the most
   * recent {@code registered_at} timestamp.
   */
  Optional<SagaDefinition> getDefinition(String sagaName);

  // ---------------------------------------------------------------------------
  // Events
  // ---------------------------------------------------------------------------

  /**
   * Records a step-level event in the event stream (no state transition).
   *
   * @param sagaId the saga instance ID
   * @param sequence the event sequence number (tracked by the caller)
   * @param event the step event to record
   */
  void recordStepEvent(String sagaId, int sequence, StepEvent event);

  /**
   * Records a saga-level status event and transitions the saga state atomically in one transaction.
   *
   * <p>The new status is derived from {@link StatusEvent#getTargetStatus()}.
   *
   * @param current the current state snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param event the status event to record
   * @return the post-transition state snapshot
   */
  SagaStateSnapshot recordStatusEvent(SagaStateSnapshot current, int sequence, StatusEvent event);

  /** Returns all events for the given saga, ordered by sequence number. */
  List<SagaEvent> getEvents(String sagaId);

  /** Returns the event count for the given saga without materializing all events. */
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
   * Finds sagas in {@link SagaStatus#RUNNING} or {@link SagaStatus#COMPENSATING} status whose
   * {@code updated_at} is older than the recovery timeout threshold.
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
   * Finds sagas in the given terminal status with {@code updated_at} older than the threshold. Used
   * by the retention manager to find purgeable COMPLETED/COMPENSATED sagas.
   *
   * <p>This method may be removed once the Admin API's {@code listStateSnapshots} query is
   * available (phase 5).
   *
   * @param status the terminal status to scan for
   * @param threshold the cutoff time — only sagas updated before this are returned
   * @param maxResults the maximum number of results to return
   * @return matching saga snapshots, oldest first
   */
  List<SagaStateSnapshot> findByStatusOlderThan(
      SagaStatus status, Instant threshold, int maxResults);

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
