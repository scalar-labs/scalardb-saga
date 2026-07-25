package com.scalar.db.saga.store;

import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
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
public interface SagaStore extends AutoCloseable {

  /** Releases resources held by this store (e.g., connection pools). Default is a no-op. */
  @Override
  default void close() {}

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
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer took
   *     the same {@code sequence} for this saga — an in-doubt commit's verifier reads a row with
   *     someone else's writer id, or a commit conflict exhausts the retries on a stale sequence
   */
  void recordStepEvent(String sagaId, int sequence, StepEvent event);

  /**
   * Records a saga-level status event and transitions the saga state atomically in one transaction,
   * stamping the transitioned row's {@code updated_at} with the transition time. A convenience for
   * {@link #recordStatusEvent(SagaStateSnapshot, int, StatusEvent, String, Instant)} with a {@code
   * null} timestamp.
   *
   * @param current the current state snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param event the status event to record
   * @param ownerId the replica recording this transition (stamped as the saga's owner)
   * @return the post-transition state snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first, so this call loses the optimistic concurrency check
   */
  default SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current, int sequence, StatusEvent event, String ownerId) {
    return recordStatusEvent(current, sequence, event, ownerId, null);
  }

  /**
   * Records a saga-level status event and transitions the saga state atomically in one transaction.
   *
   * <p>The new status is derived from {@link StatusEvent#getTargetStatus()}. The transitioned row
   * is stamped with {@code ownerId} as the replica now processing this saga (an observability
   * field), so the driver reflected in {@code owner_id} stays current across engine, recovery, and
   * admin transitions.
   *
   * <p>{@code stateUpdatedAt} sets the transitioned row's {@code updated_at}. That column is also
   * the recovery sweeper's scan key (it scans oldest-first), so passing {@link Instant#EPOCH} hands
   * the saga to the sweeper for immediate pickup — the co-committed equivalent of a following
   * {@link #markForRecovery}, without the second transaction. Pass {@code null} to stamp the
   * current transition time; that is the normal case, for which the four-argument overload reads
   * better. The event's own audit timestamp is always the real time regardless.
   *
   * @param current the current state snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param event the status event to record
   * @param ownerId the replica recording this transition (stamped as the saga's owner)
   * @param stateUpdatedAt the {@code updated_at} to stamp on the transitioned row, or {@code null}
   *     for the current transition time; {@link Instant#EPOCH} schedules the saga for immediate
   *     recovery
   * @return the post-transition state snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first, so this call loses the optimistic concurrency check
   */
  SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current,
      int sequence,
      StatusEvent event,
      String ownerId,
      @Nullable Instant stateUpdatedAt);

  /**
   * Parks a forward step on an async callback, in one transaction: appends {@code pendingEvent},
   * transitions the saga {@code RUNNING → WAITING}, and — when {@code parkedDeadline} is non-null —
   * writes a {@code saga_parked} row so the recovery sweeper can time it out. A {@code null}
   * deadline means "wait indefinitely" and writes no {@code saga_parked} row.
   *
   * @param current the current ({@code RUNNING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param pendingEvent the {@link EventType#STEP_PENDING} event marking which step parked
   * @param parkedDeadline the absolute timeout deadline, or {@code null} for an unbounded wait
   * @return the post-transition ({@code WAITING}) snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first, so this call loses the optimistic concurrency check
   */
  SagaStateSnapshot park(
      SagaStateSnapshot current,
      int sequence,
      StepEvent pendingEvent,
      @Nullable Instant parkedDeadline);

  /**
   * Resumes a parked step when its callback arrives, in one transaction: appends {@code
   * completedEvent}, transitions the saga {@code WAITING → RUNNING}, and deletes the {@code
   * saga_parked} row (if any). The optimistic check on the {@code WAITING} row makes this and the
   * deadline-timeout sweep mutually exclusive.
   *
   * @param current the current ({@code WAITING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param completedEvent the {@link EventType#STEP_COMPLETED} event carrying the callback output
   * @return the post-transition ({@code RUNNING}) snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first, so this call loses the optimistic concurrency check
   */
  SagaStateSnapshot resumeParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent completedEvent);

  /**
   * Fails (gives up on) a parked step, in one transaction: appends {@code failedEvent}, transitions
   * the saga {@code WAITING → targetStatus}, and deletes the {@code saga_parked} row (if any). Used
   * by the recovery sweep once a parked step's re-drive budget is spent (retry attempts or grace
   * period), or when its definition can't be resolved. The optimistic check on the {@code WAITING}
   * row makes this and a concurrent callback ({@link #resumeParkedStep}) / re-drive ({@link
   * #redriveParkedStep}) mutually exclusive.
   *
   * @param current the current ({@code WAITING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param failedEvent the {@link EventType#STEP_FAILED} event for the given-up step
   * @param targetStatus {@code COMPENSATING} (pre-pivot, will compensate) or {@code ESCALATED}
   *     (post-pivot, needs manual resolution)
   * @return the post-transition snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first, so this call loses the optimistic concurrency check
   * @throws IllegalArgumentException if {@code targetStatus} is not {@code COMPENSATING} or {@code
   *     ESCALATED}
   */
  SagaStateSnapshot failParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent failedEvent, SagaStatus targetStatus);

  /**
   * Un-parks a timed-out step to re-drive it, in one transaction: appends {@code redriveEvent}
   * ({@link EventType#STEP_REISSUING}), transitions the saga {@code WAITING → RUNNING}, and deletes
   * the {@code saga_parked} row. The recovery sweep then re-executes the step, which re-parks it
   * with a fresh deadline. The optimistic check on the {@code WAITING} row makes this and a
   * concurrent callback ({@link #resumeParkedStep}) / timeout ({@link #failParkedStep}) mutually
   * exclusive.
   *
   * @param current the current ({@code WAITING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param redriveEvent the {@link EventType#STEP_REISSUING} event for the un-parked step
   * @return the post-transition ({@code RUNNING}) snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first, so this call loses the optimistic concurrency check
   */
  SagaStateSnapshot redriveParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent redriveEvent);

  /** Returns all events for the given saga, ordered by sequence number. */
  List<SagaEvent> getEvents(String sagaId);

  /** Returns the event count for the given saga without materializing all events. */
  int getEventCount(String sagaId);

  // ---------------------------------------------------------------------------
  // Queries
  // ---------------------------------------------------------------------------

  /** Looks up the current state snapshot for the given saga. */
  Optional<SagaStateSnapshot> getStateSnapshot(String sagaId);

  /**
   * Reads a saga's state snapshot and its full event stream in a single transaction, so the two are
   * a coherent point-in-time view — a concurrent status transition cannot pair a stale snapshot
   * with a timeline that already contains the newer event. Returns empty if the saga does not
   * exist.
   */
  Optional<SagaStateAndEvents> getStateWithEvents(String sagaId);

  /**
   * Lists saga state snapshots matching the given query, one page at a time. Used by the Admin
   * API's listing operation.
   *
   * <p>Results are grouped by {@code (bucket, status)} and ordered by the clustering key within
   * each group; they are not globally sorted by time. Pagination uses the query's opaque {@link
   * SagaQuery#getPageToken() page token}; a {@code null} {@link SagaPage#getNextPageToken()} in the
   * result indicates the listing is complete. Listing is best-effort under concurrent mutation: a
   * saga whose status changes mid-listing may appear zero, one, or two times, but progress is
   * monotonic and terminates.
   *
   * <p>Aggregate counts ({@code countByStatus}/{@code countBySagaName}) are intentionally not part
   * of this interface — they belong in the observability (OTel) layer, not a transactional store
   * scan.
   *
   * @param query the filter, page size, and continuation token
   * @return a page of matching snapshots and a token for the next page
   * @throws IllegalArgumentException if the page token is malformed or does not match the query
   */
  SagaPage<SagaStateSnapshot> listStateSnapshots(SagaQuery query);

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  /**
   * Finds sagas in {@link SagaStatus#RUNNING} or {@link SagaStatus#COMPENSATING} status whose
   * {@code updated_at} is older than the given threshold. The caller computes the staleness cutoff
   * from its clock, mirroring {@link #findOverdueParkedSagas} and {@link #findByStatusOlderThan}.
   *
   * <p>Each call returns a batch of results. Pass the cursor from the previous result to continue
   * scanning, or {@code null} to start from the beginning. A {@code null} cursor in the returned
   * {@link Recoverables} indicates that the scan is complete.
   *
   * @param threshold the staleness cutoff — only sagas updated before this are returned
   * @param cursor the cursor from a previous call, or {@code null} to start a new scan
   * @return recoverable sagas and a cursor for the next batch
   */
  Recoverables findRecoverable(Instant threshold, @Nullable ScanCursor cursor);

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
  // Parked-step timeout (async WAITING sagas)
  // ---------------------------------------------------------------------------

  /**
   * Finds parked (async {@code WAITING}) sagas whose timeout deadline is at or before {@code
   * threshold}, from the dedicated {@code saga_parked} index. Used by recovery to time out async
   * steps whose callback never arrived. A parked step with no timeout (wait indefinitely) has no
   * index row and is never returned.
   *
   * <p>Cursor-paged one bucket per call, like {@link #findRecoverable}: pass the previous result's
   * cursor to continue, or {@code null} to start. A {@code null} {@link OverdueParked#nextCursor()}
   * in the result means the scan is complete.
   *
   * @param threshold the cutoff time — parked sagas with a deadline at or before this are returned
   * @param cursor the cursor from a previous call, or {@code null} to start a new scan
   * @return a batch of overdue parked saga IDs and a cursor for the next batch
   */
  OverdueParked findOverdueParkedSagas(Instant threshold, @Nullable ScanCursor cursor);

  // ---------------------------------------------------------------------------
  // Data retention
  // ---------------------------------------------------------------------------

  /**
   * Finds sagas in the given terminal status with {@code updated_at} older than the threshold. Used
   * by the retention manager to find purgeable COMPLETED/COMPENSATED sagas.
   *
   * <p>This method may be removed once the Admin API's {@code listStateSnapshots} query is
   * available.
   *
   * @param status the terminal status to scan for
   * @param threshold the cutoff time — only sagas updated before this are returned
   * @param maxResults the maximum number of results to return
   * @return matching saga snapshots (order is not guaranteed across buckets)
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
  record Recoverables(List<SagaStateSnapshot> sagas, @Nullable ScanCursor nextCursor) {

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
   * Result of {@link #findOverdueParkedSagas}: a batch of overdue parked saga IDs and an optional
   * cursor for the next batch.
   *
   * @param sagaIds the overdue parked saga IDs in this batch
   * @param nextCursor cursor for the next batch, or {@code null} if the scan is complete
   */
  record OverdueParked(List<String> sagaIds, @Nullable ScanCursor nextCursor) {

    /** Creates a new instance, defensively copying the IDs list. */
    public OverdueParked {
      sagaIds = List.copyOf(sagaIds);
    }

    /** Returns {@code true} if there are more results to fetch. */
    public boolean hasMore() {
      return nextCursor != null;
    }
  }

  /**
   * Opaque cursor for paginating a bucket-partitioned scan ({@link #findRecoverable}, {@link
   * #findOverdueParkedSagas}). Implementations define the internal state (e.g., the next bucket
   * index).
   */
  interface ScanCursor {}
}
