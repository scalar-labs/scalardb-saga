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
 * <p>Implementations must guarantee atomicity: every operation that changes a saga's state appends
 * its event and changes the state in a single transaction.
 *
 * <p>This interface carries {@link SagaStateSnapshot} as its currency, and that is a published
 * {@code api} type. Server-internal bookkeeping therefore stops here rather than riding it: an
 * implementation keeps such a field in its own rows and never surfaces it on the snapshot. See
 * {@link SagaStateSnapshot} for the rule. The {@code ownerId} a caller passes names the replica
 * making the change; an implementation may record it but never returns it.
 *
 * <p><b>A parked saga's log grows only through status changes.</b> Once a saga is {@code WAITING},
 * the next event appended to it is the one that moves it out: a resume, a give-up or a re-drive.
 * Nothing appends a plain step event or a claim to a parked saga. The ScalarDB store relies on this
 * to find a parked saga's state from the event just before the caller's sequence, and reports a log
 * that breaks it as damage.
 */
public interface SagaStore extends AutoCloseable {

  /** Releases resources held by this store (e.g., connection pools). Default is a no-op. */
  @Override
  default void close() {}

  // ---------------------------------------------------------------------------
  // Saga lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a new saga instance in one transaction: appends {@link EventType#SAGA_STARTED} and
   * enters the saga in the recovery sweep as {@code RUNNING}.
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
   * @param definition the definition to persist
   */
  void registerDefinition(SagaDefinition definition);

  /**
   * Looks up a saga definition by name and version.
   *
   * @param sagaName the saga definition name
   * @param definitionVersion the version to look up
   * @return the definition, or empty if that name and version is not registered
   */
  Optional<SagaDefinition> getDefinition(String sagaName, String definitionVersion);

  /**
   * Looks up the latest version of a saga definition by name. "Latest" is determined by the most
   * recent {@code registered_at} timestamp.
   *
   * @param sagaName the saga definition name
   * @return the most recently registered version, or empty if none is registered under the name
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
   *     the same {@code sequence} for this saga; the row there carries someone else's writer id, as
   *     read by an in-doubt commit's verifier or by the read-back once conflicts have exhausted the
   *     retries
   */
  void recordStepEvent(String sagaId, int sequence, StepEvent event);

  /**
   * Records a saga-level status event and transitions the saga state atomically in one transaction,
   * stamping the saga's {@code updatedAt} with the transition time. A convenience for {@link
   * #recordStatusEvent(SagaStateSnapshot, int, StatusEvent, String, Instant)} with a {@code null}
   * timestamp.
   *
   * @param current the current state snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param event the status event to record
   * @param ownerId the replica recording this transition
   * @return the post-transition state snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first: the saga's state has moved on since {@code current}, or the
   *     event row at {@code sequence} carries someone else's writer id, as read by an in-doubt
   *     commit's verifier or by the read-back once conflicts have exhausted the retries
   */
  default SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current, int sequence, StatusEvent event, String ownerId) {
    return recordStatusEvent(current, sequence, event, ownerId, null);
  }

  /**
   * Records a saga-level status event and transitions the saga state atomically in one transaction.
   *
   * <p>The new status is derived from {@link StatusEvent#getTargetStatus()}.
   *
   * <p>{@code stateUpdatedAt} sets the saga's {@code updatedAt}. That time is also the recovery
   * sweeper's scan key (it scans oldest-first), so passing {@link Instant#EPOCH} hands the saga to
   * the sweeper for immediate pickup — the co-committed equivalent of a following {@link
   * #markForRecovery}, without the second transaction. Pass {@code null} to stamp the current
   * transition time; that is the normal case, for which the four-argument overload reads better.
   * The event's own audit timestamp is always the real time regardless.
   *
   * @param current the current state snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param event the status event to record
   * @param ownerId the replica recording this transition
   * @param stateUpdatedAt the {@code updatedAt} to give the saga, or {@code null} for the current
   *     transition time; {@link Instant#EPOCH} schedules the saga for immediate recovery
   * @return the post-transition state snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first: the saga's state has moved on since {@code current}, or the
   *     event row at {@code sequence} carries someone else's writer id, as read by an in-doubt
   *     commit's verifier or by the read-back once conflicts have exhausted the retries
   */
  SagaStateSnapshot recordStatusEvent(
      SagaStateSnapshot current,
      int sequence,
      StatusEvent event,
      String ownerId,
      @Nullable Instant stateUpdatedAt);

  /**
   * Parks a forward step on an async callback, in one transaction: appends {@code pendingEvent},
   * and transitions the saga {@code RUNNING → WAITING}. With a non-null {@code parkedDeadline} the
   * recovery sweeper times the step out once the deadline passes; a {@code null} deadline means
   * "wait indefinitely", and the step is never timed out.
   *
   * @param current the current ({@code RUNNING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param pendingEvent the {@link EventType#STEP_PENDING} event marking which step parked
   * @param parkedDeadline the absolute timeout deadline, or {@code null} for an unbounded wait
   * @return the post-transition ({@code WAITING}) snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first: the saga's state has moved on since {@code current}, or the
   *     event row at {@code sequence} carries someone else's writer id, as read by an in-doubt
   *     commit's verifier or by the read-back once conflicts have exhausted the retries
   */
  SagaStateSnapshot park(
      SagaStateSnapshot current,
      int sequence,
      StepEvent pendingEvent,
      @Nullable Instant parkedDeadline);

  /**
   * Resumes a parked step when its callback arrives, in one transaction: appends {@code
   * completedEvent} and transitions the saga {@code WAITING → RUNNING}, which also takes it out of
   * the timeout sweep. The check that the saga is still in the {@code WAITING} state of {@code
   * current} makes this and the deadline-timeout sweep mutually exclusive.
   *
   * @param current the current ({@code WAITING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param completedEvent the {@link EventType#STEP_COMPLETED} event carrying the callback output
   * @return the post-transition ({@code RUNNING}) snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first: the saga's state has moved on since {@code current}, or the
   *     event row at {@code sequence} carries someone else's writer id, as read by an in-doubt
   *     commit's verifier or by the read-back once conflicts have exhausted the retries
   */
  SagaStateSnapshot resumeParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent completedEvent);

  /**
   * Fails (gives up on) a parked step, in one transaction: appends {@code failedEvent}, transitions
   * the saga {@code WAITING → targetStatus}, which also takes it out of the timeout sweep. Used by
   * the recovery sweep once a parked step's re-drive budget is spent (retry attempts or grace
   * period), or when its definition can't be resolved. The check that the saga is still in the
   * {@code WAITING} state of {@code current} makes this and a concurrent callback ({@link
   * #resumeParkedStep}) / re-drive ({@link #redriveParkedStep}) mutually exclusive.
   *
   * @param current the current ({@code WAITING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param failedEvent the {@link EventType#STEP_FAILED} event for the given-up step
   * @param targetStatus {@code COMPENSATING} (pre-pivot, will compensate) or {@code ESCALATED}
   *     (post-pivot, needs manual resolution)
   * @return the post-transition snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first: the saga's state has moved on since {@code current}, or the
   *     event row at {@code sequence} carries someone else's writer id, as read by an in-doubt
   *     commit's verifier or by the read-back once conflicts have exhausted the retries
   * @throws IllegalArgumentException if {@code targetStatus} is not {@code COMPENSATING} or {@code
   *     ESCALATED}
   */
  SagaStateSnapshot failParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent failedEvent, SagaStatus targetStatus);

  /**
   * Un-parks a timed-out step to re-drive it, in one transaction: appends {@code redriveEvent}
   * ({@link EventType#STEP_REISSUING}) and transitions the saga {@code WAITING → RUNNING}. The
   * recovery sweep then re-executes the step, which re-parks it with a fresh deadline. The check
   * that the saga is still in the {@code WAITING} state of {@code current} makes this and a
   * concurrent callback ({@link #resumeParkedStep}) / timeout ({@link #failParkedStep}) mutually
   * exclusive.
   *
   * @param current the current ({@code WAITING}) snapshot (used for optimistic concurrency)
   * @param sequence the event sequence number
   * @param redriveEvent the {@link EventType#STEP_REISSUING} event for the un-parked step
   * @return the post-transition ({@code RUNNING}) snapshot
   * @throws com.scalar.db.saga.exception.SagaConcurrentModificationException if another writer
   *     transitioned the saga first: the saga's state has moved on since {@code current}, or the
   *     event row at {@code sequence} carries someone else's writer id, as read by an in-doubt
   *     commit's verifier or by the read-back once conflicts have exhausted the retries
   */
  SagaStateSnapshot redriveParkedStep(
      SagaStateSnapshot current, int sequence, StepEvent redriveEvent);

  /**
   * Returns all events for the given saga, ordered by sequence number.
   *
   * @param sagaId the saga instance ID
   * @return the saga's events in ascending sequence order
   */
  List<SagaEvent> getEvents(String sagaId);

  /**
   * Returns the event count for the given saga without materializing all events.
   *
   * @param sagaId the saga instance ID
   * @return the number of events in the saga's stream
   */
  int getEventCount(String sagaId);

  /**
   * The newest event's type, stamp and sequence, when the saga was last claimed for recovery, and
   * the saga's state: everything recovery needs to judge whether a saga is being driven, and to
   * claim it.
   *
   * @param type the event's type
   * @param createdAt when the event was appended
   * @param sequence the event's sequence; a claim appends at the next one, so it fails if anything
   *     was appended since this read
   * @param lastClaimedAt when the saga was last claimed, if no status change has happened since,
   *     else {@code null}
   * @param state the saga's state as of this read; a claim refuses a sweep's snapshot that no
   *     longer matches it
   */
  record NewestEvent(
      EventType type,
      Instant createdAt,
      int sequence,
      @Nullable Instant lastClaimedAt,
      SagaStateSnapshot state) {}

  /**
   * Returns the newest event for this saga, or empty when it has none at all.
   *
   * <p>Recovery uses this as a progress probe: a saga whose last status change looks stale but
   * whose event stream is recent was <em>recently driven</em>, so it must not be claimed on the age
   * of its status alone. The type matters because some events say the opposite — a compensation
   * failure is written by a drive that then gives up, so it marks the end of an attempt rather than
   * progress. {@link NewestEvent#lastClaimedAt()} says whether another replica already took the
   * saga over since its last status change.
   *
   * <p>An empty result for a saga a sweep returned means the sweep table and the event log
   * disagree, which the store itself cannot produce, so the caller should treat it as damage rather
   * than as an absence of progress.
   *
   * <p>"Newest" means highest {@code sequence}, not latest wall clock. Events are stamped by
   * whichever replica appended them, so under cross-replica skew the highest-sequence row can carry
   * an earlier stamp than the one before it; that is bounded by the same small-skew assumption the
   * recovery staleness threshold already rests on.
   *
   * <p>Runs in its own read-only transaction.
   *
   * @param sagaId the saga instance ID
   * @return the newest event's type and stamp, or empty when the saga has no events
   * @throws com.scalar.db.saga.exception.SagaPersistenceException if the read fails, or if the
   *     stored event type is not one this version knows — which a rolling upgrade can produce
   */
  Optional<NewestEvent> getNewestEvent(String sagaId);

  // ---------------------------------------------------------------------------
  // Queries
  // ---------------------------------------------------------------------------

  /**
   * Looks up the current state snapshot for the given saga.
   *
   * @param sagaId the saga instance ID
   * @return the snapshot, or empty if no saga has that ID
   */
  Optional<SagaStateSnapshot> getStateSnapshot(String sagaId);

  /**
   * Reads a saga's state snapshot and its event stream in a single transaction, so the two are a
   * coherent point-in-time view — a concurrent status transition cannot pair a stale snapshot with
   * a timeline that already contains the newer event. Returns empty if the saga does not exist.
   *
   * <p>At most {@code maxEvents} events are returned, always in ascending sequence order. When the
   * stream is longer, the <b>newest</b> {@code maxEvents} events are kept (the recent history is
   * the diagnostically useful slice) and the result is flagged {@link
   * SagaStateAndEvents#truncated()}; the full history remains in the store. Pass {@link
   * Integer#MAX_VALUE} for an unbounded read.
   *
   * @param sagaId the saga id
   * @param maxEvents the maximum number of events to return; must be positive
   * @return the snapshot paired with its events, or empty if the saga does not exist
   */
  Optional<SagaStateAndEvents> getStateWithEvents(String sagaId, int maxEvents);

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
   * Creates the initial cursor for one full sweep of the bucket ring in the given owner's scattered
   * order. Each replica derives its own pseudo-random bucket permutation from its owner ID, so
   * concurrently sweeping replicas surface different sagas first instead of racing over the same
   * ones in lockstep. Feed the cursor to {@link #findRecoverable} or {@link
   * #findOverdueParkedSagas} and follow the returned cursors; the sweep ends after one revolution.
   * Obtain a fresh cursor for each new revolution — but a cursor from an unfinished revolution may
   * be carried into a later pass to resume it (the recovery manager's cross-pass resume does
   * exactly that), so implementations must not tie cursor validity to a single pass.
   *
   * <p><b>Store decorators must delegate this method</b> — falling back to a {@code null} cursor
   * silently reverts the sweep to the un-scattered ascending order.
   *
   * @param ownerId the sweeping replica's owner ID
   * @return a cursor positioned at the start of this owner's sweep order
   */
  ScanCursor initialSweepCursor(String ownerId);

  /**
   * Advances a sweep cursor past the bucket page it points at without scanning it. Sweeping callers
   * use this to skip a page whose scan keeps failing (e.g., a row that cannot be deserialized), so
   * one poison page cannot end the sweep and permanently starve the buckets after it. A pure
   * function of the cursor — no store access.
   *
   * @param cursor the cursor whose page should be skipped
   * @return the cursor for the following page, or {@code null} if the skipped page was the sweep's
   *     last
   */
  @Nullable ScanCursor advanceSweepCursor(ScanCursor cursor);

  /**
   * Finds sagas in {@link SagaStatus#RUNNING} or {@link SagaStatus#COMPENSATING} status whose
   * {@code updated_at} is older than the given threshold. The caller computes the staleness cutoff
   * from its clock, mirroring {@link #findOverdueParkedSagas} and {@link #findByStatusOlderThan}.
   *
   * <p>Each call returns a batch of results. Pass the cursor from the previous result to continue
   * scanning. Start from {@link #initialSweepCursor} to sweep in the owner's scattered order; a
   * {@code null} cursor starts an un-scattered ascending sweep from bucket 0, kept for
   * backward-compatible direct store access in tests. A {@code null} cursor in the returned {@link
   * Recoverables} indicates that the scan is complete.
   *
   * <p>A store may cap how many sagas one call returns from a bucket. When it does, the next sweep
   * of that bucket continues after the last saga returned, and starts over once it reaches the end,
   * so sagas a claim leaves in place cannot hide the ones behind them. Sagas handed to recovery at
   * {@link Instant#EPOCH} are read ahead of that position on every sweep, so a hand-off does not
   * wait for the scan to wrap.
   *
   * @param threshold the staleness cutoff — only sagas updated before this are returned
   * @param cursor the cursor from a previous call, {@link #initialSweepCursor}, or {@code null} to
   *     start an ascending scan
   * @return recoverable sagas and a cursor for the next batch
   */
  Recoverables findRecoverable(Instant threshold, @Nullable ScanCursor cursor);

  /**
   * Returns how many rows one {@link #findRecoverable} call can return, or {@code 0} if this store
   * does not page recovery scans.
   *
   * <p>The recovery manager compares its per-sweep budget against this at startup: a budget below
   * one page truncates the page, and the cut falls on whichever recoverable status the store
   * returns last. A decorator must delegate this rather than answer for itself.
   *
   * @return rows one {@code findRecoverable} call can return, or {@code 0} if not applicable
   */
  default int recoveryPageSize() {
    return 0;
  }

  /**
   * Claims a saga for recovery, provided nothing was appended to it since {@code probe} was read
   * and its status and {@code updatedAt} are still the ones {@code saga} carries. Returns empty
   * otherwise: another replica claimed it first, its driver is still alive, it changed state
   * between the sweep and the probe, or it was purged (and perhaps recreated under the same id)
   * since. Exactly one of two replicas claiming from the same probe succeeds.
   *
   * <p>The claim appends {@link EventType#SAGA_CLAIMED} at the sequence after {@code probe}'s. It
   * leaves the saga's status and {@code updatedAt} as they are, except for a saga handed to
   * recovery with {@code updatedAt} at {@code EPOCH} ({@link #markForRecovery}): that one is moved
   * to the claim time, so the next sweep does not offer it again.
   *
   * @param saga the snapshot a recovery sweep returned; {@code RUNNING} or {@code COMPENSATING}
   * @param probe the saga's newest event, read by {@link #getNewestEvent} just before the claim
   * @param newOwnerId the claiming replica's ID, recorded in the claim event
   * @return the claimed snapshot, or empty if the saga changed since the sweep or {@code probe}
   * @throws IllegalArgumentException if {@code saga} is not {@code RUNNING} or {@code COMPENSATING}
   */
  Optional<SagaStateSnapshot> claimForRecovery(
      SagaStateSnapshot saga, NewestEvent probe, String newOwnerId);

  /**
   * Hands a saga being driven here to recovery, so the next recovery sweep takes it at once: its
   * {@code updatedAt} becomes {@code EPOCH}, and {@link EventType#SAGA_RELEASED} records the
   * hand-off. Acts only on a {@code RUNNING} or {@code COMPENSATING} saga. Best effort: a failure
   * is logged, and recovery takes the saga after the staleness threshold instead.
   *
   * @param sagaId the saga instance ID
   */
  void markForRecovery(String sagaId);

  // ---------------------------------------------------------------------------
  // Parked-step timeout (async WAITING sagas)
  // ---------------------------------------------------------------------------

  /**
   * Finds parked (async {@code WAITING}) sagas whose timeout deadline is at or before {@code
   * threshold}. Used by recovery to time out async steps whose callback never arrived. A parked
   * step with no timeout (wait indefinitely) is never returned.
   *
   * <p>Cursor-paged one bucket per call, like {@link #findRecoverable}: pass the previous result's
   * cursor to continue, starting from {@link #initialSweepCursor} for the owner's scattered order
   * (or {@code null} for an ascending scan). A {@code null} {@link OverdueParked#nextCursor()} in
   * the result means the scan is complete.
   *
   * @param threshold the cutoff time — parked sagas with a deadline at or before this are returned
   * @param cursor the cursor from a previous call, {@link #initialSweepCursor}, or {@code null} to
   *     start an ascending scan
   * @return a batch of overdue parked saga IDs and a cursor for the next batch
   */
  OverdueParked findOverdueParkedSagas(Instant threshold, @Nullable ScanCursor cursor);

  // ---------------------------------------------------------------------------
  // Data retention
  // ---------------------------------------------------------------------------

  /**
   * Finds sagas in the given terminal status with {@code updated_at} older than the threshold. Used
   * by the retention manager to find purgeable COMPLETED/COMPENSATED sagas. Buckets are visited in
   * the owner's scattered sweep order (see {@link #initialSweepCursor}) starting {@code rotation}
   * positions into it, so when {@code maxResults} truncates the result, concurrently purging
   * replicas surface different sagas first — and successive passes (a caller-incremented rotation)
   * give every bucket a turn at the front, so a sustained backlog cannot starve the buckets at the
   * tail of one fixed order. A bucket whose scan fails is skipped for this call rather than
   * abandoning the buckets after it; the next call retries it. Consecutive scan failures are the
   * signature of an unavailable store rather than a poison bucket, and propagate instead of being
   * swept bucket by bucket, so the caller's pass fails loudly once.
   *
   * <p>This method may be removed once the Admin API's {@code listStateSnapshots} query is
   * available.
   *
   * @param status the terminal status to scan for
   * @param threshold the cutoff time — only sagas updated before this are returned
   * @param maxResults the maximum number of results to return
   * @param ownerId the purging replica's owner ID, selecting its bucket sweep order
   * @param rotation how many positions into the sweep order to start; callers increment it once per
   *     cleanup pass (any int is accepted; it is reduced modulo the bucket count)
   * @return matching saga snapshots (order follows the owner's rotated bucket permutation)
   */
  List<SagaStateSnapshot> findByStatusOlderThan(
      SagaStatus status, Instant threshold, int maxResults, String ownerId, int rotation);

  /**
   * Deletes all events and state for a terminal saga (COMPLETED, COMPENSATED, or ESCALATED).
   *
   * @param sagaId the saga instance ID to delete
   * @return {@code true} if this call deleted the saga; {@code false} if the saga was already
   *     purged (e.g., by a concurrently sweeping replica), so callers budgeting work can tell real
   *     purges from no-ops
   */
  boolean deleteSaga(String sagaId);

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

    /**
     * Returns {@code true} if there are more results to fetch.
     *
     * @return whether {@code nextCursor} is non-null
     */
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

    /**
     * Returns {@code true} if there are more results to fetch.
     *
     * @return whether {@code nextCursor} is non-null
     */
    public boolean hasMore() {
      return nextCursor != null;
    }
  }

  /**
   * Opaque cursor for paginating a bucket-partitioned scan ({@link #findRecoverable}, {@link
   * #findOverdueParkedSagas}). Implementations define the internal state (e.g., the next bucket
   * index). Cursors are only valid with the store that created them: passing a foreign cursor type
   * throws {@link IllegalArgumentException} rather than silently restarting an un-scattered scan.
   */
  interface ScanCursor {}
}
