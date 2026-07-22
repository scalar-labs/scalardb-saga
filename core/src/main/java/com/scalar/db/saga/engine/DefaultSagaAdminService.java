package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded implementation of the {@link SagaAdminService} control plane. Reads delegate to the
 * {@link SagaStore}; mutations record an operator-intervention event through {@link
 * SagaStore#recordStatusEvent} — an atomic CAS-guard + co-committed audit + status transition — and
 * then drive the saga inline in the direction {@link RecoveryActionResolver#resolve} chooses,
 * exactly as automatic recovery would (minus the grace-period wait).
 *
 * <p>The single-saga mutations drive inline and return the driven snapshot. The bulk {@link
 * #resetEscalated(SagaQuery, String)} sweep instead only un-escalates each row and hands the drive
 * to the recovery loop (stamping the row's {@code updated_at} to {@link Instant#EPOCH} in the same
 * transition), so one call never blocks on a whole page of participant round-trips.
 *
 * <p>A single-saga drive can also be bounded, by constructing with a positive drive deadline: past
 * it the drive is abandoned and the saga's current state returned, leaving the recovery loop to
 * finish. Both cases lean on the same property the bulk sweep already does — the status transition
 * is the durable work and the drive is only an optimization — so the caller tells a settled drive
 * from an abandoned one by whether the returned status is terminal. Embedded callers leave the
 * deadline unset and drive to completion.
 *
 * <p>The operator identity is read from the injected {@link OperatorContext}, never from the
 * caller; every mutation requires a non-blank {@code reason}, sanitized before it is persisted, and
 * an operator unfit to persist is rejected outright rather than mutated to fit.
 */
@ThreadSafe
public class DefaultSagaAdminService implements SagaAdminService {

  private static final Logger logger = LoggerFactory.getLogger(DefaultSagaAdminService.class);

  private static final int MAX_REASON_LENGTH = 1024;

  /**
   * The longest operator principal that may be stamped on an audit record. 256 clears every
   * realistic principal shape with headroom — RFC 5321 caps an email address at 254 octets, and a
   * JWT {@code sub} is far shorter in practice — while staying well under {@link
   * #MAX_REASON_LENGTH}: a principal should never run longer than a sentence of justification.
   */
  private static final int MAX_OPERATOR_LENGTH = 256;

  private final SagaStore store;
  private final SagaEngine engine;
  private final SagaDefinitionRegistry definitionRegistry;
  private final OperatorContext operatorContext;
  private final long driveDeadlineMillis;

  /** Creates a service whose single-saga drives run to completion on the calling thread. */
  DefaultSagaAdminService(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry definitionRegistry,
      OperatorContext operatorContext) {
    this(store, engine, definitionRegistry, operatorContext, 0L);
  }

  /**
   * Creates a service that bounds how long a single-saga mutation drives before returning.
   *
   * @param driveDeadlineMillis the longest a {@code recoverSaga} or single-saga {@code
   *     resetEscalated} call drives before returning the saga's current state and leaving the rest
   *     to the recovery loop. {@code 0} or less drives on the calling thread with no bound, which
   *     is what an embedded caller wants: there is no request to time out, and the caller chose to
   *     block. A server exposing these over a transport should pass its own request budget, so a
   *     slow saga cannot pin a request thread past a gateway's patience.
   */
  DefaultSagaAdminService(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry definitionRegistry,
      OperatorContext operatorContext,
      long driveDeadlineMillis) {
    this.store = store;
    this.engine = engine;
    this.definitionRegistry = definitionRegistry;
    this.operatorContext = operatorContext;
    this.driveDeadlineMillis = driveDeadlineMillis;
  }

  // ---------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------

  @Override
  public SagaPage<SagaStateSnapshot> listSagas(SagaQuery query) {
    Objects.requireNonNull(query, "query must not be null");
    return store.listStateSnapshots(query);
  }

  // ---------------------------------------------------------------------------
  // Mutations
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot recoverSaga(String sagaId, String reason) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    SagaStateSnapshot snapshot = requireSnapshot(sagaId);
    SagaStatus status = snapshot.getStatus();
    if (status == SagaStatus.WAITING) {
      throw parked(sagaId);
    }
    if (!status.isRecoverable()) {
      throw notRecoverable(sagaId, status);
    }

    SagaDefinition def = resolveDefinitionOrThrow(snapshot);
    List<SagaEvent> events = store.getEvents(sagaId);
    RecoveryAction action = RecoveryActionResolver.resolve(events, def, status);
    StatusEvent recoveringEvent =
        StatusEvent.recovering(action.targetStatus(), operator, sanitizedReason);
    return recordAndRecover(snapshot, def, events, action, recoveringEvent);
  }

  @Override
  public SagaStateSnapshot forceComplete(String sagaId, String reason) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    SagaStateSnapshot snapshot = requireSnapshot(sagaId);
    if (snapshot.getStatus() != SagaStatus.ESCALATED) {
      throw notEscalated(sagaId, snapshot.getStatus(), "force-complete");
    }
    StatusEvent forceCompletedEvent = StatusEvent.forceCompleted(operator, sanitizedReason);
    // ESCALATED -> COMPLETED, atomic with the audit; no drive (terminal). A lost CAS (a concurrent
    // admin/recovery) surfaces as SagaConcurrentModificationException (409).
    return store.recordStatusEvent(
        snapshot, store.getEventCount(sagaId), forceCompletedEvent, engine.ownerId());
  }

  @Override
  public SagaStateSnapshot resetEscalated(String sagaId, String reason) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    SagaStateSnapshot snapshot = requireSnapshot(sagaId);
    if (snapshot.getStatus() != SagaStatus.ESCALATED) {
      throw notEscalated(sagaId, snapshot.getStatus(), "reset");
    }
    SagaDefinition def = resolveDefinitionOrThrow(snapshot);
    return driveReset(snapshot, def, operator, sanitizedReason);
  }

  @Override
  public ResetResult resetEscalated(SagaQuery query, String reason) {
    Objects.requireNonNull(query, "query must not be null");
    String sanitizedReason = validateReason(reason);
    String operator = operator();

    if (query.getStatus() != null && query.getStatus() != SagaStatus.ESCALATED) {
      throw new IllegalArgumentException(
          "resetEscalated only sweeps ESCALATED sagas; conflicting status filter: "
              + query.getStatus());
    }
    // Pin the scan to ESCALATED. The status filter selects the scan position; the per-row CAS at
    // write time is the authorization boundary (a row that changed status is skipped, not forced).
    SagaQuery escalatedQuery =
        SagaQuery.newBuilder()
            .status(SagaStatus.ESCALATED)
            .updatedAfter(query.getUpdatedAfter())
            .updatedBefore(query.getUpdatedBefore())
            .pageSize(query.getPageSize())
            .pageToken(query.getPageToken())
            .build();

    SagaPage<SagaStateSnapshot> page = store.listStateSnapshots(escalatedQuery);
    int resetCount = 0;
    List<ResetResult.SkippedSaga> skipped = new ArrayList<>();
    for (SagaStateSnapshot snapshot : page.getItems()) {
      String sagaId = snapshot.getSagaId();

      SagaDefinition def = null;
      try {
        def = definitionRegistry.resolve(snapshot.getSagaName(), snapshot.getDefinitionVersion());
      } catch (SagaPersistenceException e) {
        // The stored definition could not be decoded. It yields nothing usable, so fall through to
        // the unresolvable path rather than failing the whole sweep over one saga.
        abortSweepIfStoreFailing(e);
      }
      if (def == null) {
        // Unresolvable definition — absent, or stored bytes that cannot be decoded. Either way
        // there is no plan to compute, and the remedy is the same: re-register it, then re-run.
        skipped.add(
            new ResetResult.SkippedSaga(sagaId, ResetResult.SkipReason.DEFINITION_NOT_FOUND));
        continue;
      }

      List<SagaEvent> events;
      try {
        events = store.getEvents(sagaId);
      } catch (SagaPersistenceException e) {
        // This saga's own event stream cannot be decoded, and a retry reads the same bytes. Skip
        // it rather than abort: a saga that resets successfully leaves the ESCALATED scan, so
        // aborting here would make every re-run stop on this same saga and strand every saga
        // behind it.
        abortSweepIfStoreFailing(e);
        // Log the decode failure server-side keyed by sagaId; do not surface the exception message
        // to the caller. A decode error can echo the raw stored bytes (business data or PII), and
        // only fixed daemon-owned messages are ever returned. The reason code is the caller-facing
        // contract; the specifics live in the log.
        logger.warn(
            "Corrupt event stream for saga {} during bulk resetEscalated; skipping it", sagaId, e);
        skipped.add(
            new ResetResult.SkippedSaga(sagaId, ResetResult.SkipReason.CORRUPT_EVENT_STREAM));
        continue;
      }

      try {
        markReset(snapshot, def, events, operator, sanitizedReason);
        resetCount++;
      } catch (SagaConcurrentModificationException e) {
        // Lost the CAS race to a concurrent writer — leave it for the next sweep.
        skipped.add(
            new ResetResult.SkippedSaga(sagaId, ResetResult.SkipReason.CONCURRENT_MODIFICATION));
      }
    }
    return new ResetResult(resetCount, skipped, page.getNextPageToken());
  }

  /**
   * Aborts the sweep when the failure is the store itself failing, rather than this one saga being
   * bad. A retryable persistence exception is transient infrastructure trouble — and the store has
   * already exhausted its own retries reaching this point — so every remaining row would fail the
   * same way; the sweep stops and lets the caller re-run the whole call once the store recovers.
   * Re-running loses nothing: a saga that was not reset is still {@code ESCALATED}, so the next
   * sweep resumes exactly where this one stopped. Returns for a permanent (non-retryable) failure,
   * so the caller can skip this one saga and keep sweeping.
   */
  private static void abortSweepIfStoreFailing(SagaPersistenceException e) {
    if (e.isRetryable()) {
      throw e;
    }
  }

  // ---------------------------------------------------------------------------
  // Shared intervention drive
  // ---------------------------------------------------------------------------

  /**
   * Un-escalates {@code snapshot} and drives it in the direction {@link
   * RecoveryActionResolver#resolve} chooses for its {@code ESCALATED} status — reconstructing from
   * the event stream whether it was compensating or running before it escalated — so a
   * compensation-stuck escalation resumes compensation and a post-pivot one resumes forward.
   */
  private SagaStateSnapshot driveReset(
      SagaStateSnapshot snapshot, SagaDefinition def, String operator, String reason) {
    List<SagaEvent> events = store.getEvents(snapshot.getSagaId());
    RecoveryAction action = RecoveryActionResolver.resolve(events, def, snapshot.getStatus());
    StatusEvent resetEvent = StatusEvent.reset(action.targetStatus(), operator, reason);
    return recordAndRecover(snapshot, def, events, action, resetEvent);
  }

  /**
   * Un-escalates {@code snapshot} durably — the same audit-carrying CAS transition as {@link
   * #driveReset} — but hands the drive to the recovery loop instead of running it inline. The
   * un-escalation stamps the row's {@code updated_at} with {@link Instant#EPOCH} in the same
   * transaction, so the recovery sweeper claims it on its next pass; the bulk sweep thus neither
   * blocks on a whole page of participant round-trips nor pays a second write per saga. The sweeper
   * then drives each un-escalated saga through the same {@link RecoveryActionResolver}/{@link
   * SagaEngine#recover} path the inline reset would have. The un-escalation is the durable work;
   * the drive is only an optimization, so deferring it is safe. Takes {@code events} from the
   * caller, which has already read them (and handled an unreadable stream per saga), so the sweep
   * does not read the same stream twice.
   */
  private void markReset(
      SagaStateSnapshot snapshot,
      SagaDefinition def,
      List<SagaEvent> events,
      String operator,
      String reason) {
    RecoveryAction action = RecoveryActionResolver.resolve(events, def, snapshot.getStatus());
    StatusEvent resetEvent = StatusEvent.reset(action.targetStatus(), operator, reason);
    // EPOCH stamps the un-escalated row as due immediately, handing the drive to the recovery
    // sweeper (co-committed with the transition) rather than driving it inline.
    store.recordStatusEvent(snapshot, events.size(), resetEvent, engine.ownerId(), Instant.EPOCH);
  }

  /**
   * Records the audit-carrying status transition atomically (the CAS guard), then drives the saga
   * in the resolved direction. The engine's compensate path skips its own transition when the saga
   * is already {@code COMPENSATING} (the state the audit event just set), so there is no double
   * transition.
   *
   * <p>The order is the reason a drive deadline can exist at all: the transition is recorded
   * <b>before</b> the drive starts and its failures — a lost CAS, a wrong state — surface from this
   * method regardless of the deadline. Only the drive is bounded, and the drive is merely an
   * optimization over letting the recovery loop pick the saga up, exactly as {@link #markReset}
   * already relies on. A deadline around this method as a whole could expire before the transition
   * was recorded, and would then have to report success for work that had not happened.
   */
  private SagaStateSnapshot recordAndRecover(
      SagaStateSnapshot snapshot,
      SagaDefinition def,
      List<SagaEvent> events,
      RecoveryAction action,
      StatusEvent interventionEvent) {
    ExecutionContext context = engine.replayEvents(snapshot, events);
    SagaStateSnapshot recorded =
        store.recordStatusEvent(snapshot, events.size(), interventionEvent, engine.ownerId());
    context.setCurrentState(recorded);
    context.setNextEventSequence(events.size() + 1);
    if (driveDeadlineMillis <= 0) {
      // Unbounded: drive on the calling thread, which is what an embedded caller wants and keeps
      // this path identical to one with no deadline configured at all.
      engine.recover(action, def, context);
      return context.getCurrentState();
    }
    return boundedRecover(snapshot.getSagaId(), def, action, context);
  }

  /**
   * Drives the saga on the engine's executor, waiting at most {@link #driveDeadlineMillis} for it.
   * Past the deadline the drive is abandoned — not cancelled — and the saga's current stored state
   * is returned; the recovery loop finishes what is left, since the un-escalation or resume
   * transition is already durable.
   *
   * <p>The caller distinguishes the two outcomes from the returned status: terminal means the drive
   * settled, non-terminal means it is still running. That is the same contract the daemon's bounded
   * synchronous start already exposes, where a timeout is not an error but a {@code 202}.
   *
   * <p>On expiry the state is re-read from the store rather than taken from {@code context}: the
   * abandoned drive still owns that context, which is not thread-safe, so reading it here would
   * race. A normal completion is safe to read, because awaiting the future establishes the
   * happens-before edge.
   */
  @SuppressWarnings(
      "FutureReturnValueIgnored") // the post-deadline logging handler is fire-and-forget
  private SagaStateSnapshot boundedRecover(
      String sagaId, SagaDefinition def, RecoveryAction action, ExecutionContext context) {
    CompletableFuture<Void> drive =
        CompletableFuture.runAsync(() -> engine.recover(action, def, context), engine.executor());
    try {
      drive.get(driveDeadlineMillis, TimeUnit.MILLISECONDS);
      return context.getCurrentState();
    } catch (TimeoutException e) {
      logger.info(
          "Admin drive of saga {} exceeded the {}ms deadline; returning its current state and"
              + " leaving the rest to recovery",
          sagaId,
          driveDeadlineMillis);
      // The drive outlives this request, so its eventual failure would otherwise vanish into a
      // future nobody holds.
      drive.whenComplete(
          (ignored, error) -> {
            if (error != null) {
              logger.warn("Abandoned admin drive of saga {} later failed", sagaId, error);
            }
          });
      return requireSnapshot(sagaId);
    } catch (InterruptedException e) {
      // Shutdown, most likely. The transition is durable, so reporting the current state is honest.
      Thread.currentThread().interrupt();
      return requireSnapshot(sagaId);
    } catch (ExecutionException e) {
      throw rethrow(e.getCause());
    }
  }

  /**
   * Rethrows a drive failure as it would have surfaced had the drive run on this thread, so
   * bounding it does not change which exception a caller sees.
   */
  private static RuntimeException rethrow(@Nullable Throwable cause) {
    if (cause instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    if (cause instanceof Error error) {
      throw error;
    }
    return new IllegalStateException("Admin drive failed", cause);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private SagaStateSnapshot requireSnapshot(String sagaId) {
    return store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
  }

  private SagaDefinition resolveDefinitionOrThrow(SagaStateSnapshot snapshot) {
    SagaDefinition def =
        definitionRegistry.resolve(snapshot.getSagaName(), snapshot.getDefinitionVersion());
    if (def == null) {
      throw new SagaDefinitionNotFoundException(
          snapshot.getSagaName(), snapshot.getDefinitionVersion());
    }
    return def;
  }

  private static SagaStatePreconditionException parked(String sagaId) {
    return new SagaStatePreconditionException(
        sagaId,
        SagaStatePreconditionException.Code.SAGA_PARKED,
        "Saga " + sagaId + " is WAITING on an async callback; it resolves via callback or timeout");
  }

  private static SagaStatePreconditionException notEscalated(
      String sagaId, SagaStatus status, String action) {
    return wrongState(
        sagaId,
        "Cannot " + action + " saga " + sagaId + " in status " + status + " (expected ESCALATED)");
  }

  private static SagaStatePreconditionException notRecoverable(String sagaId, SagaStatus status) {
    return wrongState(
        sagaId,
        "Cannot recover saga "
            + sagaId
            + " in status "
            + status
            + " (recover accepts RUNNING or COMPENSATING; for ESCALATED use resetEscalated or"
            + " forceComplete)");
  }

  private static SagaStatePreconditionException wrongState(String sagaId, String message) {
    return new SagaStatePreconditionException(
        sagaId, SagaStatePreconditionException.Code.SAGA_WRONG_STATE, message);
  }

  private static String validateReason(String reason) {
    Objects.requireNonNull(reason, "reason must not be null");
    String sanitized = sanitizeControlChars(reason).trim();
    if (sanitized.isEmpty()) {
      throw new IllegalArgumentException("reason must not be blank");
    }
    if (sanitized.length() > MAX_REASON_LENGTH) {
      throw new IllegalArgumentException(
          "reason must be at most " + MAX_REASON_LENGTH + " characters, got " + sanitized.length());
    }
    return sanitized;
  }

  /**
   * Replaces ISO control characters (newlines included) with spaces; log-forging defense. Spaces
   * rather than removal so that a multi-line reason keeps its word boundaries once flattened.
   */
  private static String sanitizeControlChars(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      sb.append(Character.isISOControl(c) ? ' ' : c);
    }
    return sb.toString();
  }

  /**
   * Returns the operator to stamp on the audit record, rejecting one that is unfit to persist.
   *
   * <p>Unlike {@link #validateReason}, which sanitizes, this <b>rejects</b>. A reason is free text,
   * so flattening a newline out of it is lossy but harmless. An operator is an identity: a mutated
   * principal attributes the action to someone who is not exactly that principal, which is a false
   * audit record — worse than no record at all, given the audit exists precisely to answer who did
   * this. For the same reason the value is not trimmed; a principal with edge whitespace is a
   * different principal, not a formatting artifact.
   *
   * <p>The checks live here, at the persist point every {@link OperatorContext} implementation
   * funnels through, rather than in whichever caller supplies the identity. They are latent in
   * embedded mode, whose principal is a fixed constant; they bind once a daemon wires an
   * authenticated principal through. That principal is not forgeable — it comes from a signed token
   * — but its <em>claim</em> is operator-configurable, and pointing it at a claim the end user can
   * edit (some identity providers let a user set their own {@code name} or {@code email}) would
   * otherwise put user-controlled text into an audit record unbounded and unchecked.
   *
   * <p>Failures are {@link IllegalStateException}, not {@link IllegalArgumentException}: the
   * operator is injected by the server, so an unusable one is a server or identity-provider
   * misconfiguration rather than a defect in the caller's request.
   *
   * @throws IllegalStateException if the operator is blank, over {@link #MAX_OPERATOR_LENGTH}
   *     characters, or contains a control character
   */
  private String operator() {
    String operator = operatorContext.currentOperator();
    if (operator.isBlank()) {
      throw new IllegalStateException(
          "OperatorContext returned a blank operator; rejecting the admin operation rather than"
              + " attributing it to an anonymous principal");
    }
    if (operator.length() > MAX_OPERATOR_LENGTH) {
      // The value itself is deliberately kept out of the message: it is the untrusted thing here.
      throw new IllegalStateException(
          "OperatorContext returned an operator of "
              + operator.length()
              + " characters, over the maximum of "
              + MAX_OPERATOR_LENGTH
              + "; rejecting the admin operation");
    }
    for (int i = 0; i < operator.length(); i++) {
      if (Character.isISOControl(operator.charAt(i))) {
        throw new IllegalStateException(
            "OperatorContext returned an operator containing a control character; rejecting the"
                + " admin operation rather than attributing it to a mutated principal");
      }
    }
    return operator;
  }
}
