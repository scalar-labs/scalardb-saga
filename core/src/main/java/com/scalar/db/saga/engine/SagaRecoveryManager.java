package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStore.OverdueParked;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import com.scalar.db.saga.store.SagaStore.ScanCursor;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.store.SweepScatter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic recovery manager that scans for stale sagas and resumes them.
 *
 * <p>On each recovery pass, sweeps the {@code saga_state} buckets for sagas in {@link
 * SagaStatus#RUNNING} or {@link SagaStatus#COMPENSATING} whose {@code updated_at} is older than
 * {@link RecoveryConfig#recoveryTimeoutMillis()}. For each recoverable saga:
 *
 * <ol>
 *   <li>Claims via {@link SagaStore#claimForRecovery} (optimistic concurrency).
 *   <li>Resolves the exact definition version via {@link SagaDefinitionRegistry#resolve}.
 *   <li>Replays events via {@link SagaEngine#replayEvents} to reconstruct state.
 *   <li>Resumes forward ({@code RUNNING}) or compensation ({@code COMPENSATING}).
 *   <li>Escalates to {@link SagaStatus#ESCALATED} if stuck longer than the grace period.
 * </ol>
 *
 * <p><b>Multi-replica de-collision (best effort).</b> Concurrently sweeping replicas would
 * otherwise do each other's work: the claim protocol guarantees one winner per saga, but losers
 * waste aborted claim transactions. Three coordination-free mechanisms, all derived from the
 * replica's {@code ownerId}, keep replicas apart: each replica sweeps buckets in its own scattered
 * permutation ({@link SagaStore#initialSweepCursor}); the batch budget forgives lost races (only
 * committed work and failures consume it), so contention never exhausts a pass; and the periodic
 * schedule is de-phased by a deterministic per-replica offset (the startup pass stays immediate).
 * Residual collisions are correctness-safe and visible in the per-pass summary log.
 */
class SagaRecoveryManager {

  private static final Logger logger = LoggerFactory.getLogger(SagaRecoveryManager.class);

  // This many consecutive failed page scans within one sweep mean the store itself is down, not
  // that a poison page needs skipping; the sweep stops for the pass instead of failing bucket by
  // bucket through the whole ring.
  private static final int MAX_CONSECUTIVE_FAILED_PAGES = 3;

  // A drive still holding its saga after this many recovery timeouts is stuck rather than slow: a
  // healthy one emits an event at every step boundary, and the timeout is already sized above a
  // single step's worst case. Not a knob — it only decides when a log line appears.
  private static final int HUNG_DRIVE_WARN_MULTIPLE = 10;

  private final SagaStore store;
  private final SagaEngine engine;
  private final SagaDefinitionRegistry registry;
  private final String ownerId;
  private final RecoveryConfig config;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService recoveryExecutor;
  private final Semaphore recoverySemaphore;

  // Serializes passes: a manually triggered pass and a scheduled one never overlap. Interruptible
  // (unlike a monitor) so a caller blocked behind a long in-flight pass can be cancelled.
  private final ReentrantLock passLock = new ReentrantLock();

  // Where the next pass resumes when the previous one stopped on budget; null starts a fresh
  // revolution. Guarded by passLock; a pass never overlaps another.
  private @Nullable ScanCursor staleResumeCursor;
  private @Nullable ScanCursor parkedResumeCursor;

  // When each currently-skipped saga was first seen executing here, and which have already been
  // warned about. Written by recovery tasks, so both are concurrent; pruned on the pass thread at
  // the start of each pass, which bounds them to sagas this instance is actually driving.
  private final Map<String, Instant> localActiveSince = new ConcurrentHashMap<>();
  private final Set<String> hungDriveWarned = ConcurrentHashMap.newKeySet();

  SagaRecoveryManager(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry registry,
      String ownerId,
      RecoveryConfig config) {
    this.store = store;
    this.engine = engine;
    this.registry = registry;
    this.ownerId = ownerId;
    this.config = config;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "saga-recovery");
              t.setDaemon(true);
              return t;
            });
    this.recoveryExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.recoverySemaphore = new Semaphore(config.maxConcurrentRecoveries());
  }

  // Visible for testing
  SagaRecoveryManager(
      SagaStore store,
      SagaEngine engine,
      SagaDefinitionRegistry registry,
      String ownerId,
      RecoveryConfig config,
      ScheduledExecutorService scheduler) {
    this.store = store;
    this.engine = engine;
    this.registry = registry;
    this.ownerId = ownerId;
    this.config = config;
    this.scheduler = scheduler;
    this.recoveryExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.recoverySemaphore = new Semaphore(config.maxConcurrentRecoveries());
  }

  /**
   * Starts periodic recovery scanning. Runs once immediately (startup recovery, so sagas
   * interrupted by a restart are picked up right away), then periodically at the configured
   * interval shifted by a deterministic per-replica offset. Replicas started together therefore
   * begin their periodic passes out of phase. The offsets are a best effort, not a guarantee:
   * fixed-delay scheduling measures from pass end, so a loaded replica's phase drifts and passes
   * can realign over time — those collisions are absorbed by the scattered bucket orders and the
   * claim protocol, and surface as {@code lostRaces} in the pass summary.
   */
  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget scheduled tasks
  public void start() {
    long intervalSeconds = config.recoveryIntervalSeconds();
    long offsetSeconds = SweepScatter.offsetSeconds(ownerId, "recovery", intervalSeconds);
    logger.info(
        "Recovery sweeps for owner {}: schedule offset {}s within the {}s interval",
        ownerId,
        offsetSeconds,
        intervalSeconds);
    scheduler.schedule(this::recoverSafely, 0, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(
        this::recoverSafely, intervalSeconds + offsetSeconds, intervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Stops the recovery scheduler and waits for any in-flight recovery pass to complete, respecting
   * the given deadline. Both executors are signaled to shut down immediately; remaining time is
   * used for graceful termination before force-stopping.
   *
   * @param deadlineNanos absolute {@link System#nanoTime()} deadline
   */
  public void stop(long deadlineNanos) {
    scheduler.shutdown();
    recoveryExecutor.shutdown();

    try {
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        scheduler.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
      remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        recoveryExecutor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      scheduler.shutdownNow();
      recoveryExecutor.shutdownNow();
    }
  }

  /**
   * Wraps {@link #recover()} so nothing escapes to the scheduler: a {@code Throwable} escaping a
   * periodic task cancels all its future executions, which would silently stop recovery for the
   * rest of the process.
   */
  private void recoverSafely() {
    try {
      recover();
    } catch (Throwable t) {
      logger.error("Recovery pass failed unexpectedly", t);
    }
  }

  /**
   * Outcome of one dispatched recovery task, captured at the task's commit point: the claim for a
   * stale saga, the WAITING transition for a parked one. {@code COMMITTED}, {@code DRIVE_FAILED}
   * and {@code ERROR} consume batch budget; {@code LOST_RACE} and {@code SKIPPED} are free.
   *
   * <p>{@code DRIVE_FAILED} is a claim that committed and an execution that then threw. The saga is
   * ours and its availability is spent either way, so it is charged exactly like {@code COMMITTED}
   * and additionally counted in {@code driveFailures} — the number that says "we keep winning
   * claims and the drives keep dying". It is not {@code ERROR}: there the claim itself failed and
   * the saga was never ours.
   *
   * <p>{@code ERROR} is charged conservatively: the failed operation may have committed without the
   * store being able to confirm it (an unknown-status commit whose verification read-back also
   * failed leaves the saga claimed but undriven), and a failure means the store is struggling — the
   * situation where a pass must wind down, not scan harder. A clean lost race — another actor
   * verifiably did the work — keeps the sweep scanning for free.
   *
   * <p>{@code SKIPPED} is the deliberate non-claim: the saga looked stale by its state row, but
   * something is still driving it — either a drive on this instance, or an event written recently
   * enough that someone must be. It is free because charging it would let live sagas exhaust the
   * budget and starve recovery of the genuinely dead ones. One value covers both checks because
   * nothing downstream treats them differently; the cases an operator can act on carry their own
   * log lines instead (a hung drive is named after {@value #HUNG_DRIVE_WARN_MULTIPLE} timeouts, a
   * saga with no events is reported as {@code ERROR}).
   *
   * <p>{@code ERROR} stays a separate counter so store trouble does not inflate the contention
   * signal the pass summary exists to expose.
   */
  private enum RecoveryOutcome {
    LOST_RACE,
    ERROR,
    COMMITTED,
    DRIVE_FAILED,
    SKIPPED
  }

  /** Why a sweep ended where it did, as rendered in the pass summary. */
  private enum StopReason {
    REVOLUTION("revolution"),
    BUDGET("budget"),
    ABORTED("aborted"),
    STORE_UNAVAILABLE("store-unavailable"),
    SKIPPED("skipped");

    private final String label;

    StopReason(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return label;
    }
  }

  /** Counters for one sweep of a recovery pass, reported by the per-pass summary line. */
  private static final class SweepCounters {
    int scanned;
    int committed;
    int lostRaces;
    int errors;
    int driveFailures;
    int failedPages;
    int consecutiveFailedPages;
    int skipped;
    boolean aborted;
    boolean storeUnavailable;
    StopReason stopReason = StopReason.REVOLUTION;

    /** Budget spent so far: committed work plus errors; lost races and skips are free. */
    int spent() {
      return committed + errors;
    }

    boolean isIdle() {
      return scanned == 0
          && committed == 0
          && lostRaces == 0
          && errors == 0
          && driveFailures == 0
          && failedPages == 0
          && skipped == 0;
    }

    /** The counters as one log fragment; the single place the field list is spelled out. */
    String summarize() {
      return "scanned="
          + scanned
          + " committed="
          + committed
          + " lostRaces="
          + lostRaces
          + " errors="
          + errors
          + " driveFailures="
          + driveFailures
          + " failedPages="
          + failedPages
          + " skipped="
          + skipped
          + " stop="
          + stopReason;
    }
  }

  /**
   * Scans one bucket page at the cursor and submits up to {@code limit} of its candidates to the
   * recovery executor. Each sweep supplies its own scan call and task; the round loop in {@link
   * #recover()} supplies the budget cap, page-failure isolation, and awaiting.
   */
  @FunctionalInterface
  private interface SweepPageAction {
    @Nullable ScanCursor scanAndSubmit(
        ScanCursor cursor, int limit, List<Future<RecoveryOutcome>> futures) throws Exception;
  }

  /**
   * Single recovery pass, two sweeps in the replica's scattered bucket order: stale RUNNING and
   * COMPENSATING sagas, and overdue parked (WAITING) sagas, each with its own batch budget so a
   * staleness backlog cannot starve the timeout sweep.
   *
   * <p>The pass runs in rounds. A round scans BOTH sweeps from their current cursors, submitting
   * candidates page by page up to each sweep's remaining budget without awaiting anything, so slow
   * recoveries from one bucket overlap the scanning and recoveries of every later bucket and of the
   * other sweep; only then does the round await its tasks and count their outcomes. The budget
   * counts committed work and errors; only a lost race consumes nothing (see {@link
   * RecoveryOutcome}), so when lost races leave a budget unfilled, the next round keeps scanning
   * from where the cursors stopped. In the common uncontended case a single round submits
   * everything and awaits once.
   *
   * <p>A page whose scan fails is skipped (never ending the sweep: a poison row would otherwise
   * shadow every bucket after it for the whole boot) — but {@value #MAX_CONSECUTIVE_FAILED_PAGES}
   * consecutive scan failures mean the store itself is unavailable, and the sweep stops for this
   * pass instead of sweeping an outage as one poison page per bucket. A sweep stopped by its budget
   * resumes at the same position next pass, so every bucket is reached within one revolution's
   * worth of passes under any backlog. Candidates truncated by the budget cap are not re-scanned
   * within the pass; pages return oldest first, so they lead their bucket's next visit.
   *
   * <p>Passes are serialized on an interruptible lock: a manually triggered pass and a scheduled
   * one never overlap (which also guards the resume cursors), and a caller blocked behind an
   * in-flight pass returns without running one when interrupted, with the interrupt flag set. A
   * pass interrupted mid-run cancels its in-flight tasks (interrupting them) and charges their
   * unknown outcomes conservatively before releasing the lock; a task blocked in a
   * non-interruptible store call may still be finishing that one call after the pass returns.
   */
  public void recover() {
    try {
      passLock.lockInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    try {
      runPass();
    } finally {
      passLock.unlock();
    }
  }

  private void runPass() {
    long startNanos = System.nanoTime();
    SweepCounters stale = new SweepCounters();
    SweepCounters parked = new SweepCounters();
    try {
      // Compute the staleness cutoff once from the injected clock so every page this pass uses a
      // consistent threshold.
      Instant staleThreshold = config.clock().instant().minusMillis(config.recoveryTimeoutMillis());
      // Drop bookkeeping for sagas this instance has stopped driving, so the two maps stay bounded
      // by what is actually running. On the pass thread, before any task can touch them.
      localActiveSince.keySet().removeIf(id -> !engine.isLocallyActive(id));
      hungDriveWarned.retainAll(localActiveSince.keySet());
      SweepPageAction stalePage =
          (cursor, limit, futures) -> {
            Recoverables result = store.findRecoverable(staleThreshold, cursor);
            stale.scanned += result.sagas().size();
            for (SagaStateSnapshot saga : result.sagas()) {
              if (limit-- <= 0) {
                break;
              }
              futures.add(recoveryExecutor.submit(() -> recoverOneSafely(saga, staleThreshold)));
            }
            return result.nextCursor();
          };
      // Like the staleness cutoff, computed once so every page this pass uses one deadline
      // threshold; sagas becoming overdue mid-pass are caught next pass.
      Instant parkedThreshold = config.clock().instant();
      SweepPageAction parkedPage =
          (cursor, limit, futures) -> {
            OverdueParked result = store.findOverdueParkedSagas(parkedThreshold, cursor);
            parked.scanned += result.sagaIds().size();
            for (String sagaId : result.sagaIds()) {
              if (limit-- <= 0) {
                break;
              }
              futures.add(recoveryExecutor.submit(() -> recoverParkedTimeoutOneSafely(sagaId)));
            }
            return result.nextCursor();
          };

      @Nullable ScanCursor staleCursor =
          staleResumeCursor != null ? staleResumeCursor : store.initialSweepCursor(ownerId);
      @Nullable ScanCursor parkedCursor =
          parkedResumeCursor != null ? parkedResumeCursor : store.initialSweepCursor(ownerId);

      // Terminates: cursors only advance, so total scanning is bounded by one revolution per
      // sweep per pass no matter how many rounds lost races cause.
      while (true) {
        boolean staleActive =
            staleCursor != null
                && stale.spent() < config.batchSize()
                && !stale.aborted
                && !stale.storeUnavailable;
        boolean parkedActive =
            parkedCursor != null
                && parked.spent() < config.batchSize()
                && !parked.aborted
                && !parked.storeUnavailable;
        if (!staleActive && !parkedActive) {
          break;
        }
        List<Future<RecoveryOutcome>> staleFutures = new ArrayList<>();
        List<Future<RecoveryOutcome>> parkedFutures = new ArrayList<>();
        if (staleActive) {
          staleCursor =
              scanAndSubmitRound(
                  stale,
                  staleCursor,
                  stalePage,
                  staleFutures,
                  "Staleness scan page failed; skipping to the next bucket");
        }
        if (parkedActive && !stale.aborted) {
          // The shared executor rejecting stale submissions would only reject parked ones too.
          parkedCursor =
              scanAndSubmitRound(
                  parked,
                  parkedCursor,
                  parkedPage,
                  parkedFutures,
                  "Parked timeout scan page failed; skipping to the next bucket");
        }
        awaitOutcomes(staleFutures, stale);
        awaitOutcomes(parkedFutures, parked);
        if (Thread.currentThread().isInterrupted()
            || stale.aborted
            || parked.aborted
            || (staleFutures.isEmpty() && parkedFutures.isEmpty())) {
          break;
        }
      }

      staleResumeCursor = staleCursor;
      parkedResumeCursor = parkedCursor;
      stale.stopReason = stopReason(stale, staleCursor);
      parked.stopReason =
          stale.aborted && parked.isIdle() ? StopReason.SKIPPED : stopReason(parked, parkedCursor);
    } finally {
      logPassSummary(stale, parked, startNanos);
    }
  }

  /**
   * One sweep's share of a round: from the cursor, scan pages and submit candidates up to the
   * sweep's remaining budget, without awaiting any of them. Submissions count against the budget
   * conservatively, as if they will all commit; the round loop refunds lost races by re-invoking
   * with the returned cursor. Returns the cursor for the next page, or {@code null} when the
   * revolution completed. Single page-scan failures are skipped (poison-row isolation), but {@value
   * #MAX_CONSECUTIVE_FAILED_PAGES} in a row mean the store is unavailable: the sweep stops without
   * advancing past the failing page, so the next pass retries it.
   */
  private @Nullable ScanCursor scanAndSubmitRound(
      SweepCounters counters,
      @Nullable ScanCursor start,
      SweepPageAction page,
      List<Future<RecoveryOutcome>> futures,
      String scanFailureMessage) {
    @Nullable ScanCursor cursor = start;
    int submitted = 0;
    while (cursor != null && counters.spent() + submitted < config.batchSize()) {
      if (Thread.currentThread().isInterrupted()) {
        return cursor;
      }
      int before = futures.size();
      try {
        cursor =
            page.scanAndSubmit(cursor, config.batchSize() - counters.spent() - submitted, futures);
      } catch (RejectedExecutionException e) {
        logger.warn("Recovery executor shut down; ending the pass", e);
        counters.aborted = true;
        return cursor;
      } catch (Exception e) {
        counters.failedPages++;
        if (++counters.consecutiveFailedPages >= MAX_CONSECUTIVE_FAILED_PAGES) {
          logger.warn(
              "{} consecutive page scans failed; treating the store as unavailable and ending the"
                  + " sweep for this pass",
              counters.consecutiveFailedPages,
              e);
          counters.storeUnavailable = true;
          return cursor;
        }
        logger.warn(scanFailureMessage, e);
        cursor = store.advanceSweepCursor(cursor);
        continue;
      }
      counters.consecutiveFailedPages = 0;
      submitted += futures.size() - before;
    }
    return cursor;
  }

  /**
   * Why a sweep ended where it did, for the pass summary. Budget is checked before revolution: when
   * the budget is exhausted exactly on the last bucket both are true, and budget is the actionable
   * signal — it is what an operator sizes {@code batchSize} by.
   */
  private StopReason stopReason(SweepCounters counters, @Nullable ScanCursor cursor) {
    if (counters.aborted) {
      return StopReason.ABORTED;
    }
    if (counters.storeUnavailable) {
      return StopReason.STORE_UNAVAILABLE;
    }
    if (counters.spent() >= config.batchSize()) {
      return StopReason.BUDGET;
    }
    if (cursor == null) {
      return StopReason.REVOLUTION;
    }
    // A non-null cursor with unfilled budget only remains after an interrupt.
    return StopReason.ABORTED;
  }

  /**
   * Awaits one round's tasks for one sweep and folds their outcomes into its counters.
   *
   * <p>When the pass thread is interrupted mid-await, the remaining tasks are cancelled
   * (interrupting their threads) and their outcomes drained before returning, so every task is
   * accounted for in the summary. A cancelled task stops at its next interruptible point — one
   * blocked in a non-interruptible store call may still be finishing that call when the pass
   * returns — and because it may have committed its claim before stopping, its unknown outcome is
   * charged as an error (budget spent, conservatively). The interrupt flag is restored for the
   * caller.
   */
  private void awaitOutcomes(List<Future<RecoveryOutcome>> futures, SweepCounters counters) {
    for (int i = 0; i < futures.size(); i++) {
      try {
        count(futures.get(i).get(), counters);
      } catch (InterruptedException e) {
        for (int j = i; j < futures.size(); j++) {
          futures.get(j).cancel(true);
        }
        for (int j = i; j < futures.size(); j++) {
          drainQuietly(futures.get(j), counters);
        }
        Thread.currentThread().interrupt();
        return;
      } catch (ExecutionException e) {
        // Tasks report outcomes instead of throwing; anything escaping is unexpected.
        counters.errors++;
        logger.error("Recovery task failed unexpectedly", e.getCause());
      }
    }
  }

  private static void count(RecoveryOutcome outcome, SweepCounters counters) {
    switch (outcome) {
      case COMMITTED -> counters.committed++;
      case DRIVE_FAILED -> {
        counters.committed++;
        counters.driveFailures++;
      }
      case LOST_RACE -> counters.lostRaces++;
      case ERROR -> counters.errors++;
      case SKIPPED -> counters.skipped++;
    }
  }

  /** Collects one cancelled or in-flight task's result without responding to further interrupts. */
  private void drainQuietly(Future<RecoveryOutcome> future, SweepCounters counters) {
    while (true) {
      try {
        count(future.get(), counters);
        return;
      } catch (CancellationException e) {
        // The task was stopped with its outcome unknown; charge the budget conservatively.
        counters.errors++;
        return;
      } catch (ExecutionException e) {
        counters.errors++;
        logger.error("Recovery task failed unexpectedly", e.getCause());
        return;
      } catch (InterruptedException e) {
        // Re-interrupted while draining; keep draining — the caller restores the flag once.
      }
    }
  }

  /**
   * One line per pass so multi-replica contention stays observable in production: {@code lostRaces}
   * near zero means the scattered sweeps keep replicas apart; growth means they are fighting again.
   * Demoted to DEBUG only when the pass saw nothing at all, so an idle system is not spammed every
   * interval.
   */
  private void logPassSummary(SweepCounters stale, SweepCounters parked, long startNanos) {
    long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    String format = "Recovery pass: stale[{}] parked[{}] durationMillis={}";
    if (stale.isIdle() && parked.isIdle()) {
      logger.debug(format, stale.summarize(), parked.summarize(), durationMillis);
    } else {
      logger.info(format, stale.summarize(), parked.summarize(), durationMillis);
    }
  }

  /**
   * Decides whether a stale-looking saga may be claimed, then claims and drives it.
   *
   * <p>A stale {@code updated_at} does not mean abandoned: {@code recordStepEvent} never touches
   * the state row, so a saga executing a long step looks exactly like one whose process died. Two
   * guards separate them, in this order.
   *
   * <p>The local-active check comes first and runs before a permit is acquired. It is free, and
   * permits are held for the whole synchronous drive, so evaluating it inside the permit would
   * queue skips behind long drives. It must also precede the EPOCH carve-out: a row can be
   * EPOCH-stamped while a local drive still runs — an operator resetting or force-recovering a saga
   * this instance happens to be executing does exactly that — and claiming it would kill the drive
   * this instance is running.
   *
   * <p>The progress probe then reads the newest event stamp for everything else, and skips the saga
   * when it shows activity within the staleness window. A deliberate hand-off (the caller stamped
   * {@code EPOCH} to give the saga to the sweeper) bypasses the probe, or a recent event would
   * delay it by a whole timeout.
   */
  private RecoveryOutcome recoverOneSafely(SagaStateSnapshot saga, Instant staleThreshold) {
    String sagaId = saga.getSagaId();
    // Free and in-memory: keep it outside the permit.
    if (engine.isLocallyActive(sagaId)) {
      noteLocallyActive(sagaId);
      return RecoveryOutcome.SKIPPED;
    }
    boolean deliberateHandoff = saga.getUpdatedAt().equals(Instant.EPOCH);

    try {
      recoverySemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return RecoveryOutcome.ERROR;
    }
    try {
      // Re-check inside the permit: the wait for it can be long, and this narrows the window in
      // which a drive starts locally between the check above and the claim below.
      if (engine.isLocallyActive(sagaId)) {
        noteLocallyActive(sagaId);
        return RecoveryOutcome.SKIPPED;
      }
      Optional<Instant> newestEvent;
      try {
        newestEvent = store.getNewestEventTime(sagaId);
      } catch (Throwable t) {
        // Do not fall through to the claim. A failed read is not evidence that the saga stopped
        // progressing, and claiming would rewrite the concurrency token of a drive that may well be
        // alive. The drive that follows a claim reads the same events table anyway, so it would
        // fail too: we would kill a live saga and recover nothing. Report the store trouble, leave
        // the row untouched, and let a later pass decide with real evidence.
        logger.warn(
            "Progress probe failed for saga {}; leaving it untouched for a later pass", sagaId, t);
        return RecoveryOutcome.ERROR;
      }
      if (newestEvent.isEmpty()) {
        // createSaga writes SAGA_STARTED in the same transaction as the state row, so a row with
        // no events behind it cannot come from the engine. Claiming would replay an empty history
        // and restart the saga from step 0 with no input — SAGA_STARTED is what carries it —
        // against a saga that may already hold committed side effects. Report it as an error: the
        // store is damaged, which is exactly when a pass should wind down rather than scan harder.
        logger.error(
            "Saga {} has a recoverable state row but no events; refusing to recover it, because"
                + " replaying an empty history would restart it from step 0 with no input."
                + " Investigate the store: this cannot be produced by normal operation.",
            sagaId);
        return RecoveryOutcome.ERROR;
      }
      // A deliberate hand-off is stamped EPOCH precisely to have the saga taken now, so honouring
      // recent events there would delay it by a whole timeout.
      if (!deliberateHandoff && hasRecentEvent(newestEvent.get(), staleThreshold)) {
        return RecoveryOutcome.SKIPPED;
      }
      Optional<SagaStateSnapshot> claimed;
      try {
        claimed = store.claimForRecovery(saga, ownerId);
      } catch (Throwable t) {
        // Throwable, not Exception: an escape would surface in awaitOutcomes as a context-free
        // ExecutionException instead of naming the saga here.
        logger.error("Failed to claim saga {} for recovery", saga.getSagaId(), t);
        return RecoveryOutcome.ERROR;
      }
      if (claimed.isEmpty()) {
        return RecoveryOutcome.LOST_RACE;
      }
      try {
        recoverOne(claimed.get());
        return RecoveryOutcome.COMMITTED;
      } catch (Throwable t) {
        // Log and continue — don't let one stuck saga block others. The claim committed, so the
        // budget is spent either way; the saga surfaces again after the staleness timeout.
        // Throwable, not Exception: an escape would be charged as ERROR despite the committed
        // claim, and logged without the saga id.
        logger.error("Failed to recover saga {}", saga.getSagaId(), t);
        return RecoveryOutcome.DRIVE_FAILED;
      }
    } finally {
      recoverySemaphore.release();
    }
  }

  /**
   * Whether the saga wrote an event within the staleness window, which means something is still
   * driving it and it must not be claimed.
   *
   * <p>Only the event stamp is compared. The state row's own timestamp cannot matter here: {@link
   * SagaStore#findRecoverable} returns nothing newer than {@code staleThreshold}, so every
   * candidate already has an old row by construction.
   */
  private static boolean hasRecentEvent(Instant newestEvent, Instant staleThreshold) {
    return !newestEvent.isBefore(staleThreshold);
  }

  /**
   * Records that a saga was skipped because this instance is driving it, and warns once when that
   * has been true for far longer than a drive should take.
   *
   * <p>Without this a hung drive is invisible: the skip is free and silent, so a saga whose drive
   * never releases it is passed over on every pass forever, with nothing in the log naming it.
   */
  private void noteLocallyActive(String sagaId) {
    Instant now = config.clock().instant();
    Instant since = localActiveSince.computeIfAbsent(sagaId, id -> now);
    long stuckMillis = Duration.between(since, now).toMillis();
    if (stuckMillis > HUNG_DRIVE_WARN_MULTIPLE * config.recoveryTimeoutMillis()
        && hungDriveWarned.add(sagaId)) {
      logger.warn(
          "Saga {} has been executing on this instance for {}ms, over {}x the recovery timeout."
              + " Recovery skips it while it is active, so a wedged drive is never reclaimed here;"
              + " if it is stuck rather than slow, restart this instance or hand the saga to the"
              + " sweeper.",
          sagaId,
          stuckMillis,
          HUNG_DRIVE_WARN_MULTIPLE);
    }
  }

  private void recoverOne(SagaStateSnapshot saga) {
    // Replay events to reconstruct state (needed for both recovery and escalation)
    List<SagaEvent> events = store.getEvents(saga.getSagaId());
    ExecutionContext context = engine.replayEvents(saga, events);

    // Look up the EXACT definition version the saga was started with.
    // Using a different version during recovery would corrupt business data
    // if steps were added, removed, or reordered.
    SagaDefinition def = registry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      escalate(
          context,
          "definition version "
              + saga.getDefinitionVersion()
              + " not found for "
              + saga.getSagaName());
      return;
    }

    SagaStatus status = context.getCurrentState().getStatus();

    if (status == SagaStatus.COMPENSATING) {
      recoverCompensating(def, context, events);
    } else {
      // RUNNING (or any other non-terminal, non-COMPENSATING status) — resume forward
      recoverRunning(def, context, events);
    }
  }

  private void recoverRunning(
      SagaDefinition def, ExecutionContext context, List<SagaEvent> events) {
    // Grace-period policy (recovery-specific): escalate if a step failure has been stuck past the
    // grace period. Sagas with no STEP_FAILED events (crash recovery) skip this and act directly.
    if (isStuckLongerThanGracePeriod(events, EventType.STEP_FAILED, EventType.STEP_COMPLETED)) {
      escalate(context, "step retry stuck for over " + config.compensationGracePeriod());
      return;
    }
    engine.recover(RecoveryActionResolver.resolve(events, def, SagaStatus.RUNNING), def, context);
  }

  private void recoverCompensating(
      SagaDefinition def, ExecutionContext context, List<SagaEvent> events) {
    // Grace-period policy (recovery-specific): compensation stuck past the grace period escalates.
    if (isStuckLongerThanGracePeriod(
        events, EventType.STEP_COMPENSATION_FAILED, EventType.STEP_COMPENSATED)) {
      escalate(context, "compensation stuck for over " + config.compensationGracePeriod());
      return;
    }
    engine.recover(
        RecoveryActionResolver.resolve(events, def, SagaStatus.COMPENSATING), def, context);
  }

  private RecoveryOutcome recoverParkedTimeoutOneSafely(String sagaId) {
    try {
      recoverySemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return RecoveryOutcome.ERROR;
    }
    try {
      return recoverParkedTimeoutOne(sagaId);
    } catch (SagaConcurrentModificationException e) {
      // A concurrent callback (or another replica's sweep) won the WAITING CK — nothing to do.
      logger.debug("Parked timeout for saga {} lost the WAITING race; skipping", sagaId);
      return RecoveryOutcome.LOST_RACE;
    } catch (Throwable t) {
      // Post-commit drive failures are handled inside recoverParkedTimeoutOne; anything escaping
      // here failed before the WAITING transition committed. Throwable, not Exception: an escape
      // would surface in awaitOutcomes without the saga id.
      logger.error("Failed to time out parked saga {}", sagaId, t);
      return RecoveryOutcome.ERROR;
    } finally {
      recoverySemaphore.release();
    }
  }

  /**
   * Recovers one overdue parked ({@code WAITING}) saga. While within the re-drive bounds (the park-
   * attempt count vs the step's retry max-attempts AND the elapsed-since-first-park vs the grace
   * period), it un-parks and re-issues the step — so a transient participant failure is retried
   * before the saga rolls back or escalates. Once a bound is spent it gives up: fails the parked
   * step and either compensates (pre-pivot) or escalates (post-pivot), clearing the {@code
   * saga_parked} row. No claim is taken — the optimistic WAITING-CK check in the store ops is the
   * cross-replica de-dup and the callback-vs-timeout-vs-redrive guard.
   *
   * <p>Returns the outcome for budget accounting, captured at the WAITING transition: once {@code
   * redriveParkedStep} or {@code failParkedStep} commits, the budget is spent even if the engine
   * drive afterwards fails.
   */
  private RecoveryOutcome recoverParkedTimeoutOne(String sagaId) {
    Optional<SagaStateSnapshot> snapshot = store.getStateSnapshot(sagaId);
    if (snapshot.isEmpty() || snapshot.get().getStatus() != SagaStatus.WAITING) {
      // Already resolved (a callback won, or it moved on) — nothing to do.
      return RecoveryOutcome.LOST_RACE;
    }
    SagaStateSnapshot saga = snapshot.get();
    List<SagaEvent> events = store.getEvents(sagaId);
    StepEvent parked = lastParkedEvent(events);
    if (parked == null) {
      logger.error("WAITING saga {} has no STEP_PENDING marker; leaving for inspection", sagaId);
      return RecoveryOutcome.ERROR;
    }
    int parkedIndex = parked.getStepIndex();
    String stepName = parked.getStepName();

    SagaDefinition def = registry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      // Missing definition — escalate, clearing the parked row so it is not re-swept forever.
      logger.warn(
          "Escalating parked saga {}: definition {} not found",
          sagaId,
          saga.getDefinitionVersion());
      store.failParkedStep(
          saga,
          events.size(),
          giveUpFailedEvent(
              parkedIndex, stepName, "definition " + saga.getDefinitionVersion() + " not found"),
          SagaStatus.ESCALATED);
      return RecoveryOutcome.COMMITTED;
    }

    // Re-drive (retry) the parked step while within the attempt-count and grace bounds: un-park it
    // (WAITING -> RUNNING) and re-issue it, which re-parks with a fresh deadline. Only once a bound
    // is spent do we give up below.
    RedriveDecision decision = redriveDecision(events, parkedIndex, def);
    if (decision == RedriveDecision.REDRIVE) {
      StepEvent reissueEvent = StepEvent.reissuing(parkedIndex, stepName);
      SagaStateSnapshot running = store.redriveParkedStep(saga, events.size(), reissueEvent);
      List<SagaEvent> updatedEvents = new ArrayList<>(events);
      updatedEvents.add(reissueEvent);
      try {
        ExecutionContext context = engine.replayEvents(running, updatedEvents);
        engine.resumeFrom(def, context, parkedIndex);
      } catch (Exception e) {
        logger.error(
            "Re-drive of parked saga {} failed after its WAITING transition committed", sagaId, e);
        return RecoveryOutcome.DRIVE_FAILED;
      }
      return RecoveryOutcome.COMMITTED;
    }

    // Give up: the re-drive budget is spent — record which bound was hit for the event log.
    String reason =
        decision == RedriveDecision.ATTEMPTS_EXHAUSTED
            ? "async re-drive attempts exhausted"
            : "async re-drive grace period exceeded";
    StepEvent failedEvent = giveUpFailedEvent(parkedIndex, stepName, reason);
    if (parkedIndex <= def.getPivotIndex()) {
      // Pre-pivot: WAITING -> COMPENSATING + STEP_FAILED + clear parked, then compensate from the
      // failed step (knownNotCommitted=false — the async step may have committed server-side).
      SagaStateSnapshot compensating =
          store.failParkedStep(saga, events.size(), failedEvent, SagaStatus.COMPENSATING);
      // failParkedStep appended failedEvent at events.size() and nothing else; its successful
      // WAITING-CK check proves the log was untouched since we read `events`, so append locally
      // rather than re-reading (replayEvents ignores the timestamp).
      List<SagaEvent> updatedEvents = new ArrayList<>(events);
      updatedEvents.add(failedEvent);
      try {
        ExecutionContext context = engine.replayEvents(compensating, updatedEvents);
        engine.compensateFrom(def, context, parkedIndex);
      } catch (Exception e) {
        logger.error(
            "Compensation of parked saga {} failed after its WAITING transition committed",
            sagaId,
            e);
        return RecoveryOutcome.DRIVE_FAILED;
      }
    } else {
      // Post-pivot: cannot roll back and the give-up floor does not re-drive forward — escalate.
      logger.warn(
          "Escalating parked saga {}: step {} gave up post-pivot ({})",
          sagaId,
          parkedIndex,
          reason);
      store.failParkedStep(saga, events.size(), failedEvent, SagaStatus.ESCALATED);
    }
    return RecoveryOutcome.COMMITTED;
  }

  /** The most recent {@code STEP_PENDING} event (the currently parked step), or {@code null}. */
  private static @Nullable StepEvent lastParkedEvent(List<SagaEvent> events) {
    StepEvent parked = null;
    for (SagaEvent event : events) {
      if (event.getEventType() == EventType.STEP_PENDING) {
        parked = (StepEvent) event;
      }
    }
    return parked;
  }

  /** Whether to re-drive a timed-out parked step, or which bound made it give up. */
  private enum RedriveDecision {
    REDRIVE,
    ATTEMPTS_EXHAUSTED,
    GRACE_EXCEEDED
  }

  /**
   * Decides whether a timed-out parked step is re-driven rather than given up on. Re-drives only
   * within BOTH bounds: the park-attempt count ({@code STEP_PENDING} events at the step) within the
   * step's retry max-attempts — caps hammering a dead participant under a short callback timeout —
   * AND the time since the first park within the compensation grace period — caps total stuck time
   * under a long one. Otherwise returns which bound was hit (attempts checked first).
   */
  private RedriveDecision redriveDecision(
      List<SagaEvent> events, int parkedIndex, SagaDefinition def) {
    long attempts =
        RecoveryActionResolver.stepIndices(events, EventType.STEP_PENDING)
            .filter(index -> index == parkedIndex)
            .count();
    if (attempts >= maxRedriveAttempts(def, parkedIndex)) {
      return RedriveDecision.ATTEMPTS_EXHAUSTED;
    }
    boolean withinGrace =
        events.stream()
            .filter(
                e ->
                    e instanceof StepEvent step
                        && step.getEventType() == EventType.STEP_PENDING
                        && step.getStepIndex() == parkedIndex)
            .map(SagaEvent::getTimestamp)
            .filter(Objects::nonNull)
            .findFirst()
            .map(
                firstPark ->
                    Duration.between(firstPark, config.clock().instant())
                            .compareTo(config.compensationGracePeriod())
                        <= 0)
            .orElse(false);
    return withinGrace ? RedriveDecision.REDRIVE : RedriveDecision.GRACE_EXCEEDED;
  }

  /**
   * The parked step's effective retry max-attempts: the step's own policy, else the saga default,
   * else the engine default (mirrors {@code SagaEngine.resolveRetryPolicy}).
   */
  private static int maxRedriveAttempts(SagaDefinition def, int stepIndex) {
    StepDefinition stepDef = def.getSteps().get(stepIndex);
    RetryPolicy policy = stepDef.getRetryPolicy();
    if (policy == null) {
      policy = def.getDefaultRetryPolicy();
    }
    if (policy == null) {
      policy = RetryPolicy.defaultPolicy();
    }
    return policy.getMaxAttempts();
  }

  /**
   * A {@code STEP_FAILED} event for a given-up parked step, with {@code knownNotCommitted=false}:
   * an async step whose callback never arrived may have committed server-side, so compensation must
   * include it.
   */
  private static StepEvent giveUpFailedEvent(int stepIndex, String stepName, String reason) {
    return StepEvent.failed(
        stepIndex,
        stepName,
        EventPayloadSerializer.serializeError(new StepExecutionException(reason, false), false));
  }

  /**
   * Checks if a saga has been stuck longer than the grace period by examining step-level failure
   * events. Returns {@code false} if no matching failure events exist (e.g., crash recovery where
   * the saga was interrupted, not failed).
   *
   * <p>Un-escalating restarts the clock: the stuck period is measured from the later of the
   * unresolved failure and the most recent {@code SAGA_RESET}. An escalated saga is outside the
   * recovery cycle entirely, so re-admitting it has to grant a fresh grace period — the failure it
   * was stuck on only ages, so anchoring on that alone would escalate it straight back on the next
   * pass, undoing the reset without ever driving it.
   *
   * <p>{@code SAGA_RECOVERING} deliberately does not restart the clock. It only forces a drive the
   * scheduled sweep would do anyway, on a saga that never left the recovery cycle; extending the
   * deadline would make escalation timing depend on whether an operator happened to intervene.
   */
  private boolean isStuckLongerThanGracePeriod(
      List<SagaEvent> events, EventType failureEventType, EventType successEventType) {
    // Step indices where the failure was later resolved by a success event.
    // Since events are append-only and ordered, a success at the same index
    // always follows the failure — the step cannot succeed before it fails.
    Set<Integer> resolvedIndices =
        RecoveryActionResolver.stepIndices(events, successEventType)
            .boxed()
            .collect(Collectors.toSet());

    Optional<Instant> firstUnresolvedFailure =
        events.stream()
            .filter(
                e ->
                    e instanceof StepEvent step
                        && step.getEventType() == failureEventType
                        && !resolvedIndices.contains(step.getStepIndex()))
            .map(SagaEvent::getTimestamp)
            .filter(Objects::nonNull)
            .findFirst();
    if (firstUnresolvedFailure.isEmpty()) {
      // no unresolved failure events — crash recovery or all failures resolved
      return false;
    }

    Instant anchor = firstUnresolvedFailure.get();
    Optional<Instant> reset = lastResetTimestamp(events);
    if (reset.isPresent() && reset.get().isAfter(anchor)) {
      anchor = reset.get();
    }
    return Duration.between(anchor, config.clock().instant())
            .compareTo(config.compensationGracePeriod())
        > 0;
  }

  /**
   * The most recent timestamp at which an operator un-escalated this saga, if any. Compared by
   * timestamp rather than taken as the last matching event, because events are stamped by whichever
   * replica writes them, so stream order and clock order can diverge; taking the latest also yields
   * the failure, correctly, when a fresh failure follows the reset.
   */
  private static Optional<Instant> lastResetTimestamp(List<SagaEvent> events) {
    return events.stream()
        .filter(e -> e.getEventType() == EventType.SAGA_RESET)
        .map(SagaEvent::getTimestamp)
        .filter(Objects::nonNull)
        .max(Instant::compareTo);
  }

  private void escalate(ExecutionContext context, String reason) {
    logger.warn("Escalating saga {}: {}", context.getSagaId(), reason);
    store.recordStatusEvent(
        context.getCurrentState(), context.nextSequence(), StatusEvent.escalated(reason), ownerId);
  }
}
