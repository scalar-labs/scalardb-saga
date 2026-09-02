package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SweepScatter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic retention manager that purges resolved (terminal) sagas.
 *
 * <p>Scans {@code saga_state} for entries in purgeable statuses ({@link SagaStatus#COMPLETED},
 * {@link SagaStatus#COMPENSATED}) whose {@code updated_at} is older than the configured retention
 * period, then deletes both the {@code saga_state} row and all {@code saga_events} rows for each
 * expired saga via {@link SagaStore#deleteSaga}.
 *
 * <p>{@link SagaStatus#ESCALATED} sagas are excluded — they require manual admin resolution before
 * cleanup.
 */
class SagaRetentionManager {

  private static final Logger logger = LoggerFactory.getLogger(SagaRetentionManager.class);

  private static final List<SagaStatus> PURGEABLE_STATUSES =
      Arrays.stream(SagaStatus.values()).filter(SagaStatus::isPurgeable).toList();

  private final SagaStore store;
  private final String ownerId;
  private final RetentionConfig config;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService purgeExecutor;
  private final Semaphore purgeSemaphore;

  // Rotates the sweep's starting bucket across passes; see cleanup(). Only the single scheduler
  // thread runs passes, and losing the count on restart is harmless (rotation needs no
  // continuity, only variety).
  private int passCount;

  SagaRetentionManager(SagaStore store, String ownerId, RetentionConfig config) {
    this.store = store;
    this.ownerId = ownerId;
    this.config = config;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "saga-retention");
              t.setDaemon(true);
              return t;
            });
    this.purgeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.purgeSemaphore = new Semaphore(config.maxConcurrentPurges());
  }

  // Visible for testing
  SagaRetentionManager(
      SagaStore store, String ownerId, RetentionConfig config, ScheduledExecutorService scheduler) {
    this.store = store;
    this.ownerId = ownerId;
    this.config = config;
    this.scheduler = scheduler;
    this.purgeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.purgeSemaphore = new Semaphore(config.maxConcurrentPurges());
  }

  /**
   * Starts the periodic cleanup task. Unlike the recovery manager, the first run is delayed by at
   * least the full interval — cleanup is not urgent at startup. On top of that, a deterministic
   * per-replica offset de-phases replicas started together, so their cleanup passes stop hitting
   * the store at the same moment.
   */
  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget scheduled task
  public void start() {
    long intervalSeconds = config.intervalSeconds();
    scheduler.scheduleWithFixedDelay(
        this::cleanupSafely,
        intervalSeconds + SweepScatter.offsetSeconds(ownerId, "retention", intervalSeconds),
        intervalSeconds,
        TimeUnit.SECONDS);
  }

  /**
   * Stops the retention scheduler and waits for any in-flight cleanup pass to complete, respecting
   * the given deadline. Both executors are signaled to shut down immediately; remaining time is
   * used for graceful termination before force-stopping.
   *
   * @param deadlineNanos absolute {@link System#nanoTime()} deadline
   */
  public void stop(long deadlineNanos) {
    scheduler.shutdown();
    purgeExecutor.shutdown();

    try {
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        scheduler.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
      remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        purgeExecutor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      scheduler.shutdownNow();
      purgeExecutor.shutdownNow();
    }
  }

  /**
   * Wraps {@link #cleanup()} so nothing escapes to the scheduler: a {@code Throwable} escaping a
   * periodic task cancels all its future executions, which would silently stop retention cleanup
   * for the rest of the process.
   */
  private void cleanupSafely() {
    try {
      cleanup();
    } catch (Throwable t) {
      logger.error("Retention cleanup pass failed unexpectedly", t);
    }
  }

  /**
   * Single cleanup pass: scan purgeable statuses for entries older than the retention period and
   * delete each expired saga, repeating in rounds until the batch budget is spent.
   *
   * <p>The batch budget counts <i>successful</i> purges only: a delete that turns out to be a no-op
   * (another replica already purged the saga) refunds its budget, and because purged rows are
   * physically gone, the next round's re-scan surfaces fresh candidates for the refunded budget
   * instead of the same rows — so under multi-replica contention a pass still purges up to {@code
   * maxPurgesPerPass} sagas when a backlog exists, instead of quietly under-delivering by its lost
   * races. A round that purges nothing ends the pass: the backlog is drained, the round was lost to
   * racing replicas entirely (the next pass re-scans), or the remaining candidates are failing
   * their deletes — and failing rows stay in place, so re-scanning would refetch them forever.
   * Failed deletes also refund budget, so total attempted operations may exceed {@code
   * maxPurgesPerPass}; this is acceptable — widespread delete failures indicate a store-level
   * issue, not a batch-sizing problem.
   *
   * <p>Each pass starts the bucket sweep one position further into the replica's scattered order
   * (the rotation below), so when a sustained backlog lets the front buckets fill the whole budget,
   * every bucket still gets a turn at the front within one revolution's worth of passes — a fixed
   * starting point would starve the tail buckets for as long as the backlog persists.
   */
  void cleanup() {
    Instant threshold = config.clock().instant().minus(config.retentionPeriod());
    int purged = 0;
    int rotation = passCount++;

    while (purged < config.maxPurgesPerPass()) {
      int purgedBeforeRound = purged;
      for (SagaStatus status : PURGEABLE_STATUSES) {
        purged += purgeByStatus(status, threshold, config.maxPurgesPerPass() - purged, rotation);
        if (purged >= config.maxPurgesPerPass()) {
          return; // batch limit reached — continue in next pass
        }
        if (Thread.currentThread().isInterrupted()) {
          return; // pass interrupted mid-await; its remaining work was cancelled and drained
        }
      }
      if (purged == purgedBeforeRound) {
        return; // no progress this round — drained backlog, lost races only, or failing deletes
      }
    }
  }

  private int purgeByStatus(SagaStatus status, Instant threshold, int remaining, int rotation) {
    List<SagaStateSnapshot> expired =
        store.findByStatusOlderThan(status, threshold, remaining, ownerId, rotation);
    List<Future<Boolean>> futures = new ArrayList<>();
    for (SagaStateSnapshot saga : expired) {
      futures.add(purgeExecutor.submit(() -> purgeOneSafely(saga)));
    }
    int purged = 0;
    for (int i = 0; i < futures.size(); i++) {
      try {
        if (futures.get(i).get()) {
          purged++;
        }
      } catch (InterruptedException e) {
        // Cancel the rest (interrupting their threads) and drain their outcomes instead of
        // leaving them to run on past the pass; the restored interrupt flag ends the pass in
        // cleanup(). A task blocked in a non-interruptible store call may still be finishing
        // that call when this returns.
        for (int j = i; j < futures.size(); j++) {
          futures.get(j).cancel(true);
        }
        for (int j = i; j < futures.size(); j++) {
          purged += drainQuietly(futures.get(j));
        }
        Thread.currentThread().interrupt();
        return purged;
      } catch (ExecutionException e) {
        // purgeOneSafely catches Throwable, so nothing should reach here; a surprise that does
        // still must not vanish.
        logger.error("Purge task failed unexpectedly", e.getCause());
      }
    }
    return purged;
  }

  /**
   * Drains one purge future after the pass was interrupted, counting a purge that completed before
   * its cancellation landed. A cancelled task's outcome is unknown and not counted — the pass is
   * ending, so the count no longer drives further scanning.
   */
  private int drainQuietly(Future<Boolean> future) {
    while (true) {
      try {
        return future.get() ? 1 : 0;
      } catch (CancellationException e) {
        return 0;
      } catch (ExecutionException e) {
        logger.error("Purge task failed unexpectedly", e.getCause());
        return 0;
      } catch (InterruptedException e) {
        // Re-interrupted while draining; keep draining — the caller restores the flag once.
      }
    }
  }

  private boolean purgeOneSafely(SagaStateSnapshot saga) {
    try {
      purgeSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    try {
      // False when the saga was already purged (a concurrent replica won the race); such no-ops
      // must not consume the batch budget, or intersecting sweeps would spend it deleting nothing.
      return store.deleteSaga(saga.getSagaId());
    } catch (Throwable t) {
      // Log and continue — one failed purge shouldn't block others. Throwable, not Exception: an
      // escape would surface at the await as a context-free ExecutionException instead of naming
      // the saga here.
      logger.warn("Failed to purge saga {}", saga.getSagaId(), t);
      return false;
    } finally {
      purgeSemaphore.release();
    }
  }
}
