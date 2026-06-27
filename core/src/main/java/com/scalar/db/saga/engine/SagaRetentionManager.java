package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.store.SagaStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  private final RetentionConfig config;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService purgeExecutor;
  private final Semaphore purgeSemaphore;

  SagaRetentionManager(SagaStore store, RetentionConfig config) {
    this.store = store;
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
      SagaStore store, RetentionConfig config, ScheduledExecutorService scheduler) {
    this.store = store;
    this.config = config;
    this.scheduler = scheduler;
    this.purgeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    this.purgeSemaphore = new Semaphore(config.maxConcurrentPurges());
  }

  /**
   * Starts the periodic cleanup task. Unlike the recovery manager, the first run is delayed by the
   * full interval — cleanup is not urgent at startup.
   */
  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget scheduled task
  public void start() {
    scheduler.scheduleWithFixedDelay(
        this::cleanupSafely,
        config.cleanupIntervalSeconds(),
        config.cleanupIntervalSeconds(),
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

  /** Wraps {@link #cleanup()} with exception handling so the scheduler never stops on failure. */
  private void cleanupSafely() {
    try {
      cleanup();
    } catch (Exception e) {
      logger.error("Retention cleanup pass failed unexpectedly", e);
    }
  }

  /**
   * Single cleanup pass: scan purgeable statuses for entries older than the retention period, then
   * delete each expired saga. Stops when the batch limit is reached.
   *
   * <p>Note: the batch budget counts <i>successful</i> purges only, so total attempted operations
   * may exceed {@code batchSize} if many deletes fail. This is acceptable — widespread delete
   * failures indicate a store-level issue, not a batch-sizing problem.
   */
  void cleanup() {
    Instant threshold = config.clock().instant().minus(config.retentionPeriod());
    int purged = 0;

    for (SagaStatus status : PURGEABLE_STATUSES) {
      purged += purgeByStatus(status, threshold, config.batchSize() - purged);
      if (purged >= config.batchSize()) {
        return; // batch limit reached — continue in next pass
      }
    }
  }

  private int purgeByStatus(SagaStatus status, Instant threshold, int remaining) {
    List<SagaStateSnapshot> expired = store.findByStatusOlderThan(status, threshold, remaining);
    List<Future<Boolean>> futures = new ArrayList<>();
    for (SagaStateSnapshot saga : expired) {
      futures.add(purgeExecutor.submit(() -> purgeOneSafely(saga)));
    }
    int purged = 0;
    for (Future<Boolean> future : futures) {
      try {
        if (future.get()) {
          purged++;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        // Already logged inside purgeOneSafely
      }
    }
    return purged;
  }

  private boolean purgeOneSafely(SagaStateSnapshot saga) {
    try {
      purgeSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    try {
      store.deleteSaga(saga.getSagaId());
      return true;
    } catch (Exception e) {
      // Log and continue — one failed purge shouldn't block others
      logger.warn("Failed to purge saga {}", saga.getSagaId(), e);
      return false;
    } finally {
      purgeSemaphore.release();
    }
  }
}
