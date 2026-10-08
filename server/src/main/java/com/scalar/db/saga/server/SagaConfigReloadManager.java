package com.scalar.db.saga.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules the {@link ConfigReconciler} on a fixed delay — the watcher half of configuration hot
 * reload, shaped like its living siblings {@code SagaRecoveryManager}/{@code SagaRetentionManager}:
 * a single-thread named daemon scheduler, a {@code reloadSafely()} wrapper so nothing escapes to
 * the scheduler, and a deadline-bounded {@code stop(deadlineNanos)} that awaits the in-flight pass
 * before the server's drain. When the server serves TLS or trusts an outbound CA bundle, the
 * certificate pass ({@link TlsReloader}, {@link EgressTrust}) runs right after the configuration
 * pass in the same task: the certificate files need no schedule of their own, and one thread
 * serializes the passes.
 *
 * <p>{@code scheduleWithFixedDelay} inherently serializes passes; the first scheduled pass runs one
 * full interval after {@link #start()}, because the boot pass has already applied the current
 * configuration synchronously.
 */
@ThreadSafe
final class SagaConfigReloadManager {

  private static final Logger logger = LoggerFactory.getLogger(SagaConfigReloadManager.class);

  private final ConfigReconciler reconciler;
  // Reloads the server's TLS material and the outbound CA bundle; null when neither is in use.
  private final @Nullable Runnable certificateReload;
  private final long intervalSeconds;
  private final ScheduledExecutorService scheduler;

  SagaConfigReloadManager(
      ConfigReconciler reconciler, ReloadConfig config, @Nullable Runnable certificateReload) {
    this(
        reconciler,
        config,
        certificateReload,
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "saga-config-reload");
              thread.setDaemon(true);
              return thread;
            }));
  }

  // Visible for testing
  SagaConfigReloadManager(
      ConfigReconciler reconciler,
      ReloadConfig config,
      @Nullable Runnable certificateReload,
      ScheduledExecutorService scheduler) {
    this.reconciler = reconciler;
    this.certificateReload = certificateReload;
    this.intervalSeconds = config.intervalSeconds();
    this.scheduler = scheduler;
  }

  /** Starts the periodic reload task. The caller only invokes this when the interval is > 0. */
  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget scheduled task
  void start() {
    scheduler.scheduleWithFixedDelay(
        this::reloadSafely, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    logger.info("Config reload enabled: every {}s", intervalSeconds);
    if (certificateReload != null) {
      logger.info("Certificate reload enabled: every {}s", intervalSeconds);
    }
  }

  /**
   * Wraps the passes so nothing escapes to the scheduler: a {@code Throwable} escaping a periodic
   * task cancels all its future executions, which would silently stop reload for the rest of the
   * process. Each pass is guarded on its own, so one blowing up does not cost the other its turn.
   * The passes handle their own rejection logging; these catches are for the unexpected.
   */
  private void reloadSafely() {
    try {
      reconciler.run();
    } catch (Throwable t) {
      logger.error("Config reload pass failed unexpectedly", t);
    }
    if (certificateReload != null) {
      try {
        certificateReload.run();
      } catch (Throwable t) {
        logger.error("Certificate reload pass failed unexpectedly", t);
      }
    }
  }

  /**
   * Stops the scheduler and waits, up to the deadline, for any in-flight pass to finish before the
   * caller drains the orchestrator — so a registration does not race the store's close.
   *
   * <p>The wait is bounded by the deadline, so this is best effort, not a guarantee: a pass still
   * running when the deadline passes is interrupted, and its registration may then fail against a
   * closing store. That costs nothing durable — the registration is a single transaction that
   * either committed or did not, and the next start re-applies it — but it is worth a line in the
   * log, so the shutdown is not diagnosed as data loss. Safe to call on a never-started manager.
   *
   * @param deadlineNanos absolute {@link System#nanoTime()} deadline
   */
  void stop(long deadlineNanos) {
    scheduler.shutdown();
    try {
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        // The result is not the signal: isTerminated() below decides whether a pass was actually
        // still running, which also covers an already-expired deadline (nothing was awaited).
        scheduler.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      if (!scheduler.isTerminated()) {
        logger.warn(
            "A reload pass was still running at the shutdown deadline and is being interrupted. A"
                + " definition registration in flight may fail against the closing store; it is"
                + " re-applied on the next start. A certificate pass in flight loses nothing.");
      }
      scheduler.shutdownNow();
    }
  }
}
