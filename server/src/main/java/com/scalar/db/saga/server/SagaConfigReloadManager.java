package com.scalar.db.saga.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules the {@link ConfigReloadPass} on a fixed delay — the watcher half of configuration hot
 * reload, shaped like its living siblings {@code SagaRecoveryManager}/{@code SagaRetentionManager}:
 * a single-thread named daemon scheduler, a {@code reloadSafely()} wrapper so nothing escapes to
 * the scheduler, and a {@code stop(deadlineNanos)} that awaits the in-flight pass so no
 * registration store writes happen after the server's drain begins.
 *
 * <p>{@code scheduleWithFixedDelay} inherently serializes passes; the first scheduled pass runs one
 * full interval after {@link #start()}, because the boot pass has already applied the current
 * configuration synchronously.
 */
@ThreadSafe
final class SagaConfigReloadManager {

  private static final Logger logger = LoggerFactory.getLogger(SagaConfigReloadManager.class);

  private final ConfigReloadPass pass;
  private final long intervalSeconds;
  private final ScheduledExecutorService scheduler;

  SagaConfigReloadManager(ConfigReloadPass pass, ReloadConfig config) {
    this(
        pass,
        config,
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "saga-config-reload");
              thread.setDaemon(true);
              return thread;
            }));
  }

  // Visible for testing
  SagaConfigReloadManager(
      ConfigReloadPass pass, ReloadConfig config, ScheduledExecutorService scheduler) {
    this.pass = pass;
    this.intervalSeconds = config.intervalSeconds();
    this.scheduler = scheduler;
  }

  /** Starts the periodic reload task. The caller only invokes this when the interval is > 0. */
  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget scheduled task
  void start() {
    scheduler.scheduleWithFixedDelay(
        this::reloadSafely, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    logger.info("Config reload enabled: every {}s", intervalSeconds);
  }

  /**
   * Wraps the pass so nothing escapes to the scheduler: a {@code Throwable} escaping a periodic
   * task cancels all its future executions, which would silently stop configuration reload for the
   * rest of the process. The pass handles its own rejection logging; this catch is for the
   * unexpected.
   */
  private void reloadSafely() {
    try {
      pass.run();
    } catch (Throwable t) {
      logger.error("Config reload pass failed unexpectedly", t);
    }
  }

  /**
   * Stops the scheduler and waits for any in-flight pass to complete, respecting the deadline, so
   * no registration writes race the drain that follows. Safe to call on a never-started manager.
   *
   * @param deadlineNanos absolute {@link System#nanoTime()} deadline
   */
  void stop(long deadlineNanos) {
    scheduler.shutdown();
    try {
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining > 0) {
        scheduler.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      scheduler.shutdownNow();
    }
  }
}
