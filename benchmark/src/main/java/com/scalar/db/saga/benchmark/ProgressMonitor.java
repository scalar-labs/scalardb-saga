package com.scalar.db.saga.benchmark;

import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;

/**
 * Background reporter for a running benchmark: prints one progress line per interval and flags
 * stalls — intervals in which nothing reached a terminal state while operations were in flight,
 * which is exactly what a hung engine looks like from a client.
 */
@ThreadSafe
final class ProgressMonitor implements AutoCloseable {

  /**
   * A point-in-time view of the run.
   *
   * @param issued operations attempted so far
   * @param resolved operations finished (terminal, failed, or timed out)
   * @param inFlight operations currently blocked in a call
   * @param oldestInFlightMillis age of the oldest in-flight operation, or {@code 0} when none
   * @param backlog sagas started but not yet observed terminal — the "pending processes" measure;
   *     in fire-and-forget mode this is the number building up inside the engine
   */
  record Progress(
      long issued, long resolved, long inFlight, long oldestInFlightMillis, long backlog) {}

  private final Supplier<Progress> source;
  private final long intervalMillis;
  private final Consumer<String> sink;
  private final Thread thread;

  ProgressMonitor(Supplier<Progress> source, long intervalMillis, Consumer<String> sink) {
    if (intervalMillis <= 0) {
      throw new IllegalArgumentException("intervalMillis must be > 0, got " + intervalMillis);
    }
    this.source = source;
    this.intervalMillis = intervalMillis;
    this.sink = sink;
    this.thread = new Thread(this::loop, "saga-bench-progress");
    this.thread.setDaemon(true);
  }

  void start() {
    thread.start();
  }

  private void loop() {
    long startNanos = System.nanoTime();
    long lastResolved = 0;
    long stalledMillis = 0;
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      Progress progress = source.get();
      if (progress.resolved() == lastResolved && progress.inFlight() > 0) {
        stalledMillis += intervalMillis;
      } else {
        stalledMillis = 0;
      }
      lastResolved = progress.resolved();
      long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
      sink.accept(formatLine(elapsedMillis, progress, stalledMillis));
    }
  }

  /** Renders one progress line. Package-private for testing. */
  static String formatLine(long elapsedMillis, Progress progress, long stalledMillis) {
    String line =
        String.format(
            Locale.ROOT,
            "[%5.1fs] issued=%d resolved=%d inFlight=%d oldestInFlight=%.1fs backlog=%d",
            elapsedMillis / 1000.0,
            progress.issued(),
            progress.resolved(),
            progress.inFlight(),
            progress.oldestInFlightMillis() / 1000.0,
            progress.backlog());
    if (stalledMillis > 0) {
      line += String.format(Locale.ROOT, "  ** NO PROGRESS for %.1fs **", stalledMillis / 1000.0);
    }
    return line;
  }

  @Override
  public void close() {
    thread.interrupt();
    try {
      thread.join(2_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
