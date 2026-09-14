package com.scalar.db.saga.benchmark;

import java.util.Locale;

/**
 * Immutable percentile snapshot of a {@link LatencyRecorder}, in nanoseconds.
 *
 * @param count the number of samples
 * @param p50Nanos the 50th percentile
 * @param p90Nanos the 90th percentile
 * @param p99Nanos the 99th percentile
 * @param maxNanos the exact maximum
 * @param meanNanos the mean
 */
public record LatencySummary(
    long count, long p50Nanos, long p90Nanos, long p99Nanos, long maxNanos, long meanNanos) {

  /** An empty summary (all zeros). */
  public static LatencySummary empty() {
    return new LatencySummary(0, 0, 0, 0, 0, 0);
  }

  /**
   * One-line rendering, e.g. {@code p50=1.2ms p90=3.4ms p99=9.8ms max=52.0ms mean=2.1ms (n=1000)}.
   */
  public String format() {
    if (count == 0) {
      return "no samples";
    }
    return String.format(
        Locale.ROOT,
        "p50=%s p90=%s p99=%s max=%s mean=%s (n=%d)",
        millis(p50Nanos),
        millis(p90Nanos),
        millis(p99Nanos),
        millis(maxNanos),
        millis(meanNanos),
        count);
  }

  private static String millis(long nanos) {
    return String.format(Locale.ROOT, "%.1fms", nanos / 1_000_000.0);
  }
}
