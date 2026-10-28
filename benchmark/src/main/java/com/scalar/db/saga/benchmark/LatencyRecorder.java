package com.scalar.db.saga.benchmark;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import net.jcip.annotations.ThreadSafe;

/**
 * Lock-free latency histogram with logarithmic buckets refined by 16 linear sub-buckets per power
 * of two, giving percentiles within about 6% of the true value with a fixed 960-slot footprint.
 * Values are bucketed at microsecond granularity; {@link #maxNanos()} is exact.
 *
 * <p>Reads ({@link #percentileNanos}, {@link #summary}) are safe during concurrent recording but
 * see a racy snapshot; call them after the workload quiesces for exact totals.
 */
@ThreadSafe
public final class LatencyRecorder {

  // 16 exact slots for 0..15 micros, then 16 sub-buckets per power of two up to 2^63.
  private static final int SUB_BUCKETS = 16;
  private static final int BUCKET_COUNT = SUB_BUCKETS + (63 - 3) * SUB_BUCKETS;

  private final AtomicLongArray buckets = new AtomicLongArray(BUCKET_COUNT);
  private final LongAdder count = new LongAdder();
  private final LongAdder totalNanos = new LongAdder();
  private final LongAccumulator maxNanos = new LongAccumulator(Long::max, 0L);

  /** Records one latency sample. Negative values are clamped to zero. */
  public void record(long nanos) {
    long clamped = Math.max(nanos, 0L);
    buckets.incrementAndGet(indexOf(clamped / 1_000));
    count.increment();
    totalNanos.add(clamped);
    maxNanos.accumulate(clamped);
  }

  /** The number of recorded samples. */
  public long count() {
    return count.sum();
  }

  /** The exact maximum recorded latency in nanoseconds, or {@code 0} when empty. */
  public long maxNanos() {
    return maxNanos.get();
  }

  /** The mean latency in nanoseconds, or {@code 0} when empty. */
  public long meanNanos() {
    long n = count.sum();
    return n == 0 ? 0 : totalNanos.sum() / n;
  }

  /**
   * The latency at percentile {@code p} in nanoseconds (bucket upper bound, so within ~6% above the
   * true value), or {@code 0} when empty.
   *
   * @param p the percentile in {@code (0, 100]}
   */
  public long percentileNanos(double p) {
    if (p <= 0 || p > 100) {
      throw new IllegalArgumentException("percentile must be in (0, 100], got " + p);
    }
    long n = count.sum();
    if (n == 0) {
      return 0;
    }
    long rank = Math.max(1, (long) Math.ceil(n * p / 100.0));
    long seen = 0;
    for (int i = 0; i < BUCKET_COUNT; i++) {
      seen += buckets.get(i);
      if (seen >= rank) {
        // Clamp to the exact max: the last bucket's upper bound can overshoot it.
        return Math.min(upperBoundMicros(i) * 1_000, maxNanos.get());
      }
    }
    return maxNanos.get();
  }

  /** An immutable snapshot of the current distribution. */
  public LatencySummary summary() {
    return new LatencySummary(
        count(),
        percentileNanos(50),
        percentileNanos(90),
        percentileNanos(99),
        maxNanos(),
        meanNanos());
  }

  /** Maps a microsecond value to its bucket index. Package-private for testing. */
  static int indexOf(long micros) {
    if (micros < SUB_BUCKETS) {
      return (int) micros;
    }
    int exp = 63 - Long.numberOfLeadingZeros(micros);
    int sub = (int) ((micros >>> (exp - 4)) & (SUB_BUCKETS - 1));
    return (exp - 3) * SUB_BUCKETS + sub;
  }

  /** The largest microsecond value a bucket holds. Package-private for testing. */
  static long upperBoundMicros(int index) {
    if (index < SUB_BUCKETS) {
      return index;
    }
    int exp = index / SUB_BUCKETS + 3;
    int sub = index % SUB_BUCKETS;
    return ((SUB_BUCKETS + sub + 1L) << (exp - 4)) - 1;
  }
}
