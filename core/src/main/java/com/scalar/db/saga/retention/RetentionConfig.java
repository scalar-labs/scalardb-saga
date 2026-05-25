package com.scalar.db.saga.retention;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the saga retention manager.
 *
 * @param retentionPeriod how long terminal sagas (COMPLETED, COMPENSATED) are retained before
 *     purging
 * @param cleanupIntervalSeconds how often the cleanup job runs (in seconds)
 * @param batchSize maximum number of sagas purged per cleanup pass. Should be large enough to keep
 *     up with steady-state saga throughput over one cleanup interval. For example, at 100 TPS with
 *     a 60-second interval, roughly 6,000 sagas become purgeable per pass.
 * @param maxConcurrentPurges maximum number of sagas purged concurrently within a single cleanup
 *     pass (limits database pressure from virtual threads)
 * @param clock clock for time-based decisions (inject a fixed clock for testing)
 */
public record RetentionConfig(
    Duration retentionPeriod,
    long cleanupIntervalSeconds,
    int batchSize,
    int maxConcurrentPurges,
    Clock clock) {

  /** Validates parameters. */
  public RetentionConfig {
    Objects.requireNonNull(retentionPeriod, "retentionPeriod must not be null");
    if (retentionPeriod.isNegative() || retentionPeriod.isZero()) {
      throw new IllegalArgumentException(
          "retentionPeriod must be positive, got " + retentionPeriod);
    }
    if (cleanupIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          "cleanupIntervalSeconds must be > 0, got " + cleanupIntervalSeconds);
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be > 0, got " + batchSize);
    }
    if (maxConcurrentPurges <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentPurges must be > 0, got " + maxConcurrentPurges);
    }
    Objects.requireNonNull(clock, "clock must not be null");
  }

  /** Default: 7-day retention, cleanup every 60 seconds, 10,000 sagas per batch. */
  public static RetentionConfig defaults() {
    return new RetentionConfig(Duration.ofDays(7), 60, 10_000, 10, Clock.systemUTC());
  }
}
