package com.scalar.db.saga.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the saga retention manager.
 *
 * @param retentionPeriod how long terminal sagas (COMPLETED, COMPENSATED) are retained before
 *     purging
 * @param intervalSeconds how often the cleanup job runs (in seconds)
 * @param maxPurgesPerPass maximum number of sagas actually purged per cleanup pass; deletes that
 *     turn out to be no-ops (another replica already purged the saga) do not consume it, and the
 *     pass re-scans in rounds until the budget is spent or a round makes no progress, so with a
 *     backlog the budget is achieved rather than merely bounded. Should be large enough to keep up
 *     with steady-state saga throughput over one cleanup interval. For example, at 100 TPS with a
 *     60-second interval, roughly 6,000 sagas become purgeable per pass.
 * @param maxConcurrentPurges maximum number of sagas purged concurrently within a single cleanup
 *     pass (limits database pressure from virtual threads)
 * @param clock clock for time-based decisions (inject a fixed clock for testing)
 */
public record RetentionConfig(
    Duration retentionPeriod,
    long intervalSeconds,
    int maxPurgesPerPass,
    int maxConcurrentPurges,
    Clock clock) {

  /** Validates parameters. */
  public RetentionConfig {
    Objects.requireNonNull(retentionPeriod, "retentionPeriod must not be null");
    if (retentionPeriod.isNegative() || retentionPeriod.isZero()) {
      throw new IllegalArgumentException(
          "retentionPeriod must be positive, got " + retentionPeriod);
    }
    if (intervalSeconds <= 0) {
      throw new IllegalArgumentException("intervalSeconds must be > 0, got " + intervalSeconds);
    }
    if (maxPurgesPerPass <= 0) {
      throw new IllegalArgumentException("maxPurgesPerPass must be > 0, got " + maxPurgesPerPass);
    }
    if (maxConcurrentPurges <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentPurges must be > 0, got " + maxConcurrentPurges);
    }
    Objects.requireNonNull(clock, "clock must not be null");
  }

  /** Default: 7-day retention, cleanup every 60 seconds, 10,000 purges per pass. */
  public static RetentionConfig defaults() {
    return defaults(Clock.systemUTC());
  }

  /**
   * Default configuration with a custom clock. Useful for testing with a fixed clock.
   *
   * @param clock clock for time-based decisions
   */
  public static RetentionConfig defaults(Clock clock) {
    return new RetentionConfig(Duration.ofDays(7), 60, 10_000, 10, clock);
  }
}
