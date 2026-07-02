package com.scalar.db.saga.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the saga recovery manager.
 *
 * @param recoveryTimeoutMillis staleness threshold — sagas with {@code updated_at} older than this
 *     are considered stale and eligible for recovery
 * @param recoveryIntervalSeconds how often the recovery scan runs (in seconds)
 * @param compensationGracePeriod how long a saga can remain stuck (with step failure events) before
 *     being escalated to {@link SagaStatus#ESCALATED}
 * @param batchSize maximum number of sagas recovered per pass. Should be larger than the store's
 *     {@code recoveryScanLimit} (per-status per-bucket cap) multiplied by the number of recoverable
 *     statuses to ensure multiple buckets are covered per pass. For example, with {@code
 *     recoveryScanLimit=100} and 2 recoverable statuses, each bucket can return up to 200 sagas — a
 *     {@code batchSize} of 1000 covers at least 5 buckets per pass.
 * @param maxConcurrentRecoveries maximum number of sagas recovered concurrently within a single
 *     recovery pass (limits database pressure from virtual threads)
 * @param clock clock for time-based decisions (inject a fixed clock for testing)
 */
public record RecoveryConfig(
    long recoveryTimeoutMillis,
    long recoveryIntervalSeconds,
    Duration compensationGracePeriod,
    int batchSize,
    int maxConcurrentRecoveries,
    Clock clock) {

  /** Validates parameters. */
  public RecoveryConfig {
    if (recoveryTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "recoveryTimeoutMillis must be > 0, got " + recoveryTimeoutMillis);
    }
    if (recoveryIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          "recoveryIntervalSeconds must be > 0, got " + recoveryIntervalSeconds);
    }
    Objects.requireNonNull(compensationGracePeriod, "compensationGracePeriod must not be null");
    if (compensationGracePeriod.isNegative() || compensationGracePeriod.isZero()) {
      throw new IllegalArgumentException(
          "compensationGracePeriod must be positive, got " + compensationGracePeriod);
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be > 0, got " + batchSize);
    }
    if (maxConcurrentRecoveries <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentRecoveries must be > 0, got " + maxConcurrentRecoveries);
    }
    Objects.requireNonNull(clock, "clock must not be null");
  }

  /**
   * Default: 60s timeout, scan every 30s, 4-hour grace period, batch size 1000, 10 concurrent
   * recoveries.
   */
  public static RecoveryConfig defaults() {
    return defaults(Clock.systemUTC());
  }

  /**
   * Default configuration with a custom clock. Useful for testing with a fixed clock.
   *
   * @param clock clock for time-based decisions
   */
  public static RecoveryConfig defaults(Clock clock) {
    return new RecoveryConfig(60_000, 30, Duration.ofHours(4), 1000, 10, clock);
  }
}
