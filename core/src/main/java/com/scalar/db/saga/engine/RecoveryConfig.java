package com.scalar.db.saga.engine;

// Imported for the Javadoc reference to SagaStatus.ESCALATED below; the record itself holds no
// status field.
import com.scalar.db.saga.api.SagaStatus;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the saga recovery manager.
 *
 * @param stalenessThresholdMillis a saga whose {@code updated_at} is older than this is considered
 *     abandoned and eligible for recovery. Must exceed the longest a healthy instance goes between
 *     updating a saga, or a live saga is stolen from the instance still running it.
 * @param intervalSeconds how often the recovery scan runs (in seconds); each replica shifts its
 *     schedule within this interval by a deterministic offset derived from its owner ID, so
 *     replicas started together do not scan in phase
 * @param compensationGracePeriod how long a saga can remain stuck (with step failure events) before
 *     being escalated to {@link SagaStatus#ESCALATED}
 * @param maxRecoveriesPerPass per-pass work budget (committed claims for the staleness sweep,
 *     committed WAITING transitions for the parked sweep — each sweep has its own budget). Claims
 *     lost to another replica do not consume the budget, but failed attempts do, so a degraded
 *     store gets backpressure instead of a full-ring retry storm; the sweep keeps scanning until
 *     the budget is spent or a full bucket revolution completes, and a budget-stopped sweep resumes
 *     at the same position next pass. A small value therefore never skips buckets — it only spreads
 *     a revolution across more passes, delaying recovery of the sagas behind the cut.
 * @param maxConcurrentRecoveries maximum number of sagas recovered concurrently within a single
 *     recovery pass (limits database pressure from virtual threads)
 * @param clock clock for time-based decisions (inject a fixed clock for testing)
 */
public record RecoveryConfig(
    long stalenessThresholdMillis,
    long intervalSeconds,
    Duration compensationGracePeriod,
    int maxRecoveriesPerPass,
    int maxConcurrentRecoveries,
    Clock clock) {

  /** Validates parameters. */
  public RecoveryConfig {
    if (stalenessThresholdMillis <= 0) {
      throw new IllegalArgumentException(
          "stalenessThresholdMillis must be > 0, got " + stalenessThresholdMillis);
    }
    if (intervalSeconds <= 0) {
      throw new IllegalArgumentException("intervalSeconds must be > 0, got " + intervalSeconds);
    }
    Objects.requireNonNull(compensationGracePeriod, "compensationGracePeriod must not be null");
    if (compensationGracePeriod.isNegative() || compensationGracePeriod.isZero()) {
      throw new IllegalArgumentException(
          "compensationGracePeriod must be positive, got " + compensationGracePeriod);
    }
    if (maxRecoveriesPerPass <= 0) {
      throw new IllegalArgumentException(
          "maxRecoveriesPerPass must be > 0, got " + maxRecoveriesPerPass);
    }
    if (maxConcurrentRecoveries <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentRecoveries must be > 0, got " + maxConcurrentRecoveries);
    }
    Objects.requireNonNull(clock, "clock must not be null");
  }

  /**
   * Default: 60s staleness threshold, scan every 30s, 4-hour grace period, 1000 recoveries per
   * pass, 10 concurrent recoveries.
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
