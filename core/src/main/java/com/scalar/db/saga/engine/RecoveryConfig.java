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
 * @param recoveryTimeoutMillis staleness threshold — a saga that has made no progress for this long
 *     is considered abandoned and eligible for recovery. Progress means the later of the state
 *     row's {@code updated_at} and the newest event: step execution appends events without touching
 *     the state row, so the row alone would make every long-running saga look dead. Size it above
 *     the longest a single step attempt sequence can take (worst-case attempts × step timeout plus
 *     backoff), since a healthy saga emits an event only at step boundaries. Raising it delays
 *     recovery of genuinely crashed sagas by the same amount — this value is the crash-recovery
 *     MTTR
 * @param recoveryIntervalSeconds how often the recovery scan runs (in seconds); each replica shifts
 *     its schedule within this interval by a deterministic offset derived from its owner ID, so
 *     replicas started together do not scan in phase
 * @param compensationGracePeriod how long a saga can remain stuck (with step failure events) before
 *     being escalated to {@link SagaStatus#ESCALATED}
 * @param batchSize per-pass work budget (committed claims for the staleness sweep, committed
 *     WAITING transitions for the parked sweep — each sweep has its own budget). Claims lost to
 *     another replica do not consume the budget, but failed attempts do, so a degraded store gets
 *     backpressure instead of a full-ring retry storm; the sweep keeps scanning until the budget is
 *     spent or a full bucket revolution completes, and a budget-stopped sweep resumes at the same
 *     position next pass. A small {@code batchSize} therefore never skips buckets — it only spreads
 *     a revolution across more passes, delaying recovery of the sagas behind the cut.
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
   * Default: 60s timeout, scan every 30s, 4-hour grace period, batch size 1000 (successful
   * recoveries per pass), 10 concurrent recoveries.
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
