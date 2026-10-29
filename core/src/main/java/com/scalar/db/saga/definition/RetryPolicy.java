package com.scalar.db.saga.definition;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Configuration for retry behavior with exponential backoff and equal jitter.
 *
 * <p>Use the {@link #newBuilder()} method to create custom policies, or one of the factory methods
 * for common defaults:
 *
 * <ul>
 *   <li>{@link #defaultPolicy()} — general step execution (3 attempts, 1s initial, 30s max)
 *   <li>{@link #compensationDefault()} — compensation retry (3 attempts, 1s initial, 10s max)
 *   <li>{@link #confirmDefault()} — TCC confirm phase (10 attempts, 500ms initial, 60s max)
 * </ul>
 */
@Immutable
public final class RetryPolicy {

  // General step execution — conservative defaults for forward-path steps. Three attempts handle
  // common transient failures (network blips, connection pool exhaustion) without holding saga
  // resources too long.
  private static final RetryPolicy DEFAULT = new RetryPolicy(3, 1000, 2.0, 30_000);

  // Compensation — lower interval cap than the default. Compensations run after a failure;
  // spending less time per attempt means faster overall failure handling. If compensation still
  // fails, the saga stays in COMPENSATING for recovery to retry later.
  private static final RetryPolicy COMPENSATION_DEFAULT = new RetryPolicy(3, 1000, 2.0, 10_000);

  // TCC confirm — confirms must succeed since resources are already reserved by the Try phase.
  // More attempts and a higher interval cap allow the engine to ride out sustained outages before
  // handing off to recovery.
  private static final RetryPolicy CONFIRM_DEFAULT = new RetryPolicy(10, 500, 2.0, 60_000);

  private final int maxAttempts;
  private final long initialIntervalMillis;
  private final double backoffMultiplier;
  private final long maxIntervalMillis;

  private RetryPolicy(
      int maxAttempts,
      long initialIntervalMillis,
      double backoffMultiplier,
      long maxIntervalMillis) {
    this.maxAttempts = maxAttempts;
    this.initialIntervalMillis = initialIntervalMillis;
    this.backoffMultiplier = backoffMultiplier;
    this.maxIntervalMillis = maxIntervalMillis;
  }

  /**
   * Returns the default retry policy for general step execution.
   *
   * @return the shared policy: 3 attempts, a 1s initial interval doubling up to 30s
   */
  public static RetryPolicy defaultPolicy() {
    return DEFAULT;
  }

  /**
   * Returns the default retry policy for compensation.
   *
   * @return the shared policy: 3 attempts, a 1s initial interval doubling up to 10s
   */
  public static RetryPolicy compensationDefault() {
    return COMPENSATION_DEFAULT;
  }

  /**
   * Returns the default retry policy for the TCC confirm phase.
   *
   * @return the shared policy: 10 attempts, a 500ms initial interval doubling up to 60s
   */
  public static RetryPolicy confirmDefault() {
    return CONFIRM_DEFAULT;
  }

  /**
   * Starts building a custom policy. The builder's defaults are those of {@link #defaultPolicy()}.
   *
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * The total number of attempts, the first one included, before the action is given up on.
   *
   * @return the attempt limit, at least 1
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * The backoff interval before the first retry, in milliseconds. Each later interval is the
   * previous one times {@link #getBackoffMultiplier()}, capped at {@link #getMaxIntervalMillis()};
   * the actual sleep is jittered, see {@link #sleepWithBackoff(long)}.
   *
   * @return the initial interval, greater than 0
   */
  public long getInitialIntervalMillis() {
    return initialIntervalMillis;
  }

  /**
   * The factor the backoff interval grows by after each retry.
   *
   * @return the multiplier, at least 1.0
   */
  public double getBackoffMultiplier() {
    return backoffMultiplier;
  }

  /**
   * The ceiling on the backoff interval, in milliseconds.
   *
   * @return the maximum interval, at least {@link #getInitialIntervalMillis()}
   */
  public long getMaxIntervalMillis() {
    return maxIntervalMillis;
  }

  /**
   * Sleeps with exponential backoff and equal jitter, then returns the next interval.
   *
   * <p>Jitter strategy: "Equal Jitter" from the <a
   * href="https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/">AWS
   * Architecture Blog</a>. The sleep duration is {@code half + random(0, half)} where {@code half =
   * currentInterval / 2}. This guarantees a minimum wait of {@code half} while spreading retry
   * attempts to mitigate thundering herds.
   *
   * @param currentInterval the current backoff interval in milliseconds (must be &gt; 0)
   * @return the next backoff interval (capped at {@link #getMaxIntervalMillis()})
   * @throws IllegalArgumentException if {@code currentInterval} is not positive
   * @throws InterruptedException if the thread is interrupted while sleeping
   */
  public long sleepWithBackoff(long currentInterval) throws InterruptedException {
    if (currentInterval <= 0) {
      throw new IllegalArgumentException("currentInterval must be > 0, got " + currentInterval);
    }
    long half = currentInterval / 2;
    long jitter = half > 0 ? ThreadLocalRandom.current().nextLong(half) : 0;
    Thread.sleep(half + jitter);
    return Math.min((long) (currentInterval * backoffMultiplier), maxIntervalMillis);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof RetryPolicy that)) return false;
    return maxAttempts == that.maxAttempts
        && initialIntervalMillis == that.initialIntervalMillis
        && Double.compare(backoffMultiplier, that.backoffMultiplier) == 0
        && maxIntervalMillis == that.maxIntervalMillis;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxAttempts, initialIntervalMillis, backoffMultiplier, maxIntervalMillis);
  }

  @Override
  public String toString() {
    return "RetryPolicy{"
        + "maxAttempts="
        + maxAttempts
        + ", initialIntervalMillis="
        + initialIntervalMillis
        + ", backoffMultiplier="
        + backoffMultiplier
        + ", maxIntervalMillis="
        + maxIntervalMillis
        + '}';
  }

  /** Builder for {@link RetryPolicy}; obtained from {@link RetryPolicy#newBuilder()}. */
  public static final class Builder {

    private int maxAttempts = 3;
    private long initialIntervalMillis = 1000;
    private double backoffMultiplier = 2.0;
    private long maxIntervalMillis = 30_000;

    private Builder() {}

    /**
     * Sets the total number of attempts, the first one included. Defaults to 3.
     *
     * @param maxAttempts the attempt limit, at least 1
     * @return this builder
     */
    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    /**
     * Sets the backoff interval before the first retry. Defaults to 1000.
     *
     * @param initialIntervalMillis the interval in milliseconds, greater than 0
     * @return this builder
     */
    public Builder initialIntervalMillis(long initialIntervalMillis) {
      this.initialIntervalMillis = initialIntervalMillis;
      return this;
    }

    /**
     * Sets the factor the interval grows by after each retry. Defaults to 2.0.
     *
     * @param backoffMultiplier the multiplier, finite and at least 1.0
     * @return this builder
     */
    public Builder backoffMultiplier(double backoffMultiplier) {
      this.backoffMultiplier = backoffMultiplier;
      return this;
    }

    /**
     * Sets the ceiling on the backoff interval. Defaults to 30000.
     *
     * @param maxIntervalMillis the maximum interval in milliseconds, at least the initial interval
     * @return this builder
     */
    public Builder maxIntervalMillis(long maxIntervalMillis) {
      this.maxIntervalMillis = maxIntervalMillis;
      return this;
    }

    /**
     * Validates the settings and builds the policy.
     *
     * @return the immutable policy
     * @throws IllegalArgumentException if {@code maxAttempts} is below 1, {@code
     *     initialIntervalMillis} is not positive, {@code backoffMultiplier} is not finite or is
     *     below 1.0, or {@code maxIntervalMillis} is below {@code initialIntervalMillis}
     */
    public RetryPolicy build() {
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
      }
      if (initialIntervalMillis <= 0) {
        throw new IllegalArgumentException(
            "initialIntervalMillis must be > 0, got " + initialIntervalMillis);
      }
      if (!Double.isFinite(backoffMultiplier) || backoffMultiplier < 1.0) {
        throw new IllegalArgumentException(
            "backoffMultiplier must be finite and >= 1.0, got " + backoffMultiplier);
      }
      if (maxIntervalMillis < initialIntervalMillis) {
        throw new IllegalArgumentException(
            "maxIntervalMillis ("
                + maxIntervalMillis
                + ") must be >= initialIntervalMillis ("
                + initialIntervalMillis
                + ")");
      }
      return new RetryPolicy(
          maxAttempts, initialIntervalMillis, backoffMultiplier, maxIntervalMillis);
    }
  }
}
