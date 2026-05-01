package com.scalar.db.saga.api;

import java.util.Objects;
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
public final class RetryPolicy {

  private static final RetryPolicy DEFAULT = new RetryPolicy(3, 1000, 2.0, 30_000);
  private static final RetryPolicy COMPENSATION_DEFAULT = new RetryPolicy(3, 1000, 2.0, 10_000);
  private static final RetryPolicy CONFIRM_DEFAULT = new RetryPolicy(10, 500, 2.0, 60_000);

  private final int maxAttempts;
  private final long initialIntervalMs;
  private final double backoffMultiplier;
  private final long maxIntervalMs;

  private RetryPolicy(
      int maxAttempts, long initialIntervalMs, double backoffMultiplier, long maxIntervalMs) {
    this.maxAttempts = maxAttempts;
    this.initialIntervalMs = initialIntervalMs;
    this.backoffMultiplier = backoffMultiplier;
    this.maxIntervalMs = maxIntervalMs;
  }

  /** General step execution: 3 attempts, 1s initial, 2.0x multiplier, 30s max. */
  public static RetryPolicy defaultPolicy() {
    return DEFAULT;
  }

  /** Compensation retry: 3 attempts, 1s initial, 2.0x multiplier, 10s max. */
  public static RetryPolicy compensationDefault() {
    return COMPENSATION_DEFAULT;
  }

  /** TCC confirm phase: 10 attempts, 500ms initial, 2.0x multiplier, 60s max. */
  public static RetryPolicy confirmDefault() {
    return CONFIRM_DEFAULT;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public long getInitialIntervalMs() {
    return initialIntervalMs;
  }

  public double getBackoffMultiplier() {
    return backoffMultiplier;
  }

  public long getMaxIntervalMs() {
    return maxIntervalMs;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof RetryPolicy that)) return false;
    return maxAttempts == that.maxAttempts
        && initialIntervalMs == that.initialIntervalMs
        && Double.compare(backoffMultiplier, that.backoffMultiplier) == 0
        && maxIntervalMs == that.maxIntervalMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxAttempts, initialIntervalMs, backoffMultiplier, maxIntervalMs);
  }

  @Override
  public String toString() {
    return "RetryPolicy{"
        + "maxAttempts="
        + maxAttempts
        + ", initialIntervalMs="
        + initialIntervalMs
        + ", backoffMultiplier="
        + backoffMultiplier
        + ", maxIntervalMs="
        + maxIntervalMs
        + '}';
  }

  public static final class Builder {

    private int maxAttempts = 3;
    private long initialIntervalMs = 1000;
    private double backoffMultiplier = 2.0;
    private long maxIntervalMs = 30_000;

    private Builder() {}

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder initialIntervalMs(long initialIntervalMs) {
      this.initialIntervalMs = initialIntervalMs;
      return this;
    }

    public Builder backoffMultiplier(double backoffMultiplier) {
      this.backoffMultiplier = backoffMultiplier;
      return this;
    }

    public Builder maxIntervalMs(long maxIntervalMs) {
      this.maxIntervalMs = maxIntervalMs;
      return this;
    }

    public RetryPolicy build() {
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
      }
      if (initialIntervalMs <= 0) {
        throw new IllegalArgumentException(
            "initialIntervalMs must be > 0, got " + initialIntervalMs);
      }
      if (backoffMultiplier < 1.0) {
        throw new IllegalArgumentException(
            "backoffMultiplier must be >= 1.0, got " + backoffMultiplier);
      }
      if (maxIntervalMs < initialIntervalMs) {
        throw new IllegalArgumentException(
            "maxIntervalMs ("
                + maxIntervalMs
                + ") must be >= initialIntervalMs ("
                + initialIntervalMs
                + ")");
      }
      return new RetryPolicy(maxAttempts, initialIntervalMs, backoffMultiplier, maxIntervalMs);
    }
  }
}
