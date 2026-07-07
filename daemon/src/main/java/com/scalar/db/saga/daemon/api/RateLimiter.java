package com.scalar.db.saga.daemon.api;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A fixed-window rate limiter: allows up to {@code limit} hits per {@code windowMillis} window per
 * key, counting from the first hit of each window.
 *
 * <p>Thread-safe and memory-bounded. Per-key state is updated atomically via {@link
 * ConcurrentHashMap#compute}; when the tracked-key count exceeds a cap, expired windows are pruned
 * so a churn of distinct keys cannot grow the map without bound. {@code nowMillis} is passed in
 * (not read from the clock) so the window logic is deterministically testable.
 */
final class RateLimiter {

  // Above this many tracked keys, prune expired windows before inserting more. Prevents unbounded
  // map growth from many short-lived distinct keys, without needing a background sweeper thread.
  private static final int PRUNE_THRESHOLD = 100_000;

  private final int limit;
  private final long windowMillis;
  private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

  RateLimiter(int limit, long windowMillis) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive, got " + limit);
    }
    if (windowMillis <= 0) {
      throw new IllegalArgumentException("windowMillis must be positive, got " + windowMillis);
    }
    this.limit = limit;
    this.windowMillis = windowMillis;
  }

  /**
   * Records a hit for {@code key} at {@code nowMillis} and returns whether it is within the limit.
   *
   * @param key the bucket key (e.g. a principal)
   * @param nowMillis the current time in epoch millis
   * @return {@code true} if the hit is allowed, {@code false} if the key is over its limit this
   *     window
   */
  boolean tryAcquire(String key, long nowMillis) {
    if (windows.size() > PRUNE_THRESHOLD) {
      windows.values().removeIf(window -> isExpired(window, nowMillis));
    }
    Window updated =
        windows.compute(
            key,
            (k, existing) ->
                (existing == null || isExpired(existing, nowMillis))
                    ? new Window(nowMillis, 1)
                    : new Window(existing.startMillis, existing.count + 1));
    return updated.count <= limit;
  }

  private boolean isExpired(Window window, long nowMillis) {
    return nowMillis - window.startMillis >= windowMillis;
  }

  /** An immutable per-key window: when it started and how many hits it has counted. */
  private static final class Window {
    private final long startMillis;
    private final int count;

    Window(long startMillis, int count) {
      this.startMillis = startMillis;
      this.count = count;
    }
  }
}
