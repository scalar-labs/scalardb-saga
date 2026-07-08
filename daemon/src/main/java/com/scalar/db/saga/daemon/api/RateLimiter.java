package com.scalar.db.saga.daemon.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A fixed-window rate limiter: allows up to {@code limit} hits per {@code windowMillis} window per
 * key, counting from the first hit of each window.
 *
 * <p>Thread-safe and memory-bounded. Per-key state is updated atomically via {@link
 * ConcurrentHashMap#compute}; when the tracked-key count exceeds a cap, expired windows are pruned
 * so a churn of distinct keys cannot grow the map without bound. {@code nowMillis} is passed in
 * (not read from the clock) so the window logic is deterministically testable.
 *
 * <p>Public so a single instance can be shared across transports (the REST {@code RateLimitHandler}
 * and the gRPC {@code SagaRateLimitInterceptor}), keeping a caller's budget global rather than
 * per-port.
 */
public final class RateLimiter {

  // Above this many tracked keys, prune expired windows before inserting more. Prevents unbounded
  // map growth from many short-lived distinct keys, without needing a background sweeper thread.
  private static final int PRUNE_THRESHOLD = 100_000;

  private final int limit;
  private final long windowMillis;
  private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

  // Epoch millis of the most recent prune. Gates pruning to at most once per window so a burst of
  // concurrent requests over the threshold cannot each launch a full O(n) scan (a self-inflicted
  // DoS). Pruning more frequently is near-useless: an entry only expires one window after its first
  // hit, so a second scan within the same window would remove almost nothing.
  private final AtomicLong lastPrunedMillis = new AtomicLong(0L);

  public RateLimiter(int limit, long windowMillis) {
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
  public boolean tryAcquire(String key, long nowMillis) {
    maybePrune(nowMillis);
    Window updated =
        windows.compute(
            key,
            (k, existing) ->
                (existing == null || isExpired(existing, nowMillis))
                    ? new Window(nowMillis, 1)
                    : new Window(existing.startMillis, existing.count + 1));
    return updated.count <= limit;
  }

  /**
   * Prunes expired windows when the map has grown past {@link #PRUNE_THRESHOLD}, but at most once
   * per window. The {@code compareAndSet} elects a single pruner: concurrent callers that lose the
   * CAS (or that arrive within one window of the last prune) skip the scan, so a burst of
   * over-threshold requests cannot each run a full O(n) {@code removeIf}.
   */
  private void maybePrune(long nowMillis) {
    long lastPruned = lastPrunedMillis.get();
    if (windows.size() > PRUNE_THRESHOLD
        && nowMillis - lastPruned > windowMillis
        && lastPrunedMillis.compareAndSet(lastPruned, nowMillis)) {
      windows.values().removeIf(window -> isExpired(window, nowMillis));
    }
  }

  private boolean isExpired(Window window, long nowMillis) {
    return nowMillis - window.startMillis >= windowMillis;
  }

  /** Visible for testing: the number of currently tracked keys (map size). */
  int trackedKeys() {
    return windows.size();
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
