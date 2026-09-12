package com.scalar.db.saga.server.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RateLimiterTest {

  @Test
  void tryAcquire_underLimit_allowed() {
    // Arrange — 3 per 1000ms window
    RateLimiter limiter = new RateLimiter(3, 1_000);

    // Act / Assert — first three at the same instant are allowed
    assertThat(limiter.tryAcquire("k", 0)).isTrue();
    assertThat(limiter.tryAcquire("k", 10)).isTrue();
    assertThat(limiter.tryAcquire("k", 20)).isTrue();
  }

  @Test
  void retryAfterMillis_liveWindow_returnsRemainingMillis() {
    // Arrange — window opened at t=0, so at t=300 the reset is 700ms away
    RateLimiter limiter = new RateLimiter(1, 1_000);
    limiter.tryAcquire("k", 0);

    // Act / Assert
    assertThat(limiter.retryAfterMillis("k", 300)).isEqualTo(700);
  }

  @Test
  void retryAfterMillis_untrackedKey_returnsFullWindow() {
    // Arrange — never-seen key (or one evicted between refusal and query): the conservative
    // advisory is one full window
    RateLimiter limiter = new RateLimiter(1, 1_000);

    // Act / Assert
    assertThat(limiter.retryAfterMillis("unknown", 300)).isEqualTo(1_000);
  }

  @Test
  void retryAfterMillis_expiredWindow_returnsFullWindow() {
    // Arrange
    RateLimiter limiter = new RateLimiter(1, 1_000);
    limiter.tryAcquire("k", 0);

    // Act / Assert — past the reset the entry is stale; same conservative advisory
    assertThat(limiter.retryAfterMillis("k", 1_500)).isEqualTo(1_000);
  }

  @Test
  void tryAcquire_overLimit_rejectedWithinWindow() {
    // Arrange
    RateLimiter limiter = new RateLimiter(2, 1_000);
    limiter.tryAcquire("k", 0);
    limiter.tryAcquire("k", 100);

    // Act / Assert — the third within the window is rejected
    assertThat(limiter.tryAcquire("k", 200)).isFalse();
  }

  @Test
  void tryAcquire_afterWindowElapses_countResets() {
    // Arrange
    RateLimiter limiter = new RateLimiter(2, 1_000);
    limiter.tryAcquire("k", 0);
    limiter.tryAcquire("k", 100);
    assertThat(limiter.tryAcquire("k", 200)).isFalse(); // over limit in window [0,1000)

    // Act / Assert — at 1000ms a new window starts and the counter resets
    assertThat(limiter.tryAcquire("k", 1_000)).isTrue();
    assertThat(limiter.tryAcquire("k", 1_100)).isTrue();
    assertThat(limiter.tryAcquire("k", 1_200)).isFalse();
  }

  @Test
  void tryAcquire_windowStartsFromFirstHit_notWallClockBoundary() {
    // Arrange — first hit at 500 defines the window [500, 1500)
    RateLimiter limiter = new RateLimiter(1, 1_000);
    assertThat(limiter.tryAcquire("k", 500)).isTrue();

    // Act / Assert — still within [500,1500) at 1400 → rejected; at 1500 a new window opens
    assertThat(limiter.tryAcquire("k", 1_400)).isFalse();
    assertThat(limiter.tryAcquire("k", 1_500)).isTrue();
  }

  @Test
  void tryAcquire_keysAreIndependent() {
    // Arrange
    RateLimiter limiter = new RateLimiter(1, 1_000);

    // Act / Assert — one key's exhaustion does not affect another
    assertThat(limiter.tryAcquire("alice", 0)).isTrue();
    assertThat(limiter.tryAcquire("alice", 100)).isFalse();
    assertThat(limiter.tryAcquire("bob", 100)).isTrue();
  }

  @Test
  void tryAcquire_overPruneThreshold_prunesExpiredWindows() {
    // Arrange — fill past the prune threshold with distinct keys whose windows all start at 0. No
    // prune runs during the fill: the time-gate blocks it until a full window has elapsed since the
    // initial lastPrunedMillis of 0.
    RateLimiter limiter = new RateLimiter(1, 1_000);
    fillDistinctKeys(limiter, 100_001, 0);
    assertThat(limiter.trackedKeys()).isEqualTo(100_001);

    // Act — one more hit a full window later, when every filled window has expired.
    limiter.tryAcquire("trigger", 2_000);

    // Assert — the expired windows were pruned, leaving only the triggering key.
    assertThat(limiter.trackedKeys()).isEqualTo(1);
  }

  @Test
  void tryAcquire_withinWindowOfLastPrune_skipsRedundantPrune() {
    // Arrange — over the threshold, all windows starting at 0 (so lastPrunedMillis stays 0).
    RateLimiter limiter = new RateLimiter(1, 1_000);
    fillDistinctKeys(limiter, 100_001, 0);

    // Act — a hit exactly one window later. The filled windows are now expired, but the gate
    // (nowMillis - lastPruned must exceed windowMillis) is not yet open at the boundary.
    limiter.tryAcquire("probe", 1_000);

    // Assert — pruning was skipped, so the expired windows and the probe all remain.
    assertThat(limiter.trackedKeys()).isEqualTo(100_002);

    // Act — one millisecond past the window: the gate opens and pruning resumes.
    limiter.tryAcquire("probe2", 1_001);

    // Assert — the expired windows are gone; only probe (still live) and probe2 remain.
    assertThat(limiter.trackedKeys()).isEqualTo(2);
  }

  private static void fillDistinctKeys(RateLimiter limiter, int count, long nowMillis) {
    for (int i = 0; i < count; i++) {
      limiter.tryAcquire("key-" + i, nowMillis);
    }
  }

  @Test
  void tryAcquire_overMaxTrackedKeys_evictsToStayBounded() {
    // Arrange — a tiny ceiling of 3 tracked keys (injected so the cap is cheap to exercise)
    RateLimiter limiter = new RateLimiter(5, 1_000, 3);

    // Act — four distinct keys within the same window, so none expires to be pruned
    limiter.tryAcquire("a", 0);
    limiter.tryAcquire("b", 0);
    limiter.tryAcquire("c", 0);
    assertThat(limiter.trackedKeys()).isEqualTo(3);
    limiter.tryAcquire("d", 0);

    // Assert — the hard ceiling holds; an arbitrary entry was evicted rather than growing to 4
    assertThat(limiter.trackedKeys()).isEqualTo(3);
  }

  @Test
  void constructor_nonPositiveMaxTrackedKeys_throwsException() {
    assertThatThrownBy(() -> new RateLimiter(1, 1_000, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_nonPositiveLimit_throwsException() {
    assertThatThrownBy(() -> new RateLimiter(0, 1_000))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_nonPositiveWindow_throwsException() {
    assertThatThrownBy(() -> new RateLimiter(1, 0)).isInstanceOf(IllegalArgumentException.class);
  }
}
