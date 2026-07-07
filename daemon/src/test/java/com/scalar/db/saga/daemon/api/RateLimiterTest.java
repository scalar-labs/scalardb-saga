package com.scalar.db.saga.daemon.api;

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
  void constructor_nonPositiveLimit_throwsException() {
    assertThatThrownBy(() -> new RateLimiter(0, 1_000))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void constructor_nonPositiveWindow_throwsException() {
    assertThatThrownBy(() -> new RateLimiter(1, 0)).isInstanceOf(IllegalArgumentException.class);
  }
}
