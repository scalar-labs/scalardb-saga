package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Covers the poll interval derived from the wait bound. The interval governs notice latency alone —
 * the registry covers a local drive and the read at bound expiry covers everything else — so what
 * matters is that it stays proportional to the bound between a floor and a ceiling.
 */
class BoundedWaitTest {

  @Test
  void pollIntervalMillis_defaultBoundGiven_returnsOneSixthOfIt() {
    // The 60s default yields 10s: six polls per window.
    assertThat(BoundedWait.pollIntervalMillis(60_000L)).isEqualTo(10_000L);
  }

  @Test
  void pollIntervalMillis_shortBoundGiven_returnsTheFloor() {
    // A proportional interval here would be 833ms, tight enough to spin; the floor stops that.
    assertThat(BoundedWait.pollIntervalMillis(5_000L)).isEqualTo(1_000L);
  }

  @Test
  void pollIntervalMillis_zeroBoundGiven_returnsTheFloor() {
    assertThat(BoundedWait.pollIntervalMillis(0L)).isEqualTo(1_000L);
  }

  @Test
  void pollIntervalMillis_longBoundGiven_returnsTheCeiling() {
    // Proportionality alone would give 100s here, which is the case the ceiling exists for: a short
    // saga running under a long bound should not wait that long to be noticed.
    assertThat(BoundedWait.pollIntervalMillis(600_000L)).isEqualTo(30_000L);
  }

  @Test
  void pollIntervalMillis_veryLongBoundGiven_staysAtTheCeiling() {
    assertThat(BoundedWait.pollIntervalMillis(1_800_000L)).isEqualTo(30_000L);
  }

  @Test
  void pollIntervalMillis_atTheCeilingBoundary_returnsTheCeiling() {
    // 180s is exactly six 30s polls — the largest bound that is still purely proportional.
    assertThat(BoundedWait.pollIntervalMillis(180_000L)).isEqualTo(30_000L);
  }
}
