package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatencySummaryTest {

  @Test
  public void format_noSamples_saysSo() {
    // Arrange
    LatencySummary summary = LatencySummary.empty();

    // Act & Assert
    assertThat(summary.format()).isEqualTo("no samples");
  }

  @Test
  public void format_samplesGiven_rendersAllPercentiles() {
    // Arrange
    LatencySummary summary =
        new LatencySummary(1000, 1_200_000, 3_400_000, 9_800_000, 52_000_000, 2_100_000);

    // Act
    String formatted = summary.format();

    // Assert
    assertThat(formatted)
        .contains("p50=1.2ms")
        .contains("p90=3.4ms")
        .contains("p99=9.8ms")
        .contains("max=52.0ms")
        .contains("mean=2.1ms")
        .contains("(n=1000)");
  }

  @Test
  public void empty_returnsAllZeros() {
    // Arrange & Act
    LatencySummary summary = LatencySummary.empty();

    // Assert
    assertThat(summary.count()).isZero();
    assertThat(summary.p50Nanos()).isZero();
    assertThat(summary.maxNanos()).isZero();
  }
}
