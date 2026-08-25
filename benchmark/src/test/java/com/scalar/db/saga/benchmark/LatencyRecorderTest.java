package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class LatencyRecorderTest {

  @Test
  public void percentileNanos_noSamples_returnsZero() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();

    // Act & Assert
    assertThat(recorder.percentileNanos(50)).isZero();
    assertThat(recorder.maxNanos()).isZero();
    assertThat(recorder.meanNanos()).isZero();
    assertThat(recorder.count()).isZero();
  }

  @Test
  public void percentileNanos_zeroPercentileGiven_throwsException() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();

    // Act & Assert
    assertThatThrownBy(() -> recorder.percentileNanos(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void percentileNanos_over100Given_throwsException() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();

    // Act & Assert
    assertThatThrownBy(() -> recorder.percentileNanos(100.1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void record_singleValue_percentileWithinBucketError() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();
    long nanos = 1_000_000; // 1ms

    // Act
    recorder.record(nanos);

    // Assert: the bucket upper bound is within ~7% above the true value.
    assertThat(recorder.percentileNanos(50)).isBetween(nanos, (long) (nanos * 1.07));
    assertThat(recorder.maxNanos()).isEqualTo(nanos);
    assertThat(recorder.meanNanos()).isEqualTo(nanos);
    assertThat(recorder.count()).isEqualTo(1);
  }

  @Test
  public void record_negativeValueGiven_clampsToZero() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();

    // Act
    recorder.record(-5);

    // Assert
    assertThat(recorder.count()).isEqualTo(1);
    assertThat(recorder.maxNanos()).isZero();
    assertThat(recorder.percentileNanos(50)).isZero();
  }

  @Test
  public void percentileNanos_manyValues_percentilesAreOrdered() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();
    for (int i = 1; i <= 1000; i++) {
      recorder.record(i * 100_000L); // 0.1ms .. 100ms
    }

    // Act
    long p50 = recorder.percentileNanos(50);
    long p90 = recorder.percentileNanos(90);
    long p99 = recorder.percentileNanos(99);

    // Assert
    assertThat(p50).isLessThanOrEqualTo(p90);
    assertThat(p90).isLessThanOrEqualTo(p99);
    assertThat(p99).isLessThanOrEqualTo(recorder.maxNanos());
    assertThat(p50).isBetween(50_000_000L / 2, (long) (50_000_000 * 1.07));
  }

  @Test
  public void indexOf_valuesAcrossRange_upperBoundCoversValueWithinError() {
    // Arrange: exact below 16 micros, within 1/16 above it.
    List<Long> values = new ArrayList<>();
    for (long v = 0; v < 16; v++) {
      values.add(v);
    }
    for (int exp = 4; exp < 62; exp++) {
      values.add(1L << exp);
      values.add((1L << exp) + (1L << Math.max(0, exp - 2)));
      values.add((1L << (exp + 1)) - 1);
    }

    for (long v : values) {
      // Act
      long upper = LatencyRecorder.upperBoundMicros(LatencyRecorder.indexOf(v));

      // Assert
      assertThat(upper).as("value %d", v).isGreaterThanOrEqualTo(v);
      assertThat(upper).as("value %d", v).isLessThanOrEqualTo(v + Math.max(1, v / 16));
    }
  }

  @Test
  public void record_concurrentWriters_countsAllSamples() throws Exception {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();
    int threads = 8;
    int perThread = 1000;
    CountDownLatch done = new CountDownLatch(threads);

    // Act
    for (int t = 0; t < threads; t++) {
      new Thread(
              () -> {
                for (int i = 0; i < perThread; i++) {
                  recorder.record(1_000_000);
                }
                done.countDown();
              })
          .start();
    }
    done.await();

    // Assert
    assertThat(recorder.count()).isEqualTo((long) threads * perThread);
  }

  @Test
  public void summary_valuesRecorded_populatesAllFields() {
    // Arrange
    LatencyRecorder recorder = new LatencyRecorder();
    recorder.record(1_000_000);
    recorder.record(2_000_000);

    // Act
    LatencySummary summary = recorder.summary();

    // Assert
    assertThat(summary.count()).isEqualTo(2);
    assertThat(summary.maxNanos()).isEqualTo(2_000_000);
    assertThat(summary.meanNanos()).isEqualTo(1_500_000);
    assertThat(summary.p50Nanos()).isGreaterThan(0);
    assertThat(summary.p99Nanos()).isGreaterThanOrEqualTo(summary.p50Nanos());
  }
}
