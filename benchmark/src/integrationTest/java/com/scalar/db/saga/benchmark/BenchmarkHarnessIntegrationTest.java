package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * End-to-end sanity of the two self-contained modes against a throwaway SQLite store: a small
 * synchronous run must complete every saga, with no duplicates and nothing left pending. This is
 * deliberately light load — it validates the tool, not the engine's limits.
 */
class BenchmarkHarnessIntegrationTest {

  private static final int REQUESTS = 12;

  private static BenchmarkRunner.Config smallSyncRun() {
    return new BenchmarkRunner.Config(
        StartMode.SYNC,
        4,
        REQUESTS,
        0,
        0,
        50,
        0,
        30_000,
        0,
        120_000,
        "bench-it",
        Map.of("amount", 100));
  }

  @Test
  public void embeddedHarness_smallSyncRun_completesAllSagas() throws Exception {
    try (BenchHarness harness = EmbeddedHarness.create(null, Map.of(), "bench-it", 2, 0, 0, 0)) {
      // Act
      BenchmarkReport report =
          new BenchmarkRunner(smallSyncRun(), harness.description(), line -> {})
              .run(harness.orchestrator());

      // Assert
      assertThat(report.terminalTotal()).isEqualTo(REQUESTS);
      assertThat(report.statuses()).containsEntry("COMPLETED", (long) REQUESTS);
      assertThat(report.stillPending()).isZero();
      assertThat(report.abortedForStall()).isFalse();
      assertThat(harness.duplicateStepExecutions()).isZero();
    }
  }

  @Test
  public void serverHarness_smallSyncRun_completesAllSagas() throws Exception {
    try (BenchHarness harness = ServerHarness.create(null, Map.of(), "bench-it", 2, 0, 0)) {
      // Act
      BenchmarkReport report =
          new BenchmarkRunner(smallSyncRun(), harness.description(), line -> {})
              .run(harness.orchestrator());

      // Assert
      assertThat(report.terminalTotal()).isEqualTo(REQUESTS);
      assertThat(report.statuses()).containsEntry("COMPLETED", (long) REQUESTS);
      assertThat(report.stillPending()).isZero();
      assertThat(report.abortedForStall()).isFalse();
      assertThat(harness.duplicateStepExecutions()).isZero();
    }
  }
}
