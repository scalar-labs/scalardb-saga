package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkReportTest {

  private static BenchmarkReport report(boolean abortedForStall, Map<String, Long> errors) {
    return new BenchmarkReport(
        "embedded (test)",
        StartMode.SYNC,
        8,
        10_000,
        2_000,
        100,
        98,
        2,
        90,
        1,
        5,
        3,
        abortedForStall ? 4 : 0,
        abortedForStall,
        Map.of("COMPLETED", 90L, "COMPENSATED", 5L),
        errors,
        LatencySummary.empty(),
        LatencySummary.empty());
  }

  @Test
  public void terminalTotal_sumsOpAndDrainedTerminals() {
    // Arrange
    BenchmarkReport report = report(false, Map.of());

    // Act & Assert
    assertThat(report.terminalTotal()).isEqualTo(95);
  }

  @Test
  public void format_typicalRun_containsAllCounters() {
    // Arrange
    BenchmarkReport report = report(false, Map.of("SagaConcurrentModificationException", 7L));

    // Act
    String formatted = report.format();

    // Assert
    assertThat(formatted)
        .contains("embedded (test)")
        .contains("SYNC")
        .contains("issued=100")
        .contains("started=98")
        .contains("startFailed=2")
        .contains("timedOut=1")
        .contains("terminal=95")
        .contains("stillPending=3")
        .contains("COMPLETED=90")
        .contains("COMPENSATED=5")
        .contains("SagaConcurrentModificationException=7");
    assertThat(formatted).doesNotContain("RUN ABORTED");
  }

  @Test
  public void format_noErrors_saysNone() {
    // Arrange
    BenchmarkReport report = report(false, Map.of());

    // Act & Assert
    assertThat(report.format()).contains("errors:       none");
  }

  @Test
  public void format_abortedForStall_includesWarning() {
    // Arrange
    BenchmarkReport report = report(true, Map.of());

    // Act & Assert
    assertThat(report.format()).contains("RUN ABORTED").contains("4 worker(s)");
  }

  @Test
  public void constructor_mapsGiven_copiesDefensively() {
    // Arrange
    Map<String, Long> errors = new HashMap<>();
    errors.put("A", 1L);
    BenchmarkReport report = report(false, errors);

    // Act
    errors.put("B", 2L);

    // Assert
    assertThat(report.errors()).containsOnlyKeys("A");
  }
}
