package com.scalar.db.saga.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class BenchmarkRunnerTest {

  private static BenchmarkRunner.Config config(
      StartMode startMode, int concurrency, long totalRequests) {
    return config(startMode, concurrency, totalRequests, 0, 0, 2_000, 0);
  }

  private static BenchmarkRunner.Config config(
      StartMode startMode,
      int concurrency,
      long totalRequests,
      long durationMillis,
      long opTimeoutMillis,
      long drainMillis,
      long stallAbortMillis) {
    return new BenchmarkRunner.Config(
        startMode,
        concurrency,
        totalRequests,
        durationMillis,
        0,
        10,
        opTimeoutMillis,
        drainMillis,
        0,
        stallAbortMillis,
        "bench",
        Map.of("amount", 100));
  }

  private static BenchmarkRunner runner(BenchmarkRunner.Config config) {
    return new BenchmarkRunner(config, "fake", line -> {});
  }

  @Test
  public void run_syncModeAllComplete_reportsAllTerminal() throws Exception {
    // Arrange
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completing();

    // Act
    BenchmarkReport report = runner(config(StartMode.SYNC, 4, 20)).run(orchestrator);

    // Assert
    assertThat(report.issuedOps()).isEqualTo(20);
    assertThat(report.startedOps()).isEqualTo(20);
    assertThat(report.startFailedOps()).isZero();
    assertThat(report.terminalFromOps()).isEqualTo(20);
    assertThat(report.stillPending()).isZero();
    assertThat(report.abortedForStall()).isFalse();
    assertThat(report.statuses()).containsEntry("COMPLETED", 20L);
    assertThat(report.errors()).isEmpty();
    assertThat(report.acceptLatency().count()).isEqualTo(20);
    assertThat(report.endToEndLatency().count()).isEqualTo(20);
  }

  @Test
  public void run_syncModeStartThrows_countsStartFailuresByClass() throws Exception {
    // Arrange: every 2nd start throws.
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.failingEveryNth(2);

    // Act
    BenchmarkReport report = runner(config(StartMode.SYNC, 1, 10)).run(orchestrator);

    // Assert
    assertThat(report.startFailedOps()).isEqualTo(5);
    assertThat(report.startedOps()).isEqualTo(5);
    assertThat(report.terminalFromOps()).isEqualTo(5);
    assertThat(report.errors()).containsEntry("IllegalStateException", 5L);
  }

  @Test
  public void run_syncModeNonTerminalSnapshot_resolvedInDrainPhase() throws Exception {
    // Arrange: the op's own classification read sees RUNNING; the drain's read sees COMPLETED.
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completingAfterPolls(1);

    // Act
    BenchmarkReport report = runner(config(StartMode.SYNC, 1, 1)).run(orchestrator);

    // Assert
    assertThat(report.terminalFromOps()).isZero();
    assertThat(report.drainedTerminal()).isEqualTo(1);
    assertThat(report.stillPending()).isZero();
    assertThat(report.statuses()).containsEntry("COMPLETED", 1L);
  }

  @Test
  public void run_asyncPollMode_completesAfterPolling() throws Exception {
    // Arrange
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completingAfterPolls(2);

    // Act
    BenchmarkReport report = runner(config(StartMode.ASYNC_POLL, 2, 6)).run(orchestrator);

    // Assert
    assertThat(report.terminalFromOps()).isEqualTo(6);
    assertThat(report.timedOutOps()).isZero();
    assertThat(report.statuses()).containsEntry("COMPLETED", 6L);
  }

  @Test
  public void run_asyncPollModeTimeout_countsTimedOutAndLeavesPending() throws Exception {
    // Arrange: sagas stay RUNNING far longer than the 50ms op timeout and the 200ms drain.
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completingAfterPolls(1_000_000);

    // Act
    BenchmarkReport report =
        runner(config(StartMode.ASYNC_POLL, 1, 1, 0, 50, 200, 0)).run(orchestrator);

    // Assert
    assertThat(report.timedOutOps()).isEqualTo(1);
    assertThat(report.terminalFromOps()).isZero();
    assertThat(report.drainedTerminal()).isZero();
    assertThat(report.stillPending()).isEqualTo(1);
  }

  @Test
  public void run_asyncFireMode_drainResolvesAllSagas() throws Exception {
    // Arrange
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completing();

    // Act
    BenchmarkReport report = runner(config(StartMode.ASYNC_FIRE, 4, 10)).run(orchestrator);

    // Assert
    assertThat(report.startedOps()).isEqualTo(10);
    assertThat(report.terminalFromOps()).isZero();
    assertThat(report.drainedTerminal()).isEqualTo(10);
    assertThat(report.stillPending()).isZero();
    assertThat(report.statuses()).containsEntry("COMPLETED", 10L);
  }

  @Test
  public void run_startsHangForever_watchdogAbortsAndReportsStuckWorkers() throws Exception {
    // Arrange: starts block until interrupted; the watchdog must cut the run loose.
    CountDownLatch never = new CountDownLatch(1);
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.blockingOn(never);

    // Act
    BenchmarkReport report =
        runner(config(StartMode.SYNC, 2, 2, 0, 0, 1_000, 300)).run(orchestrator);

    // Assert
    assertThat(report.abortedForStall()).isTrue();
    assertThat(report.workersStuckAtAbort()).isEqualTo(2);
    assertThat(report.terminalFromOps()).isZero();
  }

  @Test
  public void run_durationMode_stopsIssuingAtDeadline() throws Exception {
    // Arrange
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completing();

    // Act
    BenchmarkReport report =
        runner(config(StartMode.SYNC, 2, 0, 300, 0, 1_000, 0)).run(orchestrator);

    // Assert
    assertThat(report.issuedOps()).isGreaterThan(0);
    assertThat(report.runMillis()).isGreaterThanOrEqualTo(300);
    assertThat(report.stillPending()).isZero();
  }

  @Test
  public void config_zeroConcurrencyGiven_throwsException() {
    assertThatThrownBy(() -> config(StartMode.SYNC, 0, 10))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void config_zeroRequestsWithoutDuration_throwsException() {
    assertThatThrownBy(() -> config(StartMode.SYNC, 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void run_withTargetTps_pacesIssuanceToTheSchedule() throws Exception {
    // Arrange: 10 ops at 25 ops/s puts the last slot at 360ms; a closed loop would finish almost
    // instantly against this fake.
    FakeSagaOrchestrator orchestrator = FakeSagaOrchestrator.completing();
    BenchmarkRunner.Config config =
        new BenchmarkRunner.Config(
            StartMode.SYNC, 4, 10, 0, 25.0, 10, 0, 1_000, 0, 0, "bench", Map.of());

    // Act
    BenchmarkReport report = runner(config).run(orchestrator);

    // Assert
    assertThat(report.issuedOps()).isEqualTo(10);
    assertThat(report.terminalFromOps()).isEqualTo(10);
    assertThat(report.runMillis()).isGreaterThanOrEqualTo(300);
  }

  @Test
  public void config_zeroPollInterval_throwsException() {
    assertThatThrownBy(
            () ->
                new BenchmarkRunner.Config(
                    StartMode.SYNC, 1, 1, 0, 0, 0, 0, 0, 0, 0, "bench", Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void config_blankSagaName_throwsException() {
    assertThatThrownBy(
            () ->
                new BenchmarkRunner.Config(
                    StartMode.SYNC, 1, 1, 0, 0, 10, 0, 0, 0, 0, " ", Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void config_negativeDuration_throwsException() {
    assertThatThrownBy(
            () ->
                new BenchmarkRunner.Config(
                    StartMode.SYNC, 1, 1, -1, 0, 10, 0, 0, 0, 0, "bench", Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void config_negativeTargetTps_throwsException() {
    assertThatThrownBy(
            () ->
                new BenchmarkRunner.Config(
                    StartMode.SYNC, 1, 1, 0, -1.0, 10, 0, 0, 0, 0, "bench", Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
