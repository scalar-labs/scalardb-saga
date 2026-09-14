package com.scalar.db.saga.benchmark;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Aggregated outcome of one benchmark run. All counters are final values taken after the run and
 * drain phases finished.
 *
 * @param mode human-readable description of the orchestrator under test
 * @param startMode how operations started sagas
 * @param concurrency the worker count
 * @param runMillis wall time of the load phase
 * @param drainMillis wall time of the drain phase
 * @param issuedOps operations attempted
 * @param startedOps operations that obtained a saga id
 * @param startFailedOps operations whose start call threw
 * @param terminalFromOps sagas observed terminal by their own operation
 * @param timedOutOps poll operations that hit the per-operation timeout (saga still running)
 * @param drainedTerminal sagas resolved to terminal during the drain phase
 * @param stillPending sagas still non-terminal when the drain deadline expired
 * @param workersStuckAtAbort workers still blocked in an operation when the run was aborted
 * @param abortedForStall whether the run was cut short by the zero-progress watchdog
 * @param statuses terminal statuses observed, by name
 * @param errors exceptions observed, by simple class name
 * @param acceptLatency latency of the start call itself
 * @param endToEndLatency latency from start to observed terminal state
 */
public record BenchmarkReport(
    String mode,
    StartMode startMode,
    int concurrency,
    long runMillis,
    long drainMillis,
    long issuedOps,
    long startedOps,
    long startFailedOps,
    long terminalFromOps,
    long timedOutOps,
    long drainedTerminal,
    long stillPending,
    long workersStuckAtAbort,
    boolean abortedForStall,
    Map<String, Long> statuses,
    Map<String, Long> errors,
    LatencySummary acceptLatency,
    LatencySummary endToEndLatency) {

  public BenchmarkReport {
    statuses = Map.copyOf(statuses);
    errors = Map.copyOf(errors);
  }

  /** All sagas that reached a terminal state, whether observed by their operation or the drain. */
  public long terminalTotal() {
    return terminalFromOps + drainedTerminal;
  }

  /** Multi-line human-readable rendering. */
  public String format() {
    StringBuilder sb = new StringBuilder();
    sb.append("=== ScalarDB Saga benchmark ===\n");
    sb.append("mode:         ").append(mode).append('\n');
    sb.append(
        String.format(Locale.ROOT, "workload:     %s, concurrency %d%n", startMode, concurrency));
    sb.append(
        String.format(
            Locale.ROOT,
            "operations:   issued=%d started=%d startFailed=%d timedOut=%d%n",
            issuedOps,
            startedOps,
            startFailedOps,
            timedOutOps));
    sb.append(
        String.format(
            Locale.ROOT,
            "outcomes:     terminal=%d (by op=%d, drained=%d) stillPending=%d%n",
            terminalTotal(),
            terminalFromOps,
            drainedTerminal,
            stillPending));
    long totalMillis = runMillis + drainMillis;
    sb.append(
        String.format(
            Locale.ROOT,
            "wall time:    run=%.1fs drain=%.1fs (%.1f started/s, %.1f terminal/s)%n",
            runMillis / 1000.0,
            drainMillis / 1000.0,
            rate(startedOps, runMillis),
            rate(terminalTotal(), totalMillis)));
    sb.append("statuses:     ").append(formatCounts(statuses)).append('\n');
    sb.append("errors:       ").append(formatCounts(errors)).append('\n');
    sb.append("latency (start call):  ").append(acceptLatency.format()).append('\n');
    sb.append("latency (end-to-end):  ").append(endToEndLatency.format()).append('\n');
    if (abortedForStall) {
      sb.append(
          String.format(
              Locale.ROOT,
              "!! RUN ABORTED: no operation made progress within the stall window; %d worker(s)"
                  + " were still blocked in a call%n",
              workersStuckAtAbort));
    }
    return sb.toString();
  }

  private static double rate(long count, long millis) {
    return millis <= 0 ? 0.0 : count * 1000.0 / millis;
  }

  private static String formatCounts(Map<String, Long> counts) {
    if (counts.isEmpty()) {
      return "none";
    }
    StringBuilder sb = new StringBuilder();
    // TreeMap for a stable, alphabetical rendering.
    for (Map.Entry<String, Long> e : new TreeMap<>(counts).entrySet()) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(e.getKey()).append('=').append(e.getValue());
    }
    return sb.toString();
  }
}
