package com.scalar.db.saga.benchmark;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import org.jspecify.annotations.Nullable;
import picocli.CommandLine;

/**
 * The benchmark entry point. Wires one {@link BenchHarness} per {@code --mode} and hands its {@link
 * com.scalar.db.saga.api.SagaOrchestrator} to the {@link BenchmarkRunner} — the workload code is
 * identical across modes, so a difference in behavior is a difference in the implementation under
 * test, not in the benchmark.
 *
 * <p>Exit codes: {@code 0} on a completed run, {@code 3} when the zero-progress watchdog aborted
 * the run (the hang under investigation), {@code 1} on a setup error, {@code 2} on a usage error.
 */
@CommandLine.Command(
    name = "scalardb-saga-benchmark",
    description = "Load-tests ScalarDB Saga through the SagaOrchestrator interface.",
    mixinStandardHelpOptions = true,
    sortOptions = false)
public final class BenchmarkCli implements Callable<Integer> {

  /** Which orchestrator implementation the run drives. */
  enum Mode {
    /** {@code DefaultSagaOrchestrator} in this process (class steps, no network). */
    EMBEDDED,
    /** A real {@code SagaServer} booted in-process, driven over loopback gRPC. */
    SERVER,
    /** An external daemon at {@code --target}, driven over gRPC. */
    GRPC
  }

  @CommandLine.Option(
      names = {"-m", "--mode"},
      description = "Implementation under test: ${COMPLETION-CANDIDATES}.")
  private @Nullable Mode mode;

  @CommandLine.Option(
      names = "--target",
      paramLabel = "HOST:PORT",
      description = "The external daemon's gRPC address (GRPC mode).")
  private @Nullable String target;

  @CommandLine.Option(names = "--tls", description = "Use TLS for the GRPC mode channel.")
  private boolean tls;

  @CommandLine.Option(
      names = "--properties",
      paramLabel = "FILE",
      description =
          "ScalarDB store properties for the EMBEDDED and SERVER modes."
              + " Default: a throwaway SQLite database.")
  private @Nullable Path propertiesFile;

  @CommandLine.Option(
      names = {"-D", "--property"},
      paramLabel = "KEY=VALUE",
      description =
          "Extra properties layered on top (store keys, and in SERVER mode the"
              + " scalar.db.saga.server.* keys). Repeatable.")
  private Map<String, String> overrides = new LinkedHashMap<>();

  @CommandLine.Option(
      names = "--saga-name",
      description = "Definition name to register and start (default: ${DEFAULT-VALUE}).")
  private String sagaName = "bench";

  @CommandLine.Option(
      names = "--steps",
      description = "Sequential steps per saga (default: ${DEFAULT-VALUE}).")
  private int steps = 3;

  @CommandLine.Option(
      names = "--step-delay-ms",
      description = "Per-step participant latency to emulate, in ms (default: ${DEFAULT-VALUE}).")
  private long stepDelayMillis = 0;

  @CommandLine.Option(
      names = "--start-mode",
      converter = StartModeConverter.class,
      description = "sync, async-poll, or async-fire (default: sync).")
  private StartMode startMode = StartMode.SYNC;

  @CommandLine.Option(
      names = {"-c", "--concurrency"},
      description = "Concurrent workers (default: ${DEFAULT-VALUE}).")
  private int concurrency = 32;

  @CommandLine.Option(
      names = {"-n", "--requests"},
      description = "Total operations when no duration is set (default: ${DEFAULT-VALUE}).")
  private long requests = 1000;

  @CommandLine.Option(
      names = "--duration-seconds",
      description = "Run for this long instead of counting requests (default: off).")
  private long durationSeconds = 0;

  @CommandLine.Option(
      names = "--target-tps",
      description =
          "Global target issue rate in ops/sec across all workers (open-loop pacing, e.g. a"
              + " 12-thread 10-TPS-per-thread profile is -c 12 --target-tps 120); 0 issues as fast"
              + " as workers complete (default: ${DEFAULT-VALUE}).")
  private double targetTps = 0;

  @CommandLine.Option(
      names = "--poll-interval-ms",
      description = "State-poll interval for async-poll and drain (default: ${DEFAULT-VALUE}).")
  private long pollIntervalMillis = 200;

  @CommandLine.Option(
      names = "--op-timeout-ms",
      description =
          "Per-operation bound: async-poll stops polling after this, and the gRPC modes apply it"
              + " as the per-call deadline (default: none).")
  private long opTimeoutMillis = 0;

  @CommandLine.Option(
      names = "--drain-seconds",
      description =
          "How long to keep polling unresolved sagas after the load phase"
              + " (default: ${DEFAULT-VALUE}).")
  private long drainSeconds = 60;

  @CommandLine.Option(
      names = "--report-interval-seconds",
      description = "Progress-line interval; 0 disables (default: ${DEFAULT-VALUE}).")
  private long reportIntervalSeconds = 5;

  @CommandLine.Option(
      names = "--stall-abort-seconds",
      description =
          "Abort when no operation finishes for this long while some are in flight; 0 disables"
              + " (default: ${DEFAULT-VALUE}).")
  private long stallAbortSeconds = 180;

  @CommandLine.Option(
      names = "--recovery-timeout-ms",
      description =
          "Recovery staleness threshold override for EMBEDDED and SERVER modes; 0 keeps the"
              + " default (60000). Shrinking it reproduces recovery-vs-live-saga contention"
              + " quickly (default: ${DEFAULT-VALUE}).")
  private long recoveryTimeoutMillis = 0;

  @CommandLine.Option(
      names = "--recovery-interval-seconds",
      description =
          "Recovery scan interval override for EMBEDDED and SERVER modes; 0 keeps the default"
              + " (30) (default: ${DEFAULT-VALUE}).")
  private long recoveryIntervalSeconds = 0;

  @CommandLine.Option(
      names = "--print-definition",
      description =
          "Print the benchmark saga definition JSON (for registering on an external daemon)"
              + " and exit.")
  private boolean printDefinition;

  @Override
  public Integer call() throws InterruptedException {
    if (printDefinition) {
      System.out.println(BenchmarkDefinitions.serviceDefinitionJson(sagaName, steps));
      return 0;
    }
    if (mode == null) {
      System.err.println("Missing required option: --mode (or use --print-definition)");
      return CommandLine.ExitCode.USAGE;
    }
    if (mode == Mode.GRPC && target == null) {
      System.err.println("GRPC mode requires --target HOST:PORT");
      return CommandLine.ExitCode.USAGE;
    }

    BenchmarkRunner.Config config =
        new BenchmarkRunner.Config(
            startMode,
            concurrency,
            requests,
            durationSeconds * 1000,
            targetTps,
            pollIntervalMillis,
            opTimeoutMillis,
            drainSeconds * 1000,
            reportIntervalSeconds * 1000,
            stallAbortSeconds * 1000,
            sagaName,
            Map.of("amount", 100));

    try (BenchHarness harness = createHarness()) {
      System.out.println("benchmarking " + harness.description());
      BenchmarkRunner runner = new BenchmarkRunner(config, harness.description());
      BenchmarkReport report = runner.run(harness.orchestrator());
      System.out.println(report.format());
      long duplicates = harness.duplicateStepExecutions();
      if (duplicates >= 0) {
        System.out.println(
            "duplicate step executions: "
                + duplicates
                + (duplicates > 0
                    ? "  <-- sagas were re-driven while (or after) already executing"
                    : ""));
      }
      return report.abortedForStall() ? 3 : 0;
    }
  }

  private BenchHarness createHarness() {
    return switch (java.util.Objects.requireNonNull(mode)) {
      case EMBEDDED ->
          EmbeddedHarness.create(
              propertiesFile,
              overrides,
              sagaName,
              steps,
              stepDelayMillis,
              recoveryTimeoutMillis,
              recoveryIntervalSeconds);
      case SERVER ->
          ServerHarness.create(
              propertiesFile, serverOverrides(), sagaName, steps, stepDelayMillis, opTimeoutMillis);
      case GRPC ->
          GrpcHarness.create(java.util.Objects.requireNonNull(target), tls, opTimeoutMillis);
    };
  }

  /** The -D overrides plus the recovery flags mapped to the daemon's property keys. */
  private Map<String, String> serverOverrides() {
    Map<String, String> merged = new LinkedHashMap<>();
    if (recoveryTimeoutMillis > 0) {
      merged.put(
          "scalar.db.saga.server.recovery.timeout_millis", Long.toString(recoveryTimeoutMillis));
    }
    if (recoveryIntervalSeconds > 0) {
      merged.put(
          "scalar.db.saga.server.recovery.interval_seconds",
          Long.toString(recoveryIntervalSeconds));
    }
    // Explicit -D keys win over the mapped flags.
    merged.putAll(overrides);
    return merged;
  }

  /** Accepts {@code sync}, {@code async-poll}, {@code ASYNC_FIRE}, etc. */
  static final class StartModeConverter implements CommandLine.ITypeConverter<StartMode> {
    @Override
    public StartMode convert(String value) {
      return StartMode.valueOf(value.trim().replace('-', '_').toUpperCase(Locale.ROOT));
    }
  }

  public static void main(String[] args) {
    System.exit(
        new CommandLine(new BenchmarkCli())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .execute(args));
  }
}
