package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import net.jcip.annotations.ThreadSafe;

/**
 * Drives a {@link SagaOrchestrator} — embedded or remote, the runner cannot tell — with a fixed
 * number of concurrent workers and reports what came back.
 *
 * <p>Workers are <b>platform</b> threads, deliberately: the embedded engine runs its sagas on
 * virtual threads, and a load generator sharing the common virtual-thread scheduler would freeze
 * together with the system under test instead of measuring the freeze.
 *
 * <p>Three phases: the <b>load</b> phase issues operations until the request count or duration is
 * reached; a zero-progress watchdog aborts the run (interrupting stuck workers) when no operation
 * finishes for {@link Config#stallAbortMillis()} — the run then reports how many workers were still
 * blocked, which is the hang made measurable. The <b>drain</b> phase polls every saga that was
 * started but not yet observed terminal, so fire-and-forget work and timed-out polls still resolve
 * to an outcome; whatever remains non-terminal is reported as {@code stillPending}.
 */
@ThreadSafe
public final class BenchmarkRunner {

  /** After this many consecutive poll failures an operation parks its saga for the drain phase. */
  private static final int MAX_CONSECUTIVE_POLL_ERRORS = 10;

  private static final long AWAIT_SLICE_MILLIS = 500;

  /**
   * Workload configuration.
   *
   * @param startMode how each operation starts its saga and observes the outcome
   * @param concurrency the number of worker threads
   * @param totalRequests operations to issue when {@code durationMillis} is {@code 0}
   * @param durationMillis run length; {@code > 0} makes workers issue until the deadline instead of
   *     counting requests
   * @param targetTps global issue rate across all workers, for open-loop load ({@code 0} =
   *     closed-loop: issue as fast as workers complete). With {@link StartMode#ASYNC_FIRE} this
   *     reproduces a fixed arrival rate regardless of how slow the system gets — the profile that
   *     lets a backlog of in-flight sagas build
   * @param pollIntervalMillis how often poll and drain loops re-read a saga's state
   * @param opTimeoutMillis per-operation bound for {@link StartMode#ASYNC_POLL}; {@code 0} polls
   *     until terminal
   * @param drainMillis how long the drain phase keeps polling unresolved sagas; {@code 0} skips it
   * @param reportIntervalMillis progress-line interval; {@code 0} disables progress output
   * @param stallAbortMillis abort the run after this long without any operation finishing while
   *     operations are in flight; {@code 0} disables the watchdog
   * @param sagaName the registered definition to start
   * @param input the saga input passed to every start
   */
  public record Config(
      StartMode startMode,
      int concurrency,
      long totalRequests,
      long durationMillis,
      double targetTps,
      long pollIntervalMillis,
      long opTimeoutMillis,
      long drainMillis,
      long reportIntervalMillis,
      long stallAbortMillis,
      String sagaName,
      Map<String, Object> input) {

    public Config {
      if (concurrency < 1) {
        throw new IllegalArgumentException("concurrency must be >= 1, got " + concurrency);
      }
      if (durationMillis < 0) {
        throw new IllegalArgumentException("durationMillis must be >= 0, got " + durationMillis);
      }
      if (durationMillis == 0 && totalRequests < 1) {
        throw new IllegalArgumentException(
            "totalRequests must be >= 1 when no duration is set, got " + totalRequests);
      }
      if (targetTps < 0 || !Double.isFinite(targetTps)) {
        throw new IllegalArgumentException("targetTps must be >= 0 and finite, got " + targetTps);
      }
      if (pollIntervalMillis < 1) {
        throw new IllegalArgumentException(
            "pollIntervalMillis must be >= 1, got " + pollIntervalMillis);
      }
      if (opTimeoutMillis < 0) {
        throw new IllegalArgumentException("opTimeoutMillis must be >= 0, got " + opTimeoutMillis);
      }
      if (drainMillis < 0) {
        throw new IllegalArgumentException("drainMillis must be >= 0, got " + drainMillis);
      }
      if (reportIntervalMillis < 0) {
        throw new IllegalArgumentException(
            "reportIntervalMillis must be >= 0, got " + reportIntervalMillis);
      }
      if (stallAbortMillis < 0) {
        throw new IllegalArgumentException(
            "stallAbortMillis must be >= 0, got " + stallAbortMillis);
      }
      if (sagaName.isBlank()) {
        throw new IllegalArgumentException("sagaName must not be blank");
      }
      input = Map.copyOf(input);
    }
  }

  private final Config config;
  private final String modeDescription;
  private final Consumer<String> progressSink;

  public BenchmarkRunner(Config config, String modeDescription) {
    this(config, modeDescription, System.out::println);
  }

  // Visible for testing: capture progress lines instead of printing them.
  BenchmarkRunner(Config config, String modeDescription, Consumer<String> progressSink) {
    this.config = config;
    this.modeDescription = modeDescription;
    this.progressSink = progressSink;
  }

  /**
   * Runs the workload against {@code orchestrator} and returns the aggregated report. The
   * orchestrator's lifecycle belongs to the caller.
   */
  public BenchmarkReport run(SagaOrchestrator orchestrator) throws InterruptedException {
    State state = new State();
    long runStartNanos = System.nanoTime();
    long issueDeadlineNanos =
        config.durationMillis() > 0
            ? runStartNanos + TimeUnit.MILLISECONDS.toNanos(config.durationMillis())
            : Long.MAX_VALUE;

    ExecutorService workers =
        Executors.newFixedThreadPool(config.concurrency(), workerThreadFactory());
    ProgressMonitor monitor = null;
    if (config.reportIntervalMillis() > 0) {
      monitor =
          new ProgressMonitor(
              () -> progressSnapshot(state), config.reportIntervalMillis(), progressSink);
      monitor.start();
    }
    boolean abortedForStall = false;
    long workersStuck = 0;
    try {
      for (int i = 0; i < config.concurrency(); i++) {
        workers.execute(() -> workerLoop(orchestrator, state, runStartNanos, issueDeadlineNanos));
      }
      workers.shutdown();
      long lastFinished = -1;
      long lastChangeNanos = System.nanoTime();
      while (!workers.awaitTermination(AWAIT_SLICE_MILLIS, TimeUnit.MILLISECONDS)) {
        long finished = state.finishedOps.get();
        long now = System.nanoTime();
        if (finished != lastFinished) {
          lastFinished = finished;
          lastChangeNanos = now;
          continue;
        }
        boolean watchdogArmed = config.stallAbortMillis() > 0 && !state.inFlight.isEmpty();
        if (watchdogArmed
            && now - lastChangeNanos >= TimeUnit.MILLISECONDS.toNanos(config.stallAbortMillis())) {
          abortedForStall = true;
          workersStuck = state.inFlight.size();
          workers.shutdownNow();
          if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
            // Workers are blocked in a call that ignores interruption; report and move on.
            workersStuck = state.inFlight.size();
          }
          break;
        }
      }
    } finally {
      if (monitor != null) {
        monitor.close();
      }
    }
    long runMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - runStartNanos);

    long drainStartNanos = System.nanoTime();
    if (!abortedForStall && config.drainMillis() > 0) {
      // After a stall abort the store is presumed wedged; polling it would only hang the report.
      drain(orchestrator, state);
    }
    long drainMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - drainStartNanos);

    return new BenchmarkReport(
        modeDescription,
        config.startMode(),
        config.concurrency(),
        runMillis,
        drainMillis,
        state.issued.get(),
        state.started.get(),
        state.startFailed.get(),
        state.terminalFromOps.get(),
        state.timedOut.get(),
        state.drained.get(),
        state.pendingSagas.size(),
        workersStuck,
        abortedForStall,
        sum(state.statuses),
        sum(state.errors),
        state.accept.summary(),
        state.endToEnd.summary());
  }

  private static ThreadFactory workerThreadFactory() {
    AtomicLong index = new AtomicLong();
    return r -> {
      Thread t = new Thread(r, "saga-bench-worker-" + index.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }

  private void workerLoop(
      SagaOrchestrator orchestrator, State state, long runStartNanos, long issueDeadlineNanos) {
    while (!Thread.currentThread().isInterrupted() && System.nanoTime() < issueDeadlineNanos) {
      long ticket = state.issued.incrementAndGet();
      if (config.durationMillis() == 0 && ticket > config.totalRequests()) {
        state.issued.decrementAndGet();
        return;
      }
      if (!paceToSlot(runStartNanos, ticket, issueDeadlineNanos)) {
        state.issued.decrementAndGet();
        return;
      }
      long opId = state.opSeq.incrementAndGet();
      long opStartNanos = System.nanoTime();
      state.inFlight.put(opId, opStartNanos);
      try {
        runOne(orchestrator, state, opStartNanos);
      } finally {
        state.inFlight.remove(opId);
        state.finishedOps.incrementAndGet();
      }
    }
  }

  /**
   * Waits until ticket {@code ticket}'s scheduled issue time when a target rate is set. Tickets are
   * global, so the pacing is a shared arrival schedule, not per-worker. Returns {@code false} when
   * the slot lies beyond the run deadline or the wait was interrupted — the op is not run. A
   * schedule running behind (slow ops in a closed loop) issues immediately; it never bursts to
   * catch up faster than workers allow.
   */
  private boolean paceToSlot(long runStartNanos, long ticket, long issueDeadlineNanos) {
    if (config.targetTps() <= 0) {
      return true;
    }
    long nanosPerOp = (long) (1_000_000_000.0 / config.targetTps());
    long issueAtNanos = runStartNanos + (ticket - 1) * nanosPerOp;
    if (issueAtNanos >= issueDeadlineNanos) {
      return false;
    }
    long remainingNanos = issueAtNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      return true;
    }
    try {
      Thread.sleep(remainingNanos / 1_000_000, (int) (remainingNanos % 1_000_000));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  private void runOne(SagaOrchestrator orchestrator, State state, long opStartNanos) {
    String sagaId;
    try {
      sagaId =
          config.startMode() == StartMode.SYNC
              ? orchestrator.start(config.sagaName(), config.input())
              : orchestrator.startAsync(config.sagaName(), config.input());
    } catch (RuntimeException e) {
      state.startFailed.incrementAndGet();
      countError(state, e);
      return;
    }
    state.accept.record(System.nanoTime() - opStartNanos);
    state.started.incrementAndGet();
    switch (config.startMode()) {
      case SYNC -> classifyAfterSync(orchestrator, state, sagaId, opStartNanos);
      case ASYNC_POLL -> pollUntilTerminal(orchestrator, state, sagaId, opStartNanos);
      case ASYNC_FIRE -> state.pendingSagas.put(sagaId, opStartNanos);
    }
  }

  /**
   * A synchronous start has returned; read the saga once to classify the outcome. A non-terminal
   * status here is itself a signal — a bounded-sync server answered early, or the engine's drive
   * died before its terminal transition — so the saga goes to the drain phase instead of being
   * counted terminal.
   */
  private void classifyAfterSync(
      SagaOrchestrator orchestrator, State state, String sagaId, long opStartNanos) {
    try {
      SagaStateSnapshot snapshot = orchestrator.getStateSnapshot(sagaId);
      if (snapshot.getStatus().isTerminal()) {
        recordTerminal(state, snapshot.getStatus().name(), opStartNanos, true);
      } else {
        state.pendingSagas.put(sagaId, opStartNanos);
      }
    } catch (RuntimeException e) {
      countError(state, e);
      state.pendingSagas.put(sagaId, opStartNanos);
    }
  }

  private void pollUntilTerminal(
      SagaOrchestrator orchestrator, State state, String sagaId, long opStartNanos) {
    long deadlineNanos =
        config.opTimeoutMillis() > 0
            ? opStartNanos + TimeUnit.MILLISECONDS.toNanos(config.opTimeoutMillis())
            : Long.MAX_VALUE;
    int consecutiveErrors = 0;
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        state.pendingSagas.put(sagaId, opStartNanos);
        return;
      }
      try {
        SagaStateSnapshot snapshot = orchestrator.getStateSnapshot(sagaId);
        consecutiveErrors = 0;
        if (snapshot.getStatus().isTerminal()) {
          recordTerminal(state, snapshot.getStatus().name(), opStartNanos, true);
          return;
        }
      } catch (RuntimeException e) {
        countError(state, e);
        if (++consecutiveErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
          state.pendingSagas.put(sagaId, opStartNanos);
          return;
        }
      }
      if (System.nanoTime() >= deadlineNanos) {
        state.timedOut.incrementAndGet();
        state.pendingSagas.put(sagaId, opStartNanos);
        return;
      }
      try {
        Thread.sleep(config.pollIntervalMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        state.pendingSagas.put(sagaId, opStartNanos);
        return;
      }
    }
  }

  private void drain(SagaOrchestrator orchestrator, State state) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.drainMillis());
    while (!state.pendingSagas.isEmpty() && System.nanoTime() < deadlineNanos) {
      for (Map.Entry<String, Long> pending : state.pendingSagas.entrySet()) {
        // Also honored inside the round: one sweep over a large backlog against a slow store can
        // take minutes, and the drain bound must hold regardless.
        if (System.nanoTime() >= deadlineNanos) {
          return;
        }
        try {
          SagaStateSnapshot snapshot = orchestrator.getStateSnapshot(pending.getKey());
          if (snapshot.getStatus().isTerminal()) {
            recordTerminal(state, snapshot.getStatus().name(), pending.getValue(), false);
            state.pendingSagas.remove(pending.getKey());
          }
        } catch (RuntimeException e) {
          countError(state, e);
        }
      }
      if (state.pendingSagas.isEmpty()) {
        return;
      }
      Thread.sleep(config.pollIntervalMillis());
    }
  }

  private static void recordTerminal(
      State state, String statusName, long opStartNanos, boolean byOp) {
    state.statuses.computeIfAbsent(statusName, k -> new LongAdder()).increment();
    state.endToEnd.record(System.nanoTime() - opStartNanos);
    if (byOp) {
      state.terminalFromOps.incrementAndGet();
    } else {
      state.drained.incrementAndGet();
    }
  }

  private static void countError(State state, RuntimeException e) {
    state.errors.computeIfAbsent(e.getClass().getSimpleName(), k -> new LongAdder()).increment();
  }

  private static ProgressMonitor.Progress progressSnapshot(State state) {
    long now = System.nanoTime();
    long oldestMillis = 0;
    for (long startNanos : state.inFlight.values()) {
      oldestMillis = Math.max(oldestMillis, TimeUnit.NANOSECONDS.toMillis(now - startNanos));
    }
    return new ProgressMonitor.Progress(
        state.issued.get(),
        state.finishedOps.get(),
        state.inFlight.size(),
        oldestMillis,
        state.pendingSagas.size());
  }

  private static Map<String, Long> sum(Map<String, LongAdder> counters) {
    Map<String, Long> result = new HashMap<>();
    counters.forEach((key, adder) -> result.put(key, adder.sum()));
    return result;
  }

  /** All mutable state of one run, shared by the workers and the reporting thread. */
  private static final class State {
    final LatencyRecorder accept = new LatencyRecorder();
    final LatencyRecorder endToEnd = new LatencyRecorder();
    final ConcurrentHashMap<String, LongAdder> statuses = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, LongAdder> errors = new ConcurrentHashMap<>();
    // Sagas started but not yet observed terminal (fire-and-forget, poll timeouts, early returns):
    // saga id -> the operation's start nanos, resolved by the drain phase.
    final ConcurrentHashMap<String, Long> pendingSagas = new ConcurrentHashMap<>();
    // Operations currently inside a call: op id -> start nanos, for the oldest-in-flight age.
    final ConcurrentHashMap<Long, Long> inFlight = new ConcurrentHashMap<>();
    final AtomicLong issued = new AtomicLong();
    final AtomicLong started = new AtomicLong();
    final AtomicLong startFailed = new AtomicLong();
    final AtomicLong terminalFromOps = new AtomicLong();
    final AtomicLong timedOut = new AtomicLong();
    final AtomicLong drained = new AtomicLong();
    final AtomicLong finishedOps = new AtomicLong();
    final AtomicLong opSeq = new AtomicLong();
  }
}
