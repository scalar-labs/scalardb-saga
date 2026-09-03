package com.scalar.db.saga.engine;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounds how many sagas may be executing at once, so intake cannot outrun completion.
 *
 * <p>Pure mechanism: it hands out permits and counts refusals. What a refusal means on the wire is
 * the orchestrator's to decide, which is why nothing here depends on the {@code api} exception
 * types.
 *
 * <p>The bound is on <b>occupancy</b>, not on flow, and that is the whole reason it exists
 * alongside the daemon's request rate limiter. In-flight population is arrival rate times duration,
 * so a rate limit alone bounds it only while duration holds still: when a downstream slows, the
 * same permitted arrival rate produces an unboundedly larger population, which is what drives saga
 * latency past the recovery staleness threshold and starts recovery claiming sagas that are still
 * running. Duration does not appear in a rate limiter's arithmetic. It appears here.
 *
 * <p>A permit is held per <b>drive</b>, not per saga: it is taken when a start is admitted and
 * returned when that drive stops occupying the engine — at a terminal state, at a park (a saga
 * waiting on an outside system holds nothing, possibly for hours), or when the drive dies. Resumes,
 * recovery and admin drives take no permit at all, so the budget always describes work this process
 * is doing now rather than work it has ever accepted.
 */
@ThreadSafe
final class AdmissionController {

  private static final Logger logger = LoggerFactory.getLogger(AdmissionController.class);

  /**
   * How often a rejection storm is summarized. Matches the daemon's rate-limit window so the two
   * overload signals land on the same cadence; deliberately not configurable, since a knob can be
   * added later without breaking anything and one fewer setting is one fewer thing to size.
   */
  static final long SUMMARY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(60);

  private final int maxConcurrent;

  /**
   * Unfair on purpose, and it changes nothing: {@link Semaphore#tryAcquire()} barges past the queue
   * regardless of the fairness setting (its own javadoc says so), so {@code fair=true} would only
   * cost a queue nobody waits in. Nothing here ever blocks — a full cap is an immediate refusal.
   *
   * <p>That refusal is also read-only: a failed {@code tryAcquire} is a volatile load and no more,
   * so a rejection storm puts zero write traffic on the cache line the release path owns. It is the
   * argument against ever replacing this with an approximate counter.
   */
  private final Semaphore permits;

  private final LongSupplier nanoTime;
  private final LongAdder rejected = new LongAdder();

  /**
   * Start of the current summary interval; also the election token (see {@link #maybeSummarize}).
   */
  private final AtomicLong lastSummaryNanos;

  /** Cumulative rejections already reported, so each interval logs its own delta. */
  private final AtomicLong lastReportedCount = new AtomicLong();

  AdmissionController(int maxConcurrent) {
    this(maxConcurrent, System::nanoTime);
  }

  // Visible for testing: an injected nano source exercises interval behavior without sleeping.
  AdmissionController(int maxConcurrent, LongSupplier nanoTime) {
    if (maxConcurrent <= 0) {
      throw new IllegalArgumentException("maxConcurrent must be > 0, got " + maxConcurrent);
    }
    this.maxConcurrent = maxConcurrent;
    this.permits = new Semaphore(maxConcurrent, false);
    this.nanoTime = nanoTime;
    // Seeded from the clock, not from 0: nanoTime's origin is arbitrary, so a 0 seed would make the
    // first rejection look one whole epoch overdue and emit a summary of one.
    this.lastSummaryNanos = new AtomicLong(nanoTime.getAsLong());
  }

  /**
   * Takes a permit if the cap allows, and returns the lease that gives it back.
   *
   * @return the lease, or {@code null} when the cap is full — the caller refuses the start
   */
  @Nullable PermitLease acquire() {
    if (permits.tryAcquire()) {
      return new PermitLease();
    }
    rejected.increment();
    maybeSummarize();
    return null;
  }

  int maxConcurrent() {
    return maxConcurrent;
  }

  // Visible for testing: the leak and over-release assertions compare this against the cap.
  int availablePermits() {
    return permits.availablePermits();
  }

  /**
   * Logs one line per interval for the whole process, no matter how many threads are being refused.
   *
   * <p>A rejection storm is the moment a daemon can least afford a line per event, so callers do
   * not log; they land here and at most one of them per interval writes. The election is a CAS on
   * the interval start, advanced <b>before</b> the line is formatted so a slow appender cannot let
   * a second thread elect itself in the gap.
   *
   * <p>Counters are cumulative and never reset — the Prometheus convention, and it also avoids the
   * race a {@code sumThenReset} would introduce with concurrent rejectors. The elected writer takes
   * its delta with a {@code getAndSet}, so two writers can never report the same rejection twice
   * even if one is descheduled mid-summary.
   *
   * <p>Summarizing from the rejection path rather than a scheduler means a storm's final partial
   * count waits for the next rejection to be reported. That trailing edge is deliberate: it costs
   * one delayed line and saves a thread that would exist only to say nothing.
   */
  private void maybeSummarize() {
    long now = nanoTime.getAsLong();
    long last = lastSummaryNanos.get();
    // Subtraction, not comparison: nanoTime is signed and free to wrap.
    if (now - last < SUMMARY_INTERVAL_NANOS || !lastSummaryNanos.compareAndSet(last, now)) {
      return;
    }
    long total = rejected.sum();
    long delta = total - lastReportedCount.getAndSet(total);
    logger.warn(summarize(delta, total, TimeUnit.NANOSECONDS.toSeconds(now - last)));
  }

  /** The summary as one line; the single place its field list is spelled out. */
  private String summarize(long delta, long total, long elapsedSeconds) {
    return "Admission cap "
        + maxConcurrent
        + ": "
        + delta
        + " start(s) rejected in the last "
        + elapsedSeconds
        + "s ("
        + total
        + " total since start, "
        + permits.availablePermits()
        + " permits free). Sagas are arriving faster than they finish; this is the cap doing its"
        + " job, but a sustained storm means the cap, the request rate limit, or downstream"
        + " capacity needs revisiting.";
  }

  /**
   * The right to occupy one slot, returned exactly once.
   *
   * <p>Release-once is not defensive tidiness: a permit returned twice raises the cap silently and
   * permanently, and one never returned lowers it the same way. Both failures are invisible until
   * the daemon is either overloaded or refusing work it could have done, so the guard lives here
   * rather than in the discipline of every call site.
   */
  final class PermitLease {

    private final AtomicBoolean released = new AtomicBoolean();

    void release() {
      if (released.compareAndSet(false, true)) {
        permits.release();
      }
    }
  }
}
