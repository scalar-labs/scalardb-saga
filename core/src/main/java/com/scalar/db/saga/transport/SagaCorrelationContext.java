package com.scalar.db.saga.transport;

import java.time.Clock;
import java.time.Duration;
import org.jspecify.annotations.Nullable;

/**
 * Carries the current saga correlation ({@code sagaId} + {@code stepName}) and the running step's
 * {@code deadlineMillis} to an injected {@link com.scalar.db.saga.api.SagaHttpClient}, which is an
 * application singleton with no per-call context argument. The deadline lets a transport call bound
 * its per-request timeout to the step's remaining budget (see {@link #remaining()}).
 *
 * <p>The engine binds the correlation on the thread that runs a step's {@code execute}/{@code
 * compensate} (see {@code SagaEngine}), so a {@code SagaHttpClient} call made from within the step
 * reads the right values; it clears the binding when the step returns. A call made off that thread
 * (no binding) falls back to empty correlation, so a misuse never NPEs.
 *
 * <p><b>Future: migrate to {@code ScopedValue}.</b> JEP 506 finalized it in Java 25, so the
 * preview-API blocker is gone: a scoped value removes the manual {@link #bind} and {@link
 * #restore}, since it unbinds at scope exit, and it cannot be retained the way a thread-local can.
 * The swap is deferred to its own change because it alters how the engine binds correlation around
 * a step and needs its own tests. The off-thread fallback noted above would be fixed by propagation
 * into {@code StructuredTaskScope} forks, but that class is still preview on Java 25, and a library
 * cannot require {@code --enable-preview} of its consumers.
 */
public final class SagaCorrelationContext {

  /**
   * The current saga correlation, immutable. {@code deadlineMillis} is the step's absolute deadline
   * measured against {@code clock} — the engine's configured {@link Clock} (the system clock in
   * production) — or {@code 0} when the step has no deadline. {@link #remaining()} computes the
   * budget with that same clock so timeouts honor a test clock.
   */
  public record Correlation(String sagaId, String stepName, long deadlineMillis, Clock clock) {}

  private static final Correlation NONE = new Correlation("", "", 0L, Clock.systemUTC());

  private static final ThreadLocal<@Nullable Correlation> CURRENT = new ThreadLocal<>();

  private SagaCorrelationContext() {}

  /**
   * Binds the correlation for the current thread, returning the previous binding to restore. {@code
   * deadlineMillis} is the step's absolute deadline measured against {@code clock} ({@code 0} for
   * none); {@code clock} is the engine's configured {@link Clock}, so a per-request timeout derived
   * from it honors a test clock instead of bypassing it.
   */
  public static @Nullable Correlation bind(
      String sagaId, String stepName, long deadlineMillis, Clock clock) {
    Correlation previous = CURRENT.get();
    CURRENT.set(new Correlation(sagaId, stepName, deadlineMillis, clock));
    return previous;
  }

  /** Restores (or clears) the correlation binding to {@code previous}. */
  public static void restore(@Nullable Correlation previous) {
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
  }

  /** The current correlation, or empty values when none is bound. */
  static Correlation current() {
    Correlation correlation = CURRENT.get();
    return correlation != null ? correlation : NONE;
  }

  /**
   * The remaining time until the bound step deadline as a positive {@link Duration}, or {@code
   * null} when no deadline is bound — in which case the caller should fall back to the transport's
   * default per-request timeout. Floored at 1ms so a just-passed deadline yields a fast-failing
   * call rather than a non-positive timeout (which the JDK rejects).
   */
  static @Nullable Duration remaining() {
    Correlation current = current();
    if (current.deadlineMillis() <= 0) {
      return null;
    }
    return Duration.ofMillis(Math.max(1L, current.deadlineMillis() - current.clock().millis()));
  }
}
