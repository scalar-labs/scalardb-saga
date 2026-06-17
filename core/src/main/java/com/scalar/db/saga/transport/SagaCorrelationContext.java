package com.scalar.db.saga.transport;

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
 * <p><b>Future — migrate to {@code ScopedValue} on JDK 25+.</b> This uses {@link ThreadLocal} only
 * because {@code ScopedValue} is a preview API on Java 21 (this module's source/target; finalized
 * in JDK 25 via JEP 506), and a library cannot require {@code --enable-preview} of its consumers.
 * When the module moves to source/target JDK 25+, switch to {@code ScopedValue}: it removes the
 * manual {@link #bind}/{@link #restore} (auto-unbinds at scope exit), cannot retain values like a
 * thread-local, and propagates to {@code StructuredTaskScope} forks — which would also fix the
 * off-thread fallback noted above.
 */
public final class SagaCorrelationContext {

  /**
   * The current saga correlation, immutable. {@code deadlineMillis} is the step's absolute deadline
   * in epoch milliseconds — the engine clock, which is the system clock in production, so {@link
   * #remaining()} can measure it against {@link System#currentTimeMillis()} — or {@code 0} when the
   * step has no deadline.
   */
  public record Correlation(String sagaId, String stepName, long deadlineMillis) {}

  private static final Correlation NONE = new Correlation("", "", 0L);

  private static final ThreadLocal<@Nullable Correlation> CURRENT = new ThreadLocal<>();

  private SagaCorrelationContext() {}

  /**
   * Binds the correlation for the current thread, returning the previous binding to restore. {@code
   * deadlineMillis} is the step's wall-clock absolute deadline ({@code 0} for none).
   */
  public static @Nullable Correlation bind(String sagaId, String stepName, long deadlineMillis) {
    Correlation previous = CURRENT.get();
    CURRENT.set(new Correlation(sagaId, stepName, deadlineMillis));
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
    long deadline = current().deadlineMillis();
    if (deadline <= 0) {
      return null;
    }
    return Duration.ofMillis(Math.max(1L, deadline - System.currentTimeMillis()));
  }
}
