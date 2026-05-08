package com.scalar.db.saga.api;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * Output of a step execution, merged into {@link SagaContext} for subsequent steps.
 *
 * <p>Use the factory methods to create instances:
 *
 * <ul>
 *   <li>{@link #of(String, Object)} — single key-value result
 *   <li>{@link #of(Map)} — multiple key-value pairs
 *   <li>{@link #empty()} — no output data
 *   <li>{@link #pending()} — daemon mode: step is not done yet, engine should park the saga
 * </ul>
 */
@Immutable
public final class StepResult {

  private static final StepResult EMPTY = new StepResult(false, Collections.emptyMap());
  private static final StepResult PENDING = new StepResult(true, Collections.emptyMap());

  private final boolean pending;
  private final Map<String, Object> output;

  private StepResult(boolean pending, Map<String, Object> output) {
    this.pending = pending;
    this.output = output;
  }

  /** Creates a result with a single key-value pair. */
  public static StepResult of(String key, Object value) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    return new StepResult(false, Collections.singletonMap(key, value));
  }

  /** Creates a result with multiple key-value pairs. The map is defensively copied. */
  public static StepResult of(Map<String, Object> output) {
    Objects.requireNonNull(output, "output must not be null");
    return new StepResult(false, Map.copyOf(output));
  }

  /** Creates a result with no output data. */
  public static StepResult empty() {
    return EMPTY;
  }

  /**
   * Creates a pending result (daemon mode only). The engine parks the saga until an external
   * callback completes the step via {@link SagaManager#completeStep}.
   */
  public static StepResult pending() {
    return PENDING;
  }

  /** Returns {@code true} if this is a pending result (daemon mode async step). */
  public boolean isPending() {
    return pending;
  }

  /** Returns the output data map. The returned map is unmodifiable. */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification =
          "output is always an unmodifiable map (emptyMap, singletonMap, or unmodifiableMap)")
  public Map<String, Object> getOutput() {
    return output;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (!(o instanceof StepResult that)) return false;
    return pending == that.pending && output.equals(that.output);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pending, output);
  }

  @Override
  public String toString() {
    if (pending) return "StepResult{PENDING}";
    if (output.isEmpty()) return "StepResult{EMPTY}";
    return "StepResult{output=" + output + '}';
  }
}
