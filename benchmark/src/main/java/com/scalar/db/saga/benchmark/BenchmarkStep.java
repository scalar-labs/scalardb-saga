package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import net.jcip.annotations.ThreadSafe;

/**
 * No-op {@link Step} for the embedded benchmark mode: sleeps a configurable delay (emulating a
 * participant call) and returns an empty result.
 *
 * <p>Counts every execution and remembers the distinct saga IDs it ran for, so {@link
 * #duplicateExecutions()} exposes how often the same saga executed this step more than once — the
 * signature of a recovery sweep re-driving a saga whose original drive was still running (or died
 * mid-flight). Compensations are counted too: a spike of compensations on a workload with no
 * injected failures means the engine rolled back sagas it should have completed.
 */
@ThreadSafe
public final class BenchmarkStep implements Step {

  private final String name;
  private final long delayMillis;
  private final LongAdder executions = new LongAdder();
  private final LongAdder compensations = new LongAdder();
  private final Set<String> executedSagaIds = ConcurrentHashMap.newKeySet();

  /**
   * Creates a step.
   *
   * @param name the step name (must not be blank)
   * @param delayMillis how long each execution and compensation sleeps; {@code 0} for none
   */
  public BenchmarkStep(String name, long delayMillis) {
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (delayMillis < 0) {
      throw new IllegalArgumentException("delayMillis must be >= 0, got " + delayMillis);
    }
    this.name = name;
    this.delayMillis = delayMillis;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    executions.increment();
    executedSagaIds.add(context.getSagaId());
    sleepDelay();
    return StepResult.empty();
  }

  @Override
  public void compensate(SagaContext context) {
    compensations.increment();
    try {
      sleepDelay();
    } catch (StepExecutionException e) {
      // An interrupted compensation sleep: the undo is a no-op anyway, so finishing quietly beats
      // failing the compensation and re-queuing the saga for recovery.
    }
  }

  private void sleepDelay() throws StepExecutionException {
    if (delayMillis <= 0) {
      return;
    }
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StepExecutionException("step '" + name + "' interrupted mid-delay", e, false);
    }
  }

  /** Total forward executions. */
  public long executions() {
    return executions.sum();
  }

  /**
   * Executions beyond the first per saga: {@code executions - distinct sagas}. Exact once the
   * workload has quiesced; a duplicate here means some saga ran this step more than once.
   */
  public long duplicateExecutions() {
    return executions.sum() - executedSagaIds.size();
  }

  /** Total compensations. */
  public long compensations() {
    return compensations.sum();
  }
}
