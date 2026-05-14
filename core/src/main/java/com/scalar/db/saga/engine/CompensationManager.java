package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StepEvent;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes compensation in reverse order (LIFO) with retry.
 *
 * <p>On failure after retries exhausted, the saga stays in COMPENSATING for periodic recovery to
 * retry later.
 */
@ThreadSafe
class CompensationManager {

  private static final Logger logger = LoggerFactory.getLogger(CompensationManager.class);

  private final SagaStore store;
  private final RetryPolicy compensationRetryPolicy;
  private final Clock clock;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  CompensationManager(SagaStore store, RetryPolicy compensationRetryPolicy, Clock clock) {
    this.store = store;
    this.compensationRetryPolicy = compensationRetryPolicy;
    this.clock = clock;
  }

  CompensationManager(SagaStore store, RetryPolicy compensationRetryPolicy) {
    this(store, compensationRetryPolicy, Clock.systemUTC());
  }

  CompensationManager(SagaStore store) {
    this(store, RetryPolicy.compensationDefault());
  }

  /**
   * Compensates steps in reverse order from {@code fromStepIndex} down to 0.
   *
   * @param plan the execution plan
   * @param context the execution context
   * @param fromStepIndex the highest step index to compensate (inclusive)
   * @throws StepCompensationException if compensation fails after retries exhausted
   */
  void compensate(List<StepWithPolicy> plan, ExecutionContext context, int fromStepIndex) {
    for (int i = fromStepIndex; i >= 0; i--) {
      if (context.isStepCompensated(i)) {
        logger.debug("Skipping already-compensated step at index {}", i);
        continue;
      }

      StepWithPolicy stepWithPolicy = plan.get(i);
      Step step = stepWithPolicy.step();
      String stepName = step.getName();

      try {
        compensateWithRetry(step, context, stepName, i, stepWithPolicy.stepTimeoutMillis());
        recordStepCompensated(context, i, stepName);
      } catch (StepCompensationException e) {
        recordStepCompensationFailed(context, i, stepName);
        throw new StepCompensationException(stepName, i, e);
      }
    }
  }

  private void recordStepCompensated(ExecutionContext context, int stepIndex, String stepName) {
    store.recordStepEvent(
        context.getSagaId(), context.nextSequence(), StepEvent.compensated(stepIndex, stepName));
    context.advanceSequence();
    context.markStepCompensated(stepIndex);
  }

  private void recordStepCompensationFailed(
      ExecutionContext context, int stepIndex, String stepName) {
    store.recordStepEvent(
        context.getSagaId(),
        context.nextSequence(),
        StepEvent.compensationFailed(stepIndex, stepName, null));
    context.advanceSequence();
  }

  private void compensateWithRetry(
      Step step, ExecutionContext context, String stepName, int stepIndex, long stepTimeoutMillis)
      throws StepCompensationException {
    int maxAttempts = compensationRetryPolicy.getMaxAttempts();
    long interval = compensationRetryPolicy.getInitialIntervalMillis();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      Future<?> future =
          executor.submit(
              () -> {
                step.compensate(context);
                return null;
              });

      try {
        if (stepTimeoutMillis <= 0) {
          future.get();
        } else {
          long deadline = clock.millis() + stepTimeoutMillis;
          long remaining = deadline - clock.millis();
          if (remaining <= 0) {
            future.cancel(true);
            throw new StepCompensationException(
                "Compensation of step '" + stepName + "' timed out (deadline already passed)");
          }
          future.get(remaining, TimeUnit.MILLISECONDS);
        }
        return;
      } catch (TimeoutException e) {
        future.cancel(true);
        throw new StepCompensationException(
            "Compensation of step '" + stepName + "' timed out after " + stepTimeoutMillis + "ms");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        throw new StepCompensationException("Compensation of step '" + stepName + "' interrupted");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        StepCompensationException sce;
        if (cause instanceof StepCompensationException s) {
          sce = s;
        } else {
          sce = new StepCompensationException(cause != null ? cause : e);
        }

        logger.warn(
            "Compensation attempt {}/{} failed for step '{}' at index {}",
            attempt,
            maxAttempts,
            stepName,
            stepIndex,
            sce);
        if (attempt < maxAttempts) {
          try {
            interval = compensationRetryPolicy.sleepWithBackoff(interval);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw sce;
          }
        } else {
          throw sce;
        }
      }
    }
  }
}
