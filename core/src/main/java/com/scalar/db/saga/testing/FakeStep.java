package com.scalar.db.saga.testing;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Configurable {@link Step} implementation for testing.
 *
 * <p>Tracks execution and compensation history (saga IDs) in thread-safe lists. Supports failure
 * injection for both execution and compensation.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * FakeStep step = FakeStep.newBuilder("payment")
 *     .executeReturns(StepResult.of("txId", "abc-123"))
 *     .build();
 *
 * // After saga execution:
 * assertThat(step.getExecutions()).containsExactly("saga-1");
 * assertThat(step.getCompensations()).isEmpty();
 * }</pre>
 */
@ThreadSafe
public final class FakeStep implements Step {

  /** Action that may throw {@link StepExecutionException}. */
  @FunctionalInterface
  public interface ExecuteAction {
    StepResult execute(SagaContext context) throws StepExecutionException;
  }

  private final String name;
  private final ExecuteAction executeAction;
  private final @Nullable StepCompensationException compensationFailure;
  private final CopyOnWriteArrayList<String> executions = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> compensations = new CopyOnWriteArrayList<>();

  private FakeStep(Builder builder) {
    this.name = builder.name;
    this.executeAction = builder.executeAction;
    this.compensationFailure = builder.compensationFailure;
  }

  public static Builder newBuilder(String name) {
    return new Builder(Objects.requireNonNull(name, "name must not be null"));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult execute(SagaContext context) throws StepExecutionException {
    executions.add(context.getSagaId());
    return executeAction.execute(context);
  }

  @Override
  public void compensate(SagaContext context) throws StepCompensationException {
    compensations.add(context.getSagaId());
    if (compensationFailure != null) {
      throw compensationFailure;
    }
  }

  /** Returns the saga IDs that invoked {@link #execute}, in order. */
  public List<String> getExecutions() {
    return List.copyOf(executions);
  }

  /** Returns the saga IDs that invoked {@link #compensate}, in order. */
  public List<String> getCompensations() {
    return List.copyOf(compensations);
  }

  public int getExecutionCount() {
    return executions.size();
  }

  public int getCompensationCount() {
    return compensations.size();
  }

  /** Builder for {@link FakeStep}. */
  public static final class Builder {

    private final String name;
    private ExecuteAction executeAction = ctx -> StepResult.empty();
    private @Nullable StepCompensationException compensationFailure;

    private Builder(String name) {
      this.name = name;
    }

    /** Sets the result returned by {@link Step#execute}. Default: {@link StepResult#empty()}. */
    public Builder executeReturns(StepResult result) {
      Objects.requireNonNull(result, "result must not be null");
      this.executeAction = ctx -> result;
      return this;
    }

    /**
     * Sets a dynamic execute action that receives the {@link SagaContext} and returns a result. The
     * action may throw {@link StepExecutionException} to simulate failure.
     */
    public Builder executeAction(ExecuteAction action) {
      this.executeAction = Objects.requireNonNull(action, "action must not be null");
      return this;
    }

    /** Makes {@link Step#execute} throw the given exception. */
    public Builder executeFails(StepExecutionException failure) {
      Objects.requireNonNull(failure, "failure must not be null");
      this.executeAction =
          ctx -> {
            throw failure;
          };
      return this;
    }

    /** Makes {@link Step#compensate} throw the given exception. */
    public Builder compensateFails(StepCompensationException failure) {
      this.compensationFailure = Objects.requireNonNull(failure, "failure must not be null");
      return this;
    }

    public FakeStep build() {
      return new FakeStep(this);
    }
  }
}
