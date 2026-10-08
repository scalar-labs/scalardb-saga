package com.scalar.db.saga.testing;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Configurable {@link TccStep} implementation for testing.
 *
 * <p>Tracks reserve, confirm, and cancel invocations (saga IDs) in thread-safe lists. Supports
 * failure injection for each phase independently.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * FakeTccStep step = FakeTccStep.newBuilder("inventory")
 *     .reserveReturns(StepResult.of("reserved", true))
 *     .build();
 *
 * // After saga execution:
 * assertThat(step.getReservations()).containsExactly("saga-1");
 * assertThat(step.getConfirmations()).containsExactly("saga-1");
 * assertThat(step.getCancellations()).isEmpty();
 * }</pre>
 */
@ThreadSafe
public final class FakeTccStep implements TccStep {

  /** Action that may throw {@link StepExecutionException}. */
  @FunctionalInterface
  public interface ReserveAction {
    /**
     * Runs in place of {@link TccStep#reserve}.
     *
     * @param context the saga context the engine passes to the step
     * @return the reservation's result
     * @throws StepExecutionException to make the reservation fail
     */
    StepResult reserve(SagaContext context) throws StepExecutionException;
  }

  private final String name;
  private final ReserveAction reserveAction;
  private final StepResult confirmResult;
  private final @Nullable StepExecutionException confirmFailure;
  private final @Nullable StepCompensationException cancelFailure;
  private final CopyOnWriteArrayList<String> reservations = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> confirmations = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<String> cancellations = new CopyOnWriteArrayList<>();

  private FakeTccStep(Builder builder) {
    this.name = builder.name;
    this.reserveAction = builder.reserveAction;
    this.confirmResult = builder.confirmResult;
    this.confirmFailure = builder.confirmFailure;
    this.cancelFailure = builder.cancelFailure;
  }

  /**
   * Starts building a fake TCC step with the given name.
   *
   * @param name the step name, unique within its saga definition
   * @return a builder whose reserve and confirm succeed with {@link StepResult#empty()} and whose
   *     cancel succeeds, until configured
   */
  public static Builder newBuilder(String name) {
    return new Builder(Objects.requireNonNull(name, "name must not be null"));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StepResult reserve(SagaContext context) throws StepExecutionException {
    reservations.add(context.getSagaId());
    return reserveAction.reserve(context);
  }

  @Override
  public StepResult confirm(SagaContext context) throws StepExecutionException {
    confirmations.add(context.getSagaId());
    if (confirmFailure != null) {
      throw confirmFailure;
    }
    return confirmResult;
  }

  @Override
  public void cancel(SagaContext context) throws StepCompensationException {
    cancellations.add(context.getSagaId());
    if (cancelFailure != null) {
      throw cancelFailure;
    }
  }

  /**
   * Returns the saga IDs that invoked {@link #reserve}, in order.
   *
   * @return the saga IDs, oldest first; empty if the step never reserved
   */
  public List<String> getReservations() {
    return List.copyOf(reservations);
  }

  /**
   * Returns the saga IDs that invoked {@link #confirm}, in order.
   *
   * @return the saga IDs, oldest first; empty if the step never confirmed
   */
  public List<String> getConfirmations() {
    return List.copyOf(confirmations);
  }

  /**
   * Returns the saga IDs that invoked {@link #cancel}, in order.
   *
   * @return the saga IDs, oldest first; empty if the step never cancelled
   */
  public List<String> getCancellations() {
    return List.copyOf(cancellations);
  }

  /** Builder for {@link FakeTccStep}. */
  public static final class Builder {

    private final String name;
    private ReserveAction reserveAction = ctx -> StepResult.empty();
    private StepResult confirmResult = StepResult.empty();
    private @Nullable StepExecutionException confirmFailure;
    private @Nullable StepCompensationException cancelFailure;

    private Builder(String name) {
      this.name = name;
    }

    /**
     * Sets the result returned by {@link TccStep#reserve}. Default: {@link StepResult#empty()}.
     *
     * @param result the result every reservation returns
     * @return this builder
     */
    public Builder reserveReturns(StepResult result) {
      Objects.requireNonNull(result, "result must not be null");
      this.reserveAction = ctx -> result;
      return this;
    }

    /**
     * Sets a dynamic reserve action that receives the {@link SagaContext} and returns a result. The
     * action may throw {@link StepExecutionException} to simulate failure.
     *
     * @param action the action run on every reservation
     * @return this builder
     */
    public Builder reserveAction(ReserveAction action) {
      this.reserveAction = Objects.requireNonNull(action, "action must not be null");
      return this;
    }

    /**
     * Makes {@link TccStep#reserve} throw the given exception.
     *
     * @param failure the exception every reservation throws
     * @return this builder
     */
    public Builder reserveFails(StepExecutionException failure) {
      Objects.requireNonNull(failure, "failure must not be null");
      this.reserveAction =
          ctx -> {
            throw failure;
          };
      return this;
    }

    /**
     * Sets the result returned by {@link TccStep#confirm}. Default: {@link StepResult#empty()}.
     * Pass {@link StepResult#pending()} to simulate an async confirmation accepted for later
     * callback completion.
     *
     * @param result the result every confirmation returns
     * @return this builder
     */
    public Builder confirmReturns(StepResult result) {
      this.confirmResult = Objects.requireNonNull(result, "result must not be null");
      return this;
    }

    /**
     * Makes {@link TccStep#confirm} throw the given exception.
     *
     * @param failure the exception every confirmation throws
     * @return this builder
     */
    public Builder confirmFails(StepExecutionException failure) {
      this.confirmFailure = Objects.requireNonNull(failure, "failure must not be null");
      return this;
    }

    /**
     * Makes {@link TccStep#cancel} throw the given exception.
     *
     * @param failure the exception every cancellation throws
     * @return this builder
     */
    public Builder cancelFails(StepCompensationException failure) {
      this.cancelFailure = Objects.requireNonNull(failure, "failure must not be null");
      return this;
    }

    /**
     * Builds the step.
     *
     * @return the configured fake TCC step
     */
    public FakeTccStep build() {
      return new FakeTccStep(this);
    }
  }
}
