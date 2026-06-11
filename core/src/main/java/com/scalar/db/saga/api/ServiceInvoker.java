package com.scalar.db.saga.api;

import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;

/**
 * Dispatches a saga step to a remote service operation (Layer 2). An invoker is registered under a
 * logical service name (via {@link SagaManager.Builder#serviceInvokerFactory}) and routes the
 * step's {@code operation} to a typed execution or compensation — no reflection, no {@code Step}
 * class.
 *
 * <p>The built-in {@code HttpServiceInvoker} wraps user-supplied lambdas and automatically
 * propagates the saga context ({@code X-Saga-Id}/{@code X-Saga-Step}), classifies HTTP status codes
 * as retryable/non-retryable, and enforces outbound HTTP policy. Custom implementations may target
 * any transport.
 *
 * <p><b>SAGA vs TCC operations.</b> An operation is either a SAGA operation ({@link #execute} /
 * {@link #compensate}) or a TCC operation ({@link #reserve} / {@link #confirm} / {@link #cancel}),
 * depending on how the saga that references it is run. A single invoker may host a mix of both —
 * the mode is a property of the <em>operation</em>, not of the invoker. All operation methods are
 * {@code default} and unsupported out of the box; an implementation overrides only the phases it
 * provides and reports them via the matching {@code supports*} flag. The engine never calls a phase
 * it has not first confirmed via {@code supports*} at registration.
 *
 * <p><b>Why {@code default} rather than abstract.</b> The default (throwing) bodies are the
 * optional-operation idiom (cf. {@link java.util.Collection}): they let an invoker that serves a
 * single mode implement only its phases — a SAGA-only invoker overrides {@link #execute} / {@link
 * #compensate} and leaves the TCC phases inert, and vice versa — instead of writing throwing stubs
 * for the phases it does not offer. They also let this SPI grow without breaking existing
 * implementations: the TCC phases were added after the original SAGA-only design, and custom
 * invokers compiled against the older interface kept working because the new methods arrived as
 * {@code default}. A general-purpose invoker such as the built-in {@code HttpServiceInvoker}
 * implements every phase anyway, so for it these bodies are merely unreachable backstops — the
 * {@code supports*} guard keeps the engine from ever calling a phase the invoker did not implement.
 *
 * <p><b>{@code supports*} contract.</b> For each operation, {@code supportsX(op)} must return
 * {@code true} if and only if {@code X(op)} is implemented (where {@code X} is one of execute,
 * compensate, reserve, confirm, cancel). The engine validates {@code supports*} at registration and
 * dispatches the corresponding phase at runtime: a {@code supports*} that reports {@code true}
 * without a working override will surface as an {@code UnsupportedOperationException} mid-saga,
 * while an implemented phase whose {@code supports*} returns {@code false} fails fast at
 * registration. The built-in {@code HttpServiceInvoker} enforces this consistency structurally.
 *
 * <p><b>Lifecycle &amp; thread-safety:</b> an invoker is a shared singleton invoked concurrently
 * across saga executions; all methods must be thread-safe. Every phase must be idempotent (see
 * {@link Step} / {@link TccStep}) — each may be retried on crash recovery.
 */
public interface ServiceInvoker extends AutoCloseable {

  // ---------------------------------------------------------------------------
  // SAGA operations (execute / compensate)
  // ---------------------------------------------------------------------------

  /**
   * Executes the named forward operation (SAGA mode).
   *
   * @param operation the service operation name (from the step's {@code
   *     ServiceStep.getOperation()})
   * @param context the call context (saga data + saga id + step name)
   * @return the step output, merged into the saga context
   * @throws StepExecutionException if the operation fails (retryable or not)
   */
  default StepResult execute(String operation, ServiceCallContext context)
      throws StepExecutionException {
    throw new UnsupportedOperationException("execute not supported for operation: " + operation);
  }

  /**
   * Executes the named compensating operation (SAGA mode).
   *
   * @param operation the service operation name
   * @param context the call context
   * @throws StepCompensationException if compensation fails
   */
  default void compensate(String operation, ServiceCallContext context)
      throws StepCompensationException {
    throw new UnsupportedOperationException("compensate not supported for operation: " + operation);
  }

  /** Whether this invoker has a forward (SAGA) operation for {@code operation}. */
  default boolean supportsExecute(String operation) {
    return false;
  }

  /** Whether this invoker has a compensating (SAGA) operation for {@code operation}. */
  default boolean supportsCompensate(String operation) {
    return false;
  }

  // ---------------------------------------------------------------------------
  // TCC operations (reserve / confirm / cancel)
  // ---------------------------------------------------------------------------

  /**
   * Try phase (TCC mode): reserves resources tentatively.
   *
   * @param operation the service operation name
   * @param context the call context
   * @return the step output, merged into the saga context
   * @throws StepExecutionException if the reservation fails (retryable or not)
   */
  default StepResult reserve(String operation, ServiceCallContext context)
      throws StepExecutionException {
    throw new UnsupportedOperationException("reserve not supported for operation: " + operation);
  }

  /**
   * Confirm phase (TCC mode): makes the reservation permanent. Called only after all steps reserve
   * successfully; should not permanently fail.
   *
   * @param operation the service operation name
   * @param context the call context
   * @throws StepExecutionException if confirmation fails
   */
  default void confirm(String operation, ServiceCallContext context) throws StepExecutionException {
    throw new UnsupportedOperationException("confirm not supported for operation: " + operation);
  }

  /**
   * Cancel phase (TCC mode): releases the reservation. Called when any step's reserve fails.
   *
   * @param operation the service operation name
   * @param context the call context
   * @throws StepCompensationException if cancellation fails
   */
  default void cancel(String operation, ServiceCallContext context)
      throws StepCompensationException {
    throw new UnsupportedOperationException("cancel not supported for operation: " + operation);
  }

  /** Whether this invoker has a reserve (TCC) operation for {@code operation}. */
  default boolean supportsReserve(String operation) {
    return false;
  }

  /** Whether this invoker has a confirm (TCC) operation for {@code operation}. */
  default boolean supportsConfirm(String operation) {
    return false;
  }

  /** Whether this invoker has a cancel (TCC) operation for {@code operation}. */
  default boolean supportsCancel(String operation) {
    return false;
  }

  /**
   * Releases any resources held by this invoker (e.g. HTTP clients, channels). Called by the {@link
   * SagaManager} that created this invoker (via {@link ServiceInvokerFactory}) when the manager is
   * closed. Stateless invokers need not override this.
   */
  @Override
  default void close() {}
}
