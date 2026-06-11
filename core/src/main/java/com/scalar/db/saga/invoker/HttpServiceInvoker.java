package com.scalar.db.saga.invoker;

import com.scalar.db.saga.api.ServiceCallContext;
import com.scalar.db.saga.api.ServiceInvoker;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A built-in {@link ServiceInvoker} that dispatches saga steps to HTTP endpoints under a common
 * base URL, using user-supplied lambdas. The framework propagates {@code X-Saga-Id}/{@code
 * X-Saga-Step}, classifies status codes as retryable/non-retryable, and enforces the {@link
 * OutboundHttpPolicy}.
 *
 * <pre>{@code
 * HttpServiceInvoker.newBuilder("http://account-svc:8080")
 *     .operation("debit")
 *         .execution(
 *             (http, ctx) ->
 *                 StepResult.of("debitId",
 *                     http.post("/debit")
 *                         .jsonBody(Map.of("accountId", ctx.get("accountId", String.class).orElseThrow()))
 *                         .send()
 *                         .jsonObject()
 *                         .get("debit_id")))
 *         .compensation(
 *             (http, ctx) ->
 *                 http.post("/debit/reverse")
 *                     .jsonBody(Map.of("debitId", ctx.get("debitId", String.class).orElseThrow()))
 *                     .send())
 *         .add()
 *     .build();
 * }</pre>
 *
 * <p><b>Security / trust model:</b> endpoints are assumed to be operator-configured and trusted.
 * The {@code baseUrl} scheme is <em>not</em> validated — use {@code https://} for sensitive data,
 * as this invoker will not enforce it (and will send headers/bodies over plaintext {@code http://}
 * if configured). The optional {@link Builder#allowedHosts(String...) allowlist} is best-effort
 * defense-in-depth (see its docs), not a substitute for restricting where the engine can run from.
 */
public final class HttpServiceInvoker implements ServiceInvoker {

  private final String baseUrl;
  private final Map<String, HttpExecution> executions;
  private final Map<String, HttpCompensation> compensations;
  private final Map<String, HttpReservation> reservations;
  private final Map<String, HttpConfirmation> confirmations;
  private final Map<String, HttpCancellation> cancellations;
  private final HttpClient client;
  private final boolean ownsClient;
  private final HttpExchange exchange;

  private HttpServiceInvoker(Builder builder) {
    this.baseUrl = builder.baseUrl;
    this.executions = Map.copyOf(builder.executions);
    this.compensations = Map.copyOf(builder.compensations);
    this.reservations = Map.copyOf(builder.reservations);
    this.confirmations = Map.copyOf(builder.confirmations);
    this.cancellations = Map.copyOf(builder.cancellations);
    // Phase pairing is guaranteed structurally by Builder#operation (execution+compensation) and
    // Builder#tccOperation (reserve+confirm+cancel) — each registers all phases of an operation
    // together — so only the non-empty invariant needs a runtime check here.
    if (this.executions.isEmpty() && this.reservations.isEmpty()) {
      throw new IllegalStateException("HttpServiceInvoker must register at least one operation");
    }
    if (builder.client != null) {
      this.client = builder.client;
      this.ownsClient = false;
    } else {
      this.client = HttpClient.newHttpClient();
      this.ownsClient = true;
    }
    this.exchange = new HttpExchange(this.client, builder.policyBuilder.build());
  }

  /** Starts building an invoker whose endpoints are relative to {@code baseUrl}. */
  public static Builder newBuilder(String baseUrl) {
    Objects.requireNonNull(baseUrl, "baseUrl must not be null");
    if (baseUrl.isBlank()) {
      throw new IllegalArgumentException("baseUrl must not be blank");
    }
    return new Builder(baseUrl);
  }

  @Override
  public StepResult execute(String operation, ServiceCallContext context)
      throws StepExecutionException {
    HttpExecution execution = executions.get(operation);
    if (execution == null) {
      throw new StepExecutionException(
          "No execution registered for operation: " + operation, false);
    }
    try {
      return execution.apply(callContext(context), context);
    } catch (HttpCallException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public void compensate(String operation, ServiceCallContext context)
      throws StepCompensationException {
    HttpCompensation compensation = compensations.get(operation);
    if (compensation == null) {
      throw new StepCompensationException("No compensation registered for operation: " + operation);
    }
    try {
      compensation.apply(callContext(context), context);
    } catch (HttpCallException e) {
      throw new StepCompensationException(e);
    }
  }

  @Override
  public boolean supportsExecute(String operation) {
    return executions.containsKey(operation);
  }

  @Override
  public boolean supportsCompensate(String operation) {
    return compensations.containsKey(operation);
  }

  @Override
  public StepResult reserve(String operation, ServiceCallContext context)
      throws StepExecutionException {
    HttpReservation reservation = reservations.get(operation);
    if (reservation == null) {
      throw new StepExecutionException(
          "No reservation registered for operation: " + operation, false);
    }
    try {
      return reservation.apply(callContext(context), context);
    } catch (HttpCallException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public void confirm(String operation, ServiceCallContext context) throws StepExecutionException {
    HttpConfirmation confirmation = confirmations.get(operation);
    if (confirmation == null) {
      throw new StepExecutionException(
          "No confirmation registered for operation: " + operation, false);
    }
    try {
      confirmation.apply(callContext(context), context);
    } catch (HttpCallException e) {
      throw new StepExecutionException(e, e.isRetryable());
    }
  }

  @Override
  public void cancel(String operation, ServiceCallContext context)
      throws StepCompensationException {
    HttpCancellation cancellation = cancellations.get(operation);
    if (cancellation == null) {
      throw new StepCompensationException("No cancellation registered for operation: " + operation);
    }
    try {
      cancellation.apply(callContext(context), context);
    } catch (HttpCallException e) {
      throw new StepCompensationException(e);
    }
  }

  @Override
  public boolean supportsReserve(String operation) {
    return reservations.containsKey(operation);
  }

  @Override
  public boolean supportsConfirm(String operation) {
    return confirmations.containsKey(operation);
  }

  @Override
  public boolean supportsCancel(String operation) {
    return cancellations.containsKey(operation);
  }

  /**
   * Closes the underlying {@link HttpClient} — but only if this invoker created it. A client
   * supplied via {@link Builder#httpClient(HttpClient)} is left open, as the caller owns its
   * lifecycle.
   */
  @Override
  public void close() {
    if (ownsClient) {
      client.close();
    }
  }

  private HttpCallContext callContext(ServiceCallContext context) {
    return new HttpCallContext(exchange, baseUrl, context.getSagaId(), context.getStepName());
  }

  /** Builder for {@link HttpServiceInvoker}. */
  public static final class Builder {

    private final String baseUrl;
    private final Map<String, HttpExecution> executions = new HashMap<>();
    private final Map<String, HttpCompensation> compensations = new HashMap<>();
    private final Map<String, HttpReservation> reservations = new HashMap<>();
    private final Map<String, HttpConfirmation> confirmations = new HashMap<>();
    private final Map<String, HttpCancellation> cancellations = new HashMap<>();
    private final OutboundHttpPolicy.Builder policyBuilder = OutboundHttpPolicy.newBuilder();
    private @Nullable HttpClient client;

    private Builder(String baseUrl) {
      this.baseUrl = baseUrl;
    }

    /**
     * Begins registering a SAGA operation named {@code operationName}. Set its forward {@code
     * execution} and its {@code compensation} on the returned sub-builder, then call {@link
     * OperationBuilder#add()} to commit it and return to this builder. Requiring both phases before
     * {@code add()} makes "every execution has a compensation" structural — the analog of a {@code
     * Step} class implementing both {@code execute} and {@code compensate}.
     */
    public OperationBuilder operation(String operationName) {
      Objects.requireNonNull(operationName, "operationName must not be null");
      return new OperationBuilder(this, operationName);
    }

    /**
     * Begins registering a TCC operation named {@code operationName}. Set its {@code reservation}
     * (try), {@code confirmation}, and {@code cancellation} phases on the returned sub-builder,
     * then call {@link TccOperationBuilder#add()} to commit it and return to this builder.
     * Requiring all three phases before {@code add()} makes a TCC operation structurally complete —
     * the analog of a {@code TccStep} implementing reserve/confirm/cancel. Referenced by steps in a
     * {@link com.scalar.db.saga.api.SagaDefinition.SagaMode#TCC TCC} saga.
     */
    public TccOperationBuilder tccOperation(String operationName) {
      Objects.requireNonNull(operationName, "operationName must not be null");
      return new TccOperationBuilder(this, operationName);
    }

    /** Uses a custom {@link HttpClient} (e.g. with a proxy or custom TLS). */
    public Builder httpClient(HttpClient client) {
      this.client = Objects.requireNonNull(client, "client must not be null");
      return this;
    }

    /**
     * Restricts outbound calls to the given hosts (SSRF allowlist). Empty (the default) = allow
     * all. Matching is by exact, case-insensitive host name only — it does not resolve the host or
     * inspect the connect-time IP, so it does not defend against DNS rebinding or a hostname that
     * points at a private/link-local/metadata address. It is defense-in-depth for trusted,
     * operator-configured endpoints, not a sandbox.
     */
    public Builder allowedHosts(String... hosts) {
      policyBuilder.allowedHosts(hosts);
      return this;
    }

    /** Sets the maximum request/response body size in bytes. Defaults to 1 MB. */
    public Builder maxBodyBytes(long maxBodyBytes) {
      policyBuilder.maxBodyBytes(maxBodyBytes);
      return this;
    }

    public HttpServiceInvoker build() {
      return new HttpServiceInvoker(this);
    }
  }

  /**
   * Configures the {@code execution} and {@code compensation} phases of a SAGA operation. Obtained
   * from {@link Builder#operation(String)}; call {@link #add()} to commit the operation.
   */
  public static final class OperationBuilder {

    private final Builder parent;
    private final String operationName;
    private @Nullable HttpExecution execution;
    private @Nullable HttpCompensation compensation;

    private OperationBuilder(Builder parent, String operationName) {
      this.parent = parent;
      this.operationName = operationName;
    }

    /** Sets the forward execution phase. */
    public OperationBuilder execution(HttpExecution execution) {
      this.execution = Objects.requireNonNull(execution, "execution must not be null");
      return this;
    }

    /** Sets the compensation phase (pass an empty lambda if the step has no undo). */
    public OperationBuilder compensation(HttpCompensation compensation) {
      this.compensation = Objects.requireNonNull(compensation, "compensation must not be null");
      return this;
    }

    /**
     * Commits this operation to the parent builder and returns it.
     *
     * @throws IllegalStateException if either the execution or the compensation phase is unset
     */
    public Builder add() {
      if (execution == null || compensation == null) {
        throw new IllegalStateException(
            "Operation '" + operationName + "' requires both an execution and a compensation");
      }
      parent.executions.put(operationName, execution);
      parent.compensations.put(operationName, compensation);
      return parent;
    }
  }

  /**
   * Configures the {@code reservation}, {@code confirmation}, and {@code cancellation} phases of a
   * TCC operation. Obtained from {@link Builder#tccOperation(String)}; call {@link #add()} to
   * commit the operation.
   */
  public static final class TccOperationBuilder {

    private final Builder parent;
    private final String operationName;
    private @Nullable HttpReservation reservation;
    private @Nullable HttpConfirmation confirmation;
    private @Nullable HttpCancellation cancellation;

    private TccOperationBuilder(Builder parent, String operationName) {
      this.parent = parent;
      this.operationName = operationName;
    }

    /** Sets the Try (reserve) phase. */
    public TccOperationBuilder reservation(HttpReservation reservation) {
      this.reservation = Objects.requireNonNull(reservation, "reservation must not be null");
      return this;
    }

    /** Sets the Confirm phase. */
    public TccOperationBuilder confirmation(HttpConfirmation confirmation) {
      this.confirmation = Objects.requireNonNull(confirmation, "confirmation must not be null");
      return this;
    }

    /** Sets the Cancel phase. */
    public TccOperationBuilder cancellation(HttpCancellation cancellation) {
      this.cancellation = Objects.requireNonNull(cancellation, "cancellation must not be null");
      return this;
    }

    /**
     * Commits this TCC operation to the parent builder and returns it.
     *
     * @throws IllegalStateException if any of the reservation, confirmation, or cancellation phases
     *     is unset
     */
    public Builder add() {
      if (reservation == null || confirmation == null || cancellation == null) {
        throw new IllegalStateException(
            "TCC operation '"
                + operationName
                + "' requires a reservation, a confirmation, and a cancellation");
      }
      parent.reservations.put(operationName, reservation);
      parent.confirmations.put(operationName, confirmation);
      parent.cancellations.put(operationName, cancellation);
      return parent;
    }
  }
}
