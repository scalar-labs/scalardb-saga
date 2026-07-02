package com.scalar.db.saga.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.SagaStatus;
import com.scalar.db.saga.rpc.StartSagaRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * A remote {@link SagaOrchestrator} backed by a gRPC connection to the saga daemon. It implements
 * the same application-facing interface as the embedded {@code DefaultSagaOrchestrator}, so
 * application code runs unchanged embedded or remote.
 *
 * <p>Thread-safe and intended to be created once and shared: a single instance holds one {@link
 * ManagedChannel} that multiplexes all calls. Call {@link #close()} to release it.
 *
 * <p>The four {@code startAsync(..., SagaCallback)} overloads throw {@link
 * UnsupportedOperationException}: a local completion callback over a remote, fire-and-forget server
 * needs a server-streaming {@code WatchSaga} RPC, which is not yet supported. Start asynchronously
 * and poll {@link #getStateSnapshot(String)} for the outcome, or use the embedded orchestrator.
 */
@ThreadSafe
public final class GrpcSagaOrchestratorClient implements SagaOrchestrator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long CLOSE_TIMEOUT_SECONDS = 5L;

  /** Capped, jittered exponential backoff between retries of a retryable transport failure. */
  private static final long BACKOFF_BASE_MILLIS = 50L;

  private static final long BACKOFF_CAP_MILLIS = 2_000L;
  private static final int BACKOFF_MAX_SHIFT = 16;

  private final SagaServiceBlockingStub stub;
  @Nullable private final ManagedChannel ownedChannel;
  private final long defaultDeadlineMillis;

  /**
   * Visible for testing — inject a stub over an in-process channel; {@code close()} is then a
   * no-op.
   */
  GrpcSagaOrchestratorClient(SagaServiceBlockingStub stub, @Nullable ManagedChannel ownedChannel) {
    this(stub, ownedChannel, 0L);
  }

  private GrpcSagaOrchestratorClient(
      SagaServiceBlockingStub stub,
      @Nullable ManagedChannel ownedChannel,
      long defaultDeadlineMillis) {
    this.stub = Objects.requireNonNull(stub, "stub must not be null");
    this.ownedChannel = ownedChannel;
    this.defaultDeadlineMillis = defaultDeadlineMillis;
  }

  /** Creates a client connected to {@code target} over plaintext (the in-cluster default). */
  public static GrpcSagaOrchestratorClient create(String target) {
    return newBuilder().target(target).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  // --------------------------------------------------------------------------
  // start (synchronous)
  // --------------------------------------------------------------------------

  @Override
  public String start(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    // Mint the id client-side: it is the idempotency key that makes the start retryable and gives
    // the await loop an id even if the first response is lost (no server-generated-id start here).
    String sagaId = UUID.randomUUID().toString();
    startSynchronously(sagaId, sagaName, null, input);
    return sagaId;
  }

  @Override
  public void start(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startSynchronously(sagaId, sagaName, null, input);
  }

  @Override
  public String start(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    String sagaId = UUID.randomUUID().toString();
    startSynchronously(sagaId, id.name(), id.version(), input);
    return sagaId;
  }

  @Override
  public void start(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startSynchronously(sagaId, id.name(), id.version(), input);
  }

  // --------------------------------------------------------------------------
  // startAsync (no callback)
  // --------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    return startAsynchronously(null, sagaName, null, input);
  }

  @Override
  public void startAsync(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startAsynchronously(sagaId, sagaName, null, input);
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    return startAsynchronously(null, id.name(), id.version(), input);
  }

  @Override
  public void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startAsynchronously(sagaId, id.name(), id.version(), input);
  }

  // --------------------------------------------------------------------------
  // startAsync (with callback) — deferred to a future server-streaming WatchSaga RPC
  // --------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback) {
    throw callbackUnsupported();
  }

  @Override
  public void startAsync(
      String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback) {
    throw callbackUnsupported();
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    throw callbackUnsupported();
  }

  @Override
  public void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    throw callbackUnsupported();
  }

  // --------------------------------------------------------------------------
  // query + lifecycle
  // --------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot getStateSnapshot(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    try {
      SagaSnapshot snapshot = stub().getSaga(GetSagaRequest.newBuilder().setSagaId(sagaId).build());
      return ClientProtoMappers.toApi(snapshot);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new SagaNotFoundException(sagaId);
      }
      throw mapCommon(e);
    }
  }

  @Override
  public void close() {
    if (ownedChannel == null) {
      return;
    }
    ownedChannel.shutdown();
    try {
      if (!ownedChannel.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        ownedChannel.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      ownedChannel.shutdownNow();
    }
  }

  // --------------------------------------------------------------------------
  // start + await terminal (synchronous start)
  // --------------------------------------------------------------------------

  /**
   * Delivers the {@link SagaOrchestrator} synchronous-{@code start} contract over a bounded server:
   * one bounded {@code StartSaga} (start + first wait window), then a loop of idempotent {@code
   * AwaitSaga} long-polls until terminal. Both the start and the awaits ride out retryable
   * transport failures (the saga keeps running server-side regardless), bounded overall by the
   * client deadline — {@code defaultDeadlineMillis == 0} means block to terminal, matching the
   * embedded orchestrator.
   */
  private SagaSnapshot startSynchronously(
      String sagaId, String name, @Nullable String version, Map<String, Object> input) {
    long loopDeadlineNanos =
        defaultDeadlineMillis > 0L
            ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(defaultDeadlineMillis)
            : 0L;
    StartSagaRequest request = buildRequest(sagaId, name, version, input, false);
    SagaSnapshot snapshot = firstStart(request, sagaId, name, version, loopDeadlineNanos);
    return awaitLoop(sagaId, snapshot, loopDeadlineNanos);
  }

  /**
   * Issues the first {@code StartSaga}, retrying retryable transport failures. Because the id is
   * always concrete (client-minted or caller-supplied), the retry is idempotent. {@code
   * ALREADY_EXISTS} on the <i>first</i> attempt is a genuine duplicate (surfaced as {@link
   * SagaAlreadyExistsException}); on a <i>retry</i> it means our earlier attempt landed, so we
   * fetch the snapshot and proceed to the await loop.
   */
  private SagaSnapshot firstStart(
      StartSagaRequest request,
      String sagaId,
      String name,
      @Nullable String version,
      long loopDeadlineNanos) {
    boolean attempted = false;
    int retries = 0;
    while (true) {
      try {
        return callWithin(loopDeadlineNanos).startSaga(request);
      } catch (StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        if (code == Status.Code.ALREADY_EXISTS) {
          if (attempted) {
            return getSagaSnapshot(sagaId, loopDeadlineNanos);
          }
          throw mapStartException(e, name, version, sagaId);
        }
        if (isRetryable(code)) {
          attempted = true;
          guardDeadline(loopDeadlineNanos);
          backoff(retries++);
          continue;
        }
        throw mapStartException(e, name, version, sagaId);
      }
    }
  }

  /**
   * Long-polls {@code AwaitSaga} until the snapshot is terminal. A gRPC-OK non-terminal return
   * means the server's window elapsed — loop again immediately (the server long-poll paces it). A
   * retryable transport failure is absorbed with backoff and retried (the wait is idempotent).
   * Bounded overall by the client deadline.
   */
  private SagaSnapshot awaitLoop(String sagaId, SagaSnapshot snapshot, long loopDeadlineNanos) {
    int retries = 0;
    while (!isTerminal(snapshot)) {
      guardDeadline(loopDeadlineNanos);
      AwaitSagaRequest request = AwaitSagaRequest.newBuilder().setSagaId(sagaId).build();
      try {
        snapshot = callWithin(loopDeadlineNanos).awaitSaga(request);
        retries = 0;
      } catch (StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        if (isRetryable(code)) {
          guardDeadline(loopDeadlineNanos);
          backoff(retries++);
          continue;
        }
        if (code == Status.Code.NOT_FOUND) {
          // The saga was purged/TTL'd between polls — surface the same type getStateSnapshot does.
          throw new SagaNotFoundException(sagaId);
        }
        throw mapCommon(e);
      }
    }
    return snapshot;
  }

  private SagaSnapshot getSagaSnapshot(String sagaId, long loopDeadlineNanos) {
    return callWithin(loopDeadlineNanos)
        .getSaga(GetSagaRequest.newBuilder().setSagaId(sagaId).build());
  }

  private static boolean isTerminal(SagaSnapshot snapshot) {
    SagaStatus status = snapshot.getStatus();
    return status == SagaStatus.SAGA_STATUS_COMPLETED
        || status == SagaStatus.SAGA_STATUS_COMPENSATED
        || status == SagaStatus.SAGA_STATUS_ESCALATED;
  }

  private static boolean isRetryable(Status.Code code) {
    return code == Status.Code.UNAVAILABLE
        || code == Status.Code.CANCELLED
        || code == Status.Code.DEADLINE_EXCEEDED;
  }

  /** Throws {@link SagaTimeoutException} when the overall client deadline (if any) has elapsed. */
  private void guardDeadline(long loopDeadlineNanos) {
    if (loopDeadlineNanos != 0L && System.nanoTime() >= loopDeadlineNanos) {
      throw new SagaTimeoutException("Saga did not reach a terminal state within the deadline");
    }
  }

  /**
   * The stub for one loop call: with no client deadline it blocks (matching embedded); otherwise
   * its per-call deadline is the remaining loop budget (the server still clamps each call to its
   * long-poll window).
   */
  private SagaServiceBlockingStub callWithin(long loopDeadlineNanos) {
    if (loopDeadlineNanos == 0L) {
      return stub;
    }
    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(loopDeadlineNanos - System.nanoTime());
    return stub.withDeadlineAfter(Math.max(1L, remainingMillis), TimeUnit.MILLISECONDS);
  }

  private static void backoff(int attempt) {
    long exp = BACKOFF_BASE_MILLIS << Math.min(attempt, BACKOFF_MAX_SHIFT);
    long capped = Math.min(BACKOFF_CAP_MILLIS, exp);
    // Jitter in [capped/2, capped], derived from the clock (no security-sensitive RNG needed), to
    // avoid a thundering herd when many clients retry a recovered daemon at once.
    long half = capped / 2L;
    long jitter = half == 0L ? 0L : Math.floorMod(System.nanoTime(), half + 1L);
    try {
      Thread.sleep(half + jitter);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SagaRuntimeException("Interrupted while waiting to retry a saga RPC", e);
    }
  }

  // --------------------------------------------------------------------------
  // internals
  // --------------------------------------------------------------------------

  /**
   * Starts the saga {@code async=true} and returns immediately with the saga id, without waiting
   * for a terminal state. Backs all four non-blocking {@code startAsync} overloads; the {@code
   * void} ones simply ignore the returned id (which, for a client-supplied id, the caller already
   * holds).
   */
  private String startAsynchronously(
      @Nullable String sagaId, String name, @Nullable String version, Map<String, Object> input) {
    try {
      return stub().startSaga(buildRequest(sagaId, name, version, input, true)).getSagaId();
    } catch (StatusRuntimeException e) {
      throw mapStartException(e, name, version, sagaId);
    }
  }

  private SagaServiceBlockingStub stub() {
    return defaultDeadlineMillis > 0L
        ? stub.withDeadlineAfter(defaultDeadlineMillis, TimeUnit.MILLISECONDS)
        : stub;
  }

  private static StartSagaRequest buildRequest(
      @Nullable String sagaId,
      String name,
      @Nullable String version,
      Map<String, Object> input,
      boolean async) {
    StartSagaRequest.Builder builder =
        StartSagaRequest.newBuilder()
            .setName(name)
            .setAsync(async)
            .setInputJson(toInputJson(input));
    if (sagaId != null) {
      builder.setSagaId(sagaId);
    }
    if (version != null) {
      builder.setVersion(version);
    }
    return builder.build();
  }

  private static ByteString toInputJson(Map<String, Object> input) {
    if (input.isEmpty()) {
      return ByteString.EMPTY;
    }
    try {
      ByteString.Output output = ByteString.newOutput();
      OBJECT_MAPPER.writeValue(output, input);
      return output.toByteString();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to serialize saga input to JSON", e);
    }
  }

  private static UnsupportedOperationException callbackUnsupported() {
    return new UnsupportedOperationException(
        "SagaCallback over gRPC requires a server-streaming WatchSaga RPC; not yet available — "
            + "start asynchronously and poll getStateSnapshot, or use embedded mode");
  }

  /**
   * Maps a start-call {@link StatusRuntimeException} to the api exception. {@code NOT_FOUND} means
   * the <i>definition</i> wasn't found (vs a missing saga on getStateSnapshot); {@code
   * ALREADY_EXISTS} can only arise on the client-supplied-id overloads. Inverse of the daemon
   * {@code GrpcErrorMapper} — the wire status code is the contract.
   */
  private RuntimeException mapStartException(
      StatusRuntimeException e,
      String name,
      @Nullable String version,
      @Nullable String clientSagaId) {
    Status.Code code = e.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return version == null
          ? new SagaDefinitionNotFoundException(name)
          : new SagaDefinitionNotFoundException(name, version);
    }
    if (code == Status.Code.ALREADY_EXISTS) {
      return alreadyExists(clientSagaId, e);
    }
    return mapCommon(e);
  }

  private RuntimeException alreadyExists(@Nullable String clientSagaId, StatusRuntimeException e) {
    if (clientSagaId == null) {
      // Server-generated ids do not collide; an ALREADY_EXISTS without a client id is unexpected.
      return new SagaRuntimeException(
          "Unexpected ALREADY_EXISTS for a server-generated saga id", e);
    }
    SagaStateSnapshot existing;
    try {
      // The conflict response carries no snapshot — re-fetch it (the only extra round-trip, and
      // only on the rare conflict) so the exception faithfully carries the existing state.
      existing = getStateSnapshot(clientSagaId);
    } catch (RuntimeException refetchFailure) {
      // Cannot build a SagaAlreadyExistsException without the snapshot; surface the conflict as the
      // primary cause and attach the refetch failure as suppressed for debugging context.
      SagaRuntimeException conflict =
          new SagaRuntimeException(
              "Saga '" + clientSagaId + "' already exists, but fetching its current state failed",
              e);
      conflict.addSuppressed(refetchFailure);
      return conflict;
    }
    return new SagaAlreadyExistsException(clientSagaId, existing);
  }

  private static RuntimeException mapCommon(StatusRuntimeException e) {
    Status status = e.getStatus();
    String description = status.getDescription();
    switch (status.getCode()) {
      case INVALID_ARGUMENT:
        return new IllegalArgumentException(description == null ? "Invalid request" : description);
      case DEADLINE_EXCEEDED:
        return new SagaTimeoutException(
            description == null ? "Saga RPC deadline exceeded" : description, e);
      case UNAVAILABLE:
        return new SagaUnavailableException(
            description == null ? "Saga service temporarily unavailable" : description, e);
      default:
        return new SagaRuntimeException(
            "Saga RPC failed ("
                + status.getCode()
                + ")"
                + (description == null ? "" : ": " + description),
            e);
    }
  }

  /** Builder for {@link GrpcSagaOrchestratorClient}. */
  public static final class Builder {

    @Nullable private String target;
    private boolean useTls = false;
    private long defaultDeadlineMillis = 0L;

    private Builder() {}

    public Builder target(String target) {
      this.target = Objects.requireNonNull(target, "target must not be null");
      return this;
    }

    /** Plaintext transport (the default; appropriate for an isolated in-cluster network). */
    public Builder usePlaintext() {
      this.useTls = false;
      return this;
    }

    /**
     * Enables TLS. Server-side TLS termination is not yet supported, so today this is
     * forward-compat (e.g. connecting through a TLS-terminating mesh/proxy). {@link #build()} fails
     * fast if the JRE lacks ALPN (on Java 8, use 8u252+ or add {@code
     * netty-tcnative-boringssl-static}).
     */
    public Builder useTransportSecurity() {
      this.useTls = true;
      return this;
    }

    /**
     * A default per-call deadline (ms) applied to the blocking start/get calls; {@code 0} disables.
     */
    public Builder defaultDeadlineMillis(long defaultDeadlineMillis) {
      if (defaultDeadlineMillis < 0L) {
        throw new IllegalArgumentException("defaultDeadlineMillis must not be negative");
      }
      this.defaultDeadlineMillis = defaultDeadlineMillis;
      return this;
    }

    public GrpcSagaOrchestratorClient build() {
      String resolvedTarget = Objects.requireNonNull(target, "target must be set");
      ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(resolvedTarget);
      if (useTls) {
        if (!alpnAvailable()) {
          throw new IllegalStateException(
              "TLS requested but ALPN is unavailable on this JRE. On Java 8, use 8u252+ or add "
                  + "netty-tcnative-boringssl-static; otherwise use plaintext (in-cluster).");
        }
        channelBuilder.useTransportSecurity();
      } else {
        channelBuilder.usePlaintext();
      }
      ManagedChannel channel = channelBuilder.build();
      return new GrpcSagaOrchestratorClient(
          SagaServiceGrpc.newBlockingStub(channel), channel, defaultDeadlineMillis);
    }

    /**
     * Best-effort ALPN check: {@code SSLEngine.getApplicationProtocol} exists from Java 9 and was
     * backported to 8u252. Conservative — a bundled {@code tcnative} may provide ALPN where the JDK
     * does not — but it gives a clear, early failure for the common pre-8u252 case.
     */
    private static boolean alpnAvailable() {
      try {
        SSLEngine.class.getMethod("getApplicationProtocol");
        return true;
      } catch (NoSuchMethodException e) {
        return false;
      }
    }
  }
}
