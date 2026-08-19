package com.scalar.db.saga.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaDetailRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.SagaStatus;
import com.scalar.db.saga.rpc.StartSagaRequest;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *
 * <p>TLS against a private CA (e.g. a cert-manager-issued server certificate), dialing through a
 * port-forward:
 *
 * <pre>{@code
 * GrpcSagaOrchestratorClient client =
 *     GrpcSagaOrchestratorClient.newBuilder()
 *         .target("127.0.0.1:12051") // the port-forward
 *         .useTransportSecurity()
 *         .trustCaCertificate(Paths.get("/etc/saga/ca.crt"))
 *         .overrideAuthority("saga-server.internal") // the name the certificate carries
 *         .build();
 * }</pre>
 *
 * <p>On Java 8, TLS needs ALPN: use 8u252 or later. The SDK's default transport (grpc-netty-shaded)
 * also bundles tcnative, which provides ALPN on older JREs; an embedder who swaps in plain
 * grpc-netty needs {@code netty-tcnative-boringssl-static} instead.
 */
@ThreadSafe
public final class GrpcSagaOrchestratorClient implements SagaOrchestrator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Capped, jittered exponential backoff between retries of a retryable transport failure. */
  private static final long BACKOFF_BASE_MILLIS = 50L;

  private static final long BACKOFF_CAP_MILLIS = 2_000L;
  private static final int BACKOFF_MAX_SHIFT = 16;

  private final SagaServiceBlockingStub stub;
  @Nullable private final ManagedChannel ownedChannel;
  private final long defaultDeadlineMillis;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Visible for testing — inject a stub over an in-process channel; {@code close()} is then a
   * no-op.
   */
  GrpcSagaOrchestratorClient(SagaServiceBlockingStub stub, @Nullable ManagedChannel ownedChannel) {
    this(stub, ownedChannel, 0L);
  }

  /**
   * Visible for testing — inject a stub plus a nonzero default deadline over an in-process channel.
   */
  GrpcSagaOrchestratorClient(
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
      return ClientProtoMappers.fromProto(snapshot);
    } catch (StatusRuntimeException e) {
      throw mapSagaCall(e, sagaId);
    }
  }

  @Override
  public SagaDetail getSagaDetail(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    try {
      com.scalar.db.saga.rpc.SagaDetail detail =
          stub().getSagaDetail(GetSagaDetailRequest.newBuilder().setSagaId(sagaId).build());
      return ClientProtoMappers.fromProto(detail);
    } catch (StatusRuntimeException e) {
      throw mapSagaCall(e, sagaId);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true) || ownedChannel == null) {
      return;
    }
    GrpcClientSupport.shutdown(ownedChannel);
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
          throwIfClosed(sagaId);
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
          throwIfClosed(sagaId);
          guardDeadline(loopDeadlineNanos);
          backoff(retries++);
          continue;
        }
        // NOT_FOUND here means the saga was purged/TTL'd between polls.
        throw mapSagaCall(e, sagaId);
      }
    }
    return snapshot;
  }

  private SagaSnapshot getSagaSnapshot(String sagaId, long loopDeadlineNanos) {
    try {
      return callWithin(loopDeadlineNanos)
          .getSaga(GetSagaRequest.newBuilder().setSagaId(sagaId).build());
    } catch (StatusRuntimeException e) {
      // Map like getStateSnapshot, so a refetch failure surfaces as a Saga* exception rather than
      // leaking a raw gRPC StatusRuntimeException out of start().
      throw mapSagaCall(e, sagaId);
    }
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

  /**
   * Throws when the overall client deadline (if any) has elapsed. SAGA_AWAIT_TIMEOUT, not
   * REQUEST_TIMEOUT: every request so far succeeded and the saga keeps running — only the
   * wait-for-terminal budget expired, so the caller should poll by ID rather than re-send.
   */
  private void guardDeadline(long loopDeadlineNanos) {
    if (loopDeadlineNanos != 0L && System.nanoTime() >= loopDeadlineNanos) {
      throw SagaTimeoutException.awaitExpired();
    }
  }

  /**
   * Aborts the retry loop if {@link #close()} was called concurrently. Without this, a blocking
   * {@code start()} with no client deadline would retry the (now retryable) channel-shutdown errors
   * forever and never return.
   */
  private void throwIfClosed(String sagaId) {
    if (closed.get()) {
      throw new IllegalStateException(
          "Saga client was closed before saga " + sagaId + " reached a terminal state");
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
      throw new SagaRuntimeException(SagaErrorCode.REQUEST_ABORTED, ErrorMetadata.of(), e);
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
    // ALREADY_EXISTS is handled ahead of reconstruction: SAGA_ALREADY_EXISTS is deliberately not
    // reconstructible, because SagaAlreadyExistsException needs the existing snapshot and the wire
    // metadata has no room for it. Only this path can re-fetch it.
    if (code == Status.Code.ALREADY_EXISTS) {
      return alreadyExists(clientSagaId, e);
    }
    SagaRuntimeException reconstructed = GrpcClientSupport.reconstruct(e);
    if (reconstructed != null) {
      return reconstructed;
    }
    if (code == Status.Code.NOT_FOUND) {
      return version == null
          ? SagaDefinitionNotFoundException.byName(name)
          : SagaDefinitionNotFoundException.byNameAndVersion(name, version);
    }
    return GrpcClientSupport.mapTransport(e);
  }

  private RuntimeException alreadyExists(@Nullable String clientSagaId, StatusRuntimeException e) {
    if (clientSagaId == null) {
      // Server-generated ids do not collide; an ALREADY_EXISTS without a client id is a protocol
      // invariant violation.
      return new SagaRuntimeException(SagaErrorCode.INTERNAL_ERROR, ErrorMetadata.of(), e);
    }
    SagaStateSnapshot existing;
    try {
      // The conflict response carries no snapshot — re-fetch it (the only extra round-trip, and
      // only on the rare conflict) so the exception faithfully carries the existing state.
      existing = getStateSnapshot(clientSagaId);
    } catch (RuntimeException refetchFailure) {
      // Cannot build a SagaAlreadyExistsException without the snapshot (its schema requires one).
      // Surface the conflict via the raw SAGA_ALREADY_EXISTS code so callers keying on
      // getErrorCode() still see it, and attach the refetch failure as suppressed for debugging.
      SagaRuntimeException conflict =
          new SagaRuntimeException(
              SagaErrorCode.SAGA_ALREADY_EXISTS, ErrorMetadata.of("saga_id", clientSagaId), e);
      conflict.addSuppressed(refetchFailure);
      return conflict;
    }
    return new SagaAlreadyExistsException(clientSagaId, existing);
  }

  /**
   * Maps a saga-instance RPC failure ({@code getSaga}/{@code awaitSaga}) to the api exception. The
   * daemon's {@link com.google.rpc.ErrorInfo} wins when present, since it names the exact code.
   * Without one, {@code NOT_FOUND} means the saga id is gone — purged, TTL'd, or never existed — vs
   * the start path, where {@code NOT_FOUND} means the <i>definition</i> is missing (see {@link
   * #mapStartException}); everything else routes through {@code GrpcClientSupport.mapTransport}.
   */
  private static RuntimeException mapSagaCall(StatusRuntimeException e, String sagaId) {
    SagaRuntimeException reconstructed = GrpcClientSupport.reconstruct(e);
    if (reconstructed != null) {
      return reconstructed;
    }
    if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
      return new SagaNotFoundException(sagaId);
    }
    return GrpcClientSupport.mapTransport(e);
  }

  /** Builder for {@link GrpcSagaOrchestratorClient}. */
  public static final class Builder {

    @Nullable private String target;
    private boolean useTls = false;
    @Nullable private Path trustCaCertPath;
    @Nullable private String overrideAuthority;
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
     * Enables TLS — against the daemon's native TLS ({@code scalar.db.saga.server.tls.enabled}) or
     * a TLS-terminating mesh/proxy in front of it. The server certificate is validated against the
     * JVM's default trust store unless {@link #trustCaCertificate(Path)} narrows it. {@link
     * #build()} fails fast if neither the JRE nor a loaded tcnative provides ALPN (on Java 8, use
     * 8u252+; the default grpc-netty-shaded transport bundles tcnative).
     */
    public Builder useTransportSecurity() {
      this.useTls = true;
      return this;
    }

    /**
     * Trusts only the CA certificate (PEM; concatenated certificates allowed) at {@code caCertPath}
     * for this channel, replacing the JVM's default trust store. For servers whose certificate a
     * public CA did not issue — a cert-manager or Vault private CA — where default trust rejects
     * the handshake. Requires TLS: {@link #build()} fails if the channel is left plaintext, rather
     * than silently ignoring a setting that says the caller expected encryption. The file is read
     * at {@link #build()}, so a bad path fails there naming the file, not at the first RPC as an
     * opaque {@code UNAVAILABLE}.
     *
     * @param caCertPath path to the PEM CA certificate to trust
     * @return this builder
     */
    public Builder trustCaCertificate(Path caCertPath) {
      this.trustCaCertPath = Objects.requireNonNull(caCertPath, "caCertPath must not be null");
      return this;
    }

    /**
     * Validates the server certificate against {@code authority} instead of the dialed address —
     * for dialing by IP or through a port-forward while the certificate names the service's DNS
     * name. Independent of {@link #trustCaCertificate(Path)}, and legitimate without TLS too (gRPC
     * also routes on the authority).
     *
     * @param authority the name to validate the server certificate against
     * @return this builder
     */
    public Builder overrideAuthority(String authority) {
      this.overrideAuthority = Objects.requireNonNull(authority, "authority must not be null");
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
      ManagedChannel channel =
          GrpcClientSupport.openChannel(resolvedTarget, useTls, trustCaCertPath, overrideAuthority);
      return new GrpcSagaOrchestratorClient(
          SagaServiceGrpc.newBlockingStub(channel), channel, defaultDeadlineMillis);
    }
  }
}
