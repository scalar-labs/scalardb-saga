package com.scalar.db.saga.grpc;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.ExceptionRegistry;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.AdminServiceGrpc.AdminServiceBlockingStub;
import com.scalar.db.saga.rpc.InterventionRequest;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * A remote {@link SagaAdminService} — the operator control plane — backed by a gRPC connection to
 * the saga daemon's {@code AdminService}. It implements the same interface as the embedded admin
 * service, so operator tooling runs unchanged embedded or remote.
 *
 * <p>This is the <b>operator</b> client, distinct from {@link GrpcSagaOrchestratorClient} (the
 * application client): its RPCs require the {@code saga:admin} role. Against a daemon with a real
 * security provider the caller must present a credential — set one with {@link
 * Builder#callCredentials(CallCredentials)}. For the built-in providers, which read a header,
 * {@link #staticHeaderCredentials(String, String)} builds the credential (an API key, or a bearer
 * token as {@code Authorization}/{@code "Bearer …"}). Without a credential every RPC is rejected
 * {@code UNAUTHENTICATED} — except against the {@code noop} provider, which grants all roles. The
 * operator identity every mutation is attributed to is resolved server-side from that credential,
 * never from a method argument; {@code reason} is the only audit input the caller supplies.
 *
 * <p>Thread-safe and intended to be created once and shared: a single instance holds one {@link
 * ManagedChannel} that multiplexes all calls. Call {@link #close()} to release it.
 */
@ThreadSafe
public final class GrpcSagaAdminClient implements SagaAdminService {

  private final AdminServiceBlockingStub stub;
  @Nullable private final ManagedChannel ownedChannel;
  private final long defaultDeadlineMillis;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Visible for testing — inject a stub over an in-process channel; {@code close()} is a no-op. */
  GrpcSagaAdminClient(AdminServiceBlockingStub stub, @Nullable ManagedChannel ownedChannel) {
    this(stub, ownedChannel, 0L);
  }

  GrpcSagaAdminClient(
      AdminServiceBlockingStub stub,
      @Nullable ManagedChannel ownedChannel,
      long defaultDeadlineMillis) {
    this.stub = Objects.requireNonNull(stub, "stub must not be null");
    this.ownedChannel = ownedChannel;
    this.defaultDeadlineMillis = defaultDeadlineMillis;
  }

  /** Creates a client connected to {@code target} over plaintext (the in-cluster default). */
  public static GrpcSagaAdminClient create(String target) {
    return newBuilder().target(target).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builds {@link CallCredentials} that attach a fixed header value to every call — the credential
   * form the built-in security providers expect. Use it for an API key ({@code
   * staticHeaderCredentials("X-API-Key", key)}, matching the {@code apikey} provider's configured
   * header) or a fixed bearer token ({@code staticHeaderCredentials("Authorization", "Bearer " +
   * jwt)}). For a token that must refresh, supply a custom {@link CallCredentials} instead.
   *
   * @param headerName the metadata header the daemon's provider reads
   * @param headerValue the credential value
   * @return call credentials that present {@code headerValue} under {@code headerName}
   */
  public static CallCredentials staticHeaderCredentials(String headerName, String headerValue) {
    Objects.requireNonNull(headerName, "headerName must not be null");
    Objects.requireNonNull(headerValue, "headerValue must not be null");
    Metadata.Key<String> key = Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    return new CallCredentials() {
      @Override
      public void applyRequestMetadata(
          RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        Metadata headers = new Metadata();
        headers.put(key, headerValue);
        applier.apply(headers);
      }
    };
  }

  // --------------------------------------------------------------------------
  // Reads
  // --------------------------------------------------------------------------

  @Override
  public SagaPage<SagaStateSnapshot> listSagas(SagaQuery query) {
    Objects.requireNonNull(query, "query must not be null");
    try {
      return ClientProtoMappers.fromProto(
          stub().listSagas(ClientProtoMappers.toListSagasRequest(query)));
    } catch (StatusRuntimeException e) {
      throw mapCommon(e);
    }
  }

  // --------------------------------------------------------------------------
  // Mutations
  // --------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot recoverSaga(String sagaId, String reason) {
    return mutate(sagaId, reason, stub()::recoverSaga);
  }

  @Override
  public SagaStateSnapshot forceComplete(String sagaId, String reason) {
    return mutate(sagaId, reason, stub()::forceComplete);
  }

  @Override
  public SagaStateSnapshot resetEscalated(String sagaId, String reason) {
    return mutate(sagaId, reason, stub()::resetEscalated);
  }

  @Override
  public ResetResult resetEscalated(SagaQuery query, String reason) {
    Objects.requireNonNull(query, "query must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
    // The bulk wire request has no status field — the server pins the sweep to ESCALATED. Reject a
    // conflicting filter here so the same call that throws embedded does not turn into a valid
    // ESCALATED sweep remote (matching SagaAdminService's contract).
    SagaStatus status = query.getStatus();
    if (status != null && status != SagaStatus.ESCALATED) {
      throw new SagaIllegalArgumentException(
          "resetEscalated only sweeps ESCALATED sagas; conflicting status filter: " + status);
    }
    try {
      return ClientProtoMappers.fromProto(
          stub().resetEscalatedBulk(ClientProtoMappers.toResetEscalatedBulkRequest(query, reason)));
    } catch (StatusRuntimeException e) {
      throw mapCommon(e);
    }
  }

  /**
   * Runs a single-saga intervention: builds the {@link InterventionRequest}, calls {@code call},
   * maps the returned snapshot, and translates a gRPC failure to the api exception carrying {@code
   * sagaId}.
   */
  private SagaStateSnapshot mutate(String sagaId, String reason, SnapshotRpc call) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
    InterventionRequest request =
        InterventionRequest.newBuilder().setSagaId(sagaId).setReason(reason).build();
    try {
      return ClientProtoMappers.fromProto(call.apply(request));
    } catch (StatusRuntimeException e) {
      throw mapMutation(e, sagaId);
    }
  }

  @FunctionalInterface
  private interface SnapshotRpc {
    com.scalar.db.saga.rpc.SagaSnapshot apply(InterventionRequest request);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true) || ownedChannel == null) {
      return;
    }
    GrpcClientSupport.shutdown(ownedChannel);
  }

  /**
   * The stub for one call, with the default per-call deadline applied if configured. Rejects a call
   * on a closed client with a terminal {@link IllegalStateException} so a caller that retries on
   * the transient {@link com.scalar.db.saga.exception.SagaUnavailableException} does not loop on
   * channel-shutdown errors that will never recover.
   */
  private AdminServiceBlockingStub stub() {
    if (closed.get()) {
      throw new IllegalStateException("admin client has been closed");
    }
    return defaultDeadlineMillis > 0L
        ? stub.withDeadlineAfter(defaultDeadlineMillis, TimeUnit.MILLISECONDS)
        : stub;
  }

  // --------------------------------------------------------------------------
  // Error mapping — the inverse of the daemon's GrpcErrorMapper
  // --------------------------------------------------------------------------

  /**
   * Maps a single-saga mutation failure to its api exception. When the daemon attached an {@link
   * ErrorInfo}, {@link ExceptionRegistry} reconstructs the typed exception from its wire code +
   * metadata (inverting what {@code GrpcErrorMapper} put on the wire). Statuses without an {@code
   * ErrorInfo} fall through to transport-level dispatch: {@code NOT_FOUND} → {@code
   * SagaNotFoundException} using the caller-supplied id; {@code ABORTED} → {@code
   * SagaConcurrentModificationException}; everything else via {@link #mapCommon}.
   */
  private static RuntimeException mapMutation(StatusRuntimeException e, String sagaId) {
    SagaRuntimeException reconstructed = reconstructFromErrorInfo(e);
    if (reconstructed != null) {
      return reconstructed;
    }
    Status.Code code = e.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return new SagaNotFoundException(sagaId);
    }
    if (code == Status.Code.ABORTED) {
      return new SagaConcurrentModificationException(sagaId, e);
    }
    return mapCommon(e);
  }

  /**
   * Reconstructs a typed exception from the daemon's {@link ErrorInfo}, or returns {@code null} if
   * the response has no {@link ErrorInfo} (older daemon, intermediary stripped it, or a
   * transport-level failure that never reached the mapper).
   */
  private static @Nullable SagaRuntimeException reconstructFromErrorInfo(StatusRuntimeException e) {
    ErrorInfo info = errorInfo(e);
    if (info == null) {
      return null;
    }
    return ExceptionRegistry.reconstruct(info.getReason(), info.getMetadataMap());
  }

  /** The first {@link ErrorInfo} detail the daemon attached to {@code e}, or {@code null}. */
  private static @Nullable ErrorInfo errorInfo(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);
    if (status == null) {
      return null;
    }
    for (Any detail : status.getDetailsList()) {
      if (detail.is(ErrorInfo.class)) {
        try {
          return detail.unpack(ErrorInfo.class);
        } catch (InvalidProtocolBufferException malformed) {
          return null;
        }
      }
    }
    return null;
  }

  private static RuntimeException mapCommon(StatusRuntimeException e) {
    return GrpcClientSupport.mapCommon(e);
  }

  /** Builder for {@link GrpcSagaAdminClient}, mirroring the application client's builder. */
  public static final class Builder {

    @Nullable private String target;
    private boolean useTls = false;
    private long defaultDeadlineMillis = 0L;
    @Nullable private CallCredentials callCredentials;

    private Builder() {}

    public Builder target(String target) {
      this.target = Objects.requireNonNull(target, "target must not be null");
      return this;
    }

    /**
     * The credential presented on every call, resolved server-side to the operator identity.
     * Against a daemon with a real provider this is required; {@link
     * #staticHeaderCredentials(String, String)} builds one for the header-based built-in providers.
     */
    public Builder callCredentials(CallCredentials callCredentials) {
      this.callCredentials =
          Objects.requireNonNull(callCredentials, "callCredentials must not be null");
      return this;
    }

    /** Plaintext transport (the default; appropriate for an isolated in-cluster network). */
    public Builder usePlaintext() {
      this.useTls = false;
      return this;
    }

    /**
     * Enables TLS. {@link #build()} fails fast if the JRE lacks ALPN (on Java 8, use 8u252+ or add
     * {@code netty-tcnative-boringssl-static}).
     */
    public Builder useTransportSecurity() {
      this.useTls = true;
      return this;
    }

    /** A default per-call deadline (ms) applied to every admin call; {@code 0} disables. */
    public Builder defaultDeadlineMillis(long defaultDeadlineMillis) {
      if (defaultDeadlineMillis < 0L) {
        throw new IllegalArgumentException("defaultDeadlineMillis must not be negative");
      }
      this.defaultDeadlineMillis = defaultDeadlineMillis;
      return this;
    }

    public GrpcSagaAdminClient build() {
      String resolvedTarget = Objects.requireNonNull(target, "target must be set");
      ManagedChannel channel = GrpcClientSupport.openChannel(resolvedTarget, useTls);
      AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel);
      if (callCredentials != null) {
        stub = stub.withCallCredentials(callCredentials);
      }
      return new GrpcSagaAdminClient(stub, channel, defaultDeadlineMillis);
    }
  }
}
