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
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.AdminServiceGrpc.AdminServiceBlockingStub;
import com.scalar.db.saga.rpc.InterventionRequest;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.nio.file.Path;
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
      throw new IllegalArgumentException(
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
   * Maps a single-saga mutation failure to its api exception. The daemon carries a machine-readable
   * reason in an {@link ErrorInfo} detail: a {@code NOT_FOUND} whose reason is {@code
   * SAGA_DEFINITION_NOT_FOUND} reconstructs {@link SagaDefinitionNotFoundException} (the saga's
   * definition is unregistered — the caller must re-register it), otherwise it is a missing saga.
   * The wrong-state {@code FAILED_PRECONDITION} and lost-CAS {@code ABORTED} carry {@code sagaId};
   * everything else routes through {@link #mapCommon}.
   */
  private static RuntimeException mapMutation(StatusRuntimeException e, String sagaId) {
    Status.Code code = e.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return notFoundException(sagaId, errorInfo(e));
    }
    if (code == Status.Code.FAILED_PRECONDITION) {
      return preconditionException(sagaId, e);
    }
    if (code == Status.Code.ABORTED) {
      return new SagaConcurrentModificationException(sagaId, e);
    }
    return mapCommon(e);
  }

  /**
   * Distinguishes the two {@code NOT_FOUND} shapes by the {@link ErrorInfo} reason. Only {@code
   * SAGA_DEFINITION_NOT_FOUND} (with a {@code sagaName}) reconstructs {@link
   * SagaDefinitionNotFoundException}; a missing reason, name, or any other reason is a missing
   * saga.
   */
  private static RuntimeException notFoundException(String sagaId, @Nullable ErrorInfo info) {
    if (info != null && "SAGA_DEFINITION_NOT_FOUND".equals(info.getReason())) {
      String sagaName = info.getMetadataMap().get("sagaName");
      if (sagaName != null) {
        String version = info.getMetadataMap().get("version");
        return version == null
            ? new SagaDefinitionNotFoundException(sagaName)
            : new SagaDefinitionNotFoundException(sagaName, version);
      }
    }
    return new SagaNotFoundException(sagaId);
  }

  /**
   * Reconstructs the wrong-state exception. The daemon sends the machine-readable code name (e.g.
   * {@code SAGA_WRONG_STATE}) as the {@link ErrorInfo} reason, so the client recovers the {@link
   * SagaStatePreconditionException.Code} a caller switches on. An absent or unrecognized reason
   * degrades to {@link SagaRuntimeException} rather than guessing a code.
   */
  private static RuntimeException preconditionException(String sagaId, StatusRuntimeException e) {
    ErrorInfo info = errorInfo(e);
    if (info == null) {
      return mapCommon(e);
    }
    try {
      SagaStatePreconditionException.Code code =
          SagaStatePreconditionException.Code.valueOf(info.getReason());
      return new SagaStatePreconditionException(
          sagaId, code, "Saga '" + sagaId + "' is not in a state that allows this operation");
    } catch (IllegalArgumentException unknownCode) {
      return mapCommon(e);
    }
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
    return GrpcClientSupport.mapCommon(e, "Admin");
  }

  /** Builder for {@link GrpcSagaAdminClient}, mirroring the application client's builder. */
  public static final class Builder {

    @Nullable private String target;
    private boolean useTls = false;
    @Nullable private Path trustCaCertPath;
    @Nullable private String overrideAuthority;
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
     * Enables TLS — against the daemon's native TLS ({@code scalar.db.saga.server.tls.enabled}) or
     * a TLS-terminating mesh/proxy in front of it. The server certificate is validated against the
     * JVM's default trust store unless {@link #trustCaCertificate(Path)} narrows it. {@link
     * #build()} fails fast if the JRE lacks ALPN (on Java 8, use 8u252+ or add {@code
     * netty-tcnative-boringssl-static}).
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
      ManagedChannel channel =
          GrpcClientSupport.openChannel(resolvedTarget, useTls, trustCaCertPath, overrideAuthority);
      AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel);
      if (callCredentials != null) {
        stub = stub.withCallCredentials(callCredentials);
      }
      return new GrpcSagaAdminClient(stub, channel, defaultDeadlineMillis);
    }
  }
}
