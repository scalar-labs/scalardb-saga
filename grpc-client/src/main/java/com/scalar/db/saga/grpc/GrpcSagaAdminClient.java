package com.scalar.db.saga.grpc;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.AdminServiceGrpc.AdminServiceBlockingStub;
import com.scalar.db.saga.rpc.InterventionRequest;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLEngine;
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

  private static final long CLOSE_TIMEOUT_SECONDS = 5L;

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
    ownedChannel.shutdown();
    try {
      if (!ownedChannel.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        ownedChannel.shutdownNow();
      }
    } catch (InterruptedException e) {
      ownedChannel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private AdminServiceBlockingStub stub() {
    return defaultDeadlineMillis > 0L
        ? stub.withDeadlineAfter(defaultDeadlineMillis, TimeUnit.MILLISECONDS)
        : stub;
  }

  // --------------------------------------------------------------------------
  // Error mapping — the inverse of the daemon's GrpcErrorMapper
  // --------------------------------------------------------------------------

  /**
   * Maps a single-saga mutation failure to its api exception. The saga-scoped codes — {@code
   * NOT_FOUND}, the wrong-state {@code FAILED_PRECONDITION}, and the lost-CAS {@code ABORTED} —
   * reconstruct the exception with {@code sagaId}; everything else routes through {@link
   * #mapCommon}.
   */
  private static RuntimeException mapMutation(StatusRuntimeException e, String sagaId) {
    Status.Code code = e.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return new SagaNotFoundException(sagaId);
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
   * Reconstructs the wrong-state exception. The daemon sends the machine-readable code name (e.g.
   * {@code SAGA_WRONG_STATE}) as the status description, so the client recovers the {@link
   * SagaStatePreconditionException.Code} a caller switches on. An unrecognized description degrades
   * to {@link SagaRuntimeException} rather than guessing a code.
   */
  private static RuntimeException preconditionException(String sagaId, StatusRuntimeException e) {
    String description = e.getStatus().getDescription();
    if (description == null) {
      return mapCommon(e);
    }
    try {
      SagaStatePreconditionException.Code code =
          SagaStatePreconditionException.Code.valueOf(description);
      return new SagaStatePreconditionException(
          sagaId, code, "Saga '" + sagaId + "' is not in a state that allows this operation");
    } catch (IllegalArgumentException unknownCode) {
      return mapCommon(e);
    }
  }

  private static RuntimeException mapCommon(StatusRuntimeException e) {
    Status status = e.getStatus();
    String description = status.getDescription();
    switch (status.getCode()) {
      case INVALID_ARGUMENT:
        return new IllegalArgumentException(description == null ? "Invalid request" : description);
      case DEADLINE_EXCEEDED:
        return new SagaTimeoutException(
            description == null ? "Admin RPC deadline exceeded" : description, e);
      case UNAVAILABLE:
        return new SagaUnavailableException(
            description == null ? "Admin service temporarily unavailable" : description, e);
      default:
        return new SagaRuntimeException(
            "Admin RPC failed ("
                + status.getCode()
                + ")"
                + (description == null ? "" : ": " + description),
            e);
    }
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
      AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel);
      if (callCredentials != null) {
        stub = stub.withCallCredentials(callCredentials);
      }
      return new GrpcSagaAdminClient(stub, channel, defaultDeadlineMillis);
    }

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
