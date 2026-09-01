package com.scalar.db.saga.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
import com.scalar.db.saga.server.security.SagaAuthRequest;
import com.scalar.db.saga.server.security.SagaAuthUnavailableException;
import com.scalar.db.saga.server.security.SagaAuthenticationException;
import com.scalar.db.saga.server.security.SagaIdentity;
import com.scalar.db.saga.server.security.SagaRole;
import com.scalar.db.saga.server.security.SagaSecurityProvider;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SagaSecurityInterceptor} over a real in-process gRPC transport with a stub
 * provider keyed on an {@code x-test-role} metadata header: absent → {@code UNAUTHENTICATED};
 * {@code read}/{@code write}/{@code admin} → an identity holding that role. Confirms RBAC per
 * method and the auth-failure &rarr; {@code Status} mapping.
 */
class SagaSecurityInterceptorTest {

  private static final Metadata.Key<String> ROLE_HEADER =
      Metadata.Key.of("x-test-role", Metadata.ASCII_STRING_MARSHALLER);

  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<Server> servers = new ArrayList<>();
  private ManagedChannel channel;

  @BeforeEach
  void setUp() throws IOException {
    SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
    when(orchestrator.getStateSnapshot(any())).thenReturn(snapshot("s-1", SagaStatus.RUNNING));
    when(orchestrator.startAsync(anyString(), anyMap())).thenReturn("gen-1");
    String name = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(
                ServerInterceptors.intercept(
                    new SagaServiceImpl(orchestrator, cap -> Math.min(60_000L, cap)),
                    new SagaSecurityInterceptor(new RoleHeaderProvider())))
            .build()
            .start();
    servers.add(server);
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    channels.add(channel);
  }

  @AfterEach
  void tearDown() {
    channels.forEach(ManagedChannel::shutdownNow);
    servers.forEach(Server::shutdownNow);
  }

  @Test
  void getSaga_readRole_isAllowed() {
    // Act
    SagaSnapshot response =
        stub("read").getSaga(GetSagaRequest.newBuilder().setSagaId("s-1").build());

    // Assert — reached the handler
    assertThat(response.getSagaId()).isEqualTo("s-1");
  }

  @Test
  void getSaga_noCredential_isUnauthenticated() {
    // Act
    StatusRuntimeException error = callGetExpectingError(stub(null));

    // Assert — the refusal also carries the code, like every other daemon response
    assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(ErrorInfos.errorInfo(error).getReason())
        .isEqualTo(SagaErrorCode.UNAUTHENTICATED.code());
  }

  @Test
  void getSaga_adminRole_isAllowedViaHierarchy() {
    // Act — ADMIN implies READ
    SagaSnapshot response =
        stub("admin").getSaga(GetSagaRequest.newBuilder().setSagaId("s-1").build());

    // Assert
    assertThat(response.getSagaId()).isEqualTo("s-1");
  }

  @Test
  void startSaga_readRole_isPermissionDenied() {
    // Act — StartSaga requires WRITE; a read-only caller is denied
    StatusRuntimeException error =
        catchThrowableOfType(
            StatusRuntimeException.class, () -> stub("read").startSaga(startByName("transfer")));

    // Assert
    assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(ErrorInfos.errorInfo(error).getReason())
        .isEqualTo(SagaErrorCode.PERMISSION_DENIED.code());
  }

  @Test
  void startSaga_writeRole_isAllowed() {
    // Act — WRITE satisfies StartSaga; the call reaches the handler and returns the snapshot
    SagaSnapshot response = stub("write").startSaga(startByName("transfer"));

    // Assert (the id is the mocked getStateSnapshot's; reaching it proves auth passed)
    assertThat(response.getSagaId()).isEqualTo("s-1");
  }

  @Test
  void getSaga_providerThrowsUnexpectedly_isInternal() throws IOException {
    // Arrange — a provider that fails with a non-authentication RuntimeException (a bug or an
    // unwrapped transient error), not a rejected credential.
    SagaSecurityProvider throwing =
        new SagaSecurityProvider() {
          @Override
          public SagaIdentity authenticate(SagaAuthRequest request) {
            throw new IllegalStateException("boom");
          }

          @Override
          public String name() {
            return "throwing";
          }
        };

    // Act
    StatusRuntimeException error = callGetExpectingError(stubFor(throwing));

    // Assert — mapped to INTERNAL (fail closed), not UNAUTHENTICATED, so a server-side fault is not
    // reported to the caller as a bad credential. INTERNAL_ERROR, not an auth-specific code: all
    // that is known here is that the server broke unexpectedly, and a more specific code would
    // hand probing callers an oracle on the auth subsystem.
    assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    assertThat(ErrorInfos.errorInfo(error).getReason())
        .isEqualTo(SagaErrorCode.INTERNAL_ERROR.code());
  }

  @Test
  void getSaga_providerUnavailable_isUnavailable() throws IOException {
    // Arrange — a provider that cannot verify the credential because it is unavailable (e.g. the
    // JWKS endpoint is unreachable), not because the credential is bad.
    SagaSecurityProvider unavailable =
        new SagaSecurityProvider() {
          @Override
          public SagaIdentity authenticate(SagaAuthRequest request) {
            throw new SagaAuthUnavailableException("jwks unreachable", new RuntimeException());
          }

          @Override
          public String name() {
            return "unavailable";
          }
        };

    // Act
    StatusRuntimeException error = callGetExpectingError(stubFor(unavailable));

    // Assert — retryable UNAVAILABLE, not UNAUTHENTICATED (not a bad credential) or INTERNAL.
    assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(ErrorInfos.errorInfo(error).getReason())
        .isEqualTo(SagaErrorCode.SERVICE_UNAVAILABLE.code());
  }

  private SagaServiceBlockingStub stubFor(SagaSecurityProvider provider) throws IOException {
    SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
    when(orchestrator.getStateSnapshot(any())).thenReturn(snapshot("s-1", SagaStatus.RUNNING));
    String name = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(
                ServerInterceptors.intercept(
                    new SagaServiceImpl(orchestrator, cap -> Math.min(60_000L, cap)),
                    new SagaSecurityInterceptor(provider)))
            .build()
            .start();
    servers.add(server);
    ManagedChannel newChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    channels.add(newChannel);
    return SagaServiceGrpc.newBlockingStub(newChannel);
  }

  private static StatusRuntimeException callGetExpectingError(SagaServiceBlockingStub stub) {
    return catchThrowableOfType(
        StatusRuntimeException.class,
        () -> stub.getSaga(GetSagaRequest.newBuilder().setSagaId("s-1").build()));
  }

  private SagaServiceBlockingStub stub(@Nullable String role) {
    Metadata metadata = new Metadata();
    if (role != null) {
      metadata.put(ROLE_HEADER, role);
    }
    return SagaServiceGrpc.newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private static StartSagaRequest startByName(String name) {
    return StartSagaRequest.newBuilder().setName(name).setAsync(true).build();
  }

  private static SagaStateSnapshot snapshot(String sagaId, SagaStatus status) {
    Instant now = Instant.ofEpochSecond(1_700_000_000L);
    return new SagaStateSnapshot(sagaId, "transfer", status, "owner-1", "v1", now, now);
  }

  /**
   * A stub provider: an absent {@code x-test-role} metadata header is unauthenticated; {@code
   * read}/{@code write}/{@code admin} map to an identity holding exactly that role.
   */
  private static final class RoleHeaderProvider implements SagaSecurityProvider {
    @Override
    public SagaIdentity authenticate(SagaAuthRequest request) {
      String role = request.header("x-test-role").orElse(null);
      if (role == null) {
        throw new SagaAuthenticationException("missing x-test-role");
      }
      return switch (role) {
        case "read" -> SagaIdentity.of("reader", Set.of(SagaRole.READ));
        case "write" -> SagaIdentity.of("writer", Set.of(SagaRole.WRITE));
        case "admin" -> SagaIdentity.of("root", EnumSet.allOf(SagaRole.class));
        default -> throw new SagaAuthenticationException("unknown role: " + role);
      };
    }

    @Override
    public String name() {
      return "role-header-stub";
    }
  }
}
