package com.scalar.db.saga.daemon.grpc;

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
import com.scalar.db.saga.daemon.security.SagaAuthRequest;
import com.scalar.db.saga.daemon.security.SagaAuthenticationException;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
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
                    new SagaServiceImpl(orchestrator, 0L, 60_000L),
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

    // Assert
    assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
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
  }

  @Test
  void startSaga_writeRole_isAllowed() {
    // Act — WRITE satisfies StartSaga; the call reaches the handler and returns the snapshot
    SagaSnapshot response = stub("write").startSaga(startByName("transfer"));

    // Assert (the id is the mocked getStateSnapshot's; reaching it proves auth passed)
    assertThat(response.getSagaId()).isEqualTo("s-1");
  }

  @Test
  void requiredRoleFor_readMethods_returnRead() {
    assertThat(SagaSecurityInterceptor.requiredRoleFor("GetSaga")).isEqualTo(SagaRole.READ);
    assertThat(SagaSecurityInterceptor.requiredRoleFor("AwaitSaga")).isEqualTo(SagaRole.READ);
  }

  @Test
  void requiredRoleFor_writeOrUnknownMethods_returnWrite() {
    assertThat(SagaSecurityInterceptor.requiredRoleFor("StartSaga")).isEqualTo(SagaRole.WRITE);
    assertThat(SagaSecurityInterceptor.requiredRoleFor("FutureMethod")).isEqualTo(SagaRole.WRITE);
    assertThat(SagaSecurityInterceptor.requiredRoleFor(null)).isEqualTo(SagaRole.WRITE);
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
