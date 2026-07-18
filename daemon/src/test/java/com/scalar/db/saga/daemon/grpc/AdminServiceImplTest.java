package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.daemon.security.SagaAuthRequest;
import com.scalar.db.saga.daemon.security.SagaAuthenticationException;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.daemon.security.SagaSecurityProvider;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.OperatorContext;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.AdminServiceGrpc.AdminServiceBlockingStub;
import com.scalar.db.saga.rpc.InterventionRequest;
import com.scalar.db.saga.rpc.ListSagasRequest;
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
import org.mockito.ArgumentCaptor;

/**
 * Exercises {@link AdminServiceImpl} over an in-process gRPC server wrapped in the real {@link
 * SagaSecurityInterceptor} (with a role-header stub provider), so the tests cover routing, RBAC,
 * operator injection, and exception→status mapping together.
 *
 * <p>Negative authorization is the priority surface: the destructive admin RPCs must be unreachable
 * without {@code saga:admin}.
 */
class AdminServiceImplTest {

  private static final long DRIVE_DEADLINE_MILLIS = 1_000L;
  private static final Metadata.Key<String> ROLE_HEADER =
      Metadata.Key.of("x-test-role", Metadata.ASCII_STRING_MARSHALLER);
  private static final Instant TS = Instant.parse("2026-07-18T10:00:00Z");

  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<Server> servers = new ArrayList<>();
  private SagaAdminService adminService;
  private DefaultSagaOrchestrator orchestrator;
  private ManagedChannel channel;

  @BeforeEach
  void setUp() throws IOException {
    adminService = mock(SagaAdminService.class);
    orchestrator = mock(DefaultSagaOrchestrator.class);
    when(orchestrator.adminService()).thenReturn(adminService);
    when(orchestrator.adminService(any(OperatorContext.class), anyLong())).thenReturn(adminService);
    channel = startServer(new SagaSecurityInterceptor(new RoleHeaderProvider()));
  }

  @AfterEach
  void tearDown() {
    channels.forEach(ManagedChannel::shutdownNow);
    servers.forEach(Server::shutdownNow);
  }

  private ManagedChannel startServer(@Nullable SagaSecurityInterceptor interceptor)
      throws IOException {
    AdminServiceImpl impl = new AdminServiceImpl(orchestrator, DRIVE_DEADLINE_MILLIS);
    String name = InProcessServerBuilder.generateName();
    InProcessServerBuilder builder = InProcessServerBuilder.forName(name).directExecutor();
    if (interceptor == null) {
      builder.addService(impl);
    } else {
      builder.addService(ServerInterceptors.intercept(impl, interceptor));
    }
    servers.add(builder.build().start());
    ManagedChannel newChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    channels.add(newChannel);
    return newChannel;
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot("s-1", "order-saga", status, "owner", "v1", TS, TS);
  }

  private AdminServiceBlockingStub stub(@Nullable String role) {
    return stub(channel, role);
  }

  private static AdminServiceBlockingStub stub(ManagedChannel channel, @Nullable String role) {
    Metadata metadata = new Metadata();
    if (role != null) {
      metadata.put(ROLE_HEADER, role);
    }
    return AdminServiceGrpc.newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  // --- negative authorization (the priority surface) -------------------------

  @Test
  void recoverSaga_writeRoleGiven_permissionDenied() {
    assertCode(
        () ->
            stub("write")
                .recoverSaga(
                    InterventionRequest.newBuilder().setSagaId("s-1").setReason("x").build()),
        Status.Code.PERMISSION_DENIED);
  }

  @Test
  void resetEscalatedBulk_writeRoleGiven_permissionDenied() {
    assertCode(
        () -> stub("write").resetEscalatedBulk(bulkRequest()), Status.Code.PERMISSION_DENIED);
  }

  @Test
  void listSagas_noCredentialGiven_unauthenticated() {
    assertCode(
        () -> stub(null).listSagas(ListSagasRequest.getDefaultInstance()),
        Status.Code.UNAUTHENTICATED);
  }

  // --- operator injection ----------------------------------------------------

  @Test
  void recoverSaga_adminRoleGiven_attributesToAuthenticatedPrincipal() {
    // Arrange
    when(adminService.recoverSaga(eq("s-1"), any())).thenReturn(snapshot(SagaStatus.COMPENSATED));

    // Act
    stub("admin")
        .recoverSaga(InterventionRequest.newBuilder().setSagaId("s-1").setReason("stuck").build());

    // Assert — the operator context handed to the factory yields the authenticated principal, and
    // the drive deadline is the configured one (no client deadline set in-process)
    ArgumentCaptor<OperatorContext> operator = ArgumentCaptor.forClass(OperatorContext.class);
    verify(orchestrator).adminService(operator.capture(), eq(DRIVE_DEADLINE_MILLIS));
    assertThat(operator.getValue().currentOperator()).isEqualTo("root");
    verify(adminService).recoverSaga("s-1", "stuck");
  }

  @Test
  void listSagas_adminRoleGiven_mapsPage() {
    // Arrange
    when(adminService.listSagas(any()))
        .thenReturn(new SagaPage<>(List.of(snapshot(SagaStatus.ESCALATED)), "next-token"));

    // Act
    var response = stub("admin").listSagas(ListSagasRequest.getDefaultInstance());

    // Assert
    assertThat(response.getSagasCount()).isEqualTo(1);
    assertThat(response.getSagas(0).getSagaId()).isEqualTo("s-1");
    assertThat(response.getNextPageToken()).isEqualTo("next-token");
  }

  // --- exception -> status mapping -------------------------------------------

  @Test
  void recoverSaga_wrongState_failedPrecondition() {
    when(adminService.recoverSaga(eq("s-1"), any()))
        .thenThrow(
            new SagaStatePreconditionException(
                "s-1", SagaStatePreconditionException.Code.SAGA_WRONG_STATE, "wrong state"));
    assertCode(
        () ->
            stub("admin")
                .recoverSaga(
                    InterventionRequest.newBuilder().setSagaId("s-1").setReason("x").build()),
        Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void forceComplete_lostCas_aborted() {
    when(adminService.forceComplete(eq("s-1"), any()))
        .thenThrow(new SagaConcurrentModificationException("s-1"));
    assertCode(
        () ->
            stub("admin")
                .forceComplete(
                    InterventionRequest.newBuilder().setSagaId("s-1").setReason("x").build()),
        Status.Code.ABORTED);
  }

  @Test
  void recoverSaga_withoutInterceptor_internal() throws IOException {
    // A bare service with no interceptor has no identity on the context; the mutation must refuse
    // (a wiring bug) rather than attribute the intervention to nobody.
    ManagedChannel bare = startServer(null);
    assertCode(
        () ->
            stub(bare, "admin")
                .recoverSaga(
                    InterventionRequest.newBuilder().setSagaId("s-1").setReason("x").build()),
        Status.Code.INTERNAL);
  }

  private static com.scalar.db.saga.rpc.ResetEscalatedBulkRequest bulkRequest() {
    return com.scalar.db.saga.rpc.ResetEscalatedBulkRequest.newBuilder().setReason("x").build();
  }

  private static void assertCode(
      org.assertj.core.api.ThrowableAssert.ThrowingCallable call, Status.Code expected) {
    assertThatThrownBy(call)
        .isInstanceOf(StatusRuntimeException.class)
        .extracting(e -> ((StatusRuntimeException) e).getStatus().getCode())
        .isEqualTo(expected);
  }

  /** A stub provider mapping an {@code x-test-role} metadata header to an identity. */
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
