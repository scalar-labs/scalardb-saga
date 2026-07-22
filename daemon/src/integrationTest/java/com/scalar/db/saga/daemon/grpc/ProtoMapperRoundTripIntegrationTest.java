package com.scalar.db.saga.daemon.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.daemon.security.SagaIdentity;
import com.scalar.db.saga.daemon.security.SagaRole;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.grpc.GrpcSagaAdminClient;
import com.scalar.db.saga.grpc.GrpcSagaOrchestratorClient;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Guards that the daemon's server-side {@link ProtoMappers} and the client's {@code
 * ClientProtoMappers} stay exact mirror images. The two are hand-maintained in separate modules
 * (the Java 21 daemon and the Java 8 client) and each module's unit tests exercise only its own
 * side, so a wire field wired in one direction but not the other would leave both suites green
 * while the field silently drops on the round trip.
 *
 * <p>This is the only place both real mappers meet: a fully populated api value goes out through
 * the real gRPC server ({@code toProto}) and comes back through the real client ({@code
 * fromProto}), and {@code equals()} fails the build if any field did not survive. The engine is
 * mocked so the value under test is fully controlled — notably {@code operator}, which only an
 * admin-intervention event carries — while the mappers, the wire, and both adapters are real.
 */
class ProtoMapperRoundTripIntegrationTest {

  private DefaultSagaOrchestrator orchestrator;
  private SagaAdminService adminService;
  private Server server;
  private GrpcSagaOrchestratorClient appClient;
  private GrpcSagaAdminClient adminClient;

  @BeforeEach
  void setUp() throws IOException {
    orchestrator = mock(DefaultSagaOrchestrator.class);
    adminService = mock(SagaAdminService.class);
    when(orchestrator.adminService()).thenReturn(adminService);
    when(orchestrator.adminService(any(), anyLong())).thenReturn(adminService);

    server =
        NettyServerBuilder.forPort(0)
            .addService(new SagaServiceImpl(orchestrator, 0L, 0L))
            .addService(
                ServerInterceptors.intercept(new AdminServiceImpl(orchestrator, 0L), identity()))
            .build()
            .start();

    String target = "localhost:" + server.getPort();
    appClient = GrpcSagaOrchestratorClient.create(target);
    adminClient = GrpcSagaAdminClient.newBuilder().target(target).build();
  }

  @AfterEach
  void tearDown() {
    if (appClient != null) {
      appClient.close();
    }
    if (adminClient != null) {
      adminClient.close();
    }
    if (server != null) {
      server.shutdownNow();
    }
  }

  @Test
  void getSagaDetail_roundTrip_preservesEverySnapshotAndTimelineField() {
    SagaDetail expected =
        new SagaDetail(
            snapshot(SagaStatus.ESCALATED),
            List.of(
                // A step event carries stepIndex, stepName, and a failure detail.
                new TimelineEvent(
                    Instant.ofEpochSecond(1_700_000_100L, 111),
                    "STEP_FAILED",
                    2,
                    "debit",
                    null,
                    "downstream returned 500",
                    null),
                // A status event from an admin intervention carries resultingStatus, a reason
                // detail, and operator — operator is the field only this shape populates.
                new TimelineEvent(
                    Instant.ofEpochSecond(1_700_000_200L, 222),
                    "SAGA_RESET",
                    null,
                    null,
                    SagaStatus.COMPENSATING,
                    "operator reset the saga",
                    "alice")));
    when(orchestrator.getSagaDetail("s-1")).thenReturn(expected);

    assertThat(appClient.getSagaDetail("s-1")).isEqualTo(expected);
  }

  @Test
  void listSagas_roundTrip_preservesPageAndSnapshotFields() {
    // Arrange — a fully populated query so a dropped field on either mapper direction fails
    // equals() on the captured server-received query.
    SagaQuery sent =
        SagaQuery.newBuilder()
            .status(SagaStatus.ESCALATED)
            .updatedAfter(Instant.ofEpochSecond(1_700_000_000L, 111))
            .updatedBefore(Instant.ofEpochSecond(1_700_000_500L, 222))
            .pageSize(250)
            .pageToken("opaque-token-42")
            .build();
    SagaPage<SagaStateSnapshot> expected =
        new SagaPage<>(List.of(snapshot(SagaStatus.RUNNING)), "next-token");
    when(adminService.listSagas(any())).thenReturn(expected);

    // Act
    SagaPage<SagaStateSnapshot> actual = adminClient.listSagas(sent);

    // Assert — request direction: the captured server-side SagaQuery mirrors the client-side one.
    ArgumentCaptor<SagaQuery> queryCaptor = ArgumentCaptor.forClass(SagaQuery.class);
    verify(adminService).listSagas(queryCaptor.capture());
    assertThat(queryCaptor.getValue()).isEqualTo(sent);

    // Assert — response direction.
    assertThat(actual.getItems()).isEqualTo(expected.getItems());
    assertThat(actual.getNextPageToken()).isEqualTo("next-token");
  }

  @Test
  void resetEscalatedBulk_roundTrip_preservesResultAndSkipFields() {
    // Arrange — the client deliberately drops the status filter on the bulk path (the sweep is
    // pinned to ESCALATED server-side), so it is not set here; every other query field, plus the
    // reason, must round-trip through both mappers.
    SagaQuery sent =
        SagaQuery.newBuilder()
            .updatedAfter(Instant.ofEpochSecond(1_700_000_000L, 111))
            .updatedBefore(Instant.ofEpochSecond(1_700_000_500L, 222))
            .pageSize(250)
            .pageToken("opaque-token-42")
            .build();
    ResetResult expected =
        new ResetResult(
            3,
            List.of(
                new ResetResult.SkippedSaga(
                    "s-9", ResetResult.SkipReason.CORRUPT_EVENT_STREAM, "corrupt event row"),
                new ResetResult.SkippedSaga(
                    "s-8", ResetResult.SkipReason.CONCURRENT_MODIFICATION, null)),
            "more");
    when(adminService.resetEscalated(any(SagaQuery.class), any())).thenReturn(expected);

    // Act
    ResetResult actual = adminClient.resetEscalated(sent, "operator sweep");

    // Assert — request direction: SagaQuery + reason both arrive intact.
    ArgumentCaptor<SagaQuery> queryCaptor = ArgumentCaptor.forClass(SagaQuery.class);
    ArgumentCaptor<String> reasonCaptor = ArgumentCaptor.forClass(String.class);
    verify(adminService).resetEscalated(queryCaptor.capture(), reasonCaptor.capture());
    assertThat(queryCaptor.getValue()).isEqualTo(sent);
    assertThat(reasonCaptor.getValue()).isEqualTo("operator sweep");

    // Assert — response direction.
    assertThat(actual).isEqualTo(expected);
  }

  /**
   * A snapshot with every wire-carried field set. {@code ownerId} is the empty string on purpose:
   * it is deliberately not put on the wire (a server-internal recovery field), so the client fills
   * {@code ""} and a round trip of a non-empty owner would not be equal — the empty value is what
   * actually round-trips.
   */
  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(
        "s-1",
        "order-saga",
        status,
        "",
        "v3",
        Instant.ofEpochSecond(1_700_000_000L, 1),
        Instant.ofEpochSecond(1_700_000_050L, 2));
  }

  /**
   * A minimal interceptor that injects an authenticated identity onto the gRPC context, so the
   * admin mutation path's server-injected operator lookup succeeds without standing up a real
   * security provider.
   */
  private static ServerInterceptor identity() {
    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Context context =
            Context.current()
                .withValue(
                    SagaSecurityInterceptor.IDENTITY,
                    SagaIdentity.of("operator", Set.of(SagaRole.ADMIN)));
        return Contexts.interceptCall(context, call, headers, next);
      }
    };
  }
}
