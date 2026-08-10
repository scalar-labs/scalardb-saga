package com.scalar.db.saga.grpc;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.rpc.AdminServiceGrpc;
import com.scalar.db.saga.rpc.InterventionRequest;
import com.scalar.db.saga.rpc.ListSagasRequest;
import com.scalar.db.saga.rpc.ListSagasResponse;
import com.scalar.db.saga.rpc.ResetEscalatedBulkRequest;
import com.scalar.db.saga.rpc.SagaSnapshot;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link GrpcSagaAdminClient} against an in-process fake {@code AdminService}: request
 * marshaling, response mapping, and the translation of gRPC {@link Status} codes back to the api
 * exceptions (the inverse of the daemon's {@code GrpcErrorMapper}).
 */
class GrpcSagaAdminClientTest {

  private FakeAdminService fake;
  private Server server;
  private ManagedChannel channel;
  private GrpcSagaAdminClient client;

  @BeforeEach
  void setUp() throws IOException {
    fake = new FakeAdminService();
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).directExecutor().addService(fake).build().start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    client = new GrpcSagaAdminClient(AdminServiceGrpc.newBlockingStub(channel), null);
  }

  @AfterEach
  void tearDown() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  private static SagaSnapshot wireSnapshot(
      String sagaId, com.scalar.db.saga.rpc.SagaStatus status) {
    return SagaSnapshot.newBuilder()
        .setSagaId(sagaId)
        .setName("order-saga")
        .setStatus(status)
        .setDefinitionVersion("v1")
        .setCreatedAt(Timestamp.newBuilder().setSeconds(1000L))
        .setUpdatedAt(Timestamp.newBuilder().setSeconds(1000L))
        .build();
  }

  // --- reads -----------------------------------------------------------------

  @Test
  void listSagas_marshalsFilterAndMapsPage() {
    // Arrange
    fake.listResponse =
        ListSagasResponse.newBuilder()
            .addSagas(wireSnapshot("s-1", com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_ESCALATED))
            .setNextPageToken("next")
            .build();
    SagaQuery query =
        SagaQuery.newBuilder()
            .status(SagaStatus.ESCALATED)
            .updatedAfter(Instant.ofEpochSecond(500L))
            .pageSize(50)
            .build();

    // Act
    SagaPage<SagaStateSnapshot> page = client.listSagas(query);

    // Assert — the request carried the filter, and the response mapped back
    ListSagasRequest sent = requireNonNull(fake.lastList);
    assertThat(sent.getStatus()).isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_ESCALATED);
    assertThat(sent.getUpdatedAfter().getSeconds()).isEqualTo(500L);
    assertThat(sent.getPageSize()).isEqualTo(50);
    assertThat(page.getItems()).hasSize(1);
    assertThat(page.getItems().get(0).getSagaId()).isEqualTo("s-1");
    assertThat(page.getItems().get(0).getStatus()).isEqualTo(SagaStatus.ESCALATED);
    assertThat(page.getNextPageToken()).isEqualTo("next");
  }

  // --- single-saga mutations -------------------------------------------------

  @Test
  void recoverSaga_sendsSagaIdAndReasonAndMapsSnapshot() {
    fake.recoverResponse =
        wireSnapshot("s-1", com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATED);

    SagaStateSnapshot result = client.recoverSaga("s-1", "stuck downstream");

    InterventionRequest sent = requireNonNull(fake.lastIntervention);
    assertThat(sent.getSagaId()).isEqualTo("s-1");
    assertThat(sent.getReason()).isEqualTo("stuck downstream");
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void forceComplete_mapsSnapshot() {
    fake.forceCompleteResponse =
        wireSnapshot("s-1", com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPLETED);
    assertThat(client.forceComplete("s-1", "confirmed").getStatus())
        .isEqualTo(SagaStatus.COMPLETED);
  }

  @Test
  void resetEscalated_single_mapsSnapshot() {
    fake.resetResponse =
        wireSnapshot("s-1", com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATING);
    assertThat(client.resetEscalated("s-1", "retry").getStatus())
        .isEqualTo(SagaStatus.COMPENSATING);
  }

  // --- bulk reset ------------------------------------------------------------

  @Test
  void resetEscalated_bulk_marshalsAndMapsResult() {
    // Arrange
    fake.bulkResponse =
        com.scalar.db.saga.rpc.ResetResult.newBuilder()
            .setResetCount(3)
            .addSkipped(
                com.scalar.db.saga.rpc.SkippedSaga.newBuilder()
                    .setSagaId("s-9")
                    .setReason(
                        com.scalar.db.saga.rpc.SkipReason.SKIP_REASON_CONCURRENT_MODIFICATION))
            .addSkipped(
                com.scalar.db.saga.rpc.SkippedSaga.newBuilder()
                    .setSagaId("s-8")
                    .setReason(com.scalar.db.saga.rpc.SkipReason.SKIP_REASON_CORRUPT_EVENT_STREAM)
                    .setDetail("bad stream"))
            .setNextPageToken("more")
            .build();
    SagaQuery query = SagaQuery.newBuilder().pageSize(200).build();

    // Act
    ResetResult result = client.resetEscalated(query, "operator sweep");

    // Assert — the reason and paging went out; the itemized skips + reasons came back
    ResetEscalatedBulkRequest sent = requireNonNull(fake.lastBulk);
    assertThat(sent.getReason()).isEqualTo("operator sweep");
    assertThat(sent.getPageSize()).isEqualTo(200);
    assertThat(result.getResetCount()).isEqualTo(3);
    assertThat(result.getSkipped()).hasSize(2);
    assertThat(result.getSkipped().get(0).getReason())
        .isEqualTo(ResetResult.SkipReason.CONCURRENT_MODIFICATION);
    assertThat(result.getSkipped().get(1).getReason())
        .isEqualTo(ResetResult.SkipReason.CORRUPT_EVENT_STREAM);
    assertThat(result.getSkipped().get(1).getDetail()).isEqualTo("bad stream");
    assertThat(result.getNextPageToken()).isEqualTo("more");
  }

  @Test
  void resetEscalated_bulk_conflictingStatusGiven_throwsSagaIllegalArgumentBeforeRpc() {
    // Arrange
    SagaQuery query = SagaQuery.newBuilder().status(SagaStatus.COMPLETED).build();

    // Act + Assert — rejected client-side, before the wire request is ever built or sent
    assertThatThrownBy(() -> client.resetEscalated(query, "sweep"))
        .isInstanceOf(SagaIllegalArgumentException.class);
    assertThat(fake.lastBulk).isNull();
  }

  @Test
  void resetEscalated_bulk_escalatedStatusGiven_isAccepted() {
    // Arrange
    fake.bulkResponse = com.scalar.db.saga.rpc.ResetResult.newBuilder().setResetCount(1).build();
    SagaQuery query = SagaQuery.newBuilder().status(SagaStatus.ESCALATED).build();

    // Act
    ResetResult result = client.resetEscalated(query, "sweep");

    // Assert — an explicit ESCALATED filter is not a conflict; the sweep goes through
    assertThat(result.getResetCount()).isEqualTo(1);
    assertThat(fake.lastBulk).isNotNull();
  }

  // --- error mapping (inverse of GrpcErrorMapper) ----------------------------

  @Test
  void recoverSaga_notFound_throwsSagaNotFound() {
    fake.recoverError =
        statusWithReason(
            Status.Code.NOT_FOUND, SagaErrorCode.SAGA_NOT_FOUND.code(), Map.of("saga_id", "gone"));
    assertThatThrownBy(() -> client.recoverSaga("gone", "x"))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void recoverSaga_definitionNotFound_throwsSagaDefinitionNotFound() {
    // The saga exists but its definition is unregistered; the daemon distinguishes this NOT_FOUND
    // by the reason and carries the name and version so the client can reconstruct the exception.
    fake.recoverError =
        statusWithReason(
            Status.Code.NOT_FOUND,
            SagaErrorCode.SAGA_DEFINITION_VERSION_NOT_FOUND.code(),
            Map.of("saga_name", "orders", "version", "v2"));
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isInstanceOf(SagaDefinitionNotFoundException.class)
        .satisfies(
            e -> {
              SagaDefinitionNotFoundException d = (SagaDefinitionNotFoundException) e;
              assertThat(d.getSagaName()).isEqualTo("orders");
              assertThat(d.getVersion()).isEqualTo("v2");
            });
  }

  @Test
  void recoverSaga_failedPrecondition_reconstructsPreconditionWithCode() {
    // The daemon sends the SagaErrorCode.code() as the ErrorInfo reason and the exception's
    // metadata (saga_id, current_state, requested_operation) as ErrorInfo.metadata; the client
    // reconstructs the exception with the same code and metadata.
    Map<String, String> metadata = new java.util.LinkedHashMap<>();
    metadata.put("saga_id", "s-1");
    metadata.put("current_state", "RUNNING");
    metadata.put("requested_operation", "recover");
    fake.recoverError =
        statusWithReason(
            Status.Code.FAILED_PRECONDITION, SagaErrorCode.SAGA_WRONG_STATE.code(), metadata);
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isInstanceOf(SagaStatePreconditionException.class)
        .extracting(e -> ((SagaStatePreconditionException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
  }

  @Test
  void forceComplete_aborted_throwsConcurrentModification() {
    fake.forceCompleteError = Status.ABORTED.withDescription("conflict").asRuntimeException();
    assertThatThrownBy(() -> client.forceComplete("s-1", "x"))
        .isInstanceOf(SagaConcurrentModificationException.class);
  }

  @Test
  void listSagas_invalidArgumentWithoutErrorInfo_throwsSagaIllegalArgument() {
    // No ErrorInfo, so there is nothing to reconstruct and the transport status decides. The
    // daemon's description is the validation detail, so it is passed through.
    fake.listError = Status.INVALID_ARGUMENT.withDescription("bad page token").asRuntimeException();
    assertThatThrownBy(() -> client.listSagas(SagaQuery.newBuilder().build()))
        .isInstanceOf(SagaIllegalArgumentException.class)
        .hasMessageContaining("bad page token");
  }

  @Test
  void listSagas_internalWithErrorInfo_reconstructsTheServerCode() {
    // Arrange — reads used to skip ErrorInfo reconstruction entirely, so an INTERNAL response fell
    // to the transport catch-all and reported UNRECOGNIZED_SERVER_ERROR ("upgrade the client SDK")
    // for a code this client understands. Mutations already reconstructed; the two paths disagreed
    // on identical wire input.
    fake.listError =
        statusWithReason(
            Status.Code.INTERNAL, SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED.code(), Map.of());

    // Act + Assert
    assertThatThrownBy(() -> client.listSagas(SagaQuery.newBuilder().build()))
        .isInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED);
  }

  @Test
  void resetEscalated_bulk_unavailable_throwsSagaUnavailable() {
    fake.bulkError = Status.UNAVAILABLE.withDescription("down").asRuntimeException();
    assertThatThrownBy(() -> client.resetEscalated(SagaQuery.newBuilder().build(), "x"))
        .isInstanceOf(SagaUnavailableException.class);
  }

  @Test
  void recoverSaga_permissionDenied_throwsSagaPermissionDenied() {
    fake.recoverError = Status.PERMISSION_DENIED.withDescription("denied").asRuntimeException();
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isInstanceOf(SagaPermissionDeniedException.class);
  }

  @Test
  void recoverSaga_unauthenticated_throwsSagaUnauthenticated() {
    fake.recoverError =
        Status.UNAUTHENTICATED.withDescription("no credential").asRuntimeException();
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isInstanceOf(SagaUnauthenticatedException.class);
  }

  @Test
  void recoverSaga_cancelled_throwsRequestAborted() {
    // Arrange — the blocking stub reports a caller interrupt (and an in-flight call killed by a
    // concurrent shutdownNow) as CANCELLED.
    fake.recoverError = Status.CANCELLED.withDescription("Thread interrupted").asRuntimeException();

    // Act + Assert — a caller-side abort, not the version skew the catch-all would report.
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.REQUEST_ABORTED);
  }

  @Test
  void recoverSaga_resourceExhaustedWithoutErrorInfo_throwsRateLimitExceeded() {
    // Arrange — an older daemon's rate limiter closes an over-limit call with a bare
    // RESOURCE_EXHAUSTED and no ErrorInfo.
    fake.recoverError = Status.RESOURCE_EXHAUSTED.withDescription("throttled").asRuntimeException();

    // Act + Assert — retryable rate limiting, not the version skew the catch-all would report;
    // the admin client has no retry loop, so this category is the caller's only backoff signal.
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.RATE_LIMIT_EXCEEDED);
  }

  @Test
  void recoverSaga_internalWithoutErrorInfo_throwsInternalError() {
    // Arrange — an older daemon's security interceptor reports an unexpected server fault as a
    // bare INTERNAL with no ErrorInfo.
    fake.recoverError = Status.INTERNAL.withDescription("boom").asRuntimeException();

    // Act + Assert — a server fault to escalate, not the version skew the catch-all would report.
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.INTERNAL_ERROR);
  }

  @Test
  void recoverSaga_afterClose_throwsIllegalStateException() {
    // Arrange
    client.close();

    // Act + Assert — a closed client rejects further calls terminally rather than surfacing the
    // channel-shutdown failure as a (retryable) SagaUnavailableException.
    assertThatThrownBy(() -> client.recoverSaga("s-1", "x"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void listSagas_perCallDeadlineElapses_throwsSagaTimeout() {
    // Arrange — a client with a short per-call deadline, against an RPC that never responds
    GrpcSagaAdminClient timedClient =
        new GrpcSagaAdminClient(AdminServiceGrpc.newBlockingStub(channel), null, 100L);
    fake.hangListSagas = true;

    // Act + Assert — the deadline fires client-side; DEADLINE_EXCEEDED maps to SagaTimeoutException
    assertThatThrownBy(() -> timedClient.listSagas(SagaQuery.newBuilder().build()))
        .isInstanceOf(SagaTimeoutException.class);
  }

  /**
   * Builds a {@link StatusRuntimeException} carrying an {@link ErrorInfo} detail, as the daemon's
   * {@code GrpcErrorMapper} does, so the client's reason-based mapping is exercised end to end.
   */
  private static StatusRuntimeException statusWithReason(
      Status.Code code, String reason, Map<String, String> metadata) {
    ErrorInfo info = ErrorInfo.newBuilder().setReason(reason).putAllMetadata(metadata).build();
    return StatusProto.toStatusRuntimeException(
        com.google.rpc.Status.newBuilder()
            .setCode(code.value())
            .addDetails(Any.pack(info))
            .build());
  }

  /** A fake admin service with per-method response/error fields and captured requests. */
  private static final class FakeAdminService extends AdminServiceGrpc.AdminServiceImplBase {
    @Nullable ListSagasResponse listResponse;
    @Nullable StatusRuntimeException listError;
    @Nullable ListSagasRequest lastList;
    boolean hangListSagas;

    @Nullable SagaSnapshot recoverResponse;
    @Nullable StatusRuntimeException recoverError;
    @Nullable SagaSnapshot forceCompleteResponse;
    @Nullable StatusRuntimeException forceCompleteError;
    @Nullable SagaSnapshot resetResponse;
    @Nullable InterventionRequest lastIntervention;

    com.scalar.db.saga.rpc.@Nullable ResetResult bulkResponse;
    @Nullable StatusRuntimeException bulkError;
    @Nullable ResetEscalatedBulkRequest lastBulk;

    @Override
    public void listSagas(ListSagasRequest request, StreamObserver<ListSagasResponse> observer) {
      lastList = request;
      if (hangListSagas) {
        return; // never respond, so the client's per-call deadline fires
      }
      respond(observer, listResponse, listError, ListSagasResponse.getDefaultInstance());
    }

    @Override
    public void recoverSaga(InterventionRequest request, StreamObserver<SagaSnapshot> observer) {
      lastIntervention = request;
      respond(observer, recoverResponse, recoverError, SagaSnapshot.getDefaultInstance());
    }

    @Override
    public void forceComplete(InterventionRequest request, StreamObserver<SagaSnapshot> observer) {
      lastIntervention = request;
      respond(
          observer, forceCompleteResponse, forceCompleteError, SagaSnapshot.getDefaultInstance());
    }

    @Override
    public void resetEscalated(InterventionRequest request, StreamObserver<SagaSnapshot> observer) {
      lastIntervention = request;
      respond(observer, resetResponse, null, SagaSnapshot.getDefaultInstance());
    }

    @Override
    public void resetEscalatedBulk(
        ResetEscalatedBulkRequest request,
        StreamObserver<com.scalar.db.saga.rpc.ResetResult> observer) {
      lastBulk = request;
      respond(
          observer,
          bulkResponse,
          bulkError,
          com.scalar.db.saga.rpc.ResetResult.getDefaultInstance());
    }

    private static <T> void respond(
        StreamObserver<T> observer,
        @Nullable T response,
        @Nullable StatusRuntimeException error,
        T fallback) {
      if (error != null) {
        observer.onError(error);
        return;
      }
      observer.onNext(response != null ? response : fallback);
      observer.onCompleted();
    }
  }
}
