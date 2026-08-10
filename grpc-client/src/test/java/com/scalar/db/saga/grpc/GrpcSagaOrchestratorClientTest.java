package com.scalar.db.saga.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPermissionDeniedException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnauthenticatedException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaDetailRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GrpcSagaOrchestratorClientTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final SagaCallback NO_OP_CALLBACK =
      new SagaCallback() {
        @Override
        public void onCompleted(SagaStateSnapshot snapshot) {}

        @Override
        public void onCompensated(SagaStateSnapshot snapshot) {}

        @Override
        public void onEscalated(SagaStateSnapshot snapshot) {}
      };

  private FakeSagaService fake;
  private Server server;
  private ManagedChannel channel;
  private GrpcSagaOrchestratorClient client;

  @BeforeEach
  void setUp() throws IOException {
    fake = new FakeSagaService();
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).directExecutor().addService(fake).build().start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    client = new GrpcSagaOrchestratorClient(SagaServiceGrpc.newBlockingStub(channel), null);
  }

  @AfterEach
  void tearDown() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  // ---------------------------------------------------------------------------
  // Request building / routing
  // ---------------------------------------------------------------------------

  @Test
  void start_byName_mintsClientIdBuildsSyncRequestAndReturnsThatId() {
    // Arrange — a terminal start response so the call returns after the first (fused) window.
    fake.startResponse = snapshot("ignored", SagaStatus.COMPLETED);

    // Act
    String sagaId = client.start("transfer", Map.of("k", "v"));

    // Assert — the client mints the id, sends it as the idempotency key, and returns that same id.
    assertThat(sagaId).isNotEmpty();
    assertThat(fake.lastStart().getName()).isEqualTo("transfer");
    assertThat(fake.lastStart().getAsync()).isFalse();
    assertThat(fake.lastStart().hasSagaId()).isTrue();
    assertThat(fake.lastStart().getSagaId()).isEqualTo(sagaId);
    assertThat(fake.lastStart().hasVersion()).isFalse();
  }

  @Test
  void startAsync_byName_setsAsyncTrue() {
    fake.startResponse = snapshot("gen-2", SagaStatus.RUNNING);
    String sagaId = client.startAsync("transfer", Map.of());
    assertThat(sagaId).isEqualTo("gen-2");
    assertThat(fake.lastStart().getAsync()).isTrue();
  }

  @Test
  void start_clientSuppliedId_setsSagaId() {
    fake.startResponse = snapshot("my-id", SagaStatus.COMPLETED);
    client.start("my-id", "transfer", Map.of());
    assertThat(fake.lastStart().hasSagaId()).isTrue();
    assertThat(fake.lastStart().getSagaId()).isEqualTo("my-id");
  }

  @Test
  void start_versioned_setsNameAndVersion() {
    fake.startResponse = snapshot("gen-3", SagaStatus.COMPLETED);
    client.start(new SagaDefinitionId("transfer", "v2"), Map.of());
    assertThat(fake.lastStart().getName()).isEqualTo("transfer");
    assertThat(fake.lastStart().hasVersion()).isTrue();
    assertThat(fake.lastStart().getVersion()).isEqualTo("v2");
  }

  @Test
  void start_withJsonInput_serializesMapPreservingLongPrecision() throws Exception {
    // 2^53+1 would round to a double under google.protobuf.Struct; bytes input_json keeps it a
    // Long.
    fake.startResponse = snapshot("gen-4", SagaStatus.RUNNING);
    client.startAsync("transfer", Map.of("to", "alice", "id", 9007199254740993L));
    @SuppressWarnings("unchecked")
    Map<String, Object> roundTripped =
        OBJECT_MAPPER.readValue(fake.lastStart().getInputJson().toByteArray(), Map.class);
    assertThat(roundTripped).containsEntry("to", "alice").containsEntry("id", 9007199254740993L);
  }

  // ---------------------------------------------------------------------------
  // Callback overloads deferred
  // ---------------------------------------------------------------------------

  @Test
  void startAsyncWithCallback_allFourOverloads_throwUnsupported() {
    SagaDefinitionId id = new SagaDefinitionId("transfer", "v2");
    assertThatThrownBy(() -> client.startAsync("transfer", Map.of(), NO_OP_CALLBACK))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> client.startAsync("sid", "transfer", Map.of(), NO_OP_CALLBACK))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> client.startAsync(id, Map.of(), NO_OP_CALLBACK))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> client.startAsync("sid", id, Map.of(), NO_OP_CALLBACK))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ---------------------------------------------------------------------------
  // Snapshot mapping
  // ---------------------------------------------------------------------------

  @Test
  void getStateSnapshot_mapsByNameWithEmptyOwner() {
    fake.getResponse = snapshot("s-9", SagaStatus.COMPENSATED);
    SagaStateSnapshot snapshot = client.getStateSnapshot("s-9");
    assertThat(snapshot.getSagaId()).isEqualTo("s-9");
    assertThat(snapshot.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(snapshot.getOwnerId()).isEmpty();
    assertThat(snapshot.getCreatedAt().getEpochSecond()).isEqualTo(1000L);
  }

  @Test
  void getSagaDetail_mapsSnapshotAndTimelineOptionals() {
    // Arrange — a step event (index/name set, status/operator unset) and a status event (the
    // inverse), so both the set and unset sides of every optional field are exercised
    fake.detailResponse =
        com.scalar.db.saga.rpc.SagaDetail.newBuilder()
            .setSaga(snapshot("s-9", SagaStatus.COMPENSATED))
            .addTimeline(
                com.scalar.db.saga.rpc.TimelineEvent.newBuilder()
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(1000L))
                    .setType("STEP_FAILED")
                    .setStepIndex(1)
                    .setStepName("credit")
                    .setDetail("gateway down"))
            .addTimeline(
                com.scalar.db.saga.rpc.TimelineEvent.newBuilder()
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(1000L))
                    .setType("SAGA_RECOVERING")
                    .setResultingStatus(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATING)
                    .setDetail("rolling back")
                    .setOperator("bob"))
            .build();

    // Act
    SagaDetail detail = client.getSagaDetail("s-9");

    // Assert — snapshot maps, and set/unset optionals round-trip to value/null
    assertThat(detail.getSnapshot().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(detail.getTimeline()).hasSize(2);

    TimelineEvent step = detail.getTimeline().get(0);
    assertThat(step.getStepIndex()).isEqualTo(1);
    assertThat(step.getStepName()).isEqualTo("credit");
    assertThat(step.getDetail()).isEqualTo("gateway down");
    assertThat(step.getResultingStatus()).isNull();
    assertThat(step.getOperator()).isNull();

    TimelineEvent status = detail.getTimeline().get(1);
    assertThat(status.getStepIndex()).isNull();
    assertThat(status.getResultingStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(status.getOperator()).isEqualTo("bob");
  }

  @Test
  void getSagaDetail_notFound_throwsSagaNotFound() {
    fake.detailError = Status.NOT_FOUND.withDescription("no saga").asRuntimeException();
    assertThatThrownBy(() -> client.getSagaDetail("missing"))
        .isInstanceOf(SagaNotFoundException.class);
  }

  // ---------------------------------------------------------------------------
  // Status -> exception mapping
  // ---------------------------------------------------------------------------

  @Test
  void getStateSnapshot_notFound_throwsSagaNotFound() {
    fake.getError = Status.NOT_FOUND.withDescription("no saga").asRuntimeException();
    assertThatThrownBy(() -> client.getStateSnapshot("missing"))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void getStateSnapshot_permissionDenied_throwsSagaPermissionDenied() {
    fake.getError = Status.PERMISSION_DENIED.withDescription("denied").asRuntimeException();
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isInstanceOf(SagaPermissionDeniedException.class);
  }

  @Test
  void getStateSnapshot_unauthenticated_throwsSagaUnauthenticated() {
    fake.getError = Status.UNAUTHENTICATED.withDescription("no credential").asRuntimeException();
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isInstanceOf(SagaUnauthenticatedException.class);
  }

  // CANCELLED is retryable on the synchronous start path, where an interrupt already surfaces as
  // REQUEST_ABORTED from backoff(); the mapping is asserted on a single-shot read, which reaches
  // mapCommon directly.
  @Test
  void getStateSnapshot_cancelled_throwsRequestAborted() {
    // Arrange — the blocking stub reports a caller interrupt (and an in-flight call killed by a
    // concurrent shutdownNow) as CANCELLED.
    fake.getError = Status.CANCELLED.withDescription("Thread interrupted").asRuntimeException();

    // Act + Assert — a caller-side abort, not the version skew the catch-all would report.
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.REQUEST_ABORTED);
  }

  @Test
  void start_notFound_throwsSagaDefinitionNotFound() {
    fake.startError = Status.NOT_FOUND.withDescription("no def").asRuntimeException();
    assertThatThrownBy(() -> client.start("unknown", Map.of()))
        .isInstanceOf(SagaDefinitionNotFoundException.class);
  }

  @Test
  void start_definitionInvalidWithErrorInfo_throwsSagaDefinitionException() {
    // Arrange — SagaOrchestrator.start declares @throws SagaDefinitionException, and the daemon
    // maps
    // it to INVALID_ARGUMENT with the exact code in the ErrorInfo. Before reconstruction reached
    // this path the client flattened every INVALID_ARGUMENT alike, so the declared type never
    // arrived and a caller's catch block could not fire.
    fake.startError =
        statusWithReason(
            Status.Code.INVALID_ARGUMENT,
            SagaErrorCode.INVALID_DEFINITION.code(),
            Map.of("saga_name", "transfer", "detail", "duplicate step name 'debit'"));

    // Act + Assert
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isInstanceOf(SagaDefinitionException.class)
        .extracting(e -> ((SagaDefinitionException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.INVALID_DEFINITION);
  }

  @Test
  void getStateSnapshot_internalWithErrorInfo_reconstructsTheServerCode() {
    // Arrange — INTERNAL has no transport-dispatch case, so it used to fall to the catch-all and
    // report UNRECOGNIZED_SERVER_ERROR ("upgrade the client SDK") even though the daemon sent a
    // code
    // this client understands.
    fake.getError =
        statusWithReason(
            Status.Code.INTERNAL, SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED.code(), Map.of());

    // Act + Assert
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED);
  }

  @Test
  void startAsync_unknownCodeWithUnavailableStatus_throwsSagaUnavailable() {
    // Arrange — a rolling upgrade: a newer daemon sends a code this client does not know, on a
    // retryable UNAVAILABLE. Degrading it to UNRECOGNIZED_SERVER_ERROR (CLIENT_ERROR) here used to
    // stop a caller keying retries on Category.RETRYABLE_SERVER_ERROR; the status the daemon set
    // correctly must win instead.
    fake.startError = statusWithReason(Status.Code.UNAVAILABLE, "DB-SAGA-99999", Map.of());

    // Act + Assert — classified by the transport status, not the unresolvable code.
    assertThatThrownBy(() -> client.startAsync("transfer", Map.of()))
        .isInstanceOf(SagaUnavailableException.class);
  }

  @Test
  void getStateSnapshot_serverSentUnrecognizedCode_keepsIt() {
    // Arrange — a genuine DB-SAGA-49999 from the server is a registered code, not a degradation,
    // so it must round-trip with the server's own metadata rather than fall to the status.
    fake.getError =
        statusWithReason(
            Status.Code.INTERNAL,
            SagaErrorCode.UNRECOGNIZED_SERVER_ERROR.code(),
            Map.of("server_value", "SOME_ENUM_VALUE"));

    // Act + Assert
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.UNRECOGNIZED_SERVER_ERROR);
  }

  @Test
  void getStateSnapshot_errorInfoGiven_attachesTheGrpcStatusAsCause() {
    // Arrange — the registry builds every exception cause-free, so without an explicit initCause
    // the
    // gRPC status (and its description and trailers) would be lost to anyone debugging.
    fake.getError =
        statusWithReason(Status.Code.INTERNAL, SagaErrorCode.INTERNAL_ERROR.code(), Map.of());

    // Act + Assert
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .hasCauseInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void start_alreadyExistsWithErrorInfo_stillRefetchesTheExistingSnapshot() {
    // Arrange — SAGA_ALREADY_EXISTS is deliberately not reconstructible, since the typed exception
    // needs the existing snapshot and the wire metadata has no room for it. So ALREADY_EXISTS must
    // stay ahead of reconstruction even when an ErrorInfo is present, or the refetch is skipped and
    // the caller gets a bare code instead of the snapshot.
    fake.startError =
        statusWithReason(
            Status.Code.ALREADY_EXISTS,
            SagaErrorCode.SAGA_ALREADY_EXISTS.code(),
            Map.of("saga_id", "my-id"));
    fake.getResponse = snapshot("my-id", SagaStatus.COMPLETED);

    // Act + Assert
    assertThatThrownBy(() -> client.start("my-id", "transfer", Map.of()))
        .isInstanceOfSatisfying(
            SagaAlreadyExistsException.class,
            e -> assertThat(e.getExisting().getStatus()).isEqualTo(SagaStatus.COMPLETED));
  }

  @Test
  void start_invalidArgumentWithoutErrorInfo_throwsSagaIllegalArgument() {
    fake.startError = Status.INVALID_ARGUMENT.withDescription("bad input").asRuntimeException();
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isInstanceOf(SagaIllegalArgumentException.class)
        .hasMessageContaining("bad input");
  }

  // UNAVAILABLE/DEADLINE_EXCEEDED are retryable on the synchronous start path, so the mapping is
  // asserted on the single-shot startAsync path (which surfaces them immediately).
  @Test
  void startAsync_unavailable_throwsSagaUnavailable() {
    fake.startError = Status.UNAVAILABLE.withDescription("down").asRuntimeException();
    assertThatThrownBy(() -> client.startAsync("transfer", Map.of()))
        .isInstanceOf(SagaUnavailableException.class);
  }

  @Test
  void startAsync_deadlineExceeded_throwsSagaTimeout() {
    fake.startError = Status.DEADLINE_EXCEEDED.withDescription("slow").asRuntimeException();
    assertThatThrownBy(() -> client.startAsync("transfer", Map.of()))
        .isInstanceOf(SagaTimeoutException.class);
  }

  @Test
  void start_internalWithoutErrorInfo_throwsInternalError() {
    // Arrange — an older daemon's security interceptor reports an unexpected server fault as a
    // bare INTERNAL with no ErrorInfo.
    fake.startError = Status.INTERNAL.withDescription("boom").asRuntimeException();

    // Act + Assert — a server fault to escalate, not the version skew the catch-all would report.
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.INTERNAL_ERROR);
  }

  @Test
  void getStateSnapshot_unknownWithoutErrorInfo_throwsInternalError() {
    // Arrange — bare UNKNOWN is what the gRPC server runtime emits when a failure escapes the
    // daemon's handlers entirely (an Error, or a fault in interceptor code).
    fake.getError = Status.UNKNOWN.withDescription("app error").asRuntimeException();

    // Act + Assert — a server fault to escalate, not the version skew the catch-all would report.
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.INTERNAL_ERROR);
  }

  @Test
  void getStateSnapshot_resourceExhaustedWithoutErrorInfo_throwsRateLimitExceeded() {
    // Arrange — an older daemon's rate limiter closes an over-limit call with a bare
    // RESOURCE_EXHAUSTED and no ErrorInfo.
    fake.getError = Status.RESOURCE_EXHAUSTED.withDescription("throttled").asRuntimeException();

    // Act + Assert — retryable rate limiting, not the version skew the catch-all would report.
    assertThatThrownBy(() -> client.getStateSnapshot("s-1"))
        .isExactlyInstanceOf(SagaRuntimeException.class)
        .extracting(e -> ((SagaRuntimeException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.RATE_LIMIT_EXCEEDED);
  }

  @Test
  void start_clientSuppliedIdAlreadyExists_throwsWithRefetchedExisting() {
    // Arrange — startSaga fails ALREADY_EXISTS; the client's follow-up GetSaga returns the
    // existing.
    fake.startError = Status.ALREADY_EXISTS.withDescription("dup").asRuntimeException();
    fake.getResponse = snapshot("my-id", SagaStatus.COMPLETED);

    // Act + Assert
    assertThatThrownBy(() -> client.start("my-id", "transfer", Map.of()))
        .isInstanceOfSatisfying(
            SagaAlreadyExistsException.class,
            e -> {
              assertThat(e.getSagaId()).isEqualTo("my-id");
              assertThat(e.getExisting().getStatus()).isEqualTo(SagaStatus.COMPLETED);
            });
  }

  // ---------------------------------------------------------------------------
  // Synchronous start blocks to terminal via the AwaitSaga loop
  // ---------------------------------------------------------------------------

  @Test
  void start_nonTerminalStartThenTerminalAwait_blocksUntilTerminal() {
    // Arrange — start returns RUNNING (window elapsed), then AwaitSaga reports terminal.
    fake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    fake.enqueueAwaitSnapshot(snapshot("ignored", SagaStatus.COMPLETED));

    // Act
    String sagaId = client.start("transfer", Map.of());

    // Assert — exactly one await, polling the same client-minted id the start carried.
    assertThat(fake.awaitCalls).isEqualTo(1);
    assertThat(fake.lastAwait().getSagaId()).isEqualTo(sagaId);
    assertThat(fake.lastStart().getSagaId()).isEqualTo(sagaId);
  }

  @Test
  void start_awaitRunningThenTerminal_pollsUntilTerminal() {
    // Arrange — start returns RUNNING; the first AwaitSaga is a gRPC-OK but still-RUNNING window,
    // the second reports terminal — exercises the OK-non-terminal "loop again, no backoff" branch.
    fake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    fake.enqueueAwaitSnapshot(snapshot("ignored", SagaStatus.RUNNING));
    fake.enqueueAwaitSnapshot(snapshot("ignored", SagaStatus.COMPLETED));

    // Act
    String sagaId = client.start("transfer", Map.of());

    // Assert — the OK-RUNNING return looped again with no backoff; two awaits, then terminal.
    assertThat(sagaId).isNotEmpty();
    assertThat(fake.awaitCalls).isEqualTo(2);
  }

  @Test
  void start_awaitResetThenTerminal_resumesLoop() {
    // Arrange — the first AwaitSaga is reset (UNAVAILABLE); the loop retries and gets terminal.
    fake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    fake.enqueueAwaitError(Status.UNAVAILABLE.withDescription("reset").asRuntimeException());
    fake.enqueueAwaitSnapshot(snapshot("ignored", SagaStatus.COMPENSATED));

    // Act
    String sagaId = client.start("transfer", Map.of());

    // Assert — the transport reset was absorbed; two await calls, then terminal.
    assertThat(sagaId).isNotEmpty();
    assertThat(fake.awaitCalls).isEqualTo(2);
  }

  @Test
  void start_firstStartResetThenSucceeds_retriesIdempotentlyWithSameId() {
    // Arrange — the first StartSaga is reset; the retry (same minted id) lands terminal.
    fake.enqueueStartError(Status.UNAVAILABLE.withDescription("reset").asRuntimeException());
    fake.enqueueStartSnapshot(snapshot("ignored", SagaStatus.COMPLETED));

    // Act
    String sagaId = client.start("transfer", Map.of());

    // Assert — no second saga: the retry reused the minted id, and no await was needed.
    assertThat(fake.lastStart().getSagaId()).isEqualTo(sagaId);
    assertThat(fake.awaitCalls).isZero();
  }

  @Test
  void start_retryStartSeesAlreadyExists_proceedsWithoutDoubleStart() {
    // Arrange — first StartSaga reset, retry returns ALREADY_EXISTS (our prior attempt landed);
    // the client fetches the existing terminal snapshot via GetSaga instead of failing.
    fake.enqueueStartError(Status.UNAVAILABLE.withDescription("reset").asRuntimeException());
    fake.enqueueStartError(Status.ALREADY_EXISTS.withDescription("ours").asRuntimeException());
    fake.getResponse = snapshot("ignored", SagaStatus.COMPLETED);

    // Act + Assert — resolves to the existing saga, no SagaAlreadyExistsException surfaced.
    String sagaId = client.start("transfer", Map.of());
    assertThat(sagaId).isNotEmpty();
    assertThat(fake.awaitCalls).isZero();
  }

  @Test
  void start_retryAlreadyExistsThenRefetchNotFound_throwsSagaNotFound() {
    // Retry sees ALREADY_EXISTS (our earlier attempt landed), but the follow-up GetSaga finds the
    // saga purged. The refetch failure must surface as a Saga* type, not a raw
    // StatusRuntimeException.
    fake.enqueueStartError(Status.UNAVAILABLE.withDescription("reset").asRuntimeException());
    fake.enqueueStartError(Status.ALREADY_EXISTS.withDescription("ours").asRuntimeException());
    fake.getError = Status.NOT_FOUND.withDescription("purged").asRuntimeException();

    // Act + Assert
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void start_awaitReturnsNotFound_throwsSagaNotFound() {
    // Arrange — enter the await loop (RUNNING), then the saga is purged/TTL'd between polls so
    // AwaitSaga reports NOT_FOUND. The client must surface the same type getStateSnapshot does.
    fake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    fake.enqueueAwaitError(Status.NOT_FOUND.withDescription("purged").asRuntimeException());

    // Act + Assert
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void start_withDeadline_awaitKeepsFailing_throwsSagaTimeout() {
    // Arrange — a client with a small overall deadline; start returns RUNNING, then AwaitSaga keeps
    // returning UNAVAILABLE. The loop absorbs each retryable failure with backoff until the client
    // deadline elapses, at which point guardDeadline aborts the bounded wait.
    GrpcSagaOrchestratorClient deadlineClient =
        new GrpcSagaOrchestratorClient(SagaServiceGrpc.newBlockingStub(channel), null, 100L);
    fake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    fake.awaitError = Status.UNAVAILABLE.withDescription("still down").asRuntimeException();

    // Act + Assert
    assertThatThrownBy(() -> deadlineClient.start("transfer", Map.of()))
        .isInstanceOf(SagaTimeoutException.class);
  }

  @Test
  void start_clientClosedWhileAwaiting_throwsTerminallyInsteadOfRetryingForever() {
    // Arrange — the default client has no deadline, so guardDeadline never fires. Enter the await
    // loop (RUNNING); a retryable transport error then stands in for calls failing after close()
    // shuts the channel. With the client already marked closed, the loop must abort terminally
    // rather than retry the retryable error forever (a deterministic stand-in for a concurrent
    // close() racing an in-flight blocking start()).
    fake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    fake.enqueueAwaitError(Status.UNAVAILABLE.withDescription("channel shut").asRuntimeException());
    client.close();

    // Act + Assert
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void start_concurrentCloseWhileAwaiting_throwsTerminallyWithRealChannel() throws Exception {
    // A client that OWNS a real in-process channel, so close() actually shuts the transport (unlike
    // the injected-stub setup, where close() is a no-op). A background thread blocks in start()'s
    // await loop (the fake keeps returning RUNNING); closing the client mid-flight must make that
    // blocking call fail terminally rather than retry the resulting UNAVAILABLE forever.
    FakeSagaService ownFake = new FakeSagaService();
    ownFake.startResponse = snapshot("ignored", SagaStatus.RUNNING);
    ownFake.getResponse =
        snapshot("ignored", SagaStatus.RUNNING); // awaitSaga keeps returning RUNNING
    String name = InProcessServerBuilder.generateName();
    Server ownServer = InProcessServerBuilder.forName(name).addService(ownFake).build().start();
    ManagedChannel ownChannel = InProcessChannelBuilder.forName(name).build();
    GrpcSagaOrchestratorClient ownedClient =
        new GrpcSagaOrchestratorClient(SagaServiceGrpc.newBlockingStub(ownChannel), ownChannel);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      // Act — start on a background thread, wait until it is in the await loop, then close.
      Future<?> start = caller.submit(() -> ownedClient.start("transfer", Map.of()));
      assertThat(ownFake.awaitEntered.await(5, TimeUnit.SECONDS)).isTrue();
      ownedClient.close();

      // Assert — the blocking start() fails terminally instead of hanging on infinite retry.
      assertThatThrownBy(() -> start.get(5, TimeUnit.SECONDS))
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(IllegalStateException.class);
    } finally {
      caller.shutdownNow();
      ownChannel.shutdownNow();
      ownServer.shutdownNow();
    }
  }

  // ---------------------------------------------------------------------------
  // Null validation
  // ---------------------------------------------------------------------------

  @SuppressWarnings("NullAway")
  @Test
  void start_nullName_throwsNpe() {
    assertThatThrownBy(() -> client.start((String) null, Map.of()))
        .isInstanceOf(NullPointerException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void getStateSnapshot_nullId_throwsNpe() {
    assertThatThrownBy(() -> client.getStateSnapshot(null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Builds a {@link StatusRuntimeException} carrying an {@link ErrorInfo} detail, as the daemon's
   * {@code GrpcErrorMapper} does, so the client's reason-based reconstruction is exercised end to
   * end.
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

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static SagaSnapshot snapshot(String sagaId, SagaStatus status) {
    return SagaSnapshot.newBuilder()
        .setSagaId(sagaId)
        .setName("transfer")
        .setStatus(com.scalar.db.saga.rpc.SagaStatus.valueOf("SAGA_STATUS_" + status.name()))
        .setDefinitionVersion("v1")
        .setCreatedAt(Timestamp.newBuilder().setSeconds(1000L).build())
        .setUpdatedAt(Timestamp.newBuilder().setSeconds(2000L).build())
        .build();
  }

  private static final class FakeSagaService extends SagaServiceGrpc.SagaServiceImplBase {
    @Nullable StartSagaRequest lastStart;
    @Nullable AwaitSagaRequest lastAwait;
    @Nullable StatusRuntimeException startError;
    SagaSnapshot startResponse = SagaSnapshot.getDefaultInstance();
    @Nullable StatusRuntimeException getError;
    SagaSnapshot getResponse = SagaSnapshot.getDefaultInstance();
    // When set (and no await script is pending), awaitSaga keeps failing with this error — lets a
    // test drive the bounded-wait loop until the client deadline elapses.
    @Nullable StatusRuntimeException awaitError;

    // Scripted per-call outcomes; when a script is non-empty it takes precedence over the single
    // response/error fields, enabling multi-call loop and retry-after-reset tests.
    final Deque<Consumer<StreamObserver<SagaSnapshot>>> startScript = new ArrayDeque<>();
    final Deque<Consumer<StreamObserver<SagaSnapshot>>> awaitScript = new ArrayDeque<>();
    int awaitCalls;
    // Counted down the first time awaitSaga is invoked, so a test can observe that the client has
    // reached the await loop before acting (e.g. closing the client concurrently).
    final CountDownLatch awaitEntered = new CountDownLatch(1);

    StartSagaRequest lastStart() {
      return Objects.requireNonNull(lastStart);
    }

    AwaitSagaRequest lastAwait() {
      return Objects.requireNonNull(lastAwait);
    }

    void enqueueStartSnapshot(SagaSnapshot snapshot) {
      startScript.add(observer -> respondWith(observer, snapshot));
    }

    void enqueueStartError(StatusRuntimeException error) {
      startScript.add(observer -> observer.onError(error));
    }

    void enqueueAwaitSnapshot(SagaSnapshot snapshot) {
      awaitScript.add(observer -> respondWith(observer, snapshot));
    }

    void enqueueAwaitError(StatusRuntimeException error) {
      awaitScript.add(observer -> observer.onError(error));
    }

    private static void respondWith(StreamObserver<SagaSnapshot> observer, SagaSnapshot snapshot) {
      observer.onNext(snapshot);
      observer.onCompleted();
    }

    @Override
    public void startSaga(StartSagaRequest request, StreamObserver<SagaSnapshot> responseObserver) {
      lastStart = request;
      if (!startScript.isEmpty()) {
        startScript.poll().accept(responseObserver);
        return;
      }
      if (startError != null) {
        responseObserver.onError(startError);
        return;
      }
      respondWith(responseObserver, startResponse);
    }

    @Override
    public void awaitSaga(AwaitSagaRequest request, StreamObserver<SagaSnapshot> responseObserver) {
      lastAwait = request;
      awaitCalls++;
      awaitEntered.countDown();
      if (!awaitScript.isEmpty()) {
        awaitScript.poll().accept(responseObserver);
        return;
      }
      if (awaitError != null) {
        responseObserver.onError(awaitError);
        return;
      }
      respondWith(responseObserver, getResponse);
    }

    @Override
    public void getSaga(GetSagaRequest request, StreamObserver<SagaSnapshot> responseObserver) {
      if (getError != null) {
        responseObserver.onError(getError);
        return;
      }
      respondWith(responseObserver, getResponse);
    }

    @Nullable StatusRuntimeException detailError;
    com.scalar.db.saga.rpc.SagaDetail detailResponse =
        com.scalar.db.saga.rpc.SagaDetail.getDefaultInstance();

    @Override
    public void getSagaDetail(
        GetSagaDetailRequest request,
        StreamObserver<com.scalar.db.saga.rpc.SagaDetail> responseObserver) {
      if (detailError != null) {
        responseObserver.onError(detailError);
        return;
      }
      responseObserver.onNext(detailResponse);
      responseObserver.onCompleted();
    }
  }
}
