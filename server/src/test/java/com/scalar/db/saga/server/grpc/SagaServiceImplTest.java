package com.scalar.db.saga.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.TimelineEvent;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaDetailRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit-tests {@link SagaServiceImpl} over a real in-process gRPC transport with a mocked {@link
 * SagaOrchestrator}, covering request routing, the exception&rarr;{@code Status} mapping, and —
 * most importantly — the leak discipline (internal exception messages must never reach a caller).
 *
 * <p>The unary tests run on the {@code directExecutor} (server logic on the calling thread —
 * deterministic). The {@code async=false} sync tests drive the bounded wait via the orchestrator
 * callback — fired synchronously (callback-reached branch), from a short-lived thread within the
 * bound (genuine timed wait), or never (the bound elapses and the running snapshot is returned).
 */
class SagaServiceImplTest {

  private static final Instant TS = Instant.ofEpochSecond(1_700_000_000L, 123);

  private SagaOrchestrator orchestrator;
  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<Server> servers = new ArrayList<>();

  @BeforeEach
  void setUp() {
    orchestrator = mock(SagaOrchestrator.class);
  }

  @AfterEach
  void tearDown() {
    channels.forEach(ManagedChannel::shutdownNow);
    servers.forEach(Server::shutdownNow);
  }

  // ---------------------------------------------------------------------------
  // Routing / delegation
  // ---------------------------------------------------------------------------

  @Test
  void startSaga_byNameServerGeneratedAsync_delegatesToStartAsyncAndReturnsRunningSnapshot() {
    // Arrange
    when(orchestrator.startAsync("transfer", Map.of())).thenReturn("gen-1");
    when(orchestrator.getStateSnapshot("gen-1")).thenReturn(snapshot("gen-1", SagaStatus.RUNNING));

    // Act
    SagaSnapshot response = stub(0).startSaga(startByName("transfer", true));

    // Assert
    verify(orchestrator).startAsync("transfer", Map.of());
    assertThat(response.getSagaId()).isEqualTo("gen-1");
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_RUNNING);
  }

  @Test
  void startSaga_byNameAndVersionAsync_delegatesToVersionedStartAsync() {
    // Arrange
    when(orchestrator.startAsync(any(SagaDefinitionId.class), eq(Map.of()))).thenReturn("gen-2");
    when(orchestrator.getStateSnapshot("gen-2")).thenReturn(snapshot("gen-2", SagaStatus.RUNNING));
    StartSagaRequest request =
        StartSagaRequest.newBuilder().setName("transfer").setVersion("v2").setAsync(true).build();

    // Act
    stub(0).startSaga(request);

    // Assert
    ArgumentCaptor<SagaDefinitionId> captor = ArgumentCaptor.forClass(SagaDefinitionId.class);
    verify(orchestrator).startAsync(captor.capture(), eq(Map.of()));
    assertThat(captor.getValue().name()).isEqualTo("transfer");
    assertThat(captor.getValue().version()).isEqualTo("v2");
  }

  @Test
  void startSaga_clientSuppliedIdAsync_delegatesToVoidStartAsyncWithThatId() {
    // Arrange
    when(orchestrator.getStateSnapshot("my-id")).thenReturn(snapshot("my-id", SagaStatus.RUNNING));
    StartSagaRequest request =
        StartSagaRequest.newBuilder().setSagaId("my-id").setName("transfer").setAsync(true).build();

    // Act
    SagaSnapshot response = stub(0).startSaga(request);

    // Assert
    verify(orchestrator).startAsync("my-id", "transfer", Map.of());
    assertThat(response.getSagaId()).isEqualTo("my-id");
  }

  @Test
  void getSaga_delegatesToGetStateSnapshot() {
    // Arrange
    when(orchestrator.getStateSnapshot("s-9")).thenReturn(snapshot("s-9", SagaStatus.COMPLETED));

    // Act
    SagaSnapshot response = stub(0).getSaga(GetSagaRequest.newBuilder().setSagaId("s-9").build());

    // Assert
    verify(orchestrator).getStateSnapshot("s-9");
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPLETED);
  }

  @Test
  void getSagaDetail_delegatesAndMapsTimeline() {
    // Arrange — a nullable-field mix: a step event (index/name, no status) and an intervention
    // status event (status/operator/reason)
    SagaStateSnapshot snap = snapshot("s-7", SagaStatus.COMPENSATED);
    TimelineEvent stepFailed =
        new TimelineEvent(TS, "STEP_FAILED", 1, "credit", null, "gateway down", null);
    TimelineEvent recovering =
        new TimelineEvent(
            TS, "SAGA_RECOVERING", null, null, SagaStatus.COMPENSATING, "rolling back", "bob");
    when(orchestrator.getSagaDetail("s-7"))
        .thenReturn(new SagaDetail(snap, List.of(stepFailed, recovering)));

    // Act
    com.scalar.db.saga.rpc.SagaDetail response =
        stub(0).getSagaDetail(GetSagaDetailRequest.newBuilder().setSagaId("s-7").build());

    // Assert — the snapshot maps, and the nullable fields round-trip as set/unset optionals
    verify(orchestrator).getSagaDetail("s-7");
    assertThat(response.getSaga().getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATED);
    assertThat(response.getTimelineCount()).isEqualTo(2);

    com.scalar.db.saga.rpc.TimelineEvent step = response.getTimeline(0);
    assertThat(step.getType()).isEqualTo("STEP_FAILED");
    assertThat(step.getStepIndex()).isEqualTo(1);
    assertThat(step.getStepName()).isEqualTo("credit");
    assertThat(step.getDetail()).isEqualTo("gateway down");
    assertThat(step.hasResultingStatus()).isFalse();
    assertThat(step.hasOperator()).isFalse();

    com.scalar.db.saga.rpc.TimelineEvent status = response.getTimeline(1);
    assertThat(status.hasStepIndex()).isFalse();
    assertThat(status.getResultingStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATING);
    assertThat(status.getOperator()).isEqualTo("bob");
    assertThat(status.getDetail()).isEqualTo("rolling back");
  }

  @Test
  void getSagaDetail_sagaNotFound_returnsNotFound() {
    when(orchestrator.getSagaDetail("missing")).thenThrow(new SagaNotFoundException("missing"));

    assertCode(
        () -> stub(0).getSagaDetail(GetSagaDetailRequest.newBuilder().setSagaId("missing").build()),
        Status.Code.NOT_FOUND);
  }

  @Test
  void startSaga_withJsonInput_passesParsedMapPreservingLongPrecision() {
    // Arrange — 2^53+1 would round to a double under google.protobuf.Struct, so input is sent as
    // JSON bytes.
    when(orchestrator.startAsync(eq("transfer"), any())).thenReturn("gen-3");
    when(orchestrator.getStateSnapshot("gen-3")).thenReturn(snapshot("gen-3", SagaStatus.RUNNING));
    StartSagaRequest request =
        StartSagaRequest.newBuilder()
            .setName("transfer")
            .setInputJson(ByteString.copyFromUtf8("{\"to\":\"alice\",\"id\":9007199254740993}"))
            .setAsync(true)
            .build();

    // Act
    stub(0).startSaga(request);

    // Assert
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
    verify(orchestrator).startAsync(eq("transfer"), captor.capture());
    assertThat(captor.getValue())
        .containsEntry("to", "alice")
        .containsEntry("id", 9007199254740993L);
  }

  // ---------------------------------------------------------------------------
  // Exception -> Status mapping
  // ---------------------------------------------------------------------------

  @Test
  void getSaga_sagaNotFound_returnsNotFound() {
    when(orchestrator.getStateSnapshot("missing")).thenThrow(new SagaNotFoundException("missing"));

    assertCode(
        () -> stub(0).getSaga(GetSagaRequest.newBuilder().setSagaId("missing").build()),
        Status.Code.NOT_FOUND);
  }

  @Test
  void startSaga_snapshotMissingAfterStart_returnsInternalNotNotFound() {
    // The just-started saga vanishes before the post-start read — a server invariant violation, not
    // a
    // client error: surface INTERNAL, not NOT_FOUND (which the client would map to the wrong
    // SagaDefinitionNotFoundException).
    when(orchestrator.startAsync("transfer", Map.of())).thenReturn("gen-x");
    when(orchestrator.getStateSnapshot("gen-x")).thenThrow(new SagaNotFoundException("gen-x"));

    assertCode(() -> stub(0).startSaga(startByName("transfer", true)), Status.Code.INTERNAL);
  }

  @Test
  void startSaga_definitionNotFound_returnsNotFound() {
    when(orchestrator.startAsync("unknown", Map.of()))
        .thenThrow(new SagaDefinitionNotFoundException("unknown"));

    assertCode(() -> stub(0).startSaga(startByName("unknown", true)), Status.Code.NOT_FOUND);
  }

  @Test
  void startSaga_blankName_returnsInvalidArgumentWithDaemonMessage() {
    assertThatThrownBy(() -> stub(0).startSaga(startByName("", true)))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> {
              assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
              // INVALID_REQUEST wraps the daemon-authored detail; the exact message the daemon
              // authored ("'name' is required") rides in the metadata detail.
              assertThat(e.getStatus().getDescription())
                  .contains(SagaErrorCode.INVALID_REQUEST.code())
                  .contains("'name' is required");
            });
  }

  @Test
  void startSaga_engineRejectsArgument_returnsInvalidArgumentWithoutEchoingEngineWording() {
    when(orchestrator.startAsync("transfer", Map.of()))
        .thenThrow(new IllegalArgumentException("engine-internal wording about the bad value"));

    assertThatThrownBy(() -> stub(0).startSaga(startByName("transfer", true)))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> {
              assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
              assertThat(e.getStatus().getDescription())
                  .contains(SagaErrorCode.INVALID_ARGUMENT.code())
                  .doesNotContain("engine-internal");
            });
  }

  @Test
  void startSaga_duplicateClientSuppliedId_returnsAlreadyExists() {
    doThrow(new SagaAlreadyExistsException("dup", snapshot("dup", SagaStatus.RUNNING)))
        .when(orchestrator)
        .startAsync("dup", "transfer", Map.of());
    StartSagaRequest request =
        StartSagaRequest.newBuilder().setSagaId("dup").setName("transfer").setAsync(true).build();

    assertCode(() -> stub(0).startSaga(request), Status.Code.ALREADY_EXISTS);
  }

  // ---------------------------------------------------------------------------
  // P1 leak discipline — internal messages must NEVER reach a caller
  // ---------------------------------------------------------------------------

  @Test
  void startSaga_retryablePersistenceError_returnsUnavailableWithoutLeakingMessage() {
    when(orchestrator.startAsync("transfer", Map.of()))
        .thenThrow(
            SagaPersistenceException.storeUnavailable(
                new RuntimeException("DB write failed on secret_table host=10.0.0.5")));

    assertThatThrownBy(() -> stub(0).startSaga(startByName("transfer", true)))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> {
              assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
              assertThat(e.getStatus().getDescription())
                  .contains(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE.code())
                  .doesNotContain("secret_table", "10.0.0.5");
            });
  }

  @Test
  void startSaga_permanentPersistenceError_returnsInternalWithoutLeakingMessage() {
    // A permanent persistence failure (e.g. serialization) must not be reported as a retryable
    // UNAVAILABLE — the client would retry it futilely. It maps to INTERNAL instead.
    when(orchestrator.startAsync("transfer", Map.of()))
        .thenThrow(
            SagaPersistenceException.serializationFailed(
                new RuntimeException("Failed to serialize payload for secret_table")));

    assertThatThrownBy(() -> stub(0).startSaga(startByName("transfer", true)))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> {
              assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
              assertThat(e.getStatus().getDescription())
                  .contains(SagaErrorCode.PERSISTENCE_SERIALIZATION_FAILED.code())
                  .doesNotContain("secret_table");
            });
  }

  @Test
  void startSaga_unmappedRuntimeException_returnsInternalWithoutLeakingMessage() {
    when(orchestrator.startAsync("transfer", Map.of()))
        .thenThrow(new IllegalStateException("SECRET stacktrace detail at Engine.java:42"));

    assertThatThrownBy(() -> stub(0).startSaga(startByName("transfer", true)))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> {
              assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
              assertThat(e.getStatus().getDescription())
                  .contains(SagaErrorCode.INTERNAL_ERROR.code())
                  .doesNotContain("SECRET", "Engine.java");
            });
  }

  // ---------------------------------------------------------------------------
  // Bounded sync (async=false)
  // ---------------------------------------------------------------------------

  @Test
  void startSaga_syncTerminalCallbackFires_returnsTerminalSnapshot() {
    // Arrange — the orchestrator fires the completion callback synchronously, so the latch is
    // already counted down when the bounded wait begins: await returns immediately and the bound is
    // not exercised. This covers the "terminal reached" branch deterministically; the genuine
    // within-bound timing is covered by the next test.
    SagaStateSnapshot terminal = snapshot("gen-s", SagaStatus.COMPLETED);
    when(orchestrator.startAsync(eq("transfer"), eq(Map.of()), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onCompleted(terminal);
              return "gen-s";
            });

    // Act — the bound is irrelevant (the callback already fired); kept small to make that clear.
    SagaSnapshot response = stub(1_000).startSaga(startByName("transfer", false));

    // Assert
    assertThat(response.getSagaId()).isEqualTo("gen-s");
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPLETED);
  }

  @Test
  void startSaga_syncCompletesWithinBound_returnsTerminalSnapshot() {
    // Arrange — the saga completes after a short delay (~20ms) that is well within the 5s bound.
    // The
    // callback fires from a separate thread *while* the server is blocked in the bounded wait, so
    // this genuinely exercises "await blocks until the saga is terminal, then returns it".
    SagaStateSnapshot terminal = snapshot("gen-s2", SagaStatus.COMPLETED);
    when(orchestrator.startAsync(eq("transfer"), eq(Map.of()), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              SagaCallback callback = invocation.getArgument(2, SagaCallback.class);
              Thread completer =
                  new Thread(
                      () -> {
                        try {
                          Thread.sleep(20);
                          callback.onCompleted(terminal);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      });
              completer.setDaemon(true);
              completer.start();
              return "gen-s2";
            });

    // Act
    SagaSnapshot response = stub(5_000).startSaga(startByName("transfer", false));

    // Assert
    assertThat(response.getSagaId()).isEqualTo("gen-s2");
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPLETED);
  }

  @Test
  void startSaga_syncBoundElapsesBeforeTerminal_returnsRunningSnapshotWithoutCancelling() {
    // Arrange — the callback never fires, so the (50ms) bound elapses; the saga keeps running and
    // the server returns the in-flight snapshot fetched via getStateSnapshot (no cancellation).
    when(orchestrator.startAsync(eq("transfer"), eq(Map.of()), any(SagaCallback.class)))
        .thenReturn("gen-t");
    when(orchestrator.getStateSnapshot("gen-t")).thenReturn(snapshot("gen-t", SagaStatus.RUNNING));

    // Act
    SagaSnapshot response = stub(50).startSaga(startByName("transfer", false));

    // Assert
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_RUNNING);
    verify(orchestrator).getStateSnapshot("gen-t");
  }

  @Test
  void startSaga_syncClientDeadlineElapsesBeforeTerminal_returnsRunningWithoutDeadlineExceeded() {
    // Arrange — a large sync.timeout_millis, but the client sets a short gRPC deadline and the
    // callback never fires. The bound is min(sync.timeout_millis, deadline - 100ms slack), so the
    // deadline side wins: the wait ends ~one slack before the gRPC deadline and the server returns
    // OK + RUNNING (the saga keeps running) rather than letting the call expire as
    // DEADLINE_EXCEEDED.
    when(orchestrator.startAsync(eq("transfer"), eq(Map.of()), any(SagaCallback.class)))
        .thenReturn("gen-d");
    when(orchestrator.getStateSnapshot("gen-d")).thenReturn(snapshot("gen-d", SagaStatus.RUNNING));

    // Act — sync.timeout_millis is large (30s) so the client's 500ms deadline is the binding bound.
    SagaSnapshot response =
        stub(30_000)
            .withDeadlineAfter(500, TimeUnit.MILLISECONDS)
            .startSaga(startByName("transfer", false));

    // Assert
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_RUNNING);
    verify(orchestrator).getStateSnapshot("gen-d");
  }

  @Test
  void startSaga_noTimeoutNoDeadline_boundedByMaxWaitReturnsTerminal() {
    // Arrange — sync.timeout_millis=0 and no client deadline, so the wait is bounded only by the
    // sync.max_wait_millis ceiling. The callback fires synchronously, so it returns the terminal
    // snapshot immediately, well under the ceiling.
    SagaStateSnapshot terminal = snapshot("gen-u", SagaStatus.COMPLETED);
    when(orchestrator.startAsync(eq("transfer"), eq(Map.of()), any(SagaCallback.class)))
        .thenAnswer(
            invocation -> {
              invocation.getArgument(2, SagaCallback.class).onCompleted(terminal);
              return "gen-u";
            });

    // Act — sync.timeout_millis=0, no deadline.
    SagaSnapshot response = stub(0).startSaga(startByName("transfer", false));

    // Assert
    assertThat(response.getSagaId()).isEqualTo("gen-u");
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPLETED);
  }

  @Test
  void startSaga_noTimeoutNoDeadline_maxWaitElapsesReturnsRunningWithoutCancelling() {
    // Arrange — sync.timeout_millis=0 and no deadline, so a small sync.max_wait_millis is the only
    // ceiling on the wait. The callback never fires, so that ceiling elapses and the server returns
    // the in-flight RUNNING snapshot (the saga keeps running) rather than blocking forever.
    when(orchestrator.startAsync(eq("transfer"), eq(Map.of()), any(SagaCallback.class)))
        .thenReturn("gen-m");
    when(orchestrator.getStateSnapshot("gen-m")).thenReturn(snapshot("gen-m", SagaStatus.RUNNING));

    // Act — sync.timeout_millis=0, no deadline, a 50ms ceiling.
    SagaSnapshot response = stub(0, 50).startSaga(startByName("transfer", false));

    // Assert
    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_RUNNING);
    verify(orchestrator).getStateSnapshot("gen-m");
  }

  // ---------------------------------------------------------------------------
  // AwaitSaga (long-poll on an existing saga)
  // ---------------------------------------------------------------------------

  @Test
  void awaitSaga_alreadyTerminal_returnsImmediately() {
    when(orchestrator.getStateSnapshot("s-1")).thenReturn(snapshot("s-1", SagaStatus.COMPLETED));

    SagaSnapshot response = stub(0).awaitSaga(awaitRequest("s-1"));

    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPLETED);
    verify(orchestrator).getStateSnapshot("s-1");
  }

  @Test
  void awaitSaga_runningThenTerminal_pollsUntilTerminal() {
    when(orchestrator.getStateSnapshot("s-2"))
        .thenReturn(snapshot("s-2", SagaStatus.RUNNING))
        .thenReturn(snapshot("s-2", SagaStatus.COMPENSATED));

    SagaSnapshot response = stub(0).awaitSaga(awaitRequest("s-2"));

    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_COMPENSATED);
    verify(orchestrator, atLeast(2)).getStateSnapshot("s-2");
  }

  @Test
  void awaitSaga_boundElapsesWhileRunning_returnsRunning() {
    when(orchestrator.getStateSnapshot("s-3")).thenReturn(snapshot("s-3", SagaStatus.RUNNING));

    // A small client-requested window so the poll loop ends promptly while the saga still runs.
    SagaSnapshot response = stub(0).awaitSaga(awaitRequest("s-3", 50L));

    assertThat(response.getStatus())
        .isEqualTo(com.scalar.db.saga.rpc.SagaStatus.SAGA_STATUS_RUNNING);
  }

  @Test
  void awaitSaga_sagaNotFound_returnsNotFound() {
    when(orchestrator.getStateSnapshot("missing")).thenThrow(new SagaNotFoundException("missing"));

    assertCode(() -> stub(0).awaitSaga(awaitRequest("missing")), Status.Code.NOT_FOUND);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void assertCode(ThrowingCallable call, Status.Code expected) {
    assertThatThrownBy(call)
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(expected));
  }

  private SagaServiceBlockingStub stub(long syncTimeoutMillis) {
    return stub(syncTimeoutMillis, 60_000L);
  }

  private SagaServiceBlockingStub stub(long syncTimeoutMillis, long syncMaxWaitMillis) {
    String name = InProcessServerBuilder.generateName();
    try {
      servers.add(
          InProcessServerBuilder.forName(name)
              .directExecutor()
              .addService(new SagaServiceImpl(orchestrator, syncTimeoutMillis, syncMaxWaitMillis))
              .build()
              .start());
    } catch (IOException e) {
      throw new AssertionError("failed to start in-process gRPC server", e);
    }
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    channels.add(channel);
    return SagaServiceGrpc.newBlockingStub(channel);
  }

  private static StartSagaRequest startByName(String name, boolean async) {
    return StartSagaRequest.newBuilder().setName(name).setAsync(async).build();
  }

  private static AwaitSagaRequest awaitRequest(String sagaId) {
    return AwaitSagaRequest.newBuilder().setSagaId(sagaId).build();
  }

  private static AwaitSagaRequest awaitRequest(String sagaId, long maxWaitMillis) {
    return AwaitSagaRequest.newBuilder().setSagaId(sagaId).setMaxWaitMillis(maxWaitMillis).build();
  }

  private static SagaStateSnapshot snapshot(String sagaId, SagaStatus status) {
    Instant now = Instant.ofEpochSecond(1_700_000_000L, 123);
    return new SagaStateSnapshot(sagaId, "transfer", status, "owner-1", "v1", now, now);
  }
}
