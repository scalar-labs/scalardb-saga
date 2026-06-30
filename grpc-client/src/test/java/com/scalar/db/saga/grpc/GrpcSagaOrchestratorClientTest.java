package com.scalar.db.saga.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.exception.SagaUnavailableException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
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
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
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
  void start_notFound_throwsSagaDefinitionNotFound() {
    fake.startError = Status.NOT_FOUND.withDescription("no def").asRuntimeException();
    assertThatThrownBy(() -> client.start("unknown", Map.of()))
        .isInstanceOf(SagaDefinitionNotFoundException.class);
  }

  @Test
  void start_invalidArgument_throwsIllegalArgument() {
    fake.startError = Status.INVALID_ARGUMENT.withDescription("bad input").asRuntimeException();
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
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
  void start_internal_throwsSagaRuntimeException() {
    fake.startError = Status.INTERNAL.withDescription("boom").asRuntimeException();
    assertThatThrownBy(() -> client.start("transfer", Map.of()))
        .isExactlyInstanceOf(SagaRuntimeException.class);
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

    // Scripted per-call outcomes; when a script is non-empty it takes precedence over the single
    // response/error fields, enabling multi-call loop and retry-after-reset tests.
    final Deque<Consumer<StreamObserver<SagaSnapshot>>> startScript = new ArrayDeque<>();
    final Deque<Consumer<StreamObserver<SagaSnapshot>>> awaitScript = new ArrayDeque<>();
    int awaitCalls;

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
      if (!awaitScript.isEmpty()) {
        awaitScript.poll().accept(responseObserver);
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
  }
}
