package com.scalar.db.saga.daemon.grpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.daemon.api.InvalidRequestException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaDetailRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaDetail;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The gRPC rendering of the saga lifecycle API — the wire-protocol parallel of {@link
 * com.scalar.db.saga.daemon.api.SagaResource} (REST). Stateless except for the injected {@link
 * SagaOrchestrator} (the same instance the REST routes use); any per-request latch/reference is
 * local to the call.
 *
 * <p><b>Sync vs async.</b> {@code async=true} starts the saga and returns the running snapshot
 * immediately. {@code async=false} blocks until the saga is terminal, bounded by {@code min(}{@code
 * sync_timeout_millis}, remaining gRPC call deadline{@code )}; when that bound elapses it returns
 * the in-flight snapshot (whose status — the source of truth — is non-terminal, the gRPC analogue
 * of REST's {@code 202}) and <b>the saga keeps running</b>. The wait runs on the server's
 * virtual-thread executor, so a blocked call is cheap.
 *
 * <p><b>AwaitSaga.</b> A long-poll on an <i>existing</i> saga: it blocks for one bounded window and
 * returns the terminal snapshot if reached, else the current non-terminal snapshot. The client
 * loops it (after a bounded {@code StartSaga}) to deliver a block-until-terminal {@code start()}
 * over short, resumable calls. Unlike the start path it cannot attach the saga's in-process
 * completion callback (the saga may run on another replica), so it observes via store polling.
 */
@ThreadSafe
public final class SagaServiceImpl extends SagaServiceGrpc.SagaServiceImplBase {

  /**
   * Store-poll interval for {@link #awaitSaga} while waiting for an existing saga to go terminal.
   */
  private static final long AWAIT_POLL_INTERVAL_MILLIS = 200L;

  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
      new TypeReference<>() {};

  private final SagaOrchestrator orchestrator;
  private final long syncTimeoutMillis;
  private final long syncMaxWaitMillis;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public SagaServiceImpl(
      SagaOrchestrator orchestrator, long syncTimeoutMillis, long syncMaxWaitMillis) {
    this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator must not be null");
    this.syncTimeoutMillis = syncTimeoutMillis;
    this.syncMaxWaitMillis = syncMaxWaitMillis;
  }

  @Override
  public void startSaga(StartSagaRequest request, StreamObserver<SagaSnapshot> responseObserver) {
    try {
      requireName(request.getName());
      Map<String, Object> input = parseInput(request.getInputJson());
      SagaStateSnapshot snapshot =
          request.getAsync()
              ? startAsyncAndSnapshot(request, input)
              : startBoundedSync(request, input);
      respond(responseObserver, snapshot);
    } catch (RuntimeException e) {
      // Route everything through the mapper so nothing escapes as UNKNOWN with an internal message.
      responseObserver.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  @Override
  public void awaitSaga(AwaitSagaRequest request, StreamObserver<SagaSnapshot> responseObserver) {
    try {
      long requestedCap = request.hasMaxWaitMillis() ? request.getMaxWaitMillis() : Long.MAX_VALUE;
      respond(
          responseObserver,
          awaitTerminalOrBound(request.getSagaId(), computeBoundMillis(requestedCap)));
    } catch (RuntimeException e) {
      responseObserver.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  @Override
  public void getSaga(GetSagaRequest request, StreamObserver<SagaSnapshot> responseObserver) {
    try {
      respond(responseObserver, orchestrator.getStateSnapshot(request.getSagaId()));
    } catch (RuntimeException e) {
      responseObserver.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  @Override
  public void getSagaDetail(
      GetSagaDetailRequest request, StreamObserver<SagaDetail> responseObserver) {
    try {
      responseObserver.onNext(
          ProtoMappers.toProto(orchestrator.getSagaDetail(request.getSagaId())));
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      responseObserver.onError(GrpcErrorMapper.toStatusRuntimeException(e));
    }
  }

  /**
   * Polls the store until {@code sagaId} is terminal or {@code boundMillis} elapses, returning the
   * latest snapshot either way. The first {@link SagaOrchestrator#getStateSnapshot} also validates
   * existence (throws {@link com.scalar.db.saga.exception.SagaNotFoundException} → {@code
   * NOT_FOUND}). Runs on a virtual thread, so the polling sleep is cheap. Stops early if the client
   * cancels or its connection drops ({@link Context#isCancelled()}), so we do not keep polling the
   * store for a caller that is gone.
   */
  private SagaStateSnapshot awaitTerminalOrBound(String sagaId, long boundMillis) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(boundMillis);
    SagaStateSnapshot snapshot = orchestrator.getStateSnapshot(sagaId);
    while (!snapshot.getStatus().isTerminal() && !Context.current().isCancelled()) {
      long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
      if (remainingMillis <= 0L) {
        break;
      }
      try {
        Thread.sleep(Math.min(AWAIT_POLL_INTERVAL_MILLIS, remainingMillis));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      snapshot = orchestrator.getStateSnapshot(sagaId);
    }
    return snapshot;
  }

  private SagaStateSnapshot startAsyncAndSnapshot(
      StartSagaRequest request, Map<String, Object> input) {
    String sagaId = dispatchStart(request, input, null);
    return snapshotAfterStart(sagaId);
  }

  private SagaStateSnapshot startBoundedSync(StartSagaRequest request, Map<String, Object> input) {
    AtomicReference<SagaStateSnapshot> terminal = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    String sagaId = dispatchStart(request, input, terminalSignal(done, terminal));
    long boundMillis = computeBoundMillis(Long.MAX_VALUE);
    boolean reached;
    try {
      reached = done.await(boundMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      reached = false;
    }
    // Never cancel the saga: if the bound elapsed first, return the in-flight (non-terminal)
    // snapshot — its status is the source of truth and the saga keeps running.
    return reached ? Objects.requireNonNull(terminal.get()) : snapshotAfterStart(sagaId);
  }

  /**
   * Reads the snapshot of a saga that was just started. {@code createSaga} persisted it
   * synchronously, so a {@link SagaNotFoundException} from this read is a server-side invariant
   * violation (e.g. the saga was purged in the narrow window before the read), not a client error.
   * Surface it as {@code INTERNAL} via the catch-all rather than {@code NOT_FOUND}, so a start
   * RPC's {@code NOT_FOUND} unambiguously means the saga <i>definition</i> was not found.
   */
  private SagaStateSnapshot snapshotAfterStart(String sagaId) {
    try {
      return orchestrator.getStateSnapshot(sagaId);
    } catch (SagaNotFoundException e) {
      throw new IllegalStateException("Saga " + sagaId + " not found immediately after start", e);
    }
  }

  /**
   * Computes a wait bound (ms): the {@code sync_max_wait_millis} ceiling, further tightened by the
   * caller's {@code requestedCapMillis} (AwaitSaga's {@code max_wait_millis}; {@link
   * Long#MAX_VALUE} for the start path), {@code sync_timeout_millis} (when set), and the remaining
   * call deadline minus a slack (when the client set one). Always in {@code [0,
   * sync_max_wait_millis]} — the wait is never unbounded.
   */
  private long computeBoundMillis(long requestedCapMillis) {
    long bound = Math.min(syncMaxWaitMillis, requestedCapMillis);
    if (syncTimeoutMillis > 0L) {
      bound = Math.min(bound, syncTimeoutMillis);
    }
    // Floor at 0: here 0 means "return immediately" for the await, so a tight/expired client
    // deadline correctly collapses the wait to nothing.
    return GrpcDeadlines.tightenToCallDeadline(bound, 0L);
  }

  /**
   * Routes to the {@link SagaOrchestrator} {@code startAsync} overload matching the request:
   * name-vs-versioned, server-generated-vs-client-supplied id, with or without the completion
   * {@code callback}. Returns the saga id (the supplied one, or the generated one).
   */
  private String dispatchStart(
      StartSagaRequest request, Map<String, Object> input, @Nullable SagaCallback callback) {
    boolean clientSupplied = request.hasSagaId();
    if (request.hasVersion()) {
      SagaDefinitionId id = new SagaDefinitionId(request.getName(), request.getVersion());
      if (clientSupplied) {
        String sagaId = request.getSagaId();
        if (callback == null) {
          orchestrator.startAsync(sagaId, id, input);
        } else {
          orchestrator.startAsync(sagaId, id, input, callback);
        }
        return sagaId;
      }
      return callback == null
          ? orchestrator.startAsync(id, input)
          : orchestrator.startAsync(id, input, callback);
    }
    String name = request.getName();
    if (clientSupplied) {
      String sagaId = request.getSagaId();
      if (callback == null) {
        orchestrator.startAsync(sagaId, name, input);
      } else {
        orchestrator.startAsync(sagaId, name, input, callback);
      }
      return sagaId;
    }
    return callback == null
        ? orchestrator.startAsync(name, input)
        : orchestrator.startAsync(name, input, callback);
  }

  private static void requireName(String name) {
    // allMatch is vacuously true for an empty string, so this also rejects "".
    if (name.codePoints().allMatch(Character::isWhitespace)) {
      throw new InvalidRequestException("'name' is required");
    }
  }

  private Map<String, Object> parseInput(ByteString inputJson) {
    if (inputJson.isEmpty()) {
      return Map.of();
    }
    Map<String, Object> input;
    try {
      input = objectMapper.readValue(inputJson.newInput(), MAP_TYPE);
    } catch (IOException e) {
      throw new InvalidRequestException("malformed input_json");
    }
    if (input == null) {
      throw new InvalidRequestException("input_json must be a JSON object");
    }
    return input;
  }

  /** A {@link SagaCallback} that captures the terminal snapshot and releases {@code done}. */
  private static SagaCallback terminalSignal(
      CountDownLatch done, AtomicReference<SagaStateSnapshot> terminal) {
    return new SagaCallback() {
      @Override
      public void onCompleted(SagaStateSnapshot saga) {
        terminal.set(saga);
        done.countDown();
      }

      @Override
      public void onCompensated(SagaStateSnapshot saga) {
        terminal.set(saga);
        done.countDown();
      }

      @Override
      public void onEscalated(SagaStateSnapshot saga) {
        terminal.set(saga);
        done.countDown();
      }
    };
  }

  private static void respond(StreamObserver<SagaSnapshot> observer, SagaStateSnapshot snapshot) {
    observer.onNext(ProtoMappers.toProto(snapshot));
    observer.onCompleted();
  }
}
