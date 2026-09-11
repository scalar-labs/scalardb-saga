package com.scalar.db.saga.server.grpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.SagaInvalidRequestException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaDetailRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaDetail;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.StartSagaRequest;
import com.scalar.db.saga.server.BoundedWait;
import com.scalar.db.saga.server.SagaWaiterRegistry;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongUnaryOperator;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The gRPC rendering of the saga lifecycle API — the wire-protocol parallel of {@link
 * com.scalar.db.saga.server.api.SagaResource} (REST). Holds no per-request state: the injected
 * {@link SagaOrchestrator} (the same instance the REST routes use), the server's shutdown signal,
 * and the {@link com.scalar.db.saga.server.SagaWaiterRegistry} are all shared and process-wide;
 * every latch or reference belonging to one call is local to it.
 *
 * <p><b>Sync vs async.</b> {@code async=true} starts the saga and returns the running snapshot
 * immediately. {@code async=false} blocks until the saga is terminal, bounded by the {@code
 * sync.max_wait_millis} ceiling tightened by {@code sync.timeout_millis}, and then further by the
 * remaining gRPC call deadline. A saga that parks on an async step does not end the wait early: it
 * may still finish inside the bound, and the read at bound expiry reports that outcome whichever
 * replica produced it. When the wait ends without a terminal state it returns the in-flight
 * snapshot (whose status — the source of truth — is non-terminal, the gRPC analogue of REST's
 * {@code 202}) and <b>the saga keeps running</b>. The wait runs on the server's virtual-thread
 * executor, so a blocked call is cheap.
 *
 * <p><b>AwaitSaga.</b> A long-poll on an <i>existing</i> saga: it blocks for one bounded window and
 * returns the terminal snapshot if reached, else the current non-terminal snapshot. The client
 * loops it (after a bounded {@code StartSaga}) to deliver a block-until-terminal {@code start()}
 * over short, resumable calls. Unlike the start path it cannot attach the saga's in-process
 * completion callback, which belongs to the drive that started the saga, so it waits on the
 * registry instead: a drive settling the saga here wakes it at once, and a saga settled on another
 * replica is caught by the poll behind the wait.
 */
@ThreadSafe
public final class SagaServiceImpl extends SagaServiceGrpc.SagaServiceImplBase {

  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
      new TypeReference<>() {};

  private final SagaOrchestrator orchestrator;
  // The shared synchronous-wait policy, already bound to the server's configuration. A function
  // rather than a value because AwaitSaga supplies a different per-call cap on every request.
  private final LongUnaryOperator syncWaitBound;
  // Completed when the server begins shutting down; ends a bounded wait early. See
  // startBoundedSync.
  private final CompletableFuture<Void> shutdownSignal;
  // Wakes a bounded wait the moment the saga settles on this process, including on the drive that
  // resumes it after an asynchronous step, which carries no SagaCallback.
  private final SagaWaiterRegistry waiterRegistry;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public SagaServiceImpl(
      SagaOrchestrator orchestrator,
      LongUnaryOperator syncWaitBound,
      CompletableFuture<Void> shutdownSignal,
      SagaWaiterRegistry waiterRegistry) {
    this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator must not be null");
    this.syncWaitBound = Objects.requireNonNull(syncWaitBound, "syncWaitBound must not be null");
    this.shutdownSignal = Objects.requireNonNull(shutdownSignal, "shutdownSignal must not be null");
    this.waiterRegistry = Objects.requireNonNull(waiterRegistry, "waiterRegistry must not be null");
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
   * Waits until {@code sagaId} is terminal or {@code boundMillis} elapses, returning the latest
   * snapshot either way. The first {@link SagaOrchestrator#getStateSnapshot} also validates
   * existence (throws {@link com.scalar.db.saga.exception.SagaNotFoundException} → {@code
   * NOT_FOUND}).
   *
   * <p>Registration precedes that first read, deliberately: a saga settling in between would
   * otherwise notify nobody, and the caller would wait out its whole bound for an outcome that had
   * already happened. Runs on a virtual thread, so a blocked wait is cheap.
   */
  private SagaStateSnapshot awaitTerminalOrBound(String sagaId, long boundMillis) {
    CompletableFuture<SagaStateSnapshot> settledLocally = new CompletableFuture<>();
    try (SagaWaiterRegistry.Waiter waiter = waiterRegistry.register(sagaId, settledLocally)) {
      SagaStateSnapshot snapshot = orchestrator.getStateSnapshot(sagaId);
      if (snapshot.getStatus().isTerminal()) {
        return snapshot;
      }
      // Polling from the start, unlike a start: an awaited saga may be driven on any replica, so
      // no push can be assumed to reach us and there is no park signal to wait for.
      //
      // A cancelled call answers from the snapshot above rather than reading again. The response is
      // discarded — the caller is gone — so paying a store transaction for it is waste, and waste
      // on the path a departing client takes, which is where a struggling deployment sheds load.
      // This is what the poll loop this replaced did implicitly, by returning the last snapshot it
      // held; expressing it in the read itself keeps it out of BoundedWait, which has no business
      // knowing why a wait ended.
      return BoundedWait.awaitWithin(
          settledLocally,
          abortSignal(),
          null,
          boundMillis,
          () -> Context.current().isCancelled() ? snapshot : orchestrator.getStateSnapshot(sagaId));
    }
  }

  /**
   * Ends a bounded wait early when the server begins shutting down, or when the client cancels or
   * its connection drops.
   *
   * <p>The cancellation half is why this is a listener rather than a per-tick {@link
   * Context#isCancelled()} check: the poll interval scales with the bound and reaches tens of
   * seconds, so a departed caller would otherwise hold its thread until the next tick.
   */
  private CompletableFuture<?> abortSignal() {
    CompletableFuture<Void> cancelled = new CompletableFuture<>();
    // Fires immediately if the context is already cancelled. The listener is scoped to this call's
    // context, so it is discarded with the call and needs no explicit removal.
    Context.current().addListener(context -> cancelled.complete(null), Runnable::run);
    return CompletableFuture.anyOf(shutdownSignal, cancelled);
  }

  private SagaStateSnapshot startAsyncAndSnapshot(
      StartSagaRequest request, Map<String, Object> input) {
    String sagaId = dispatchStart(request, input, null);
    return snapshotAfterStart(sagaId);
  }

  private SagaStateSnapshot startBoundedSync(StartSagaRequest request, Map<String, Object> input) {
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();
    // Completed when the saga parks, which is the first moment it can be resumed somewhere no push
    // reaches us. Until then the drive is here and a poll could only find what the callback or the
    // registry will deliver sooner, so the wait does not read the store at all.
    CompletableFuture<Void> parked = new CompletableFuture<>();
    // One future, completed by whichever mechanism sees the saga settle first. The callback is
    // handed out before the saga id exists and covers the window until the registration below — on
    // a server-generated id there is no id to register under until dispatchStart returns, by which
    // time the saga may already have settled. The registry covers every drive after that, above all
    // the one that resumes the saga after an asynchronous step and carries no callback.
    String sagaId = dispatchStart(request, input, outcomeSignal(settled, parked));
    SagaStateSnapshot answer;
    try (SagaWaiterRegistry.Waiter waiter = waiterRegistry.register(sagaId, settled)) {
      answer =
          BoundedWait.awaitWithin(
              settled,
              abortSignal(),
              parked,
              computeBoundMillis(Long.MAX_VALUE),
              () -> snapshotAfterStart(sagaId));
    }
    // Never cancel the saga. Shutdown short-circuits the wait rather than letting it run to the
    // bound: the bound is a maximum, not a promise to wait, and a terminating server cannot advance
    // the saga anyway. Whatever ended the wait, this is the freshest state — its status is the
    // source of truth, and a non-terminal one is the gRPC analogue of REST's 202.
    return answer;
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
   * Computes a wait bound (ms): the {@code sync.max_wait_millis} ceiling, further tightened by the
   * caller's {@code requestedCapMillis} (AwaitSaga's {@code max_wait_millis}; {@link
   * Long#MAX_VALUE} for the start path), {@code sync.timeout_millis} (when set), and the remaining
   * call deadline minus a slack (when the client set one). Always in {@code [0,
   * sync.max_wait_millis]} — the wait is never unbounded.
   */
  private long computeBoundMillis(long requestedCapMillis) {
    long bound = syncWaitBound.applyAsLong(requestedCapMillis);
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
      throw new SagaInvalidRequestException("'name' is required");
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
      throw new SagaInvalidRequestException("malformed input_json");
    }
    if (input == null) {
      throw new SagaInvalidRequestException("input_json must be a JSON object");
    }
    return input;
  }

  /**
   * A {@link SagaCallback} that captures the terminal snapshot and releases the wait. Parking does
   * not release it: a parked saga may still finish inside the bound, and {@link
   * #startBoundedSync}'s read at bound expiry sees that outcome whichever replica produced it.
   */
  private static SagaCallback outcomeSignal(
      CompletableFuture<SagaStateSnapshot> settled, CompletableFuture<Void> parked) {
    return new SagaCallback() {
      @Override
      public void onParked(SagaStateSnapshot saga) {
        // Not an outcome — the wait continues. It only means the saga can now be resumed
        // elsewhere, so the wait should start polling for what a local push can no longer catch.
        parked.complete(null);
      }

      @Override
      public void onCompleted(SagaStateSnapshot saga) {
        settled.complete(saga);
      }

      @Override
      public void onCompensated(SagaStateSnapshot saga) {
        settled.complete(saga);
      }

      @Override
      public void onEscalated(SagaStateSnapshot saga) {
        settled.complete(saga);
      }
    };
  }

  private static void respond(StreamObserver<SagaSnapshot> observer, SagaStateSnapshot snapshot) {
    observer.onNext(ProtoMappers.toProto(snapshot));
    observer.onCompleted();
  }
}
