package com.scalar.db.saga.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.exception.ErrorMetadata;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaRuntimeException;
import com.scalar.db.saga.exception.SagaTimeoutException;
import com.scalar.db.saga.rpc.AwaitSagaRequest;
import com.scalar.db.saga.rpc.GetSagaDetailRequest;
import com.scalar.db.saga.rpc.GetSagaRequest;
import com.scalar.db.saga.rpc.SagaServiceGrpc;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceBlockingStub;
import com.scalar.db.saga.rpc.SagaServiceGrpc.SagaServiceStub;
import com.scalar.db.saga.rpc.SagaSnapshot;
import com.scalar.db.saga.rpc.SagaStatus;
import com.scalar.db.saga.rpc.StartSagaRequest;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * A remote {@link SagaOrchestrator} backed by a gRPC connection to the saga daemon. It implements
 * the same application-facing interface as the embedded {@code DefaultSagaOrchestrator}, so
 * application code runs unchanged embedded or remote.
 *
 * <p>Thread-safe and intended to be created once and shared: a single instance holds one {@link
 * ManagedChannel} that multiplexes all calls. Call {@link #close()} to release it.
 *
 * <p>The {@code startAsync(..., SagaCallback)} overloads return once the saga is started, then
 * watch it in the background with {@code AwaitSaga} and call the callback when it reaches a
 * terminal state. Unlike the embedded orchestrator, the watch keeps going through a park on an
 * asynchronous step, so {@link SagaCallback#onParked} is never called and the terminal method
 * always is, unless the watch stops first. It stops on {@link #close()}, and on a failure that
 * retrying cannot fix, such as the saga being purged, which is logged. The callback lives in this
 * process only: if the process exits, it is not called. A callback normally runs on a gRPC thread,
 * so hand long work to your own executor. When the start already finds the saga terminal, which a
 * retried start can, it runs on the calling thread before {@code startAsync} returns.
 *
 * <p>Every start, {@code startAsync} included, retries a server it cannot reach until {@link
 * Builder#defaultDeadlineMillis} runs out. With no deadline set, a start against a server that is
 * down waits until the server is back. Set a deadline in production.
 *
 * <p>TLS against a private CA (e.g. a cert-manager-issued server certificate), dialing through a
 * port-forward:
 *
 * <pre>{@code
 * GrpcSagaOrchestratorClient client =
 *     GrpcSagaOrchestratorClient.newBuilder()
 *         .target("127.0.0.1:12051") // the port-forward
 *         .useTransportSecurity()
 *         .trustCaCertificate(Paths.get("/etc/saga/ca.crt"))
 *         .overrideAuthority("saga-server.internal") // the name the certificate carries
 *         .build();
 * }</pre>
 *
 * <p>On Java 8, TLS needs ALPN: use 8u252 or later. The SDK's default transport (grpc-netty-shaded)
 * also bundles tcnative, which provides ALPN on older JREs; an embedder who swaps in plain
 * grpc-netty needs {@code netty-tcnative-boringssl-static} instead.
 */
@ThreadSafe
public final class GrpcSagaOrchestratorClient implements SagaOrchestrator {

  private static final Logger LOGGER = Logger.getLogger(GrpcSagaOrchestratorClient.class.getName());

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Capped, jittered exponential backoff between retries of a retryable transport failure. */
  private static final long BACKOFF_BASE_MILLIS = 50L;

  private static final long BACKOFF_CAP_MILLIS = 2_000L;
  private static final int BACKOFF_MAX_SHIFT = 16;

  private final SagaServiceBlockingStub stub;
  private final SagaServiceStub watchStub;
  @Nullable private final ManagedChannel ownedChannel;
  private final long defaultDeadlineMillis;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  // Every callback watch runs its calls inside this context, so close() cancels all of them at once
  // instead of waiting out the channel's drain for long polls that would otherwise hold it open.
  // Rooted, not derived from the current context: a client built inside a gRPC call would otherwise
  // tie every watch to that call, and its end or deadline would silently cancel them.
  private final Context.CancellableContext watchContext = Context.ROOT.withCancellation();

  // Delays a watch's retry after a transport failure. Created on first use, so a client that never
  // takes a callback starts no thread.
  @GuardedBy("this") @Nullable private ScheduledExecutorService retryScheduler;

  /**
   * Visible for testing — inject a stub over an in-process channel; {@code close()} is then a
   * no-op.
   */
  GrpcSagaOrchestratorClient(SagaServiceBlockingStub stub, @Nullable ManagedChannel ownedChannel) {
    this(stub, ownedChannel, 0L);
  }

  /**
   * Visible for testing — inject a stub plus a nonzero default deadline over an in-process channel.
   */
  GrpcSagaOrchestratorClient(
      SagaServiceBlockingStub stub,
      @Nullable ManagedChannel ownedChannel,
      long defaultDeadlineMillis) {
    this.stub = Objects.requireNonNull(stub, "stub must not be null");
    this.watchStub = SagaServiceGrpc.newStub(stub.getChannel());
    this.ownedChannel = ownedChannel;
    this.defaultDeadlineMillis = defaultDeadlineMillis;
  }

  /**
   * Creates a client connected to {@code target} over plaintext (the in-cluster default).
   *
   * @param target the saga server's gRPC target, typically {@code host:port}
   * @return a client owning a channel to that server
   */
  public static GrpcSagaOrchestratorClient create(String target) {
    return newBuilder().target(target).build();
  }

  /**
   * Starts building a client, for when plaintext with no deadline is not enough: TLS, a private CA,
   * an authority override, or a default deadline.
   *
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  // --------------------------------------------------------------------------
  // start (synchronous)
  // --------------------------------------------------------------------------

  @Override
  public String start(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    // Mint the id client-side: it is the idempotency key that makes the start retryable and gives
    // the await loop an id even if the first response is lost (no server-generated-id start here).
    String sagaId = UUID.randomUUID().toString();
    startSynchronously(sagaId, sagaName, null, input);
    return sagaId;
  }

  @Override
  public void start(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startSynchronously(sagaId, sagaName, null, input);
  }

  @Override
  public String start(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    String sagaId = UUID.randomUUID().toString();
    startSynchronously(sagaId, id.name(), id.version(), input);
    return sagaId;
  }

  @Override
  public void start(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startSynchronously(sagaId, id.name(), id.version(), input);
  }

  // --------------------------------------------------------------------------
  // startAsync (no callback)
  // --------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    return startAsynchronously(null, sagaName, null, input, null);
  }

  @Override
  public void startAsync(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startAsynchronously(sagaId, sagaName, null, input, null);
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    return startAsynchronously(null, id.name(), id.version(), input, null);
  }

  @Override
  public void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    startAsynchronously(sagaId, id.name(), id.version(), input, null);
  }

  // --------------------------------------------------------------------------
  // startAsync (with callback)
  // --------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    return startAsynchronously(null, sagaName, null, input, callback);
  }

  @Override
  public void startAsync(
      String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    startAsynchronously(sagaId, sagaName, null, input, callback);
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    return startAsynchronously(null, id.name(), id.version(), input, callback);
  }

  @Override
  public void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    startAsynchronously(sagaId, id.name(), id.version(), input, callback);
  }

  // --------------------------------------------------------------------------
  // query + lifecycle
  // --------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot getStateSnapshot(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    try {
      SagaSnapshot snapshot = stub().getSaga(GetSagaRequest.newBuilder().setSagaId(sagaId).build());
      return ClientProtoMappers.fromProto(snapshot);
    } catch (StatusRuntimeException e) {
      throw mapSagaCall(e, sagaId);
    }
  }

  @Override
  public SagaDetail getSagaDetail(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    try {
      com.scalar.db.saga.rpc.SagaDetail detail =
          stub().getSagaDetail(GetSagaDetailRequest.newBuilder().setSagaId(sagaId).build());
      return ClientProtoMappers.fromProto(detail);
    } catch (StatusRuntimeException e) {
      throw mapSagaCall(e, sagaId);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    watchContext.cancel(null);
    synchronized (this) {
      if (retryScheduler != null) {
        retryScheduler.shutdownNow();
      }
    }
    if (ownedChannel != null) {
      GrpcClientSupport.shutdown(ownedChannel);
    }
  }

  // --------------------------------------------------------------------------
  // start + await terminal (synchronous start)
  // --------------------------------------------------------------------------

  /**
   * Delivers the {@link SagaOrchestrator} synchronous-{@code start} contract over a bounded server:
   * one bounded {@code StartSaga} (start + first wait window), then a loop of idempotent {@code
   * AwaitSaga} long-polls until terminal. Both the start and the awaits ride out retryable
   * transport failures (the saga keeps running server-side regardless), bounded overall by the
   * client deadline — {@code defaultDeadlineMillis == 0} means block to terminal, matching the
   * embedded orchestrator.
   */
  private SagaSnapshot startSynchronously(
      String sagaId, String name, @Nullable String version, Map<String, Object> input) {
    long loopDeadlineNanos =
        defaultDeadlineMillis > 0L
            ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(defaultDeadlineMillis)
            : 0L;
    StartSagaRequest request = buildRequest(sagaId, name, version, input, false);
    SagaSnapshot snapshot = firstStart(request, sagaId, name, version, loopDeadlineNanos);
    return awaitLoop(sagaId, snapshot, loopDeadlineNanos);
  }

  /**
   * Issues the first {@code StartSaga}, retrying retryable transport failures. Because the id is
   * always concrete (client-minted or caller-supplied), the retry is idempotent. {@code
   * ALREADY_EXISTS} on the <i>first</i> attempt is a genuine duplicate (surfaced as {@link
   * SagaAlreadyExistsException}); on a <i>retry</i> it means our earlier attempt landed, so we
   * fetch the snapshot and proceed to the await loop.
   *
   * <p>An admission refusal after a retry is reconciled the same way and for the same reason: the
   * earlier attempt may have created the saga, and that saga may be what is holding the last
   * permit. Overload is reported only once the store agrees nothing is there.
   */
  private SagaSnapshot firstStart(
      StartSagaRequest request,
      String sagaId,
      String name,
      @Nullable String version,
      long loopDeadlineNanos) {
    boolean attempted = false;
    int retries = 0;
    while (true) {
      try {
        return callWithin(loopDeadlineNanos).startSaga(request);
      } catch (StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        if (code == Status.Code.ALREADY_EXISTS) {
          if (attempted) {
            return getSagaSnapshot(sagaId, loopDeadlineNanos);
          }
          throw mapStartException(e, name, version, sagaId);
        }
        if (isRetryable(e)) {
          attempted = true;
          throwIfClosed(sagaId);
          guardDeadline(sagaId, loopDeadlineNanos);
          backoff(retries++);
          continue;
        }
        // An overload refusal is normally a definite answer: nothing was persisted, the ID is
        // free. After an ambiguous attempt it is not. That attempt may well have created this
        // saga — and a saga this client started may be the very thing holding the last permit, so
        // the refusal and the success can be the same request seen twice. Reconcile the way the
        // ALREADY_EXISTS path above does, and report overload only once the store agrees nothing
        // is there.
        if (attempted && GrpcClientSupport.isEngineOverloaded(e)) {
          try {
            return getSagaSnapshot(sagaId, loopDeadlineNanos);
          } catch (SagaNotFoundException notFound) {
            // Nothing landed, so the refusal was the honest answer after all.
          } catch (RuntimeException reconcileFailure) {
            // The reconcile itself failed, so it settled nothing. Overload is still the only thing
            // the server actually told us, and it is what a caller keys backpressure on — losing it
            // to a timeout on this second call would be worst exactly when it matters, since an
            // overloaded daemon is when this call is slowest. Keep it and carry the failure along,
            // as the ALREADY_EXISTS refetch does.
            RuntimeException overloaded = mapStartException(e, name, version, sagaId);
            overloaded.addSuppressed(reconcileFailure);
            throw overloaded;
          }
        }
        throw mapStartException(e, name, version, sagaId);
      }
    }
  }

  /**
   * Long-polls {@code AwaitSaga} until the snapshot is terminal. A gRPC-OK non-terminal return
   * means the server's window elapsed — loop again immediately (the server long-poll paces it). A
   * retryable transport failure is absorbed with backoff and retried (the wait is idempotent).
   * Bounded overall by the client deadline.
   */
  private SagaSnapshot awaitLoop(String sagaId, SagaSnapshot snapshot, long loopDeadlineNanos) {
    int retries = 0;
    while (!isTerminal(snapshot)) {
      guardDeadline(sagaId, loopDeadlineNanos);
      AwaitSagaRequest request = AwaitSagaRequest.newBuilder().setSagaId(sagaId).build();
      try {
        snapshot = callWithin(loopDeadlineNanos).awaitSaga(request);
        retries = 0;
      } catch (StatusRuntimeException e) {
        if (isRetryable(e)) {
          throwIfClosed(sagaId);
          guardDeadline(sagaId, loopDeadlineNanos);
          backoff(retries++);
          continue;
        }
        // NOT_FOUND here means the saga was purged/TTL'd between polls.
        throw mapSagaCall(e, sagaId);
      }
    }
    return snapshot;
  }

  private SagaSnapshot getSagaSnapshot(String sagaId, long loopDeadlineNanos) {
    try {
      return callWithin(loopDeadlineNanos)
          .getSaga(GetSagaRequest.newBuilder().setSagaId(sagaId).build());
    } catch (StatusRuntimeException e) {
      // Map like getStateSnapshot, so a refetch failure surfaces as a Saga* exception rather than
      // leaking a raw gRPC StatusRuntimeException out of start().
      throw mapSagaCall(e, sagaId);
    }
  }

  private static boolean isTerminal(SagaSnapshot snapshot) {
    SagaStatus status = snapshot.getStatus();
    return status == SagaStatus.SAGA_STATUS_COMPLETED
        || status == SagaStatus.SAGA_STATUS_COMPENSATED
        || status == SagaStatus.SAGA_STATUS_ESCALATED;
  }

  /**
   * Whether the client should absorb {@code e} and try again.
   *
   * <p>An admission refusal is excluded deliberately, even though it arrives on a retryable status
   * and is marked retryable on the wire. That marking is a statement about <b>safety</b> — nothing
   * was persisted, so repeating the request cannot double-start a saga — not an instruction to
   * repeat it now. Retrying inside the SDK would spend the caller's whole deadline on a condition
   * only the caller can weigh: shedding the request, queueing it, trying another region, or telling
   * its own user, are all better answers than waiting, and none of them are available to code
   * buried in a transport client. It would also keep pressure on a server that has just said it is
   * saturated.
   *
   * <p>So the refusal is surfaced at once as {@link
   * com.scalar.db.saga.exception.SagaOverloadedException}, and the caller decides. Every other
   * retryable failure is still absorbed here, where the request may never have arrived at all.
   */
  private static boolean isRetryable(StatusRuntimeException e) {
    Status.Code code = e.getStatus().getCode();
    boolean retryableStatus =
        code == Status.Code.UNAVAILABLE
            || code == Status.Code.CANCELLED
            || code == Status.Code.DEADLINE_EXCEEDED;
    return retryableStatus && !GrpcClientSupport.isEngineOverloaded(e);
  }

  /**
   * Throws when the overall client deadline (if any) has elapsed. SAGA_AWAIT_TIMEOUT, not
   * REQUEST_TIMEOUT: no one request failed, the caller's whole budget ran out. The answer is to
   * poll the id; anything sent again has to reuse it.
   *
   * <p>Both loops end here, on different footing. From {@code awaitLoop} the start succeeded and
   * the saga is known to be running; only the wait-for-terminal budget expired. From {@code
   * firstStart} the start is still being retried, so whether anything was persisted is exactly what
   * is unknown. One code covers both because the answer is the same either way: poll the id.
   *
   * <p>Which is why the exception carries {@code sagaId}. On the generated-id overloads it is the
   * caller's only copy; the id was minted here and the {@code return sagaId} that would hand it
   * over is never reached. Without it the caller is left with work that may be running server-side
   * and no way to poll, inspect, or compensate it.
   */
  private void guardDeadline(String sagaId, long loopDeadlineNanos) {
    if (loopDeadlineNanos != 0L && System.nanoTime() >= loopDeadlineNanos) {
      throw SagaTimeoutException.awaitExpired(sagaId);
    }
  }

  /**
   * Aborts the retry loop if {@link #close()} was called concurrently. Without this, a blocking
   * {@code start()} with no client deadline would retry the (now retryable) channel-shutdown errors
   * forever and never return.
   */
  private void throwIfClosed(String sagaId) {
    if (closed.get()) {
      throw new IllegalStateException(
          "Saga client was closed before saga " + sagaId + " reached a terminal state");
    }
  }

  /**
   * The stub for one loop call: with no client deadline it blocks (matching embedded); otherwise
   * its per-call deadline is the remaining loop budget (the server still clamps each call to its
   * long-poll window).
   */
  private SagaServiceBlockingStub callWithin(long loopDeadlineNanos) {
    if (loopDeadlineNanos == 0L) {
      return stub;
    }
    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(loopDeadlineNanos - System.nanoTime());
    return stub.withDeadlineAfter(Math.max(1L, remainingMillis), TimeUnit.MILLISECONDS);
  }

  private static void backoff(int attempt) {
    try {
      Thread.sleep(backoffMillis(attempt));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SagaRuntimeException(SagaErrorCode.REQUEST_ABORTED, ErrorMetadata.of(), e);
    }
  }

  private static long backoffMillis(int attempt) {
    long exp = BACKOFF_BASE_MILLIS << Math.min(attempt, BACKOFF_MAX_SHIFT);
    long capped = Math.min(BACKOFF_CAP_MILLIS, exp);
    // Jitter in [capped/2, capped], derived from the clock (no security-sensitive RNG needed), to
    // avoid a thundering herd when many clients retry a recovered daemon at once.
    long half = capped / 2L;
    long jitter = half == 0L ? 0L : Math.floorMod(System.nanoTime(), half + 1L);
    return half + jitter;
  }

  // --------------------------------------------------------------------------
  // callback watch
  // --------------------------------------------------------------------------

  /**
   * Calls {@code callback} once the saga is terminal, re-issuing {@code AwaitSaga} until it is. A
   * non-terminal answer means the server's window elapsed, or the saga parked, so the watch asks
   * again at once; a retryable transport failure is retried after a backoff. Nothing here blocks a
   * thread: each call is asynchronous, and its answer arrives on a gRPC thread.
   */
  private void watch(String sagaId, SagaSnapshot snapshot, SagaCallback callback, int retries) {
    if (closed.get()) {
      return;
    }
    if (isTerminal(snapshot)) {
      dispatch(sagaId, snapshot, callback);
      return;
    }
    AwaitSagaRequest request = AwaitSagaRequest.newBuilder().setSagaId(sagaId).build();
    watchContext.run(
        () ->
            watchStub.awaitSaga(
                request,
                new StreamObserver<SagaSnapshot>() {
                  @Override
                  public void onNext(SagaSnapshot next) {
                    watch(sagaId, next, callback, 0);
                  }

                  @Override
                  public void onError(Throwable t) {
                    if (closed.get()) {
                      return;
                    }
                    if (t instanceof StatusRuntimeException
                        && isRetryable((StatusRuntimeException) t)) {
                      scheduleRetry(sagaId, snapshot, callback, retries);
                      return;
                    }
                    LOGGER.log(
                        Level.WARNING,
                        "Stopped watching saga " + sagaId + "; its callback will not be called",
                        t);
                  }

                  @Override
                  public void onCompleted() {}
                }));
  }

  // The future is dropped, so the task reports its own failure rather than leaving it there.
  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleRetry(
      String sagaId, SagaSnapshot snapshot, SagaCallback callback, int retries) {
    try {
      retryScheduler()
          .schedule(
              () -> {
                try {
                  watch(sagaId, snapshot, callback, retries + 1);
                } catch (RuntimeException e) {
                  LOGGER.log(
                      Level.WARNING,
                      "Stopped watching saga " + sagaId + "; its callback will not be called",
                      e);
                }
              },
              backoffMillis(retries),
              TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // close() ran after the caller's closed check; the watch ends with the client.
    }
  }

  private synchronized ScheduledExecutorService retryScheduler() {
    // Checked under the lock close() takes after setting the flag, so a scheduler is never created
    // once close() has looked for one to shut down.
    if (closed.get()) {
      throw new RejectedExecutionException("client is closed");
    }
    if (retryScheduler == null) {
      retryScheduler =
          Executors.newSingleThreadScheduledExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "saga-client-watch-retry");
                thread.setDaemon(true);
                return thread;
              });
    }
    return retryScheduler;
  }

  private static void dispatch(String sagaId, SagaSnapshot snapshot, SagaCallback callback) {
    // Caught so that a failing callback is reported against its saga rather than surfacing inside
    // gRPC's call machinery as an anonymous listener failure.
    try {
      SagaStateSnapshot saga = ClientProtoMappers.fromProto(snapshot);
      SagaStatus status = snapshot.getStatus();
      if (status == SagaStatus.SAGA_STATUS_COMPLETED) {
        callback.onCompleted(saga);
      } else if (status == SagaStatus.SAGA_STATUS_COMPENSATED) {
        callback.onCompensated(saga);
      } else {
        callback.onEscalated(saga);
      }
    } catch (RuntimeException e) {
      LOGGER.log(Level.WARNING, "The callback for saga " + sagaId + " threw", e);
    }
  }

  // --------------------------------------------------------------------------
  // internals
  // --------------------------------------------------------------------------

  /**
   * Starts the saga {@code async=true} and returns its id without waiting for a terminal state,
   * then, when {@code callback} is given, watches it in the background. Backs all eight {@code
   * startAsync} overloads; the {@code void} ones ignore the returned id, which the caller already
   * holds. The start retries transport failures exactly as the blocking {@code start} does, and is
   * safe to retry for the same reason: the id is minted here when the caller supplies none.
   */
  private String startAsynchronously(
      @Nullable String sagaId,
      String name,
      @Nullable String version,
      Map<String, Object> input,
      @Nullable SagaCallback callback) {
    String id = sagaId != null ? sagaId : UUID.randomUUID().toString();
    long deadlineNanos =
        defaultDeadlineMillis > 0L
            ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(defaultDeadlineMillis)
            : 0L;
    SagaSnapshot snapshot =
        firstStart(buildRequest(id, name, version, input, true), id, name, version, deadlineNanos);
    if (callback != null) {
      watch(id, snapshot, callback, 0);
    }
    return id;
  }

  private SagaServiceBlockingStub stub() {
    return defaultDeadlineMillis > 0L
        ? stub.withDeadlineAfter(defaultDeadlineMillis, TimeUnit.MILLISECONDS)
        : stub;
  }

  private static StartSagaRequest buildRequest(
      String sagaId,
      String name,
      @Nullable String version,
      Map<String, Object> input,
      boolean async) {
    StartSagaRequest.Builder builder =
        StartSagaRequest.newBuilder()
            .setSagaId(sagaId)
            .setName(name)
            .setAsync(async)
            .setInputJson(toInputJson(input));
    if (version != null) {
      builder.setVersion(version);
    }
    return builder.build();
  }

  private static ByteString toInputJson(Map<String, Object> input) {
    if (input.isEmpty()) {
      return ByteString.EMPTY;
    }
    try {
      ByteString.Output output = ByteString.newOutput();
      OBJECT_MAPPER.writeValue(output, input);
      return output.toByteString();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to serialize saga input to JSON", e);
    }
  }

  /**
   * Maps a start-call {@link StatusRuntimeException} to the api exception. {@code NOT_FOUND} means
   * the <i>definition</i> wasn't found (vs a missing saga on getStateSnapshot). Inverse of the
   * daemon {@code GrpcErrorMapper} — the wire status code is the contract.
   */
  private RuntimeException mapStartException(
      StatusRuntimeException e, String name, @Nullable String version, String clientSagaId) {
    Status.Code code = e.getStatus().getCode();
    // ALREADY_EXISTS is handled ahead of reconstruction: SAGA_ALREADY_EXISTS is deliberately not
    // reconstructible, because SagaAlreadyExistsException needs the existing snapshot and the wire
    // metadata has no room for it. Only this path can re-fetch it.
    if (code == Status.Code.ALREADY_EXISTS) {
      return alreadyExists(clientSagaId, e);
    }
    SagaRuntimeException reconstructed = GrpcClientSupport.reconstruct(e);
    if (reconstructed != null) {
      return reconstructed;
    }
    if (code == Status.Code.NOT_FOUND) {
      return version == null
          ? SagaDefinitionNotFoundException.byName(name)
          : SagaDefinitionNotFoundException.byNameAndVersion(name, version);
    }
    return GrpcClientSupport.mapTransport(e);
  }

  private RuntimeException alreadyExists(String clientSagaId, StatusRuntimeException e) {
    SagaStateSnapshot existing;
    try {
      // The conflict response carries no snapshot — re-fetch it (the only extra round-trip, and
      // only on the rare conflict) so the exception faithfully carries the existing state.
      existing = getStateSnapshot(clientSagaId);
    } catch (RuntimeException refetchFailure) {
      // Cannot build a SagaAlreadyExistsException without the snapshot (its schema requires one).
      // Surface the conflict via the raw SAGA_ALREADY_EXISTS code so callers keying on
      // getErrorCode() still see it, and attach the refetch failure as suppressed for debugging.
      SagaRuntimeException conflict =
          new SagaRuntimeException(
              SagaErrorCode.SAGA_ALREADY_EXISTS, ErrorMetadata.of("saga_id", clientSagaId), e);
      conflict.addSuppressed(refetchFailure);
      return conflict;
    }
    return new SagaAlreadyExistsException(clientSagaId, existing);
  }

  /**
   * Maps a saga-instance RPC failure ({@code getSaga}/{@code awaitSaga}) to the api exception. The
   * daemon's {@link com.google.rpc.ErrorInfo} wins when present, since it names the exact code.
   * Without one, {@code NOT_FOUND} means the saga id is gone — purged, TTL'd, or never existed — vs
   * the start path, where {@code NOT_FOUND} means the <i>definition</i> is missing (see {@link
   * #mapStartException}); everything else routes through {@code GrpcClientSupport.mapTransport}.
   */
  private static RuntimeException mapSagaCall(StatusRuntimeException e, String sagaId) {
    SagaRuntimeException reconstructed = GrpcClientSupport.reconstruct(e);
    if (reconstructed != null) {
      return reconstructed;
    }
    if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
      return new SagaNotFoundException(sagaId);
    }
    return GrpcClientSupport.mapTransport(e);
  }

  /** Builder for {@link GrpcSagaOrchestratorClient}. */
  public static final class Builder {

    @Nullable private String target;
    private boolean useTls = false;
    @Nullable private Path trustCaCertPath;
    @Nullable private String overrideAuthority;
    private long defaultDeadlineMillis = 0L;

    private Builder() {}

    /**
     * The saga server to connect to, as a gRPC target, typically {@code host:port}. Required.
     *
     * @param target the gRPC target
     * @return this builder
     */
    public Builder target(String target) {
      this.target = Objects.requireNonNull(target, "target must not be null");
      return this;
    }

    /**
     * Plaintext transport (the default; appropriate for an isolated in-cluster network).
     *
     * @return this builder
     */
    public Builder usePlaintext() {
      this.useTls = false;
      return this;
    }

    /**
     * Enables TLS — against the daemon's native TLS ({@code scalar.db.saga.server.tls.enabled}) or
     * a TLS-terminating mesh/proxy in front of it. The server certificate is validated against the
     * JVM's default trust store unless {@link #trustCaCertificate(Path)} narrows it. {@link
     * #build()} fails fast if neither the JRE nor a loaded tcnative provides ALPN (on Java 8, use
     * 8u252+; the default grpc-netty-shaded transport bundles tcnative).
     *
     * @return this builder
     */
    public Builder useTransportSecurity() {
      this.useTls = true;
      return this;
    }

    /**
     * Trusts only the CA certificate (PEM; concatenated certificates allowed) at {@code caCertPath}
     * for this channel, replacing the JVM's default trust store. For servers whose certificate a
     * public CA did not issue — a cert-manager or Vault private CA — where default trust rejects
     * the handshake. Requires TLS: {@link #build()} fails if the channel is left plaintext, rather
     * than silently ignoring a setting that says the caller expected encryption. The file is read
     * at {@link #build()}, so a bad path fails there naming the file, not at the first RPC as an
     * opaque {@code UNAVAILABLE}.
     *
     * @param caCertPath path to the PEM CA certificate to trust
     * @return this builder
     */
    public Builder trustCaCertificate(Path caCertPath) {
      this.trustCaCertPath = Objects.requireNonNull(caCertPath, "caCertPath must not be null");
      return this;
    }

    /**
     * Validates the server certificate against {@code authority} instead of the dialed address —
     * for dialing by IP or through a port-forward while the certificate names the service's DNS
     * name. Independent of {@link #trustCaCertificate(Path)}, and legitimate without TLS too (gRPC
     * also routes on the authority).
     *
     * @param authority the name to validate the server certificate against
     * @return this builder
     */
    public Builder overrideAuthority(String authority) {
      this.overrideAuthority = Objects.requireNonNull(authority, "authority must not be null");
      return this;
    }

    /**
     * How long a call may take, in milliseconds; {@code 0} (the default) is no bound. On {@code
     * getStateSnapshot} and {@code getSagaDetail} it is a per-call deadline. On {@code startAsync}
     * it bounds the start including its retries; with no bound, a {@code startAsync} against a
     * server that is down retries until the server is back, as {@code start} does. On the blocking
     * {@code start} it is the whole wait-for-terminal budget, spanning the start and every {@code
     * AwaitSaga} the loop issues after it. It does not bound the background watch behind a {@link
     * SagaCallback}, which runs until the saga is terminal or the client is closed.
     *
     * <p>That second role bounds the caller, not the server: {@code
     * scalar.db.saga.server.sync.max_wait_millis} caps how long one server call holds a connection,
     * and the blocking {@code start} re-issues {@code AwaitSaga} past it until the saga is
     * terminal. With no bound set here, a saga that parks and never settles — which is possible
     * whenever neither a step timeout nor a saga timeout is configured — blocks the calling thread
     * indefinitely.
     *
     * <p>When the budget elapses the blocking {@code start} throws {@link SagaTimeoutException}
     * with {@link SagaErrorCode#SAGA_AWAIT_TIMEOUT}, carrying the saga ID in {@link
     * SagaTimeoutException#getSagaId()}. Poll that ID to learn where the start stands: usually the
     * saga is running and will settle on its own, but the budget can also expire while the first
     * request is still being retried, and then the poll is what says whether anything was
     * persisted.
     *
     * <p>One narrow path misses that exception. When a retried start is answered {@code
     * ALREADY_EXISTS} the client fetches the saga on what is left of the budget, and a budget that
     * runs out on that fetch surfaces as {@link SagaErrorCode#REQUEST_TIMEOUT} with no ID, though
     * the earlier attempt did land. Supplying the saga ID instead of letting the client mint one
     * keeps a handle in every case.
     *
     * @param defaultDeadlineMillis the bound in milliseconds, or {@code 0} for no bound
     * @return this builder
     * @throws IllegalArgumentException if negative
     */
    public Builder defaultDeadlineMillis(long defaultDeadlineMillis) {
      if (defaultDeadlineMillis < 0L) {
        throw new IllegalArgumentException("defaultDeadlineMillis must not be negative");
      }
      this.defaultDeadlineMillis = defaultDeadlineMillis;
      return this;
    }

    /**
     * Opens the channel and creates the client. A configuration that cannot work fails here rather
     * than at the first call: a trusted CA on a plaintext channel, TLS without ALPN, or a CA
     * certificate file that cannot be read.
     *
     * @return the client, which owns the channel until {@link GrpcSagaOrchestratorClient#close()}
     * @throws NullPointerException if {@link #target(String)} was not set
     * @throws IllegalStateException if {@link #trustCaCertificate(Path)} was set without {@link
     *     #useTransportSecurity()}, or TLS was requested but ALPN is unavailable
     * @throws IllegalArgumentException if the CA certificate file cannot be read
     */
    public GrpcSagaOrchestratorClient build() {
      String resolvedTarget = Objects.requireNonNull(target, "target must be set");
      ManagedChannel channel =
          GrpcClientSupport.openChannel(resolvedTarget, useTls, trustCaCertPath, overrideAuthority);
      return new GrpcSagaOrchestratorClient(
          SagaServiceGrpc.newBlockingStub(channel), channel, defaultDeadlineMillis);
    }
  }
}
