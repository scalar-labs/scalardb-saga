package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaAdminService;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinitionParser;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaDefinitionNotServedException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaOverloadedException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStoreFactory;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.transport.CallbackUrlProvider;
import com.scalar.db.saga.transport.HttpEndpointManager;
import com.scalar.db.saga.transport.HttpEndpointRegistrar;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default (in-process) implementation of {@link SagaOrchestrator}. Delegates saga lifecycle logic
 * to {@link SagaEngine}, handles definition registration via {@link SagaDefinitionRegistry}, async
 * execution via virtual threads, and callback dispatch. Beyond the lean {@link SagaOrchestrator}
 * surface it also exposes registration, recovery, and admin operations, and is constructed via
 * {@link #newBuilder()}.
 */
@ThreadSafe
public class DefaultSagaOrchestrator implements SagaOrchestrator {

  /**
   * The shutdown mode applied when {@link Builder#shutdownMode(ShutdownMode)} is not called.
   * Exposed so a front end that configures the engine from an external source (e.g. the daemon's
   * properties file) can document and apply the same default instead of restating it.
   */
  public static final ShutdownMode DEFAULT_SHUTDOWN_MODE = ShutdownMode.WAIT_CURRENT_STEP;

  /**
   * The shutdown timeout in milliseconds applied when {@link Builder#shutdownTimeoutMillis(long)}
   * is not called. Exposed for the same reason as {@link #DEFAULT_SHUTDOWN_MODE}.
   */
  public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 30_000L;

  /**
   * The default saga timeout in milliseconds applied when {@link
   * Builder#defaultSagaTimeoutMillis(long)} is not called: {@code 0}, meaning no default is applied
   * and definitions without a timeout of their own run without one. Exposed for the same reason as
   * {@link #DEFAULT_SHUTDOWN_MODE}.
   */
  public static final long DEFAULT_SAGA_TIMEOUT_MILLIS = 0L;

  /**
   * The timeline bound applied when {@link Builder#maxTimelineEvents(int)} is not called:
   * effectively unbounded, so an in-process (embedded) caller always sees a saga's full timeline. A
   * remote front end serving {@link #getSagaDetail} over a network should configure a real bound —
   * an unbounded timeline of a pathological saga can exceed a wire message limit and make the saga
   * undiagnosable exactly when it matters.
   */
  public static final int DEFAULT_MAX_TIMELINE_EVENTS = Integer.MAX_VALUE;

  /**
   * The concurrent-execution cap applied when {@link Builder#maxConcurrentSagaExecutions(int)} is
   * not called: {@code 0}, meaning no cap.
   *
   * <p>Off by default because the right value is a property of a deployment's store, hosts and saga
   * durations, and a wrong one is worse than none: too low refuses work the daemon could have done,
   * and too high is the unbounded behavior with extra machinery. The sizing method is on the
   * builder setter.
   */
  public static final int DEFAULT_MAX_CONCURRENT_SAGA_EXECUTIONS = 0;

  private static final Logger logger = LoggerFactory.getLogger(DefaultSagaOrchestrator.class);

  // Embedded mode has no authenticated user, so admin interventions are attributed to this fixed
  // principal. A remote front end injects the request's authenticated identity instead.
  private static final String EMBEDDED_OPERATOR = "embedded";

  private final SagaEngine engine;
  private final SagaStore store;
  private final SagaDefinitionRegistry definitionRegistry;
  private final SagaRecoveryManager recoveryManager;
  private final SagaRetentionManager retentionManager;
  private final long shutdownTimeoutMillis;
  private final int maxTimelineEvents;

  /** The admission cap, or {@code null} when none is configured — the default. */
  private final @Nullable AdmissionController admissionController;

  private final ExecutorService asyncExecutor;
  private volatile boolean closed;

  /**
   * The saga names this deployment currently serves, or {@code null} when nothing has published a
   * set.
   *
   * <p>Null is the embedded default: an application that registers a definition means to run it,
   * and has its own call sites to stop calling. A front end whose configuration decides what is
   * served — the daemon, whose definition files do — publishes a snapshot here after each
   * configuration pass, which is what makes removing a definition file retire the saga.
   *
   * <p>Pushed rather than pulled, and an immutable snapshot rather than a live view: a start reads
   * it without a lock, so it cannot contend with a configuration pass on the hot path.
   *
   * <p>A pass rejected before it applied anything publishes nothing, so the previously served set
   * keeps serving. One rejected after that point does publish: what it committed before failing is
   * live, and a definition registered by a pass that then fails on an unrelated one has to be
   * served rather than refused until some later pass concludes. So the served set can narrow
   * mid-pass, and anything added between the withdrawal and the registration has to keep that true.
   */
  private volatile @Nullable Set<String> servedDefinitions;

  DefaultSagaOrchestrator(
      SagaEngine engine,
      SagaStore store,
      SagaDefinitionRegistry definitionRegistry,
      SagaRecoveryManager recoveryManager,
      SagaRetentionManager retentionManager,
      long shutdownTimeoutMillis,
      int maxTimelineEvents,
      int maxConcurrentSagaExecutions) {
    this(
        engine,
        store,
        definitionRegistry,
        recoveryManager,
        retentionManager,
        shutdownTimeoutMillis,
        maxTimelineEvents,
        maxConcurrentSagaExecutions,
        Executors.newVirtualThreadPerTaskExecutor());
  }

  // Visible for testing
  DefaultSagaOrchestrator(
      SagaEngine engine,
      SagaStore store,
      SagaDefinitionRegistry definitionRegistry,
      SagaRecoveryManager recoveryManager,
      SagaRetentionManager retentionManager,
      long shutdownTimeoutMillis,
      int maxTimelineEvents,
      int maxConcurrentSagaExecutions,
      ExecutorService asyncExecutor) {
    this.engine = engine;
    this.store = store;
    this.definitionRegistry = definitionRegistry;
    this.recoveryManager = recoveryManager;
    this.retentionManager = retentionManager;
    this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    this.maxTimelineEvents = maxTimelineEvents;
    // No cap means no controller, rather than a controller with an unreachable cap: the seams then
    // hold a null and skip the semaphore entirely, so the default costs nothing.
    this.admissionController =
        maxConcurrentSagaExecutions > 0
            ? new AdmissionController(maxConcurrentSagaExecutions)
            : null;
    this.asyncExecutor = asyncExecutor;
  }

  /** Creates a new builder for constructing a {@link DefaultSagaOrchestrator}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  // ---------------------------------------------------------------------------
  // Registration
  // ---------------------------------------------------------------------------

  public void register(SagaDefinition definition) {
    Objects.requireNonNull(definition, "definition must not be null");
    // Eagerly resolve all steps — fail fast on missing resources or unresolvable constructors.
    // This must happen before persisting to the store, so invalid definitions are never stored.
    engine.getOrBuildPlan(definition);
    definitionRegistry.register(definition);
  }

  /**
   * Publishes the set of saga names this deployment serves; every other registered saga is refused
   * at start with {@link SagaDefinitionNotServedException}.
   *
   * <p>For a front end whose configuration decides what runs. The store is append-only, so a
   * definition stays registered long after the configuration that introduced it is gone; without
   * this, nothing could ever be taken out of service. Sagas already running are unaffected — they
   * resume by version and never come through the start check.
   *
   * <p>Call it after each configuration change, with the complete set.
   *
   * @param sagaNames the saga names to serve, copied defensively
   */
  public void serve(Set<String> sagaNames) {
    this.servedDefinitions = Set.copyOf(sagaNames);
  }

  /**
   * The definition of {@code sagaName} that a name-only start would run — the store's latest — or
   * {@code null} when nothing is registered under that name.
   *
   * <p>For a caller that maintains definition files and needs to know whether they still describe
   * what is serving. Registered content is immutable and the store is append-only, so re-writing an
   * older version's file registers nothing and leaves the newer version winning; without asking,
   * such a caller cannot tell that its files and the fleet disagree. It gets the whole definition
   * rather than the version alone because, on finding them disagreeing, what serves is the only
   * thing left worth validating.
   */
  public @Nullable SagaDefinition latestDefinition(String sagaName) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    return definitionRegistry.resolve(sagaName);
  }

  /**
   * Whether {@code version} of {@code sagaName} is already registered.
   *
   * <p>With {@link #latestDefinition} this distinguishes the two ways a definition file can name a
   * version that is not serving: a NEW version, which is an ordinary upgrade about to become the
   * latest, and an OLDER one that is already stored, which is a rollback that will register nothing
   * and leave the newer version running.
   */
  public boolean isDefinitionRegistered(String sagaName, String version) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(version, "version must not be null");
    return definitionRegistry.resolve(sagaName, version) != null;
  }

  public void register(Path definitionFile) {
    Objects.requireNonNull(definitionFile, "definitionFile must not be null");
    register(SagaDefinitionParser.parseFile(definitionFile));
  }

  /**
   * The narrow seam for replacing the full HTTP endpoint set at runtime (configuration hot reload).
   * See {@link HttpEndpointRegistrar} for the swap semantics — reuse on unchanged topology,
   * in-place header rotation, graceful retirement — and the embedded-mode contract: a class step's
   * injected {@code SagaHttpClient} is pinned when its plan is built and is NOT rebound by a swap.
   *
   * <p>The returned registrar shares this orchestrator's lifecycle: a swap applied after {@link
   * #close()} throws {@link IllegalStateException}, so a hot-reload caller racing shutdown must be
   * prepared for it.
   */
  public HttpEndpointRegistrar httpEndpointRegistrar() {
    return engine.httpEndpointRegistrar();
  }

  // ---------------------------------------------------------------------------
  // Synchronous start
  // ---------------------------------------------------------------------------

  @Override
  public String start(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    return executeAdmitted(def, null, input);
  }

  @Override
  public void start(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    executeAdmitted(def, sagaId, input);
  }

  @Override
  public String start(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    return executeAdmitted(def, null, input);
  }

  @Override
  public void start(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    executeAdmitted(def, sagaId, input);
  }

  // ---------------------------------------------------------------------------
  // Asynchronous start
  // ---------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    return startAsyncInternal(def, null, input, null).getSagaId();
  }

  @Override
  public String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    return startAsyncInternal(def, null, input, callback).getSagaId();
  }

  @Override
  public void startAsync(String sagaId, String sagaName, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    startAsyncInternal(def, sagaId, input, null);
  }

  @Override
  public void startAsync(
      String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(sagaName, "sagaName must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireLatestDefinition(sagaName);
    startAsyncInternal(def, sagaId, input, callback);
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    return startAsyncInternal(def, null, input, null).getSagaId();
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    return startAsyncInternal(def, null, input, callback).getSagaId();
  }

  @Override
  public void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    startAsyncInternal(def, sagaId, input, null);
  }

  @Override
  public void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(input, "input must not be null");
    Objects.requireNonNull(callback, "callback must not be null");
    ensureOpen();
    SagaDefinition def = requireVersionedDefinition(id);
    startAsyncInternal(def, sagaId, input, callback);
  }

  /**
   * Runs everything that can reject this request on its own merits, before anything scarce is spent
   * on it.
   *
   * <p>These checks also run inside the engine, which is where they authoritatively decide; running
   * them here as well is about <b>order</b>, not about coverage. At a full cap the permit is the
   * first thing a start meets, so without this a caller whose input holds a null, or whose ID could
   * never be stored, would be told the server is busy and to try again — sending them round a loop
   * that cannot terminate, for a request that was wrong on arrival.
   *
   * <p>Both are pure and cheap: a walk of the input map, and one regex. The payload-size limit is
   * deliberately <b>not</b> hoisted — it needs the input serialized, and paying for that twice on
   * every start to reorder one error at a full cap is the wrong trade. An oversized payload at a
   * full cap therefore still reports overload; it is the one case where this ordering does not
   * hold, and it is far rarer than a null.
   */
  private void validateBeforeAdmitting(@Nullable String sagaId, Map<String, Object> input) {
    ExecutionContext.validateInput(input);
    if (sagaId != null) {
      store.validateSagaId(sagaId);
    }
  }

  /**
   * Takes a permit for a drive about to start, or refuses the start.
   *
   * <p>Called after validation and definition resolution, deliberately: a malformed request and an
   * unknown saga are the caller's mistakes and must keep their own answers even at a full cap,
   * where a 503 would tell them to retry something that can never succeed. It is called before
   * anything is persisted, equally deliberately: a refused start leaves no saga and no consumed ID,
   * which is what makes the advice to retry true.
   *
   * @return the lease to release when the drive ends, or {@code null} when no cap is configured
   * @throws SagaOverloadedException when the cap is full
   */
  private AdmissionController.@Nullable PermitLease admit() {
    AdmissionController controller = admissionController;
    if (controller == null) {
      return null;
    }
    AdmissionController.PermitLease lease = controller.acquire();
    if (lease == null) {
      throw new SagaOverloadedException();
    }
    return lease;
  }

  /** Returns a permit, tolerating the no-cap case so both seams keep one shape. */
  private static void release(AdmissionController.@Nullable PermitLease lease) {
    if (lease != null) {
      lease.release();
    }
  }

  /**
   * Shared synchronous-start path: the drive holds a permit for exactly as long as it occupies this
   * thread. A saga that parks releases here too — {@code execute} returns when the saga stops
   * progressing, and a saga waiting on an outside system is not occupying the engine.
   */
  private String executeAdmitted(
      SagaDefinition def, @Nullable String sagaId, Map<String, Object> input) {
    validateBeforeAdmitting(sagaId, input);
    AdmissionController.PermitLease lease = admit();
    try {
      return engine.execute(def, sagaId, input);
    } finally {
      release(lease);
    }
  }

  /**
   * Shared async-launch path. Persists the saga synchronously (so it is recoverable even if the
   * process crashes before the virtual thread starts), then submits execution to a virtual thread.
   */
  private SagaStateSnapshot startAsyncInternal(
      SagaDefinition def,
      @Nullable String sagaId,
      Map<String, Object> input,
      @Nullable SagaCallback callback) {
    validateBeforeAdmitting(sagaId, input);
    // Admitted before the copy: a refused start should pay for nothing it does not need, and the
    // copy is the first thing here that costs anything per request.
    AdmissionController.PermitLease lease = admit();
    SagaStateSnapshot saga;
    Map<String, Object> copiedInput;
    try {
      // Defensive copy: the async thread reads this map after we return to the caller, so a caller
      // that mutates its map post-return would otherwise race the read (CME or a torn copy).
      copiedInput = new HashMap<>(input);

      // Persist synchronously — saga is recoverable from this point
      saga = engine.createSaga(def, sagaId, copiedInput);
    } catch (Throwable t) {
      // Nothing was handed to the executor, so this thread still owns the permit.
      release(lease);
      throw t;
    }

    // Dispatch execution to a virtual thread; fire-and-forget, saga state is persisted so
    // recovery handles failures. The lease goes with it: from here the drive owns it.
    submitAsync(def, saga, copiedInput, callback, lease);

    return saga;
  }

  private void submitAsync(
      SagaDefinition def,
      SagaStateSnapshot saga,
      Map<String, Object> input,
      @Nullable SagaCallback callback,
      AdmissionController.@Nullable PermitLease lease) {
    try {
      // execute() (not submit()) because the result is ignored: submit() would return a Future we
      // drop, which both trips Error Prone and silently swallows failures. The inner catches handle
      // failures instead, logging any Throwable (incl. Error); the saga is persisted, so recovery
      // is the backstop.
      asyncExecutor.execute(
          () -> {
            // What execution decided, or null if it threw before deciding anything. Recorded
            // rather than branched on, because the dispatch below must stay in the finally: a saga
            // that reached a terminal state and then threw still owes its caller that callback.
            SagaStateSnapshot executed = null;
            try {
              executed = engine.executeSaga(def, saga, input);
            } catch (Throwable t) {
              // Saga state is persisted — recovery will pick it up
              logger.error("Async saga {} failed unexpectedly", saga.getSagaId(), t);
            } finally {
              // Released before the callback runs, and that order is load-bearing: a client told
              // its saga completed or parked may start the next one immediately, and would be
              // refused by a permit this drive has finished with. It also means a callback that
              // throws or blocks forever cannot hold capacity hostage.
              release(lease);
              try {
                dispatchCallback(saga.getSagaId(), callback, executed);
              } catch (Throwable t) {
                logger.error("Failed to dispatch callback for saga {}", saga.getSagaId(), t);
              }
            }
          });
    } catch (RejectedExecutionException e) {
      // The task will never run, so nothing else will return this permit.
      release(lease);
      // Race between close() and execute() — saga is already persisted, recovery will handle it
      logger.warn(
          "Async executor rejected saga {} (shutting down); recovery will handle it",
          saga.getSagaId(),
          e);
    } catch (Throwable t) {
      // An executor that fails some other way owes the same accounting. A pathological one that
      // both schedules the task and throws would release twice; the lease absorbs that, which is
      // why it is release-once rather than a bare semaphore call.
      release(lease);
      throw t;
    }
  }

  /**
   * Reports what execution did to the caller's callback, if it supplied one.
   *
   * @param executed the state execution left the saga in, or {@code null} when it threw before
   *     reaching one — in which case the cause is already logged and there is no outcome to report
   */
  private void dispatchCallback(
      String sagaId, @Nullable SagaCallback callback, @Nullable SagaStateSnapshot executed) {
    if (callback == null) {
      return;
    }
    // Execution threw before reaching a verdict, so there is no local answer. Fall back to the
    // store — it may well have completed the saga and failed afterwards, and that caller is still
    // owed its terminal callback. This read carries the staleness the verdict exists to avoid, but
    // it is the only source available, and it is confined to the exceptional path.
    boolean aborted = executed == null;
    SagaStateSnapshot result;
    if (executed == null) {
      result = store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
    } else {
      result = executed;
    }
    // Every status is listed and there is no default, deliberately: a new SagaStatus must not
    // silently inherit another one's callback. Without a default arm, Error Prone's
    // MissingCasesInEnumSwitch — escalated to an error in the java conventions — fails the build
    // when one is added, forcing the choice here.
    switch (result.getStatus()) {
      case COMPLETED -> callback.onCompleted(result);
      case COMPENSATED -> callback.onCompensated(result);
      case ESCALATED -> callback.onEscalated(result);
      // Parked on an async step, waiting for that step's callback or its deadline. Report it
      // rather than only logging: a caller in a bounded synchronous start has nothing else to
      // wake on, and would otherwise wait out its whole bound for a saga that stopped progressing
      // in milliseconds.
      case WAITING -> callback.onParked(result);
      // Execution returned without the saga resting anywhere. Since this is execution's own
      // verdict rather than a later read of shared state, a resume landing elsewhere can no longer
      // masquerade as either case below — which is what makes the error worth acting on.
      case RUNNING, COMPENSATING -> {
        if (aborted) {
          logger.warn(
              "Saga {} was left in {} by a failed execution; recovery will reclaim it",
              sagaId,
              result.getStatus());
        } else if (engine.isShuttingDown()) {
          // The designed hand-off: under WAIT_CURRENT_STEP the engine finishes the running step,
          // marks the saga for recovery, and returns with it still RUNNING. Debug rather than warn,
          // because a rolling restart produces one per in-flight saga and none is actionable.
          logger.debug(
              "Saga {} left in {} by shutdown; recovery will reclaim it",
              sagaId,
              result.getStatus());
        } else {
          logger.error(
              "Saga {} finished executing but is still {} — this is an engine invariant violation,"
                  + " please report it; recovery will reclaim the saga",
              sagaId,
              result.getStatus());
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Query
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot getStateSnapshot(String sagaId) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    return store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
  }

  // ---------------------------------------------------------------------------
  // Admin
  // ---------------------------------------------------------------------------

  /**
   * Returns the {@link SagaAdminService} control plane backed by this orchestrator's store and
   * engine: list sagas, and recover, force-complete, or reset the ones that need an operator. This
   * is the embedded default of {@link #adminService(OperatorContext, long)} — interventions are
   * attributed to a fixed embedded principal, and single-saga drives run to completion on the
   * calling thread. A server that authenticates its callers and must not block a request thread
   * indefinitely passes its own operator context and drive deadline to that method instead.
   */
  public SagaAdminService adminService() {
    return adminService(() -> EMBEDDED_OPERATOR, 0L);
  }

  @Override
  public SagaDetail getSagaDetail(String sagaId) {
    // An application read of its own saga's state and timeline — no operator, no drive.
    return SagaDetailReader.read(store, sagaId, maxTimelineEvents);
  }

  /**
   * Builds a {@link SagaAdminService} that attributes interventions to the given operator context
   * and bounds how long a single-saga mutation drives before returning. A daemon builds one per
   * request whose {@code operatorContext} yields that request's authenticated principal, so the
   * audit records who acted without the operator identity ever being a caller-supplied parameter.
   *
   * <p>The returned service shares this orchestrator's store and engine — it is a thin view, not a
   * second engine — so building one per request is cheap and holds no resource of its own.
   *
   * @param operatorContext supplies the principal to stamp on the audit record
   * @param driveDeadlineMillis the longest a single-saga {@code recoverSaga}/{@code resetEscalated}
   *     drives before returning the saga's current state and leaving the rest to the recovery loop;
   *     {@code 0} or less drives on the calling thread with no bound
   * @return the admin service view
   */
  public SagaAdminService adminService(OperatorContext operatorContext, long driveDeadlineMillis) {
    Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    return new DefaultSagaAdminService(
        store, engine, definitionRegistry, operatorContext, driveDeadlineMillis);
  }

  // ---------------------------------------------------------------------------
  // Daemon mode only
  // ---------------------------------------------------------------------------

  /**
   * Resumes a parked ({@code WAITING}) saga when the async callback for its parked step arrives:
   * atomically records {@code STEP_COMPLETED} (carrying {@code output}), transitions {@code WAITING
   * -> RUNNING}, and deletes the {@code saga_parked} row. Returns as soon as the step is durably
   * resumed — the {@code RUNNING} snapshot, not the saga's final state — and runs the forward tail
   * on the async executor rather than the caller's thread.
   *
   * <p>This lets the daemon callback endpoint ack a participant the moment its result is persisted,
   * instead of holding the request open for the whole downstream saga. The exceptions below are
   * thrown synchronously (before dispatch), so a caller can still map them. If the detached drive
   * throws or the process dies mid-tail, the saga is {@code RUNNING} and recovery resumes it — the
   * same backstop as any running saga.
   *
   * @param sagaId the parked saga
   * @param stepName the step the callback completes (must be the currently parked step)
   * @param output the step's output, merged into the saga context for downstream steps
   * @return the {@code RUNNING} snapshot immediately after the parked step is resumed
   * @throws IllegalStateException if the saga is not {@code WAITING}
   * @throws IllegalArgumentException if {@code stepName} is not the currently parked step
   * @throws SagaConcurrentModificationException if a concurrent deadline-timeout sweep resolves the
   *     parked step first (the callback lost the race)
   */
  public SagaStateSnapshot completeStepAsync(
      String sagaId, String stepName, Map<String, Object> output) {
    ResumedStep resumed = resumeParked(sagaId, stepName, output);
    try {
      // execute() (not submit()) because the result is ignored: submit() would return a Future we
      // drop, which both trips Error Prone and silently swallows failures. The inner catch handles
      // failures instead, logging any Throwable (incl. Error); the saga is persisted as RUNNING, so
      // recovery is the backstop.
      asyncExecutor.execute(
          () -> {
            try {
              engine.resumeFrom(resumed.def(), resumed.context(), resumed.stepIndex() + 1);
            } catch (Throwable t) {
              logger.error("Async completion drive for saga {} failed unexpectedly", sagaId, t);
            }
          });
    } catch (RejectedExecutionException e) {
      // Race between close() and execute() — the step is resumed (RUNNING); recovery will drive it.
      logger.warn(
          "Async executor rejected completion drive for saga {} (shutting down); "
              + "recovery will handle it",
          sagaId,
          e);
    }
    return resumed.running();
  }

  /**
   * Phase 1 of completing a parked step: validates the saga is {@code WAITING} and {@code stepName}
   * is the parked step, atomically records {@code STEP_COMPLETED} + {@code WAITING -> RUNNING} and
   * deletes the {@code saga_parked} row, then rebuilds the execution context. Kept separate from
   * the forward drive so {@link #completeStepAsync} can return once this synchronous phase commits
   * and run the drive on the async executor. Returns everything the forward drive needs.
   */
  private ResumedStep resumeParked(String sagaId, String stepName, Map<String, Object> output) {
    Objects.requireNonNull(sagaId, "sagaId must not be null");
    Objects.requireNonNull(stepName, "stepName must not be null");
    Objects.requireNonNull(output, "output must not be null");
    ensureOpen();

    SagaStateSnapshot saga = getStateSnapshot(sagaId);
    if (saga.getStatus() != SagaStatus.WAITING) {
      throw new IllegalStateException(
          "Cannot complete step for saga " + sagaId + " in status " + saga.getStatus());
    }

    List<SagaEvent> events = store.getEvents(sagaId);
    int stepIndex = parkedStepIndex(events, sagaId, stepName);
    SagaDefinition def = resolveDefinition(saga);

    // Atomic: STEP_COMPLETED + WAITING -> RUNNING + delete the saga_parked row. The optimistic
    // WAITING-CK check makes this and a concurrent deadline-timeout sweep mutually exclusive.
    String payload = EventPayloadSerializer.serialize(output);
    StepEvent completedEvent = StepEvent.completed(stepIndex, stepName, payload);
    SagaStateSnapshot running = store.resumeParkedStep(saga, events.size(), completedEvent);

    // Defense in depth: a successful resume must land in RUNNING. resumeParkedStep already throws
    // SagaConcurrentModificationException if a concurrent timeout sweep won the WAITING CK, so this
    // guards only against a store contract violation — never drive forward on a non-RUNNING
    // snapshot.
    if (running.getStatus() != SagaStatus.RUNNING) {
      throw new IllegalStateException(
          "resumeParkedStep for saga "
              + sagaId
              + " returned unexpected status "
              + running.getStatus());
    }

    // Replay to fold the callback output into context. The resume appended completedEvent at
    // events.size() and nothing else, and its WAITING-CK check proves the log was untouched since
    // we read `events` (a parked saga's log grows only through a CK-changing transition). So we
    // append locally instead of re-reading (replayEvents ignores the event timestamp).
    List<SagaEvent> updatedEvents = new ArrayList<>(events);
    updatedEvents.add(completedEvent);
    ExecutionContext context = engine.replayEvents(running, updatedEvents);
    return new ResumedStep(running, def, context, stepIndex);
  }

  /** Phase-1 result of completing a parked step: the resumed saga and where to drive it from. */
  private record ResumedStep(
      SagaStateSnapshot running, SagaDefinition def, ExecutionContext context, int stepIndex) {}

  /**
   * Returns the step index of the currently parked step — the most recent {@code STEP_PENDING} for
   * a {@code WAITING} saga — validating that {@code stepName} matches it.
   */
  private static int parkedStepIndex(List<SagaEvent> events, String sagaId, String stepName) {
    StepEvent parked = null;
    for (SagaEvent event : events) {
      if (event.getEventType() == EventType.STEP_PENDING) {
        parked = (StepEvent) event;
      }
    }
    if (parked == null) {
      throw new IllegalStateException("Saga " + sagaId + " has no parked step to complete");
    }
    if (!parked.getStepName().equals(stepName)) {
      throw new IllegalArgumentException(
          "Callback step '"
              + stepName
              + "' does not match the parked step '"
              + parked.getStepName()
              + "' for saga "
              + sagaId);
    }
    return parked.getStepIndex();
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  /**
   * Runs one recovery pass now (both sweeps: stale sagas and overdue parked sagas), returning when
   * the pass has finished its work.
   *
   * <p>Passes never overlap: if a scheduled pass (see {@link #startBackgroundTasks()}) is already
   * in flight, this call blocks until that pass completes and then runs its own. The wait honors
   * thread interruption — an interrupted caller returns without having run a pass, with the
   * interrupt flag set. A pass interrupted mid-run cancels its in-flight recovery tasks and drains
   * their results before returning (their unknown outcomes are charged as errors), so no task
   * outlives its pass.
   *
   * <p>A pass continues a budget-stopped bucket revolution rather than restarting it, so a saga
   * just marked via {@link SagaStore#markForRecovery} is not guaranteed to be reached by the next
   * single call — only within one revolution's worth of passes.
   */
  public void recover() {
    ensureOpen();
    recoveryManager.recover();
  }

  // ---------------------------------------------------------------------------
  // Background Tasks
  // ---------------------------------------------------------------------------

  public void startBackgroundTasks() {
    ensureOpen();
    recoveryManager.start();
    retentionManager.start();
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void close() {
    closed = true;
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMillis);

    retentionManager.stop(deadline);
    recoveryManager.stop(deadline);

    asyncExecutor.shutdown();
    engine.shutdown();

    long remainingNanos = deadline - System.nanoTime();
    try {
      if (remainingNanos <= 0
          || !asyncExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
        asyncExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      asyncExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      // Sagas are drained. The engine.shutdown() above already released the HTTP clients held by
      // the step instantiator's registries (it owns them); here the orchestrator closes the store,
      // the one external resource it owns directly.
      try {
        store.close();
      } catch (Exception e) {
        logger.error("Failed to close store", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("orchestrator is closed");
    }
  }

  private SagaDefinition requireLatestDefinition(String sagaName) {
    SagaDefinition def = definitionRegistry.resolve(sagaName);
    if (def == null) {
      throw SagaDefinitionNotFoundException.byName(sagaName);
    }
    requireServed(sagaName);
    return def;
  }

  private SagaDefinition requireVersionedDefinition(SagaDefinitionId id) {
    SagaDefinition def = definitionRegistry.resolve(id.name(), id.version());
    if (def == null) {
      throw SagaDefinitionNotFoundException.byId(id);
    }
    // Whether a saga is served is a property of the NAME, so pinning a version is refused on the
    // same basis as a name-only start rather than being a way around it.
    requireServed(id.name());
    return def;
  }

  /**
   * Refuses a start of a saga this deployment does not serve.
   *
   * <p>Checked after the definition resolves, so a name nobody ever registered stays a not-found
   * rather than becoming this: the two are different problems with different fixes. Resolving first
   * also means the check costs nothing — the served set is an in-memory snapshot and the store read
   * had to happen anyway.
   *
   * <p>Only new starts come through here. In-flight sagas resume through {@link
   * #resolveDefinition}, so ceasing to serve a saga stops new work without stranding work already
   * running, or the admin operations that drive it to a conclusion.
   */
  private void requireServed(String sagaName) {
    Set<String> served = servedDefinitions;
    if (served != null && !served.contains(sagaName)) {
      throw SagaDefinitionNotServedException.of(sagaName);
    }
  }

  private SagaDefinition resolveDefinition(SagaStateSnapshot saga) {
    SagaDefinition def =
        definitionRegistry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      throw SagaDefinitionNotFoundException.byNameAndVersion(
          saga.getSagaName(), saga.getDefinitionVersion());
    }
    return def;
  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  /**
   * DI-free builder for constructing a {@link DefaultSagaOrchestrator}. Wires together the engine,
   * step resolver, definition registry, and store. Obtain one via {@link
   * DefaultSagaOrchestrator#newBuilder()}.
   *
   * <p><b>Three step resolution modes:</b>
   *
   * <ol>
   *   <li><b>No dependencies (default):</b> Steps must have a single public no-arg constructor.
   *   <li><b>Resource injection:</b> Register shared resources via {@link #resource(Class, Object)}
   *       or {@link #resource(Class, Object, String)}. Steps must have exactly one public
   *       constructor whose parameter types match registered resources.
   *   <li><b>Custom resolver:</b> Supply a {@link StepResolver} via {@link
   *       #stepResolver(StepResolver)} for full control over step instantiation (e.g., manual
   *       lookup, DI framework integration).
   * </ol>
   *
   * <p>{@code resource()} and {@code stepResolver()} are mutually exclusive — calling both causes
   * {@link #build()} to throw.
   *
   * <pre>{@code
   * DefaultSagaOrchestrator orchestrator = DefaultSagaOrchestrator.newBuilder()
   *     .storeFactory(ScalarDbSagaStoreFactory.create(props))
   *     .resource(EmailClient.class, emailClient)
   *     .build();
   * }</pre>
   */
  public static final class Builder {

    private @Nullable SagaStoreFactory storeFactory;
    // Mirrors the store's saga-ID discipline: the owner id lands in state rows and log lines, so
    // it gets the same character set and length bound.
    private static final java.util.regex.Pattern OWNER_ID_PATTERN =
        java.util.regex.Pattern.compile("[a-zA-Z0-9._-]{1,128}");

    private String ownerId = java.util.UUID.randomUUID().toString();
    private ShutdownMode shutdownMode = DEFAULT_SHUTDOWN_MODE;
    private long shutdownTimeoutMillis = DEFAULT_SHUTDOWN_TIMEOUT_MILLIS;
    private long defaultSagaTimeoutMillis = DEFAULT_SAGA_TIMEOUT_MILLIS;
    private int maxTimelineEvents = DEFAULT_MAX_TIMELINE_EVENTS;
    private int maxConcurrentSagaExecutions = DEFAULT_MAX_CONCURRENT_SAGA_EXECUTIONS;
    private Clock clock = Clock.systemUTC();
    private ResourceRegistry.@Nullable Builder resourceRegistryBuilder;
    private @Nullable StepResolver customStepResolver;
    private final Map<String, HttpServiceConfig> httpEndpoints = new HashMap<>();
    private @Nullable RecoveryConfig recoveryConfig;
    private @Nullable RetentionConfig retentionConfig;
    private @Nullable CallbackUrlProvider callbackUrlProvider;

    private Builder() {}

    /**
     * Caps how many sagas may be executing at once; a start arriving at the cap is refused
     * immediately with {@link SagaOverloadedException} rather than queued. Defaults to {@link
     * #DEFAULT_MAX_CONCURRENT_SAGA_EXECUTIONS} (0, no cap).
     *
     * <p>A permit is held per drive, so a saga parked on an outside system holds nothing, and
     * resumes, recovery and admin drives are never refused. The refusal happens after validation
     * and before anything is persisted: the saga does not exist and its ID is still free.
     *
     * <p><b>Sizing.</b> Steady-state in-flight population is arrival rate times saga duration
     * (Little's law), so start from the arrival rate you intend to serve and the duration you
     * measure, add headroom for bursts, and bound the result so that worst-case saga latency stays
     * inside the recovery staleness threshold — beyond it, recovery begins claiming sagas that are
     * still running, which is the collapse this cap exists to prevent. Measure rather than guess:
     * the benchmark harness reports in-flight population and worst-case latency for a given
     * workload, and that output is the sizing method.
     *
     * <p><b>Size it together with the daemon's request rate limit</b> ({@code
     * scalar.db.saga.server.max_start_requests_per_minute}), which bounds a different quantity:
     * that limits how often one principal may ask, at the transport; this limits how much work the
     * engine is doing for everyone. Neither substitutes for the other — a rate limit cannot see
     * duration, so a downstream slowdown multiplies the in-flight population at an unchanged
     * arrival rate, while a cap alone leaves one caller free to spend everyone's capacity on cheap
     * refusals. Both are off by default. The check that connects them: aggregate admitted arrival
     * (principals times per-principal limit) times typical duration should sit comfortably below
     * this cap, so the rate limiter does the everyday shaping and the cap is the backstop that
     * fires only when duration blows out. If it sits above, callers within their limits are refused
     * routinely and "server full" stops being an exceptional signal.
     *
     * @param maxConcurrentSagaExecutions the maximum number of concurrently executing sagas, or 0
     *     for no cap; must not be negative
     * @return this builder
     */
    public Builder maxConcurrentSagaExecutions(int maxConcurrentSagaExecutions) {
      if (maxConcurrentSagaExecutions < 0) {
        throw new IllegalArgumentException(
            "maxConcurrentSagaExecutions must not be negative, got " + maxConcurrentSagaExecutions);
      }
      this.maxConcurrentSagaExecutions = maxConcurrentSagaExecutions;
      return this;
    }

    /**
     * Sets the store factory. The factory's {@link SagaStoreFactory#createStore()} method is called
     * during {@link #build()}, and the resulting store is closed during {@link
     * DefaultSagaOrchestrator#close()}.
     *
     * <p>For testing, a lambda returning a mock store can be used:
     *
     * <pre>{@code
     * DefaultSagaOrchestrator.newBuilder().storeFactory(() -> mockStore).build();
     * }</pre>
     *
     * @param factory the store factory
     * @return this builder
     */
    public Builder storeFactory(SagaStoreFactory factory) {
      this.storeFactory = Objects.requireNonNull(factory, "factory must not be null");
      return this;
    }

    /**
     * Sets the owner ID for this engine instance. Defaults to a random UUID. The value is stamped
     * on claimed saga rows and echoed in log lines, so it is validated like a saga ID: {@code
     * [a-zA-Z0-9._-]{1,128}}.
     *
     * @param ownerId the owner ID (e.g., pod name, hostname)
     * @return this builder
     * @throws IllegalArgumentException if the value has other characters or an invalid length
     */
    public Builder ownerId(String ownerId) {
      Objects.requireNonNull(ownerId, "ownerId must not be null");
      if (!OWNER_ID_PATTERN.matcher(ownerId).matches()) {
        throw new IllegalArgumentException("ownerId must match " + OWNER_ID_PATTERN.pattern());
      }
      this.ownerId = ownerId;
      return this;
    }

    /**
     * Sets the shutdown mode. Defaults to {@link #DEFAULT_SHUTDOWN_MODE}.
     *
     * @param shutdownMode the shutdown mode
     * @return this builder
     */
    public Builder shutdownMode(ShutdownMode shutdownMode) {
      this.shutdownMode = Objects.requireNonNull(shutdownMode, "shutdownMode must not be null");
      return this;
    }

    /**
     * Sets the shutdown timeout in milliseconds. Defaults to {@value
     * #DEFAULT_SHUTDOWN_TIMEOUT_MILLIS}.
     *
     * @param shutdownTimeoutMillis the shutdown timeout
     * @return this builder
     */
    public Builder shutdownTimeoutMillis(long shutdownTimeoutMillis) {
      this.shutdownTimeoutMillis = shutdownTimeoutMillis;
      return this;
    }

    /**
     * Sets a default saga timeout in milliseconds, applied at execution to any definition that
     * specifies no timeout of its own ({@code timeoutMillis == 0}). The default is applied at every
     * execution entry — start, recovery resume, and parked resume — by recomputing the deadline at
     * each drive, so an in-flight saga's effective timeout follows the value configured at each
     * resumption, and the stored definition never carries a baked-in copy of it. Disabling the
     * default ({@code 0}) is one-way for parked sagas, though: a saga whose park deadline came only
     * from this default re-parks with no deadline on its next drive, which does not merely widen
     * its deadline but removes it from the parked-timeout sweep for good; raising the default again
     * later cannot re-bound it, and only its callback or a forced completion moves it. Defaults to
     * {@value #DEFAULT_SAGA_TIMEOUT_MILLIS}: definitions without a timeout run without one.
     *
     * @param defaultSagaTimeoutMillis the default saga timeout; {@code 0} applies none
     * @return this builder
     * @throws IllegalArgumentException if the value is negative
     */
    public Builder defaultSagaTimeoutMillis(long defaultSagaTimeoutMillis) {
      if (defaultSagaTimeoutMillis < 0) {
        throw new IllegalArgumentException(
            "defaultSagaTimeoutMillis must be >= 0, got " + defaultSagaTimeoutMillis);
      }
      this.defaultSagaTimeoutMillis = defaultSagaTimeoutMillis;
      return this;
    }

    /**
     * Bounds the timeline returned by {@link SagaOrchestrator#getSagaDetail(String)}: when a saga's
     * history is longer, the newest {@code maxTimelineEvents} events are returned and the detail is
     * flagged {@link SagaDetail#isTruncated() truncated}. The full history remains in the store.
     * Defaults to {@link #DEFAULT_MAX_TIMELINE_EVENTS} (effectively unbounded, the embedded-mode
     * behavior); a remote front end should set a real bound.
     *
     * @param maxTimelineEvents the maximum number of timeline events per detail read; must be
     *     positive
     * @return this builder
     */
    public Builder maxTimelineEvents(int maxTimelineEvents) {
      if (maxTimelineEvents < 1) {
        throw new IllegalArgumentException(
            "maxTimelineEvents must be positive, got " + maxTimelineEvents);
      }
      this.maxTimelineEvents = maxTimelineEvents;
      return this;
    }

    /**
     * Sets the clock (for testing). Defaults to {@link Clock#systemUTC()}.
     *
     * @param clock the clock
     * @return this builder
     */
    public Builder clock(Clock clock) {
      this.clock = Objects.requireNonNull(clock, "clock must not be null");
      return this;
    }

    /**
     * Registers a named resource for constructor injection during step resolution.
     *
     * @param type the resource type
     * @param instance the resource instance
     * @param name the qualifier name (must match {@code @Named} on constructor parameters)
     * @return this builder
     */
    public <T> Builder resource(Class<T> type, T instance, String name) {
      Objects.requireNonNull(type, "type must not be null");
      Objects.requireNonNull(instance, "instance must not be null");
      Objects.requireNonNull(name, "name must not be null");
      getOrCreateResourceRegistryBuilder().add(type, instance, name);
      return this;
    }

    /**
     * Registers an unnamed resource for constructor injection during step resolution.
     *
     * @param type the resource type
     * @param instance the resource instance
     * @return this builder
     */
    public <T> Builder resource(Class<T> type, T instance) {
      Objects.requireNonNull(type, "type must not be null");
      Objects.requireNonNull(instance, "instance must not be null");
      getOrCreateResourceRegistryBuilder().add(type, instance);
      return this;
    }

    /**
     * Sets a custom step resolver for full control over step instantiation. Mutually exclusive with
     * {@link #resource}.
     *
     * @param stepResolver the step resolver
     * @return this builder
     */
    public Builder stepResolver(StepResolver stepResolver) {
      Objects.requireNonNull(stepResolver, "stepResolver must not be null");
      this.customStepResolver = stepResolver;
      return this;
    }

    /**
     * Begins registering an HTTP endpoint under {@code name} pointing at {@code baseUrl}. One
     * mode-free remote method: the registered endpoint both (a) injects an {@code @Named(name)
     * SagaHttpClient} into code steps and (b) backs declaratively-defined service steps whose
     * {@code service} resolves to it. The framework propagates the saga correlation headers,
     * enforces the outbound HTTP policy (SSRF allowlist + body limits), and disables redirect
     * following.
     *
     * <p>{@code httpEndpoint} is <b>orthogonal</b> to step resolution: it may be combined with
     * either {@link #resource} or {@link #stepResolver(StepResolver)} (those two remain mutually
     * exclusive; {@code httpEndpoint} does not trip that rule).
     *
     * <p>Configure the optional outbound policy / client on the returned sub-builder, then call
     * {@link HttpEndpointBuilder#add()} to commit it and return to this builder:
     *
     * <pre>{@code
     * DefaultSagaOrchestrator.newBuilder()
     *     .storeFactory(...)
     *     .httpEndpoint("account-svc", "https://account-svc:8443")
     *         .allowedHosts("account-svc")
     *         .maxBodyBytes(2_000_000)
     *         .add()
     *     .build();
     * }</pre>
     *
     * @param name the endpoint name (the {@code @Named} qualifier for injection and the {@code
     *     service} referenced by declarative steps)
     * @param baseUrl the endpoint base URL (e.g. {@code http://account-svc:8080})
     * @return a sub-builder for this endpoint's optional outbound configuration
     */
    public HttpEndpointBuilder httpEndpoint(String name, String baseUrl) {
      Objects.requireNonNull(name, "name must not be null");
      Objects.requireNonNull(baseUrl, "baseUrl must not be null");
      if (name.isBlank()) {
        throw new IllegalArgumentException("name must not be blank");
      }
      if (baseUrl.isBlank()) {
        throw new IllegalArgumentException("baseUrl must not be blank");
      }
      // Fail fast here at build time rather than at the first saga run. The endpoint name is
      // safe context; the shared validator deliberately does not echo the URL (see its javadoc).
      try {
        HttpServiceConfig.validateBaseUrl(baseUrl);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("endpoint '" + name + "': " + e.getMessage(), e);
      }
      return new HttpEndpointBuilder(name, baseUrl);
    }

    /**
     * Overrides the default recovery configuration.
     *
     * @param recoveryConfig the recovery configuration
     * @return this builder
     */
    public Builder recoveryConfig(RecoveryConfig recoveryConfig) {
      this.recoveryConfig =
          Objects.requireNonNull(recoveryConfig, "recoveryConfig must not be null");
      return this;
    }

    /**
     * Overrides the default retention configuration.
     *
     * @param retentionConfig the retention configuration
     * @return this builder
     */
    public Builder retentionConfig(RetentionConfig retentionConfig) {
      this.retentionConfig =
          Objects.requireNonNull(retentionConfig, "retentionConfig must not be null");
      return this;
    }

    /**
     * Sets the provider that mints the callback URL injected into an async step's outgoing request
     * (daemon mode). When unset, async steps cannot be provisioned and registering an async
     * definition fails fast.
     *
     * @param callbackUrlProvider the callback URL provider
     * @return this builder
     */
    public Builder callbackUrlProvider(CallbackUrlProvider callbackUrlProvider) {
      this.callbackUrlProvider =
          Objects.requireNonNull(callbackUrlProvider, "callbackUrlProvider must not be null");
      return this;
    }

    /**
     * Builds and returns a configured {@link DefaultSagaOrchestrator}.
     *
     * @return the orchestrator
     * @throws IllegalStateException if no store factory is configured, or if both {@code
     *     resource()} and {@code stepResolver()} were called
     */
    public DefaultSagaOrchestrator build() {
      if (storeFactory == null) {
        throw new IllegalStateException(
            "SagaStoreFactory is required — call storeFactory() before build()");
      }
      if (resourceRegistryBuilder != null && customStepResolver != null) {
        throw new IllegalStateException(
            "resource() and stepResolver() are mutually exclusive — use one or the other");
      }

      SagaStore store = null;
      HttpEndpointManager endpointManager = null;
      try {
        store = storeFactory.createStore();
        // The orchestrator owns the HTTP endpoints created from httpEndpoint(...): they are closed
        // on close (or here if build fails) — mirroring the store's lifecycle. A code step's
        // SagaHttpClient and a declarative step against the same endpoint share one HttpExchange
        // (one client, one policy).
        endpointManager = HttpEndpointManager.create(httpEndpoints, callbackUrlProvider);
        StepResolver resolver = buildStepResolver();

        RecoveryConfig resolvedRecoveryConfig =
            recoveryConfig != null ? recoveryConfig : RecoveryConfig.defaults(clock);
        RetentionConfig resolvedRetentionConfig =
            retentionConfig != null ? retentionConfig : RetentionConfig.defaults(clock);

        SagaEngine.ShutdownConfig shutdownConfig =
            new SagaEngine.ShutdownConfig(shutdownMode, shutdownTimeoutMillis);
        StepInstantiator stepInstantiator = new StepInstantiator(resolver, endpointManager);
        SagaEngine engine =
            new SagaEngine(
                store, stepInstantiator, ownerId, shutdownConfig, defaultSagaTimeoutMillis, clock);
        SagaDefinitionRegistry definitionRegistry = new SagaDefinitionRegistry(store);

        SagaRecoveryManager recoveryManager =
            new SagaRecoveryManager(
                store, engine, definitionRegistry, ownerId, resolvedRecoveryConfig);
        SagaRetentionManager retentionManager =
            new SagaRetentionManager(store, ownerId, resolvedRetentionConfig);

        return new DefaultSagaOrchestrator(
            engine,
            store,
            definitionRegistry,
            recoveryManager,
            retentionManager,
            shutdownTimeoutMillis,
            maxTimelineEvents,
            maxConcurrentSagaExecutions);
      } catch (Throwable t) {
        // Roll back the resources that hold real external connections: the store (DB sessions) and
        // the HTTP endpoint manager (holds HTTP clients). Each is null if its own creation threw,
        // so each close is null-guarded. The engine and the recovery/retention managers constructed
        // inside the try only hold executors that stay inert until started — their threads spin up
        // on start()/first task, never during build — so a failed build leaves them with no live
        // threads to stop, and GC reclaims them. Hence no engine.shutdown() here.
        //
        // Catch Throwable, not Exception: ownership transfers to the caller only on a successful
        // return, so an Error raised after the store is created would otherwise unwind past this
        // block and leak the store. Everything below createStore() can raise one: a
        // NoClassDefFoundError or ExceptionInInitializerError from first-touch loading of the HTTP
        // client stack, or an OutOfMemoryError while building the clients or the engine's
        // executors. The resources are still released, and t is rethrown unchanged. Precise rethrow
        // keeps this compiling without a throws clause: the try body raises no checked exceptions.
        if (endpointManager != null) {
          try {
            endpointManager.close();
          } catch (Throwable closeException) {
            t.addSuppressed(closeException);
          }
        }
        if (store != null) {
          try {
            store.close();
          } catch (Throwable closeException) {
            t.addSuppressed(closeException);
          }
        }
        throw t;
      }
    }

    private StepResolver buildStepResolver() {
      if (customStepResolver != null) {
        return customStepResolver;
      }
      ResourceRegistry resourceRegistry =
          resourceRegistryBuilder != null
              ? resourceRegistryBuilder.build()
              : ResourceRegistry.newBuilder().build();
      return new ReflectiveStepResolver(resourceRegistry);
    }

    private ResourceRegistry.Builder getOrCreateResourceRegistryBuilder() {
      if (resourceRegistryBuilder == null) {
        resourceRegistryBuilder = ResourceRegistry.newBuilder();
      }
      return resourceRegistryBuilder;
    }

    /** Accumulates one HTTP endpoint's optional outbound config until {@link #add()}. */
    public final class HttpEndpointBuilder {

      private final String name;
      private final String baseUrl;
      private final List<String> allowedHosts = new ArrayList<>();
      private final Map<String, String> defaultHeaders = new LinkedHashMap<>();
      private long maxBodyBytes = -1; // -1 = use the default
      private @Nullable HttpClient httpClient;

      private HttpEndpointBuilder(String name, String baseUrl) {
        this.name = name;
        this.baseUrl = baseUrl;
      }

      /**
       * Restricts outbound calls for this endpoint to the given hosts (SSRF allowlist). Empty (the
       * default) = allow all. Matching is by exact, case-insensitive host name only — it does not
       * resolve the host or inspect the connect-time IP, so it does not defend against DNS
       * rebinding or a hostname that points at a private/link-local/metadata address. It is
       * defense-in-depth for trusted, operator-configured endpoints, not a sandbox.
       *
       * @param hosts the allowed host names
       * @return this sub-builder
       */
      public HttpEndpointBuilder allowedHosts(String... hosts) {
        Objects.requireNonNull(hosts, "hosts must not be null");
        for (String host : hosts) {
          allowedHosts.add(Objects.requireNonNull(host, "host must not be null"));
        }
        return this;
      }

      /**
       * Sets the maximum request/response body size in bytes for this endpoint. Defaults to 1 MB.
       *
       * @param maxBodyBytes the maximum body size; must be {@code > 0}
       * @return this sub-builder
       */
      public HttpEndpointBuilder maxBodyBytes(long maxBodyBytes) {
        if (maxBodyBytes <= 0) {
          throw new IllegalArgumentException("maxBodyBytes must be > 0, got " + maxBodyBytes);
        }
        this.maxBodyBytes = maxBodyBytes;
        return this;
      }

      /**
       * Uses a custom {@link HttpClient} (e.g. with a proxy or custom TLS) for this endpoint. The
       * caller owns the supplied client's lifecycle — unlike the framework-created default client,
       * it is <em>not</em> closed on {@link DefaultSagaOrchestrator#close()}. The caller is
       * responsible for its redirect policy; the framework-created default disables redirects to
       * protect the SSRF allowlist.
       *
       * @param client the client to use for this endpoint
       * @return this sub-builder
       */
      public HttpEndpointBuilder httpClient(HttpClient client) {
        this.httpClient = Objects.requireNonNull(client, "client must not be null");
        return this;
      }

      /**
       * Adds a default header sent on <em>every</em> request to this endpoint — by both declarative
       * steps and the injected {@link com.scalar.db.saga.api.SagaHttpClient SagaHttpClient} for
       * code steps. This is the channel for authentication and other secrets (e.g. {@code
       * Authorization}), since default headers are <b>not</b> persisted in the saga definition.
       *
       * <p>Per-name precedence: a default header here is overridden by a per-call header of the
       * same name (only code steps set per-call headers, via {@link
       * com.scalar.db.saga.api.SagaHttpClient.Request#header(String, String)}); the framework
       * correlation headers ({@code X-Saga-Id}/{@code X-Saga-Step}) are always set by the
       * framework. Repeatable; a later call for the same name replaces the earlier value.
       *
       * @param name the header name
       * @param value the header value
       * @return this sub-builder
       */
      public HttpEndpointBuilder defaultHeader(String name, String value) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(value, "value must not be null");
        defaultHeaders.put(name, value);
        return this;
      }

      /**
       * Adds all of {@code headers} as endpoint default headers (see {@link #defaultHeader(String,
       * String)}). Merged with any previously set default headers; a key present in {@code headers}
       * replaces an earlier value for the same name.
       *
       * @param headers the default headers to add
       * @return this sub-builder
       */
      public HttpEndpointBuilder defaultHeaders(Map<String, String> headers) {
        Objects.requireNonNull(headers, "headers must not be null");
        headers.forEach(
            (name, value) -> {
              Objects.requireNonNull(name, "header name must not be null");
              Objects.requireNonNull(value, "header value must not be null");
              defaultHeaders.put(name, value);
            });
        return this;
      }

      /**
       * Registers this endpoint on the parent builder and returns it for chaining.
       *
       * @return the parent builder
       */
      public Builder add() {
        if (httpEndpoints.containsKey(name)) {
          throw new IllegalArgumentException("HTTP endpoint already registered: " + name);
        }
        httpEndpoints.put(
            name,
            new HttpServiceConfig(baseUrl, allowedHosts, maxBodyBytes, httpClient, defaultHeaders));
        return Builder.this;
      }
    }
  }
}
