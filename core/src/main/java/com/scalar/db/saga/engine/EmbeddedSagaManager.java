package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinitionParser;
import com.scalar.db.saga.api.SagaManager;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StepEvent;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link SagaManager}. Delegates saga lifecycle logic to {@link
 * SagaEngine}, handles definition registration via {@link SagaDefinitionRegistry}, async execution
 * via virtual threads, and callback dispatch.
 */
@ThreadSafe
class EmbeddedSagaManager implements SagaManager {

  private static final Logger logger = LoggerFactory.getLogger(EmbeddedSagaManager.class);

  private final SagaEngine engine;
  private final SagaStore store;
  private final SagaDefinitionRegistry registry;
  private final long shutdownTimeoutMillis;
  private final ExecutorService asyncExecutor;
  private volatile boolean closed;

  EmbeddedSagaManager(
      SagaEngine engine,
      SagaStore store,
      SagaDefinitionRegistry registry,
      long shutdownTimeoutMillis) {
    this(
        engine,
        store,
        registry,
        shutdownTimeoutMillis,
        Executors.newVirtualThreadPerTaskExecutor());
  }

  // Visible for testing
  EmbeddedSagaManager(
      SagaEngine engine,
      SagaStore store,
      SagaDefinitionRegistry registry,
      long shutdownTimeoutMillis,
      ExecutorService asyncExecutor) {
    this.engine = engine;
    this.store = store;
    this.registry = registry;
    this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    this.asyncExecutor = asyncExecutor;
  }

  // ---------------------------------------------------------------------------
  // Registration
  // ---------------------------------------------------------------------------

  @Override
  public void register(SagaDefinition definition) {
    // Eagerly resolve all steps — fail fast on missing resources or unresolvable constructors.
    // This must happen before persisting to the store, so invalid definitions are never stored.
    engine.getOrBuildPlan(definition);
    registry.register(definition);
  }

  @Override
  public void register(Path definitionFile) {
    register(SagaDefinitionParser.parseFile(definitionFile));
  }

  // ---------------------------------------------------------------------------
  // Synchronous start
  // ---------------------------------------------------------------------------

  @Override
  public String start(String sagaName, Map<String, Object> input) {
    ensureOpen();
    SagaDefinition def = requireDefinition(sagaName);
    return engine.execute(def, null, input);
  }

  @Override
  public void start(String sagaId, String sagaName, Map<String, Object> input) {
    ensureOpen();
    SagaDefinition def = requireDefinition(sagaName);
    engine.execute(def, sagaId, input);
  }

  // ---------------------------------------------------------------------------
  // Asynchronous start
  // ---------------------------------------------------------------------------

  @Override
  public String startAsync(String sagaName, Map<String, Object> input) {
    return startAsyncInternal(null, sagaName, input, null).getSagaId();
  }

  @Override
  public String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback) {
    return startAsyncInternal(null, sagaName, input, callback).getSagaId();
  }

  @Override
  public void startAsync(String sagaId, String sagaName, Map<String, Object> input) {
    startAsyncInternal(sagaId, sagaName, input, null);
  }

  @Override
  public void startAsync(
      String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback) {
    startAsyncInternal(sagaId, sagaName, input, callback);
  }

  /**
   * Shared async-launch path. Persists the saga synchronously (so it is recoverable even if the
   * process crashes before the virtual thread starts), then submits execution to a virtual thread.
   */
  private SagaStateSnapshot startAsyncInternal(
      @Nullable String sagaId,
      String sagaName,
      Map<String, Object> input,
      @Nullable SagaCallback callback) {
    ensureOpen();
    SagaDefinition def = requireDefinition(sagaName);

    // Persist synchronously — saga is recoverable from this point
    SagaStateSnapshot saga = engine.createSaga(def, sagaId, input);

    // Submit execution to a virtual thread. The returned Future is intentionally unused:
    // saga state is persisted, so recovery handles failures. Storing the future would require
    // managing its lifecycle (fire-and-forget pattern).
    submitAsync(def, saga, input, callback);

    return saga;
  }

  @SuppressWarnings("FutureReturnValueIgnored") // fire-and-forget; recovery handles failures
  private void submitAsync(
      SagaDefinition def,
      SagaStateSnapshot saga,
      Map<String, Object> input,
      @Nullable SagaCallback callback) {
    try {
      asyncExecutor.submit(
          () -> {
            try {
              engine.executeSaga(def, saga, input);
            } catch (Exception e) {
              // Saga state is persisted — recovery will pick it up
              logger.error("Async saga {} failed unexpectedly", saga.getSagaId(), e);
            } finally {
              try {
                dispatchCallback(saga.getSagaId(), callback);
              } catch (Exception e) {
                logger.error("Failed to dispatch callback for saga {}", saga.getSagaId(), e);
              }
            }
          });
    } catch (RejectedExecutionException e) {
      // Race between close() and submit — saga is already persisted, recovery will handle it
      logger.warn(
          "Async executor rejected saga {} (shutting down); recovery will handle it",
          saga.getSagaId(),
          e);
    }
  }

  private void dispatchCallback(String sagaId, @Nullable SagaCallback callback) {
    if (callback == null) {
      return;
    }
    SagaStateSnapshot result =
        store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
    switch (result.getStatus()) {
      case COMPLETED -> callback.onCompleted(result);
      case COMPENSATED -> callback.onCompensated(result);
      case ESCALATED -> callback.onEscalated(result);
      default ->
          logger.warn("Saga {} ended in non-terminal status: {}", sagaId, result.getStatus());
    }
  }

  // ---------------------------------------------------------------------------
  // Resume / Compensate
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot resume(String sagaId) {
    SagaStateSnapshot saga = getStateSnapshot(sagaId);
    if (saga.getStatus() != SagaStatus.RUNNING) {
      throw new IllegalStateException(
          "Cannot resume saga " + sagaId + " in status " + saga.getStatus());
    }
    SagaDefinition def = resolveDefinition(saga);
    List<SagaEvent> events = store.getEvents(sagaId);
    ExecutionContext context = engine.replayEvents(saga, events);

    int lastCompleted = stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1);

    return engine.resumeFrom(def, context, lastCompleted + 1);
  }

  @Override
  public SagaStateSnapshot compensate(String sagaId) {
    SagaStateSnapshot saga = getStateSnapshot(sagaId);
    if (saga.getStatus() != SagaStatus.COMPENSATING) {
      throw new IllegalStateException(
          "Cannot compensate saga " + sagaId + " in status " + saga.getStatus());
    }
    SagaDefinition def = resolveDefinition(saga);
    List<SagaEvent> events = store.getEvents(sagaId);
    ExecutionContext context = engine.replayEvents(saga, events);

    int lastCompensated =
        stepIndices(events, EventType.STEP_COMPENSATED).min().orElse(Integer.MAX_VALUE);

    // Compensate from the step before the last compensated one (or from last completed if none)
    int fromStep;
    if (lastCompensated < Integer.MAX_VALUE) {
      fromStep = lastCompensated - 1;
    } else {
      // No compensation started yet — find the last completed step
      fromStep = stepIndices(events, EventType.STEP_COMPLETED).max().orElse(-1);
    }

    engine.compensateFrom(def, context, fromStep);
    return context.getCurrentState();
  }

  // ---------------------------------------------------------------------------
  // Query
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot getStateSnapshot(String sagaId) {
    return store.getStateSnapshot(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
  }

  // ---------------------------------------------------------------------------
  // Daemon mode only
  // ---------------------------------------------------------------------------

  @Override
  public SagaStateSnapshot completeStep(
      String sagaId, String stepName, Map<String, Object> output) {
    // completeStep resumes a parked (WAITING) saga when an external callback arrives.
    // It is only available in daemon mode, which uses a separate SagaManager implementation.
    throw new UnsupportedOperationException("completeStep is only available in daemon mode");
  }

  // ---------------------------------------------------------------------------
  // Recovery
  // ---------------------------------------------------------------------------

  @Override
  public void startRecovery() {
    // Recovery manager is not yet implemented (Task 1.11).
    // This is a no-op placeholder until SagaRecoveryManager is available.
    logger.info("Recovery not yet implemented — skipping startRecovery()");
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void close() {
    closed = true;
    asyncExecutor.shutdown();
    engine.shutdown();
    try {
      if (!asyncExecutor.awaitTermination(shutdownTimeoutMillis, TimeUnit.MILLISECONDS)) {
        asyncExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      asyncExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("SagaManager is closed");
    }
  }

  private SagaDefinition requireDefinition(String sagaName) {
    SagaDefinition def = registry.get(sagaName);
    if (def == null) {
      throw new SagaDefinitionNotFoundException(sagaName);
    }
    return def;
  }

  private SagaDefinition resolveDefinition(SagaStateSnapshot saga) {
    SagaDefinition def = registry.resolve(saga.getSagaName(), saga.getDefinitionVersion());
    if (def == null) {
      throw new SagaDefinitionNotFoundException(saga.getSagaName());
    }
    return def;
  }

  private static IntStream stepIndices(List<SagaEvent> events, EventType eventType) {
    return events.stream()
        .filter(e -> e instanceof StepEvent)
        .map(e -> (StepEvent) e)
        .filter(e -> e.getEventType() == eventType)
        .mapToInt(StepEvent::getStepIndex);
  }
}
