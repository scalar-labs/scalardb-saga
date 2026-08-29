package com.scalar.db.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.definition.RetryPolicy;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.engine.DefaultSagaOrchestrator;
import com.scalar.db.saga.engine.RecoveryConfig;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaSchema;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import com.scalar.db.saga.store.SagaStore.ScanCursor;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.store.SweepScatter;
import com.scalar.db.saga.testing.CrashingStoreDecorator;
import com.scalar.db.saga.testing.FakeStep;
import com.scalar.db.saga.testing.FakeTccStep;
import com.scalar.db.saga.testing.ForwardingSagaStore;
import com.scalar.db.saga.testing.SimulatedCrashError;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SagaIntegrationTest {

  private static final String STEP_CLASS = FakeStep.class.getName();
  private static final String TCC_STEP_CLASS = FakeTccStep.class.getName();

  private Path tempDbPath;
  private Properties props;

  @BeforeEach
  void setUp() throws Exception {
    tempDbPath = Files.createTempFile("saga-test-", ".db");

    props = new Properties();
    props.setProperty("scalar.db.storage", "jdbc");
    props.setProperty(
        "scalar.db.contact_points",
        "jdbc:sqlite:" + tempDbPath.toAbsolutePath() + "?busy_timeout=10000");
    props.setProperty("scalar.db.saga.store.num_buckets", "1");
  }

  @AfterEach
  void tearDown() throws Exception {
    Files.deleteIfExists(tempDbPath);
  }

  private DefaultSagaOrchestrator buildOrchestrator(Map<String, Object> steps) {
    return DefaultSagaOrchestrator.newBuilder()
        .storeFactory(ScalarDbSagaStoreFactory.create(props))
        .stepResolver(
            (name, cls, ctx) -> {
              Object step = steps.get(name);
              if (step == null) {
                throw new IllegalArgumentException("No step registered for: " + name);
              }
              return step;
            })
        .build();
  }

  private DefaultSagaOrchestrator buildOrchestrator(
      SagaStore sagaStore, Map<String, Object> steps) {
    return DefaultSagaOrchestrator.newBuilder()
        .storeFactory(() -> sagaStore)
        .stepResolver(
            (name, cls, ctx) -> {
              Object step = steps.get(name);
              if (step == null) {
                throw new IllegalArgumentException("No step registered for: " + name);
              }
              return step;
            })
        .build();
  }

  private DefaultSagaOrchestrator buildOrchestrator(Map<String, Object> steps, Clock clock) {
    return DefaultSagaOrchestrator.newBuilder()
        .storeFactory(ScalarDbSagaStoreFactory.create(props))
        .stepResolver(
            (name, cls, ctx) -> {
              Object step = steps.get(name);
              if (step == null) {
                throw new IllegalArgumentException("No step registered for: " + name);
              }
              return step;
            })
        .clock(clock)
        .build();
  }

  /** A test {@link Clock} whose instant can be advanced to drive deadline-based behavior. */
  private static final class MutableClock extends Clock {
    private volatile Instant instant;

    MutableClock(Instant instant) {
      this.instant = instant;
    }

    void advance(Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public Instant instant() {
      return instant;
    }

    @Override
    public long millis() {
      return instant.toEpochMilli();
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }

  // ---------------------------------------------------------------------------
  // Saga Lifecycle
  // ---------------------------------------------------------------------------

  @Nested
  class SagaLifecycle {

    @Test
    void start_withBackwardRecoveryAllStepsSucceed_completes() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.of("a", 1)).build();
      FakeStep step2 = FakeStep.newBuilder("step2").executeReturns(StepResult.of("b", 2)).build();
      FakeStep step3 = FakeStep.newBuilder("step3").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .step("step3", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2, "step3", step3))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(step1.getExecutionCount()).isEqualTo(1);
        assertThat(step2.getExecutionCount()).isEqualTo(1);
        assertThat(step3.getExecutionCount()).isEqualTo(1);
        assertThat(step1.getCompensationCount()).isEqualTo(0);
        assertThat(step2.getCompensationCount()).isEqualTo(0);
        assertThat(step3.getCompensationCount()).isEqualTo(0);
      }
    }

    @Test
    void start_withBackwardRecoveryLastStepFails_compensatesInReverseOrder() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      FakeStep step3 =
          FakeStep.newBuilder("step3")
              .executeFails(new StepExecutionException("step3 failed", false))
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .step("step3", STEP_CLASS)
              .add()
              .build();

      String sagaId;
      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2, "step3", step3))) {
        orchestrator.register(def);

        // Act
        sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(step1.getExecutionCount()).isEqualTo(1);
        assertThat(step2.getExecutionCount()).isEqualTo(1);
        assertThat(step3.getExecutionCount()).isEqualTo(1);
        // step3's execute failed, but its non-delivery is not proven, so it may have committed →
        // it is compensated too (compensate from i, not i-1).
        assertThat(step3.getCompensationCount()).isEqualTo(1);
        assertThat(step2.getCompensationCount()).isEqualTo(1);
        assertThat(step1.getCompensationCount()).isEqualTo(1);
      }

      // Verify compensation event ordering (LIFO: step3 → step2 → step1) by reading persisted
      // events through an independent store — the orchestrator owns and has already closed its own
      // store.
      try (SagaStore eventStore = ScalarDbSagaStoreFactory.create(props).createStore()) {
        List<SagaEvent> events = eventStore.getEvents(sagaId);
        List<StepEvent> compensatedEvents =
            events.stream()
                .filter(e -> e.getEventType() == EventType.STEP_COMPENSATED)
                .map(e -> (StepEvent) e)
                .toList();
        assertThat(compensatedEvents).hasSize(3);
        assertThat(compensatedEvents.get(0).getStepName()).isEqualTo("step3");
        assertThat(compensatedEvents.get(1).getStepName()).isEqualTo("step2");
        assertThat(compensatedEvents.get(2).getStepName()).isEqualTo("step1");
      }
    }

    @Test
    void start_withForwardRecoveryAllStepsSucceed_completes() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(RecoveryStrategy.FORWARD)
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
      }
    }

    @Test
    void start_withForwardRecoveryStepFails_staysRunning() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 =
          FakeStep.newBuilder("step2")
              .executeFails(new StepExecutionException("step2 failed", false))
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(RecoveryStrategy.FORWARD)
              .defaultRetryPolicy(
                  RetryPolicy.newBuilder()
                      .maxAttempts(1)
                      .initialIntervalMillis(1)
                      .maxIntervalMillis(1)
                      .build())
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert — FORWARD strategy: no compensation, saga stays RUNNING
        assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
        assertThat(step1.getCompensationCount()).isEqualTo(0);
      }
    }

    @Test
    void start_withMixedRecoveryFailureBeforePivot_compensates() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 =
          FakeStep.newBuilder("step2")
              .executeFails(new StepExecutionException("failed", false))
              .build();
      FakeStep step3 = FakeStep.newBuilder("step3").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(RecoveryStrategy.MIXED)
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .pivot(true)
              .add()
              .step("step3", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2, "step3", step3))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert — failure at pivot (step2), compensate step1
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(step1.getCompensationCount()).isEqualTo(1);
      }
    }

    @Test
    void start_withMixedRecoveryFailureAfterPivot_staysRunning() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      FakeStep step3 =
          FakeStep.newBuilder("step3")
              .executeFails(new StepExecutionException("failed", false))
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(RecoveryStrategy.MIXED)
              .defaultRetryPolicy(
                  RetryPolicy.newBuilder()
                      .maxAttempts(1)
                      .initialIntervalMillis(1)
                      .maxIntervalMillis(1)
                      .build())
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .pivot(true)
              .add()
              .step("step3", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2, "step3", step3))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert — failure after pivot: no compensation, saga stays RUNNING for recovery
        assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
        assertThat(step1.getCompensationCount()).isEqualTo(0);
        assertThat(step2.getCompensationCount()).isEqualTo(0);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // TCC Lifecycle
  // ---------------------------------------------------------------------------

  @Nested
  class TccLifecycle {

    @Test
    void start_allReservesSucceed_confirmsAndCompletes() {
      // Arrange
      FakeTccStep step1 =
          FakeTccStep.newBuilder("step1").reserveReturns(StepResult.of("res1", "ok")).build();
      FakeTccStep step2 = FakeTccStep.newBuilder("step2").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .step("step1", TCC_STEP_CLASS)
              .add()
              .step("step2", TCC_STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("tcc-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(step1.getReservations()).hasSize(1);
        assertThat(step2.getReservations()).hasSize(1);
        assertThat(step1.getConfirmations()).hasSize(1);
        assertThat(step2.getConfirmations()).hasSize(1);
        assertThat(step1.getCancellations()).isEmpty();
        assertThat(step2.getCancellations()).isEmpty();
      }
    }

    @Test
    void start_secondReserveFails_cancelsBothReserves() {
      // Arrange — step2's reserve fails without proving non-delivery (the default), so its
      // reservation may have committed and must be cancelled too (cancel from i).
      FakeTccStep step1 = FakeTccStep.newBuilder("step1").build();
      FakeTccStep step2 =
          FakeTccStep.newBuilder("step2")
              .reserveFails(new StepExecutionException("reserve failed", false))
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .step("step1", TCC_STEP_CLASS)
              .add()
              .step("step2", TCC_STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("tcc-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(step1.getReservations()).hasSize(1);
        assertThat(step2.getReservations()).hasSize(1);
        assertThat(step1.getCancellations()).hasSize(1);
        assertThat(step2.getCancellations()).hasSize(1);
        assertThat(step1.getConfirmations()).isEmpty();
        assertThat(step2.getConfirmations()).isEmpty();
      }
    }

    @Test
    void start_confirmFails_staysRunning() {
      // Arrange
      FakeTccStep step1 = FakeTccStep.newBuilder("step1").build();
      FakeTccStep step2 =
          FakeTccStep.newBuilder("step2")
              .confirmFails(new StepExecutionException("confirm failed", false))
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .step("step1", TCC_STEP_CLASS)
              .add()
              .step("step2", TCC_STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("tcc-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert — confirm is past the pivot, so no compensation; saga stays RUNNING
        assertThat(result.getStatus()).isEqualTo(SagaStatus.RUNNING);
        assertThat(step1.getReservations()).hasSize(1);
        assertThat(step2.getReservations()).hasSize(1);
        assertThat(step1.getConfirmations()).hasSize(1);
        assertThat(step2.getConfirmations()).hasSize(1);
        assertThat(step1.getCancellations()).isEmpty();
        assertThat(step2.getCancellations()).isEmpty();
      }
    }

    @Test
    void start_reserveProducesOutput_outputFlowsToConfirm() {
      // Arrange
      FakeTccStep step1 =
          FakeTccStep.newBuilder("step1")
              .reserveReturns(StepResult.of("reserveKey", "reserveValue"))
              .build();
      FakeTccStep step2 =
          FakeTccStep.newBuilder("step2")
              .reserveAction(
                  ctx -> {
                    // Verify step1's reserve output is available
                    String val = ctx.get("reserveKey", String.class).orElseThrow();
                    return StepResult.of("step2Saw", val);
                  })
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .step("step1", TCC_STEP_CLASS)
              .add()
              .step("step2", TCC_STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("tcc-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Crash Recovery
  // ---------------------------------------------------------------------------

  @Nested
  class CrashRecovery {

    @Test
    void recover_crashAfterFirstStep_completesRemainingSteps() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.of("a", 1)).build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      Map<String, Object> steps = Map.of("step1", step1, "step2", step2);

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      String sagaId = "crash-test-saga-1";

      // First run — with crash decorator
      try (SagaStore baseStore = ScalarDbSagaStoreFactory.create(props).createStore();
          SagaStore crashingStore = new CrashingStoreDecorator(baseStore, 0);
          DefaultSagaOrchestrator orchestrator = buildOrchestrator(crashingStore, steps)) {
        orchestrator.register(def);

        try {
          orchestrator.start(sagaId, "test-saga", Map.of());
        } catch (SimulatedCrashError expected) {
          // Expected crash
        }

        // Verify saga is still RUNNING after crash
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.RUNNING);
        assertThat(step1.getExecutionCount()).isEqualTo(1);
        assertThat(step2.getExecutionCount()).isEqualTo(0);
      }

      // Restart — new orchestrator with fresh store, no crash decorator
      try (SagaStore recoveryStore = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator recovered = buildOrchestrator(recoveryStore, steps)) {
        recoveryStore.markForRecovery(sagaId);
        recovered.recover();

        // Assert — saga completes after recovery
        SagaStateSnapshot result = recovered.getStateSnapshot(sagaId);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        // step1 not re-executed (its completion event was persisted before crash)
        assertThat(step1.getExecutionCount()).isEqualTo(1);
        // step2 executed during recovery
        assertThat(step2.getExecutionCount()).isEqualTo(1);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Context Data Flow
  // ---------------------------------------------------------------------------

  @Nested
  class ContextDataFlow {

    @Test
    void start_stepProducesOutput_accessibleBySubsequentSteps() {
      // Arrange
      FakeStep step1 =
          FakeStep.newBuilder("step1").executeReturns(StepResult.of("orderId", 42)).build();

      FakeStep step2 =
          FakeStep.newBuilder("step2")
              .executeAction(
                  ctx -> {
                    int orderId = ctx.get("orderId", Integer.class).orElseThrow();
                    return StepResult.of("doubled", orderId * 2);
                  })
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
      }
    }

    @Test
    void start_inputGiven_accessibleByFirstStep() {
      // Arrange
      FakeStep step1 =
          FakeStep.newBuilder("step1")
              .executeAction(
                  ctx -> {
                    String name = ctx.get("name", String.class).orElseThrow();
                    return StepResult.of("greeting", "Hello " + name);
                  })
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga").saga().step("step1", STEP_CLASS).add().build();

      try (DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1))) {
        orchestrator.register(def);

        // Act
        String sagaId = orchestrator.start("test-saga", Map.of("name", "World"));
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Idempotency and Concurrency
  // ---------------------------------------------------------------------------

  @Nested
  class IdempotencyAndConcurrency {

    @Test
    void start_duplicateClientSuppliedId_throwsSagaAlreadyExistsException() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga").saga().step("step1", STEP_CLASS).add().build();

      try (DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1))) {
        orchestrator.register(def);

        // Act — first execution succeeds
        orchestrator.start("my-saga-id", "test-saga", Map.of());

        // Assert — second execution with same ID fails
        assertThatThrownBy(() -> orchestrator.start("my-saga-id", "test-saga", Map.of()))
            .isInstanceOf(SagaAlreadyExistsException.class);
      }
    }

    @Test
    void start_concurrentExecution_allComplete() throws Exception {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        int numSagas = 5;
        CopyOnWriteArrayList<SagaStateSnapshot> results = new CopyOnWriteArrayList<>();

        // Act — run multiple sagas concurrently
        ExecutorService executor = Executors.newFixedThreadPool(numSagas);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new java.util.ArrayList<>();

        for (int i = 0; i < numSagas; i++) {
          String sagaId = "saga-" + i;
          futures.add(
              executor.submit(
                  () -> {
                    try {
                      latch.await();
                      orchestrator.start(sagaId, "test-saga", Map.of("idx", sagaId));
                      SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);
                      results.add(result);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }));
        }
        latch.countDown(); // Start all threads
        try {
          for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
          }
        } finally {
          executor.shutdown();
          executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        // Assert — all sagas completed
        assertThat(results).hasSize(numSagas);
        assertThat(results).allMatch(r -> r.getStatus() == SagaStatus.COMPLETED);
        assertThat(step1.getExecutionCount()).isEqualTo(numSagas);
        assertThat(step2.getExecutionCount()).isEqualTo(numSagas);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Compensate
  // ---------------------------------------------------------------------------

  @Nested
  class Compensate {

    @Test
    void recover_sagaStuckInCompensating_completesCompensation() {
      // Arrange — step2 fails execution → backward compensation of step1 also fails → COMPENSATING
      FakeStep step1 =
          FakeStep.newBuilder("step1")
              .compensateFails(new StepCompensationException("comp failed"))
              .build();
      FakeStep step2 =
          FakeStep.newBuilder("step2")
              .executeFails(new StepExecutionException("exec failed", false))
              .build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      String sagaId;
      try (DefaultSagaOrchestrator manager1 =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        manager1.register(def);

        // Act — start saga; step2 fails, step1 compensation also fails → COMPENSATING
        sagaId = manager1.start("test-saga", Map.of());
        assertThat(manager1.getStateSnapshot(sagaId).getStatus())
            .isEqualTo(SagaStatus.COMPENSATING);
      }

      // New orchestrator with fresh store and a step1 that can compensate successfully
      FakeStep step1Fixed = FakeStep.newBuilder("step1").build();
      try (SagaStore store2 = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator manager2 =
              buildOrchestrator(store2, Map.of("step1", step1Fixed, "step2", step2))) {

        // Act — recovery picks up the COMPENSATING saga and completes compensation
        store2.markForRecovery(sagaId);
        manager2.recover();

        // Assert
        SagaStateSnapshot result = manager2.getStateSnapshot(sagaId);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(step1Fixed.getCompensationCount()).isEqualTo(1);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Async Start
  // ---------------------------------------------------------------------------

  @Nested
  class AsyncStart {

    @Test
    void startAsync_allStepsSucceed_completesAsynchronously() throws Exception {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator =
          buildOrchestrator(Map.of("step1", step1, "step2", step2))) {
        orchestrator.register(def);

        // Act — startAsync returns immediately
        String sagaId = orchestrator.startAsync("test-saga", Map.of());
        assertThat(sagaId).isNotNull();

        // Wait for async completion
        SagaStateSnapshot result = awaitTerminal(orchestrator, sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(step1.getExecutionCount()).isEqualTo(1);
        assertThat(step2.getExecutionCount()).isEqualTo(1);
      }
    }

    @Test
    void startAsync_withCallback_callbackInvokedOnCompletion() throws Exception {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();

      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga").saga().step("step1", STEP_CLASS).add().build();

      try (DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1))) {
        orchestrator.register(def);

        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicReference<SagaStateSnapshot> callbackResult = new AtomicReference<>();

        SagaCallback callback =
            new SagaCallback() {
              @Override
              public void onCompleted(SagaStateSnapshot saga) {
                callbackResult.set(saga);
                callbackLatch.countDown();
              }

              @Override
              public void onCompensated(SagaStateSnapshot saga) {
                callbackResult.set(saga);
                callbackLatch.countDown();
              }

              @Override
              public void onEscalated(SagaStateSnapshot saga) {
                callbackResult.set(saga);
                callbackLatch.countDown();
              }
            };

        // Act
        String sagaId = orchestrator.startAsync("test-saga", Map.of(), callback);

        // Wait for callback
        assertThat(callbackLatch.await(10, TimeUnit.SECONDS)).isTrue();

        // Assert
        assertThat(callbackResult.get()).isNotNull();
        SagaStateSnapshot result =
            Objects.requireNonNull(callbackResult.get(), "callback not invoked");
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(result.getSagaId()).isEqualTo(sagaId);
      }
    }

    private SagaStateSnapshot awaitTerminal(DefaultSagaOrchestrator orchestrator, String sagaId)
        throws InterruptedException {
      long deadline = System.currentTimeMillis() + 10_000;
      while (System.currentTimeMillis() < deadline) {
        SagaStateSnapshot snapshot = orchestrator.getStateSnapshot(sagaId);
        if (snapshot.getStatus() == SagaStatus.COMPLETED
            || snapshot.getStatus() == SagaStatus.COMPENSATED
            || snapshot.getStatus() == SagaStatus.ESCALATED) {
          return snapshot;
        }
        Thread.sleep(50);
      }
      throw new AssertionError("Saga " + sagaId + " did not reach terminal status within timeout");
    }
  }

  // ---------------------------------------------------------------------------
  // Versioned Start
  // ---------------------------------------------------------------------------

  @Nested
  class VersionedStart {

    @Test
    void start_withSagaDefinitionIdV1_usesV1Definition() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();

      SagaDefinition v1 =
          SagaDefinition.newBuilder("versioned-saga")
              .saga()
              .version("1.0")
              .step("step1", STEP_CLASS)
              .add()
              .build();

      SagaDefinition v2 =
          SagaDefinition.newBuilder("versioned-saga")
              .saga()
              .version("2.0")
              .step("step1", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1))) {
        orchestrator.register(v1);
        orchestrator.register(v2);

        // Act — start with explicit version 1.0
        SagaDefinitionId idV1 = new SagaDefinitionId("versioned-saga", "1.0");
        String sagaId = orchestrator.start(idV1, Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(result.getDefinitionVersion()).isEqualTo("1.0");
      }
    }

    @Test
    void start_withSagaDefinitionIdV2_usesV2Definition() {
      // Arrange
      FakeStep step1 = FakeStep.newBuilder("step1").build();

      SagaDefinition v1 =
          SagaDefinition.newBuilder("versioned-saga")
              .saga()
              .version("1.0")
              .step("step1", STEP_CLASS)
              .add()
              .build();

      SagaDefinition v2 =
          SagaDefinition.newBuilder("versioned-saga")
              .saga()
              .version("2.0")
              .step("step1", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1))) {
        orchestrator.register(v1);
        orchestrator.register(v2);

        // Act — start with explicit version 2.0
        SagaDefinitionId idV2 = new SagaDefinitionId("versioned-saga", "2.0");
        String sagaId = orchestrator.start(idV2, Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(result.getDefinitionVersion()).isEqualTo("2.0");
      }
    }

    @Test
    void start_withNameOnly_usesLatestVersion() {
      // Arrange — register two versions, name-only start should use latest
      FakeStep step1 = FakeStep.newBuilder("step1").build();

      SagaDefinition v1 =
          SagaDefinition.newBuilder("versioned-saga")
              .saga()
              .version("1.0")
              .step("step1", STEP_CLASS)
              .add()
              .build();

      SagaDefinition v2 =
          SagaDefinition.newBuilder("versioned-saga")
              .saga()
              .version("2.0")
              .step("step1", STEP_CLASS)
              .add()
              .build();

      try (DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1))) {
        orchestrator.register(v1);
        orchestrator.register(v2);

        // Act — start by name only (should resolve to latest = v2)
        String sagaId = orchestrator.start("versioned-saga", Map.of());
        SagaStateSnapshot result = orchestrator.getStateSnapshot(sagaId);

        // Assert
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(result.getDefinitionVersion()).isEqualTo("2.0");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Parked-step timeout (async callback deadline expiry)
  // ---------------------------------------------------------------------------

  @Nested
  class ParkedStepTimeout {

    @Test
    void recover_overdueParkedStepBeforePivot_compensates() throws Exception {
      // Arrange — step2 parks (returns pending); the saga-level timeout bounds the class-step park.
      // The clock starts at real "now" so the grace check aligns with the store's event timestamps.
      MutableClock clock = new MutableClock(Instant.now());
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").executeReturns(StepResult.pending()).build();
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .timeoutMillis(60_000)
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (SagaStore store = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator orchestrator =
              buildOrchestrator(Map.of("step1", step1, "step2", step2), clock)) {
        orchestrator.register(def);

        // Act 1 — start: step1 completes, step2 parks (WAITING) with deadline = start + 60s
        String sagaId = orchestrator.start("test-saga", Map.of());
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.WAITING);

        // Act 2 — advance past the park deadline AND the re-drive grace period (default 4h), so the
        // timeout gives up (compensates) rather than re-driving, then run a recovery pass
        clock.advance(Duration.ofHours(5));
        orchestrator.recover();

        // Assert — timed out (pre-pivot) -> compensated; the completed step rolled back; row
        // cleared
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus())
            .isEqualTo(SagaStatus.COMPENSATED);
        assertThat(step1.getCompensationCount()).isEqualTo(1);
        assertThat(store.findOverdueParkedSagas(farFuture(), null).sagaIds()).isEmpty();
      }
    }

    @Test
    void recover_overdueParkedStepAfterPivot_escalates() throws Exception {
      // Arrange — FORWARD saga (pivot = -1), so the parked step is post-pivot -> escalate. The
      // clock starts at real "now" so the grace check aligns with the store's event timestamps.
      MutableClock clock = new MutableClock(Instant.now());
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.pending()).build();
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(RecoveryStrategy.FORWARD)
              .timeoutMillis(60_000)
              .step("step1", STEP_CLASS)
              .add()
              .build();

      try (SagaStore store = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator orchestrator = buildOrchestrator(Map.of("step1", step1), clock)) {
        orchestrator.register(def);

        String sagaId = orchestrator.start("test-saga", Map.of());
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.WAITING);

        // Advance past the park deadline AND the re-drive grace period (default 4h) so the timeout
        // gives up (escalates) rather than re-driving.
        clock.advance(Duration.ofHours(5));
        orchestrator.recover();

        // Assert — post-pivot timeout escalates (no compensation); parked row cleared
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus())
            .isEqualTo(SagaStatus.ESCALATED);
        assertThat(step1.getCompensationCount()).isEqualTo(0);
        assertThat(store.findOverdueParkedSagas(farFuture(), null).sagaIds()).isEmpty();
      }
    }

    @Test
    void recover_overdueParkedStepWithinBounds_redrivesAndCompletes() throws Exception {
      // Arrange — step2 parks on its first execute, then completes when the re-drive re-issues it.
      // The clock starts at real "now" so the grace check aligns with the store's event timestamps.
      MutableClock clock = new MutableClock(Instant.now());
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      AtomicInteger step2Calls = new AtomicInteger();
      FakeStep step2 =
          FakeStep.newBuilder("step2")
              .executeAction(
                  ctx ->
                      step2Calls.getAndIncrement() == 0
                          ? StepResult.pending()
                          : StepResult.of("done", true))
              .build();
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .timeoutMillis(60_000)
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (SagaStore store = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator orchestrator =
              buildOrchestrator(Map.of("step1", step1, "step2", step2), clock)) {
        orchestrator.register(def);

        String sagaId = orchestrator.start("test-saga", Map.of());
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.WAITING);

        // Advance past the park deadline but WITHIN the grace period (and under maxAttempts): the
        // sweep re-drives (re-issues) step2, which now completes -> the saga finishes.
        clock.advance(Duration.ofMinutes(2));
        orchestrator.recover();

        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus())
            .isEqualTo(SagaStatus.COMPLETED);
        assertThat(step2.getExecutionCount()).isEqualTo(2); // initial park + one re-drive
        assertThat(step1.getCompensationCount()).isEqualTo(0);
        assertThat(store.findOverdueParkedSagas(farFuture(), null).sagaIds()).isEmpty();
      }
    }

    private Instant farFuture() {
      return Instant.parse("2100-01-01T00:00:00Z");
    }
  }

  // ---------------------------------------------------------------------------
  // Multi-replica sweep scatter (scattered order, success budget, page isolation)
  // ---------------------------------------------------------------------------

  @Nested
  class MultiReplicaSweeps {

    private static final int NUM_BUCKETS = 4;
    private final SagaSchema schema = new SagaSchema(NUM_BUCKETS);

    private SagaDefinition twoStepDefinition() {
      return SagaDefinition.newBuilder("test-saga")
          .saga()
          .step("step1", STEP_CLASS)
          .add()
          .step("step2", STEP_CLASS)
          .add()
          .build();
    }

    /** A saga ID that {@code SagaSchema.bucketOf} maps into the requested bucket. */
    private String sagaIdInBucket(int bucket, String prefix) {
      for (int i = 0; ; i++) {
        String candidate = prefix + i;
        if (schema.bucketOf(candidate) == bucket) {
          return candidate;
        }
      }
    }

    private DefaultSagaOrchestrator replica(
        SagaStore store, Map<String, Object> steps, String ownerId, RecoveryConfig config) {
      return DefaultSagaOrchestrator.newBuilder()
          .storeFactory(() -> store)
          .stepResolver((name, cls, ctx) -> Objects.requireNonNull(steps.get(name)))
          .ownerId(ownerId)
          .recoveryConfig(config)
          .build();
    }

    /**
     * Starts {@code sagaIds} so each crashes after step 0 (persisted) and stays RUNNING, then marks
     * every one for immediate recovery — a ready-made stale backlog.
     */
    private void crashStartBacklog(
        SagaDefinition def, Map<String, Object> steps, List<String> sagaIds) {
      try (SagaStore baseStore = ScalarDbSagaStoreFactory.create(props).createStore();
          SagaStore crashingStore = new CrashingStoreDecorator(baseStore, 0);
          DefaultSagaOrchestrator starter = buildOrchestrator(crashingStore, steps)) {
        starter.register(def);
        for (String sagaId : sagaIds) {
          try {
            starter.start(sagaId, "test-saga", Map.of());
          } catch (SimulatedCrashError expected) {
            // Each start "crashes" after step1's completion is persisted.
          }
          baseStore.markForRecovery(sagaId);
        }
      }
    }

    private long completedCount(DefaultSagaOrchestrator orchestrator, List<String> sagaIds) {
      return sagaIds.stream()
          .filter(id -> orchestrator.getStateSnapshot(id).getStatus() == SagaStatus.COMPLETED)
          .count();
    }

    @Test
    void recover_twoReplicasDrainBacklog_eachSagaRecoveredExactlyOnce() {
      // Arrange — 8 crashed sagas across 4 buckets; two replicas with budgets of 2 per pass, so
      // neither can drain the backlog alone in one pass.
      props.setProperty("scalar.db.saga.store.num_buckets", String.valueOf(NUM_BUCKETS));
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.of("a", 1)).build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      Map<String, Object> steps = Map.of("step1", step1, "step2", step2);
      List<String> sagaIds = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        sagaIds.add(sagaIdInBucket(i % NUM_BUCKETS, "drain-" + i + "-"));
      }
      crashStartBacklog(twoStepDefinition(), steps, sagaIds);

      RecoveryConfig smallBudget =
          new RecoveryConfig(60_000, 30, Duration.ofHours(4), 2, 10, Clock.systemUTC());
      try (SagaStore storeA = ScalarDbSagaStoreFactory.create(props).createStore();
          SagaStore storeB = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator replicaA = replica(storeA, steps, "replica-a", smallBudget);
          DefaultSagaOrchestrator replicaB = replica(storeB, steps, "replica-b", smallBudget)) {

        // Act — alternate passes until the backlog drains
        int passes = 0;
        while (completedCount(replicaA, sagaIds) < sagaIds.size() && passes < 12) {
          replicaA.recover();
          replicaB.recover();
          passes++;
        }

        // Assert — every saga recovered, and exactly once across BOTH replicas: step1 ran only at
        // start (never re-executed) and step2 ran exactly once per saga, whichever replica won it
        assertThat(completedCount(replicaA, sagaIds)).isEqualTo(sagaIds.size());
        assertThat(step1.getExecutionCount()).isEqualTo(sagaIds.size());
        assertThat(step2.getExecutionCount()).isEqualTo(sagaIds.size());
      }
    }

    @Test
    void recover_budgetOfOne_drainsOneSagaPerPassAcrossAllBuckets() {
      // Arrange — one crashed saga per bucket, budget of 1 successful recovery per pass
      props.setProperty("scalar.db.saga.store.num_buckets", String.valueOf(NUM_BUCKETS));
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.of("a", 1)).build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      Map<String, Object> steps = Map.of("step1", step1, "step2", step2);
      List<String> sagaIds = new ArrayList<>();
      for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
        sagaIds.add(sagaIdInBucket(bucket, "budget-" + bucket + "-"));
      }
      crashStartBacklog(twoStepDefinition(), steps, sagaIds);

      RecoveryConfig budgetOfOne =
          new RecoveryConfig(60_000, 30, Duration.ofHours(4), 1, 10, Clock.systemUTC());
      try (SagaStore store = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator orchestrator = replica(store, steps, "replica-a", budgetOfOne)) {

        // Act & Assert — each pass recovers exactly one saga (the budget counts successes), and
        // the budget-stopped sweep resumes across passes until every bucket's saga is recovered
        for (int pass = 1; pass <= NUM_BUCKETS; pass++) {
          orchestrator.recover();
          assertThat(completedCount(orchestrator, sagaIds)).isEqualTo(pass);
        }
      }
    }

    @Test
    void recover_driveFailsAfterClaim_budgetSpentWithoutClaimSpree() {
      // Arrange — three crashed sagas in distinct buckets; the recovering replica's event reads
      // fail, so every claim commits but every drive fails
      props.setProperty("scalar.db.saga.store.num_buckets", String.valueOf(NUM_BUCKETS));
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.of("a", 1)).build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      Map<String, Object> steps = Map.of("step1", step1, "step2", step2);
      List<String> sagaIds = new ArrayList<>();
      for (int bucket = 0; bucket < 3; bucket++) {
        sagaIds.add(sagaIdInBucket(bucket, "spree-" + bucket + "-"));
      }
      crashStartBacklog(twoStepDefinition(), steps, sagaIds);

      RecoveryConfig budgetOfOne =
          new RecoveryConfig(60_000, 30, Duration.ofHours(4), 1, 10, Clock.systemUTC());
      try (SagaStore baseStore = ScalarDbSagaStoreFactory.create(props).createStore();
          DriveFailingStore failingStore = new DriveFailingStore(baseStore);
          DefaultSagaOrchestrator orchestrator =
              replica(failingStore, steps, "replica-fail", budgetOfOne)) {
        failingStore.failEventReads.set(true);

        // Act — one pass with budget 1
        orchestrator.recover();

        // Assert — the committed-but-failed claim spent the budget: exactly one saga was claimed,
        // not a full-revolution claim spree that would hide all three from other replicas
        failingStore.failEventReads.set(false);
        long claimed =
            sagaIds.stream()
                .filter(id -> "replica-fail".equals(orchestrator.getStateSnapshot(id).getOwnerId()))
                .count();
        assertThat(claimed).isEqualTo(1);
        assertThat(completedCount(orchestrator, sagaIds)).isZero();
      }
    }

    @Test
    void recover_poisonFirstPage_otherBucketsStillSweptSamePass() {
      // Arrange — one crashed saga per bucket; the first scanned page of the pass fails
      props.setProperty("scalar.db.saga.store.num_buckets", String.valueOf(NUM_BUCKETS));
      FakeStep step1 = FakeStep.newBuilder("step1").executeReturns(StepResult.of("a", 1)).build();
      FakeStep step2 = FakeStep.newBuilder("step2").build();
      Map<String, Object> steps = Map.of("step1", step1, "step2", step2);
      List<String> sagaIds = new ArrayList<>();
      for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
        sagaIds.add(sagaIdInBucket(bucket, "poison-" + bucket + "-"));
      }
      crashStartBacklog(twoStepDefinition(), steps, sagaIds);

      String ownerId = "poison-owner";
      try (SagaStore baseStore = ScalarDbSagaStoreFactory.create(props).createStore();
          FirstPageFailingStore poisonStore = new FirstPageFailingStore(baseStore);
          DefaultSagaOrchestrator orchestrator =
              replica(poisonStore, steps, ownerId, RecoveryConfig.defaults())) {

        // Act — the first page's scan throws; the sweep must skip it and recover the rest
        orchestrator.recover();

        // Assert — every bucket except the poisoned first one recovered in the same pass, and the
        // un-recovered saga sits exactly in the owner permutation's first bucket
        assertThat(completedCount(orchestrator, sagaIds)).isEqualTo(NUM_BUCKETS - 1);
        int poisonedBucket = SweepScatter.permutation(SweepScatter.seed(ownerId), NUM_BUCKETS)[0];
        List<String> stillRunning =
            sagaIds.stream()
                .filter(id -> orchestrator.getStateSnapshot(id).getStatus() == SagaStatus.RUNNING)
                .toList();
        assertThat(stillRunning).hasSize(1);
        assertThat(schema.bucketOf(stillRunning.get(0))).isEqualTo(poisonedBucket);

        // Act 2 — the poison was transient (first call only): the next pass recovers the rest
        orchestrator.recover();
        assertThat(completedCount(orchestrator, sagaIds)).isEqualTo(NUM_BUCKETS);
      }
    }

    @Test
    void recover_twoReplicasRaceOverdueParkedSaga_exactlyOneCompensates() throws Exception {
      // Arrange — a parked (WAITING) saga past both its deadline and the grace period; two
      // replicas sweep it concurrently. The WAITING-CK check must let exactly one give up.
      MutableClock clock = new MutableClock(Instant.now());
      // The manager clock runs 5h ahead of the store's wall-clock row stamps, which would make
      // any row the winner just transitioned look instantly "stale" to the loser's staleness
      // sweep — a double-drive window this test is not about. A staleness timeout far larger
      // than the clock advance keeps that sweep inert; only the parked sweep acts.
      RecoveryConfig parkedSweepOnly =
          new RecoveryConfig(
              Duration.ofDays(36_500).toMillis(), 30, Duration.ofHours(4), 1000, 10, clock);
      FakeStep step1 = FakeStep.newBuilder("step1").build();
      FakeStep step2 = FakeStep.newBuilder("step2").executeReturns(StepResult.pending()).build();
      Map<String, Object> steps = Map.of("step1", step1, "step2", step2);
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .timeoutMillis(60_000)
              .step("step1", STEP_CLASS)
              .add()
              .step("step2", STEP_CLASS)
              .add()
              .build();

      try (SagaStore storeA = ScalarDbSagaStoreFactory.create(props).createStore();
          SagaStore storeB = ScalarDbSagaStoreFactory.create(props).createStore();
          DefaultSagaOrchestrator replicaA =
              DefaultSagaOrchestrator.newBuilder()
                  .storeFactory(() -> storeA)
                  .stepResolver((name, cls, ctx) -> Objects.requireNonNull(steps.get(name)))
                  .ownerId("replica-a")
                  .clock(clock)
                  .recoveryConfig(parkedSweepOnly)
                  .build();
          DefaultSagaOrchestrator replicaB =
              DefaultSagaOrchestrator.newBuilder()
                  .storeFactory(() -> storeB)
                  .stepResolver((name, cls, ctx) -> Objects.requireNonNull(steps.get(name)))
                  .ownerId("replica-b")
                  .clock(clock)
                  .recoveryConfig(parkedSweepOnly)
                  .build()) {
        replicaA.register(def);
        String sagaId = replicaA.start("test-saga", Map.of());
        assertThat(replicaA.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.WAITING);
        clock.advance(Duration.ofHours(5));

        // Act — both replicas sweep at the same moment
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
          CountDownLatch go = new CountDownLatch(1);
          Future<?> passA =
              pool.submit(
                  () -> {
                    go.await();
                    replicaA.recover();
                    return null;
                  });
          Future<?> passB =
              pool.submit(
                  () -> {
                    go.await();
                    replicaB.recover();
                    return null;
                  });
          go.countDown();
          passA.get(60, TimeUnit.SECONDS);
          passB.get(60, TimeUnit.SECONDS);
        } finally {
          pool.shutdown();
        }

        // Assert — timed out exactly once: compensated, and step1 rolled back exactly once
        assertThat(replicaA.getStateSnapshot(sagaId).getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(step1.getCompensationCount()).isEqualTo(1);
      }
    }

    /** Forwards everything; event reads fail while the flag is set (post-claim drive failure). */
    private static final class DriveFailingStore extends ForwardingSagaStore {
      final AtomicBoolean failEventReads = new AtomicBoolean(false);

      DriveFailingStore(SagaStore delegate) {
        super(delegate);
      }

      @Override
      public List<SagaEvent> getEvents(String sagaId) {
        if (failEventReads.get()) {
          throw new RuntimeException("simulated post-claim read failure");
        }
        return delegate().getEvents(sagaId);
      }
    }

    /** Forwards everything; the very first {@code findRecoverable} call fails (poison page). */
    private static final class FirstPageFailingStore extends ForwardingSagaStore {
      private final AtomicInteger findRecoverableCalls = new AtomicInteger();

      FirstPageFailingStore(SagaStore delegate) {
        super(delegate);
      }

      @Override
      public Recoverables findRecoverable(Instant threshold, @Nullable ScanCursor cursor) {
        if (findRecoverableCalls.getAndIncrement() == 0) {
          throw new RuntimeException("simulated poison page");
        }
        return delegate().findRecoverable(threshold, cursor);
      }
    }
  }
}
