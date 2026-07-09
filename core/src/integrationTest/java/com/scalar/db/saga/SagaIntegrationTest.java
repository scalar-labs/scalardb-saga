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
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.ScalarDbSagaStoreFactory;
import com.scalar.db.saga.store.StepEvent;
import com.scalar.db.saga.testing.CrashingStoreDecorator;
import com.scalar.db.saga.testing.FakeStep;
import com.scalar.db.saga.testing.FakeTccStep;
import com.scalar.db.saga.testing.SimulatedCrashError;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
import java.util.concurrent.atomic.AtomicReference;
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
      MutableClock clock = new MutableClock(Instant.parse("2025-01-01T00:00:00Z"));
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

        // Act 2 — advance past the park deadline, then run a recovery pass
        clock.advance(Duration.ofMinutes(2));
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
      // Arrange — FORWARD saga (pivot = -1), so the parked step is post-pivot -> escalate
      MutableClock clock = new MutableClock(Instant.parse("2025-01-01T00:00:00Z"));
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

        clock.advance(Duration.ofMinutes(2));
        orchestrator.recover();

        // Assert — post-pivot timeout escalates (no compensation); parked row cleared
        assertThat(orchestrator.getStateSnapshot(sagaId).getStatus())
            .isEqualTo(SagaStatus.ESCALATED);
        assertThat(step1.getCompensationCount()).isEqualTo(0);
        assertThat(store.findOverdueParkedSagas(farFuture(), null).sagaIds()).isEmpty();
      }
    }

    private Instant farFuture() {
      return Instant.parse("2100-01-01T00:00:00Z");
    }
  }
}
