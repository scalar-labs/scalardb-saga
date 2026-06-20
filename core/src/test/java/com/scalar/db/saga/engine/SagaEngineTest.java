package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.ShutdownMode;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResolver;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.exception.StepExecutionException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class SagaEngineTest {

  private static final Instant NOW = Instant.now();
  private static final String OWNER_ID = "engine-1";

  private SagaStore store;
  private final java.util.concurrent.ConcurrentHashMap<String, Object> stepMap =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final StepResolver stepResolver =
      (name, cls, ctx) -> {
        Object step = stepMap.get(name);
        if (step == null) {
          throw new IllegalArgumentException("No step registered: " + name);
        }
        return step;
      };
  private SagaEngine engine;

  @BeforeEach
  void setUp() {
    stepMap.clear();
    store = mock(SagaStore.class);
    engine =
        new SagaEngine(
            store,
            new StepInstantiator(stepResolver, HttpEndpointRegistry.create(Map.of())),
            OWNER_ID,
            new SagaEngine.ShutdownConfig(ShutdownMode.WAIT_CURRENT_STEP, 5000),
            Clock.systemUTC());
  }

  /** Registers a step for resolution by name. */
  private void registerStep(String name, Object step) {
    stepMap.put(name, step);
  }

  @AfterEach
  void tearDown() {
    engine.close();
  }

  private static RetryPolicy fastRetryPolicy() {
    return RetryPolicy.newBuilder()
        .maxAttempts(3)
        .initialIntervalMillis(1)
        .maxIntervalMillis(1)
        .build();
  }

  private SagaStateSnapshot runningSnapshot(String sagaId) {
    return new SagaStateSnapshot(sagaId, "test-saga", SagaStatus.RUNNING, OWNER_ID, "v1", NOW, NOW);
  }

  private SagaStateSnapshot compensatingSnapshot(String sagaId) {
    return new SagaStateSnapshot(
        sagaId, "test-saga", SagaStatus.COMPENSATING, OWNER_ID, "v1", NOW, NOW);
  }

  private Step successStep(String name) {
    Step step = mock(Step.class);
    when(step.getName()).thenReturn(name);
    try {
      when(step.execute(any(SagaContext.class))).thenReturn(StepResult.empty());
    } catch (StepExecutionException e) {
      throw new RuntimeException(e);
    }
    return step;
  }

  private Step outputStep(String name, Map<String, Object> output) {
    Step step = mock(Step.class);
    when(step.getName()).thenReturn(name);
    try {
      when(step.execute(any(SagaContext.class))).thenReturn(StepResult.of(output));
    } catch (StepExecutionException e) {
      throw new RuntimeException(e);
    }
    return step;
  }

  private Step failingStep(String name, boolean retryable) {
    Step step = mock(Step.class);
    when(step.getName()).thenReturn(name);
    try {
      when(step.execute(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("step failed", retryable));
    } catch (StepExecutionException e) {
      throw new RuntimeException(e);
    }
    return step;
  }

  private SagaDefinition sagaDefinition(String... stepNames) {
    var builder = SagaDefinition.newBuilder("test-saga").saga();
    for (String name : stepNames) {
      builder.step(name, "com.example." + name).add();
    }
    return builder.build();
  }

  private SagaDefinition sagaDefinitionWithRetry(String... stepNames) {
    var builder =
        SagaDefinition.newBuilder("test-saga").saga().defaultRetryPolicy(fastRetryPolicy());
    for (String name : stepNames) {
      builder.step(name, "com.example." + name).add();
    }
    return builder.build();
  }

  // =========================================================================
  // createSaga
  // =========================================================================

  @Nested
  class CreateSaga {

    @Test
    void createSaga_validDefinition_persistsAndReturnsSnapshot() {
      // Arrange
      SagaDefinition def = sagaDefinition("s1");
      SagaStateSnapshot expected = runningSnapshot("saga-1");
      when(store.createSaga(any(), eq("test-saga"), eq(OWNER_ID), any(), eq("1.0")))
          .thenReturn(expected);

      // Act
      SagaStateSnapshot result = engine.createSaga(def, "saga-1", Map.of());

      // Assert
      assertThat(result).isEqualTo(expected);
      verify(store).createSaga("saga-1", "test-saga", OWNER_ID, Map.of(), "1.0");
    }

    @Test
    void createSaga_shuttingDown_throwsIllegalStateException() {
      // Arrange
      engine.shutdown();
      SagaDefinition def = sagaDefinition("s1");

      // Act & Assert
      assertThatThrownBy(() -> engine.createSaga(def, null, Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void createSaga_nullSagaIdGiven_passesNull() {
      // Arrange
      SagaDefinition def = sagaDefinition("s1");
      SagaStateSnapshot expected = runningSnapshot("auto-generated");
      when(store.createSaga(eq(null), eq("test-saga"), eq(OWNER_ID), any(), eq("1.0")))
          .thenReturn(expected);

      // Act
      SagaStateSnapshot result = engine.createSaga(def, null, Map.of());

      // Assert
      assertThat(result.getSagaId()).isEqualTo("auto-generated");
    }
  }

  // =========================================================================
  // execute / executeSaga (happy path)
  // =========================================================================

  @Nested
  class ExecuteHappyPath {

    @Test
    void executeSaga_threeStepsGiven_completesAllSteps() throws Exception {
      // Arrange
      Step step1 = successStep("s1");
      Step step2 = successStep("s2");
      Step step3 = successStep("s3");
      registerStep("s1", step1);
      registerStep("s2", step2);
      registerStep("s3", step3);
      SagaDefinition def = sagaDefinitionWithRetry("s1", "s2", "s3");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.createSaga(any(), anyString(), anyString(), any(), anyString())).thenReturn(saga);
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — all steps executed
      verify(step1).execute(any(SagaContext.class));
      verify(step2).execute(any(SagaContext.class));
      verify(step3).execute(any(SagaContext.class));
      // SAGA_COMPLETED transition recorded
      ArgumentCaptor<StatusEvent> eventCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(any(), anyInt(), eventCaptor.capture());
      assertThat(eventCaptor.getValue().getEventType()).isEqualTo(EventType.SAGA_COMPLETED);
    }

    @Test
    void executeSaga_singleStepGiven_completesSuccessfully() throws Exception {
      // Arrange
      Step step1 = successStep("s1");
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert
      verify(step1).execute(any(SagaContext.class));
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getValue().getEventType()).isEqualTo(EventType.SAGA_COMPLETED);
    }

    @Test
    void execute_convenienceMethod_createsThenExecutesAndReturnsSagaId() throws Exception {
      // Arrange
      Step step1 = successStep("s1");
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.createSaga(any(), anyString(), anyString(), any(), anyString())).thenReturn(saga);
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      String sagaId = engine.execute(def, "saga-1", Map.of("key", "val"));

      // Assert
      assertThat(sagaId).isEqualTo("saga-1");
      verify(store).createSaga("saga-1", "test-saga", OWNER_ID, Map.of("key", "val"), "1.0");
      verify(step1).execute(any(SagaContext.class));
    }

    @Test
    void executeSaga_stepsProduceOutput_mergedIntoContext() throws Exception {
      // Arrange
      Step step1 = outputStep("s1", Map.of("key1", "val1"));
      Step step2 = successStep("s2");
      registerStep("s1", step1);
      registerStep("s2", step2);
      SagaDefinition def = sagaDefinitionWithRetry("s1", "s2");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step2 received the context with step1's output
      ArgumentCaptor<SagaContext> ctxCaptor = ArgumentCaptor.forClass(SagaContext.class);
      verify(step2).execute(ctxCaptor.capture());
      assertThat(ctxCaptor.getValue().get("key1", String.class)).contains("val1");
    }

    @Test
    void executeSaga_stepReturnsPending_parksWithoutRecordingCompletion() throws Exception {
      // Arrange — step1 returns pending, step2 should not be reached
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class))).thenReturn(StepResult.pending());
      Step step2 = successStep("s2");
      registerStep("s1", step1);
      registerStep("s2", step2);
      SagaDefinition def = sagaDefinitionWithRetry("s1", "s2");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step2 never executed (saga parked)
      verify(step2, never()).execute(any(SagaContext.class));
      // No STEP_COMPLETED event recorded for step1
      verify(store, never()).recordStepEvent(anyString(), anyInt(), any(StepEvent.class));
      // No SAGA_COMPLETED transition
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(StatusEvent.class));
    }
  }

  // =========================================================================
  // resumeFrom
  // =========================================================================

  @Nested
  class ResumeFrom {

    @Test
    void resumeFrom_middleStep_executesRemainingStepsAndReturnsState() throws Exception {
      // Arrange — 3 steps, resume from step 1 (step 0 already completed)
      Step step0 = successStep("s0");
      Step step1 = successStep("s1");
      Step step2 = successStep("s2");
      registerStep("s0", step0);
      registerStep("s1", step1);
      registerStep("s2", step2);
      SagaDefinition def = sagaDefinitionWithRetry("s0", "s1", "s2");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      SagaStateSnapshot completedSaga =
          new SagaStateSnapshot(
              "saga-1", "test-saga", SagaStatus.COMPLETED, OWNER_ID, "v1", NOW, NOW);
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(completedSaga);
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);

      // Act
      SagaStateSnapshot result = engine.resumeFrom(def, context, 1);

      // Assert — step 0 skipped, steps 1 and 2 executed
      verify(step0, never()).execute(any(SagaContext.class));
      verify(step1).execute(any(SagaContext.class));
      verify(step2).execute(any(SagaContext.class));
      assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }
  }

  // =========================================================================
  // Pivot boundary behavior
  // =========================================================================

  @Nested
  class PivotBoundary {

    @Test
    void executeSaga_failureBeforePivot_compensatesBackward() throws Exception {
      // Arrange — 3 steps: s1 succeeds, s2 fails (all are compensatable: BACKWARD strategy)
      Step step1 = successStep("s1");
      Step step2 = failingStep("s2", false);
      Step step3 = successStep("s3");
      registerStep("s1", step1);
      registerStep("s2", step2);
      registerStep("s3", step3);
      SagaDefinition def = sagaDefinitionWithRetry("s1", "s2", "s3");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      // recordStatusEvent returns same snapshot for simplicity
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step3 never executed
      verify(step3, never()).execute(any(SagaContext.class));
      // Compensation was triggered: step1 compensated
      verify(step1).compensate(any(SagaContext.class));
      // Two transitions in order: SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void executeSaga_failureAfterPivot_emitsStepFailedAndReturns() throws Exception {
      // Arrange — MIXED: pivot at step1 (index 1), step2 (index 2) is after pivot
      Step step0 = successStep("s0");
      Step step1 = successStep("s1");
      Step step2 = failingStep("s2", false);
      registerStep("s0", step0);
      registerStep("s1", step1);
      registerStep("s2", step2);
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(SagaDefinition.RecoveryStrategy.MIXED)
              .defaultRetryPolicy(fastRetryPolicy())
              .step("s0", "com.example.s0")
              .add()
              .step("s1", "com.example.s1")
              .pivot(true)
              .add()
              .step("s2", "com.example.s2")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — STEP_FAILED event appended, no compensation
      verify(step0, never()).compensate(any(SagaContext.class));
      verify(step1, never()).compensate(any(SagaContext.class));
      // STEP_FAILED event appended (step-level, not a transition)
      ArgumentCaptor<StepEvent> eventCaptor = ArgumentCaptor.forClass(StepEvent.class);
      verify(store, times(3)).recordStepEvent(eq("saga-1"), anyInt(), eventCaptor.capture());
      List<StepEvent> events = eventCaptor.getAllValues();
      // Last event should be STEP_FAILED
      assertThat(events.get(events.size() - 1).getEventType()).isEqualTo(EventType.STEP_FAILED);
    }
  }

  // =========================================================================
  // TCC
  // =========================================================================

  @Nested
  class Tcc {

    private TccStep createTccStep(String name) {
      TccStep step = mock(TccStep.class);
      when(step.getName()).thenReturn(name);
      try {
        when(step.reserve(any(SagaContext.class))).thenReturn(StepResult.empty());
      } catch (StepExecutionException e) {
        throw new RuntimeException(e);
      }
      return step;
    }

    @Test
    void executeSaga_tccDefinitionGiven_expandsToTwoNPlan() throws Exception {
      // Arrange — 2 TCC steps → 4 execution slots (2 reserves + 2 confirms)
      TccStep tcc1 = createTccStep("t1");
      TccStep tcc2 = createTccStep("t2");
      registerStep("t1", tcc1);
      registerStep("t2", tcc2);
      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .defaultRetryPolicy(fastRetryPolicy())
              .step("t1", "com.example.t1")
              .add()
              .step("t2", "com.example.t2")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — all reserves execute before any confirms
      InOrder inOrder = inOrder(tcc1, tcc2);
      inOrder.verify(tcc1).reserve(any(SagaContext.class));
      inOrder.verify(tcc2).reserve(any(SagaContext.class));
      inOrder.verify(tcc1).confirm(any(SagaContext.class));
      inOrder.verify(tcc2).confirm(any(SagaContext.class));
    }

    @Test
    void executeSaga_tccAllStepsSucceed_emitsOnlyCompleted() throws Exception {
      // Arrange
      TccStep tcc1 = createTccStep("t1");
      registerStep("t1", tcc1);
      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .defaultRetryPolicy(fastRetryPolicy())
              .step("t1", "com.example.t1")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — only SAGA_COMPLETED transition (no CONFIRMING status)
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(1)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getValue().getEventType()).isEqualTo(EventType.SAGA_COMPLETED);
    }

    @Test
    void executeSaga_tccReserveFailure_cancelsCompletedReserves() throws Exception {
      // Arrange — 2 TCC steps, second reserve fails
      TccStep tcc1 = createTccStep("t1");
      TccStep tcc2 = mock(TccStep.class);
      when(tcc2.getName()).thenReturn("t2");
      when(tcc2.reserve(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("reserve failed", false));
      registerStep("t1", tcc1);
      registerStep("t2", tcc2);
      SagaDefinition def =
          SagaDefinition.newBuilder("tcc-saga")
              .tcc()
              .defaultRetryPolicy(fastRetryPolicy())
              .step("t1", "com.example.t1")
              .add()
              .step("t2", "com.example.t2")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — tcc1.cancel was called (compensation)
      verify(tcc1).cancel(any(SagaContext.class));
      // tcc1.confirm never called
      verify(tcc1, never()).confirm(any(SagaContext.class));
      // Transitions: SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }
  }

  // =========================================================================
  // Timeout
  // =========================================================================

  @Nested
  class Timeout {

    @Test
    void executeSaga_stepTimeout_treatedAsFailure() throws Exception {
      // Arrange — step that blocks forever
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenAnswer(
              invocation -> {
                Thread.sleep(500); // Block long enough to trigger 50ms timeout
                return StepResult.empty();
              });
      registerStep("s1", step1);
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .defaultRetryPolicy(
                  RetryPolicy.newBuilder()
                      .maxAttempts(1)
                      .initialIntervalMillis(1)
                      .maxIntervalMillis(1)
                      .build())
              .step("s1", "com.example.s1")
              .timeoutMillis(50) // 50ms step timeout
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — STEP_FAILED event appended (timeout is non-retryable failure at/before pivot)
      // Compensation triggered: SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void executeSaga_sagaTimeoutBeforePivot_compensatesCompletedSteps() throws Exception {
      // Arrange — clock advances past saga deadline between steps
      Clock mockClock = mock(Clock.class);
      // Timeline: saga starts at t=0, deadline = 0+1000 = 1000ms
      // t=100: timeout check for step 0 (not expired)
      // t=200: step deadline calculation for step 0
      // t=300: executeWithRetry for step 0
      // t=1100: timeout check for step 1 (expired, 1100 > 1000)
      when(mockClock.millis()).thenReturn(0L, 100L, 200L, 300L, 1100L);
      SagaEngine clockEngine =
          new SagaEngine(
              store,
              new StepInstantiator(stepResolver, HttpEndpointRegistry.create(Map.of())),
              OWNER_ID,
              new SagaEngine.ShutdownConfig(ShutdownMode.WAIT_CURRENT_STEP, 5000),
              mockClock);

      Step step0 = successStep("s0");
      Step step1 = successStep("s1");
      registerStep("s0", step0);
      registerStep("s1", step1);

      // pivot index = 1 (last step), so step 0 is before pivot
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .timeoutMillis(1000) // saga timeout = 1000ms, clock will return 2000ms
              .step("s0", "com.example.s0")
              .add()
              .step("s1", "com.example.s1")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      clockEngine.executeSaga(def, saga, Map.of());
      clockEngine.close();

      // Assert — step 0 executed, step 1 NOT executed (saga timed out before it)
      verify(step0).execute(any(SagaContext.class));
      verify(step1, never()).execute(any(SagaContext.class));
      // Compensation triggered: SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void executeSaga_sagaTimeoutAfterPivot_noCompensation() throws Exception {
      // Arrange — clock advances past saga deadline after the pivot
      Clock mockClock = mock(Clock.class);
      // Timeline: saga starts at t=0, deadline = 0+1000 = 1000ms
      // t=100: timeout check for step 0 (OK)
      // t=200: step deadline for step 0
      // t=300: executeWithRetry for step 0
      // t=400: timeout check for step 1 (OK)
      // t=500: step deadline for step 1
      // t=600: executeWithRetry for step 1
      // t=1100: timeout check for step 2 (expired, 1100 > 1000)
      when(mockClock.millis()).thenReturn(0L, 100L, 200L, 300L, 400L, 500L, 600L, 1100L);
      SagaEngine clockEngine =
          new SagaEngine(
              store,
              new StepInstantiator(stepResolver, HttpEndpointRegistry.create(Map.of())),
              OWNER_ID,
              new SagaEngine.ShutdownConfig(ShutdownMode.WAIT_CURRENT_STEP, 5000),
              mockClock);

      Step step0 = successStep("s0");
      Step step1 = successStep("s1");
      Step step2 = successStep("s2");
      registerStep("s0", step0);
      registerStep("s1", step1);
      registerStep("s2", step2);

      // MIXED strategy: s1 is the pivot. s0 is before pivot, s2 is after pivot.
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(SagaDefinition.RecoveryStrategy.MIXED)
              .timeoutMillis(1000)
              .step("s0", "com.example.s0")
              .add()
              .step("s1", "com.example.s1")
              .pivot(true)
              .add()
              .step("s2", "com.example.s2")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      clockEngine.executeSaga(def, saga, Map.of());
      clockEngine.close();

      // Assert — step 0 and step 1 executed, step 2 NOT executed (saga timed out)
      verify(step0).execute(any(SagaContext.class));
      verify(step1).execute(any(SagaContext.class));
      verify(step2, never()).execute(any(SagaContext.class));
      // No compensation (timed out after pivot) — no SAGA_COMPENSATING transition
      verify(store, never()).recordStatusEvent(any(), anyInt(), eq(StatusEvent.compensating()));
    }
  }

  // =========================================================================
  // Graceful shutdown
  // =========================================================================

  @Nested
  class GracefulShutdown {

    @Test
    void executeSaga_shuttingDownWaitCurrentStep_stopsBetweenSteps() throws Exception {
      // Arrange
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenAnswer(
              invocation -> {
                // Trigger shutdown during step1 execution
                engine.shutdown();
                return StepResult.empty();
              });
      Step step2 = successStep("s2");
      registerStep("s1", step1);
      registerStep("s2", step2);
      SagaDefinition def = sagaDefinitionWithRetry("s1", "s2");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Re-create engine with WAIT_CURRENT_STEP mode
      engine.close();
      engine =
          new SagaEngine(
              store,
              new StepInstantiator(stepResolver, HttpEndpointRegistry.create(Map.of())),
              OWNER_ID,
              new SagaEngine.ShutdownConfig(ShutdownMode.WAIT_CURRENT_STEP, 5000),
              Clock.systemUTC());

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step2 never executed (stopped between steps)
      verify(step2, never()).execute(any(SagaContext.class));
    }

    @Test
    void executeSaga_alreadyActive_marksForRecoveryAndSkipsExecution() throws Exception {
      // Arrange — start saga-1 on a background thread and block it
      CountDownLatch stepStarted = new CountDownLatch(1);
      CountDownLatch stepRelease = new CountDownLatch(1);
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenAnswer(
              invocation -> {
                stepStarted.countDown();
                stepRelease.await();
                return StepResult.empty();
              });
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      Thread sagaThread = new Thread(() -> engine.executeSaga(def, saga, Map.of()));
      sagaThread.start();
      stepStarted.await();

      // Act — attempt to execute the same saga again
      engine.executeSaga(def, saga, Map.of());

      // Assert — duplicate was rejected and marked for recovery
      verify(store).markForRecovery("saga-1");
      // Only one step execution (the original, not the duplicate)
      verify(step1, times(1)).execute(any(SagaContext.class));

      // Cleanup
      stepRelease.countDown();
      sagaThread.join(5000);
    }

    @Test
    void resumeFrom_alreadyActive_marksForRecoveryAndReturnsCurrentState() throws Exception {
      // Arrange — start saga-1 on a background thread and block it
      CountDownLatch stepStarted = new CountDownLatch(1);
      CountDownLatch stepRelease = new CountDownLatch(1);
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenAnswer(
              invocation -> {
                stepStarted.countDown();
                stepRelease.await();
                return StepResult.empty();
              });
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      Thread sagaThread = new Thread(() -> engine.executeSaga(def, saga, Map.of()));
      sagaThread.start();
      stepStarted.await();

      // Act — attempt to resume the same saga
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      SagaStateSnapshot result = engine.resumeFrom(def, context, 0);

      // Assert — duplicate was rejected, returns current state unchanged
      verify(store).markForRecovery("saga-1");
      assertThat(result).isEqualTo(saga);

      // Cleanup
      stepRelease.countDown();
      sagaThread.join(5000);
    }

    @Test
    void compensateFrom_alreadyActive_marksForRecoveryAndSkipsCompensation() throws Exception {
      // Arrange — start saga-1 on a background thread and block it
      CountDownLatch stepStarted = new CountDownLatch(1);
      CountDownLatch stepRelease = new CountDownLatch(1);
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenAnswer(
              invocation -> {
                stepStarted.countDown();
                stepRelease.await();
                return StepResult.empty();
              });
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      Thread sagaThread = new Thread(() -> engine.executeSaga(def, saga, Map.of()));
      sagaThread.start();
      stepStarted.await();

      // Act — attempt to compensate the same saga
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      engine.compensateFrom(def, context, 0);

      // Assert — duplicate was rejected and marked for recovery
      verify(store).markForRecovery("saga-1");
      // No compensation was triggered
      verify(step1, never()).compensate(any(SagaContext.class));
      // No status transitions recorded for the duplicate
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(StatusEvent.class));

      // Cleanup
      stepRelease.countDown();
      sagaThread.join(5000);
    }

    @Test
    void resumeFrom_shuttingDown_marksForRecoveryAndReturnsCurrentState() {
      // Arrange
      Step step1 = successStep("s1");
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      engine.shutdown();

      // Act
      SagaStateSnapshot result = engine.resumeFrom(def, context, 0);

      // Assert
      verify(store).markForRecovery("saga-1");
      assertThat(result).isEqualTo(saga);
    }

    @Test
    void compensateFrom_shuttingDown_marksForRecoveryAndSkipsCompensation() {
      // Arrange
      Step step1 = successStep("s1");
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = compensatingSnapshot("saga-1");
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      engine.shutdown();

      // Act
      engine.compensateFrom(def, context, 0);

      // Assert
      verify(store).markForRecovery("saga-1");
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(StatusEvent.class));
    }

    @Test
    void shutdown_sagaOutlastsTimeout_marksForRecovery() throws Exception {
      // Arrange — step blocks until signaled, outlasting the shutdown timeout
      CountDownLatch stepStarted = new CountDownLatch(1);
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenAnswer(
              invocation -> {
                stepStarted.countDown(); // Signal that the step is running
                Thread.sleep(500); // Block longer than shutdown timeout
                return StepResult.empty();
              });
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Re-create engine with very short shutdown timeout
      engine.close();
      engine =
          new SagaEngine(
              store,
              new StepInstantiator(stepResolver, HttpEndpointRegistry.create(Map.of())),
              OWNER_ID,
              new SagaEngine.ShutdownConfig(ShutdownMode.WAIT_ALL_SAGAS, 50),
              Clock.systemUTC()); // 50ms timeout

      // Start saga in background and wait until it's actively running
      Thread sagaThread = new Thread(() -> engine.executeSaga(def, saga, Map.of()));
      sagaThread.start();
      stepStarted.await(); // Reliable: only proceeds once step is executing

      // Act
      engine.shutdown();

      // Assert — saga still active when timeout expired, so marked for recovery
      verify(store).markForRecovery("saga-1");
      sagaThread.join(5000);
    }
  }

  // =========================================================================
  // executeWithRetry
  // =========================================================================

  @Nested
  class ExecuteWithRetry {

    @Test
    void executeWithRetry_retryableFailure_retriesAndSucceeds() throws Exception {
      // Arrange — step fails twice then succeeds
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("transient", true))
          .thenThrow(new StepExecutionException("transient", true))
          .thenReturn(StepResult.empty());
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step executed 3 times, saga completed
      verify(step1, times(3)).execute(any(SagaContext.class));
      ArgumentCaptor<StatusEvent> eventCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(any(), anyInt(), eventCaptor.capture());
      assertThat(eventCaptor.getValue().getEventType()).isEqualTo(EventType.SAGA_COMPLETED);
    }

    @Test
    void executeWithRetry_nonRetryableFailure_throwsImmediately() throws Exception {
      // Arrange
      Step step1 = failingStep("s1", false);
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step called once (non-retryable), compensating triggered
      verify(step1, times(1)).execute(any(SagaContext.class));
      // SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void executeWithRetry_retriesExhausted_compensates() throws Exception {
      // Arrange — always fails
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("always fails", true));
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — 3 attempts (maxAttempts=3), then compensation
      verify(step1, times(3)).execute(any(SagaContext.class));

      // SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }
  }

  // =========================================================================
  // replayEvents
  // =========================================================================

  @Nested
  class ReplayEvents {

    @Test
    void replayEvents_sagaStartedAndStepCompleted_reconstructsContext() {
      // Arrange
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      List<SagaEvent> events =
          List.of(
              StatusEvent.started("{\"input1\":\"hello\"}"),
              StepEvent.completed(0, "s1", "{\"key1\":\"val1\"}"));

      // Act
      ExecutionContext context = engine.replayEvents(saga, events);

      // Assert
      assertThat(context.getSagaId()).isEqualTo("saga-1");
      assertThat(context.get("input1", String.class)).contains("hello");
      assertThat(context.get("key1", String.class)).contains("val1");
    }

    @Test
    void replayEvents_stepFailed_marksFailedIndex() {
      // Arrange
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      List<SagaEvent> events =
          List.of(
              StatusEvent.started("{}"),
              StepEvent.completed(0, "s1", null),
              StepEvent.failed(1, "s2", null));

      // Act
      ExecutionContext context = engine.replayEvents(saga, events);

      // Assert
      assertThat(context.hasFailureEvent(1)).isTrue();
      assertThat(context.hasFailureEvent(0)).isFalse();
    }

    @Test
    void replayEvents_stepCompensated_tracksCompensatedIndex() {
      // Arrange
      SagaStateSnapshot saga = compensatingSnapshot("saga-1");
      List<SagaEvent> events =
          List.of(
              StatusEvent.started("{}"),
              StepEvent.completed(0, "s1", null),
              StepEvent.failed(1, "s2", null),
              StatusEvent.compensating(),
              StepEvent.compensated(0, "s1"));

      // Act
      ExecutionContext context = engine.replayEvents(saga, events);

      // Assert
      assertThat(context.isStepCompensated(0)).isTrue();
      assertThat(context.isStepCompensated(1)).isFalse();
    }

    @Test
    void replayEvents_twoEventsGiven_setsNextEventSequence() {
      // Arrange
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      List<SagaEvent> events =
          List.of(StatusEvent.started("{}"), StepEvent.completed(0, "s1", null));

      // Act
      ExecutionContext context = engine.replayEvents(saga, events);

      // Assert
      assertThat(context.nextSequence()).isEqualTo(2);
    }
  }

  // =========================================================================
  // compensateFrom
  // =========================================================================

  @Nested
  class CompensateFrom {

    @Test
    void compensateFrom_sagaMode_transitionsAndCompensates() throws Exception {
      // Arrange
      Step step0 = successStep("s1");
      registerStep("s1", step0);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.compensateFrom(def, context, 0);

      // Assert — transition to COMPENSATING then COMPENSATED
      ArgumentCaptor<StatusEvent> eventCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), eventCaptor.capture());
      assertThat(eventCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(eventCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void compensateFrom_alreadyCompensating_skipsTransition() throws Exception {
      // Arrange
      Step step0 = successStep("s1");
      registerStep("s1", step0);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = compensatingSnapshot("saga-1");
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.compensateFrom(def, context, 0);

      // Assert — only SAGA_COMPENSATED transition (no SAGA_COMPENSATING since already in that
      // state)
      ArgumentCaptor<StatusEvent> eventCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(1)).recordStatusEvent(any(), anyInt(), eventCaptor.capture());
      assertThat(eventCaptor.getValue().getEventType()).isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void compensateFrom_compensationFails_staysCompensating() throws Exception {
      // Arrange
      Step step0 = mock(Step.class);
      when(step0.getName()).thenReturn("s1");
      doThrow(new StepCompensationException("persistent"))
          .when(step0)
          .compensate(any(SagaContext.class));
      registerStep("s1", step0);
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = compensatingSnapshot("saga-1");
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.compensateFrom(def, context, 0);

      // Assert — no SAGA_COMPENSATED transition (stays COMPENSATING)
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(StatusEvent.class));
    }
  }

  // =========================================================================
  // compensateSteps (migrated from CompensationManagerTest)
  // =========================================================================

  @Nested
  class CompensateSteps {

    private ExecutionContext createCompensatingContext() {
      SagaStateSnapshot state = compensatingSnapshot("saga-1");
      return new ExecutionContext("saga-1", Map.of(), state);
    }

    private Step createStep(String name) {
      Step step = mock(Step.class);
      when(step.getName()).thenReturn(name);
      return step;
    }

    private List<StepWithPolicy> createPlan(Step... steps) {
      List<StepWithPolicy> plan = new ArrayList<>();
      for (Step step : steps) {
        plan.add(new StepWithPolicy(step, fastRetryPolicy(), fastRetryPolicy(), 0));
      }
      return plan;
    }

    @Test
    void compensateSteps_threeStepsGiven_compensatesInReverseOrder() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      Step step1 = createStep("step1");
      Step step2 = createStep("step2");
      List<StepWithPolicy> plan = createPlan(step0, step1, step2);
      ExecutionContext context = createCompensatingContext();

      // Act
      engine.compensateSteps(plan, context, 2);

      // Assert — verify LIFO order
      var inOrder = org.mockito.Mockito.inOrder(step2, step1, step0);
      inOrder.verify(step2).compensate(context);
      inOrder.verify(step1).compensate(context);
      inOrder.verify(step0).compensate(context);
    }

    @Test
    void compensateSteps_retryableFailure_retriesUpToMaxAttempts() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      doThrow(new StepCompensationException("transient"))
          .doThrow(new StepCompensationException("transient"))
          .doNothing()
          .when(step0)
          .compensate(any(SagaContext.class));
      List<StepWithPolicy> plan = createPlan(step0);
      ExecutionContext context = createCompensatingContext();

      // Act
      engine.compensateSteps(plan, context, 0);

      // Assert — 3 attempts total (2 failures + 1 success)
      verify(step0, times(3)).compensate(context);
      verify(store).recordStepEvent(eq("saga-1"), anyInt(), any(StepEvent.class));
    }

    @Test
    void compensateSteps_allRetriesExhausted_appendsFailedEventAndThrows() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      doThrow(new StepCompensationException("persistent"))
          .when(step0)
          .compensate(any(SagaContext.class));
      List<StepWithPolicy> plan = createPlan(step0);
      ExecutionContext context = createCompensatingContext();

      // Act & Assert
      assertThatThrownBy(() -> engine.compensateSteps(plan, context, 0))
          .isInstanceOf(StepCompensationException.class);

      // Verify all 3 retry attempts were made
      verify(step0, times(3)).compensate(any(SagaContext.class));

      // Verify STEP_COMPENSATION_FAILED event appended
      ArgumentCaptor<StepEvent> eventCaptor = ArgumentCaptor.forClass(StepEvent.class);
      verify(store).recordStepEvent(eq("saga-1"), anyInt(), eventCaptor.capture());
      assertThat(eventCaptor.getValue().getEventType())
          .isEqualTo(EventType.STEP_COMPENSATION_FAILED);
    }

    @Test
    void compensateSteps_singleStepGiven_compensatesSuccessfully() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      List<StepWithPolicy> plan = createPlan(step0);
      ExecutionContext context = createCompensatingContext();

      // Act
      engine.compensateSteps(plan, context, 0);

      // Assert
      verify(step0).compensate(context);
      ArgumentCaptor<StepEvent> eventCaptor = ArgumentCaptor.forClass(StepEvent.class);
      verify(store).recordStepEvent(eq("saga-1"), anyInt(), eventCaptor.capture());
      assertThat(eventCaptor.getValue().getEventType()).isEqualTo(EventType.STEP_COMPENSATED);
    }

    @Test
    void compensateSteps_noSteps_completesImmediately() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      List<StepWithPolicy> plan = createPlan(step0);
      ExecutionContext context = createCompensatingContext();

      // Act — fromStepIndex = -1 means no steps to compensate
      engine.compensateSteps(plan, context, -1);

      // Assert
      verify(step0, never()).compensate(any(SagaContext.class));
      verify(store, never()).recordStepEvent(anyString(), anyInt(), any(StepEvent.class));
    }

    @Test
    void compensateSteps_alreadyCompensatedStep_skipped() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      Step step1 = createStep("step1");
      List<StepWithPolicy> plan = createPlan(step0, step1);
      ExecutionContext context = createCompensatingContext();
      context.markStepCompensated(1); // step1 already compensated

      // Act
      engine.compensateSteps(plan, context, 1);

      // Assert — step1 skipped, step0 compensated
      verify(step1, never()).compensate(any(SagaContext.class));
      verify(step0).compensate(context);
    }

    @Test
    void compensateSteps_success_appendsStepCompensatedEvents() throws Exception {
      // Arrange
      Step step0 = createStep("step0");
      Step step1 = createStep("step1");
      List<StepWithPolicy> plan = createPlan(step0, step1);
      ExecutionContext context = createCompensatingContext();

      // Act
      engine.compensateSteps(plan, context, 1);

      // Assert — two STEP_COMPENSATED events
      ArgumentCaptor<StepEvent> eventCaptor = ArgumentCaptor.forClass(StepEvent.class);
      verify(store, times(2)).recordStepEvent(eq("saga-1"), anyInt(), eventCaptor.capture());
      List<StepEvent> events = eventCaptor.getAllValues();
      assertThat(events).hasSize(2);
      assertThat(events.get(0).getEventType()).isEqualTo(EventType.STEP_COMPENSATED);
      assertThat(events.get(0).getStepIndex()).isEqualTo(1);
      assertThat(events.get(1).getEventType()).isEqualTo(EventType.STEP_COMPENSATED);
      assertThat(events.get(1).getStepIndex()).isEqualTo(0);
    }
  }

  // =========================================================================
  // resolveRetryPolicy
  // =========================================================================

  @Nested
  class ResolveRetryPolicyTests {

    @Test
    void executeSaga_stepWithOverridePolicy_usesStepPolicy() throws Exception {
      // Arrange — step with custom policy (1 attempt)
      RetryPolicy oneAttempt =
          RetryPolicy.newBuilder()
              .maxAttempts(1)
              .initialIntervalMillis(1)
              .maxIntervalMillis(1)
              .build();
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("fail", true));
      registerStep("s1", step1);
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .defaultRetryPolicy(fastRetryPolicy()) // default is 3 attempts
              .step("s1", "com.example.s1")
              .retryPolicy(oneAttempt) // step override: 1 attempt
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — only 1 attempt (step policy overrides definition default)
      verify(step1, times(1)).execute(any(SagaContext.class));
    }

    @Test
    void executeSaga_noStepOverride_usesDefinitionDefault() throws Exception {
      // Arrange — no step-level policy
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("fail", true));
      registerStep("s1", step1);
      // fastRetryPolicy() has maxAttempts=3
      SagaDefinition def = sagaDefinitionWithRetry("s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — 3 attempts (from definition default)
      verify(step1, times(3)).execute(any(SagaContext.class));
    }

    @Test
    void executeSaga_noDefault_usesGlobalDefault() throws Exception {
      // Arrange — no step policy, no definition default
      Step step1 = mock(Step.class);
      when(step1.getName()).thenReturn("s1");
      when(step1.execute(any(SagaContext.class)))
          .thenThrow(new StepExecutionException("fail", true));
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinition("s1"); // no defaultRetryPolicy
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — 3 attempts (global RetryPolicy.defaultPolicy() has maxAttempts=3)
      verify(step1, times(3)).execute(any(SagaContext.class));
    }
  }

  // =========================================================================
  // recordStepCompleted failure handling
  //
  // executeWithRetry and recordStepCompleted have different compensation scopes:
  //   - executeWithRetry failure   → step i did NOT run → compensate from i - 1
  //   - recordStepCompleted failure → step i DID run    → compensate from i
  // =========================================================================

  @Nested
  class RecordStepCompletedFailure {

    @Test
    void executeSagaSteps_recordStepCompletedThrows_compensatesIncludingCurrentStep()
        throws Exception {
      // Arrange — 2 steps, both execute successfully
      Step step0 = successStep("s0");
      Step step1 = successStep("s1");
      registerStep("s0", step0);
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s0", "s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // recordStepCompleted for step 1 throws — step 1 executed but recording failed.
      // Compensation must include step 1 (compensate from i, not i-1).
      doThrow(new RuntimeException("DB error"))
          .when(store)
          .recordStepEvent(
              anyString(),
              anyInt(),
              argThat(
                  event ->
                      event != null
                          && event.getStepIndex() == 1
                          && event.getEventType() == EventType.STEP_COMPLETED));

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step 1 IS compensated (its side effect exists)
      verify(step1).compensate(any(SagaContext.class));
      verify(step0).compensate(any(SagaContext.class));
      // SAGA_COMPENSATING → SAGA_COMPENSATED
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(2)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getAllValues().get(0).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATING);
      assertThat(transitionCaptor.getAllValues().get(1).getEventType())
          .isEqualTo(EventType.SAGA_COMPENSATED);
    }

    @Test
    void executeSagaSteps_executionFailure_doesNotCompensateCurrentStep() throws Exception {
      // Regression: execution failure at step 1 compensates from i-1 (only step 0),
      // NOT from i. This contrasts with the recordStepCompleted failure test above
      // where step 1 IS compensated because its execute() already succeeded.

      // Arrange
      Step step0 = successStep("s0");
      Step step1 = failingStep("s1", false);
      registerStep("s0", step0);
      registerStep("s1", step1);
      SagaDefinition def = sagaDefinitionWithRetry("s0", "s1");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any())).thenReturn(saga);

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — step 1 NOT compensated (it failed to execute, no side effect)
      verify(step1, never()).compensate(any(SagaContext.class));
      // step 0 IS compensated
      verify(step0).compensate(any(SagaContext.class));
    }

    @Test
    void executeSagaSteps_recordStepCompletedThrowsPastPivot_noCompensation() throws Exception {
      // Arrange — MIXED strategy: s0, s1 (pivot), s2 (past pivot)
      Step step0 = successStep("s0");
      Step step1 = successStep("s1");
      Step step2 = successStep("s2");
      registerStep("s0", step0);
      registerStep("s1", step1);
      registerStep("s2", step2);
      SagaDefinition def =
          SagaDefinition.newBuilder("test-saga")
              .saga()
              .recoveryStrategy(SagaDefinition.RecoveryStrategy.MIXED)
              .defaultRetryPolicy(fastRetryPolicy())
              .step("s0", "com.example.s0")
              .add()
              .step("s1", "com.example.s1")
              .pivot(true)
              .add()
              .step("s2", "com.example.s2")
              .add()
              .build();
      SagaStateSnapshot saga = runningSnapshot("saga-1");

      // recordStepCompleted for step 2 (past pivot) throws
      doThrow(new RuntimeException("DB error"))
          .when(store)
          .recordStepEvent(
              anyString(),
              anyInt(),
              argThat(
                  event ->
                      event != null
                          && event.getStepIndex() == 2
                          && event.getEventType() == EventType.STEP_COMPLETED));

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — no compensation (i > pivotIndex), saga stays RUNNING for recovery
      verify(step0, never()).compensate(any(SagaContext.class));
      verify(step1, never()).compensate(any(SagaContext.class));
      verify(step2, never()).compensate(any(SagaContext.class));
      verify(store, never()).recordStatusEvent(any(), anyInt(), any());
    }

    @Test
    void executeSagaSteps_recordStepCompletedThrowsAndCompensationFails_staysCompensating()
        throws Exception {
      // Arrange — 1 step: execution succeeds, recording fails, compensation also fails.
      // Saga should stay COMPENSATING for recovery to retry.
      Step step0 = mock(Step.class);
      when(step0.getName()).thenReturn("s0");
      when(step0.execute(any(SagaContext.class))).thenReturn(StepResult.empty());
      doThrow(new StepCompensationException("compensation failed"))
          .when(step0)
          .compensate(any(SagaContext.class));
      registerStep("s0", step0);
      SagaDefinition def = sagaDefinitionWithRetry("s0");
      SagaStateSnapshot saga = runningSnapshot("saga-1");
      when(store.recordStatusEvent(any(), anyInt(), any()))
          .thenReturn(compensatingSnapshot("saga-1"));

      // recordStepCompleted for step 0 throws
      doThrow(new RuntimeException("DB error"))
          .when(store)
          .recordStepEvent(
              anyString(),
              anyInt(),
              argThat(
                  event ->
                      event != null
                          && event.getStepIndex() == 0
                          && event.getEventType() == EventType.STEP_COMPLETED));

      // Act
      engine.executeSaga(def, saga, Map.of());

      // Assert — compensation attempted but failed
      verify(step0, atLeastOnce()).compensate(any(SagaContext.class));
      // Only SAGA_COMPENSATING transition (no SAGA_COMPENSATED)
      ArgumentCaptor<StatusEvent> transitionCaptor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store, times(1)).recordStatusEvent(any(), anyInt(), transitionCaptor.capture());
      assertThat(transitionCaptor.getValue().getEventType()).isEqualTo(EventType.SAGA_COMPENSATING);
    }
  }
}
