package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.RetryPolicy;
import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.exception.StepCompensationException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StepEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CompensationManagerTest {

  private static final Instant NOW = Instant.now();
  private static final SagaStateSnapshot DEFAULT_STATE =
      new SagaStateSnapshot(
          "saga-1", "test-saga", SagaStatus.COMPENSATING, "owner-1", "v1", NOW, NOW);

  // Use fast retry policy (1ms intervals, 1ms max) to avoid test slowness
  private static final RetryPolicy FAST_RETRY =
      RetryPolicy.newBuilder().maxAttempts(3).initialIntervalMillis(1).maxIntervalMillis(1).build();

  private SagaStore store;
  private CompensationManager manager;

  @BeforeEach
  void setUp() {
    store = mock(SagaStore.class);
    manager = new CompensationManager(store, FAST_RETRY);
  }

  private ExecutionContext createContext() {
    return new ExecutionContext("saga-1", Map.of(), DEFAULT_STATE);
  }

  private Step createStep(String name) {
    Step step = mock(Step.class);
    when(step.getName()).thenReturn(name);
    return step;
  }

  private List<StepWithPolicy> createPlan(Step... steps) {
    List<StepWithPolicy> plan = new ArrayList<>();
    for (Step step : steps) {
      plan.add(new StepWithPolicy(step, FAST_RETRY, 0));
    }
    return plan;
  }

  @Test
  void compensate_threeSteps_compensatesInReverseOrder() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    Step step1 = createStep("step1");
    Step step2 = createStep("step2");
    List<StepWithPolicy> plan = createPlan(step0, step1, step2);
    ExecutionContext context = createContext();

    // Act
    manager.compensate(plan, context, 2);

    // Assert — verify LIFO order
    var inOrder = org.mockito.Mockito.inOrder(step2, step1, step0);
    inOrder.verify(step2).compensate(context);
    inOrder.verify(step1).compensate(context);
    inOrder.verify(step0).compensate(context);
  }

  @Test
  void compensate_retryableFailure_retriesUpToMaxAttempts() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    doThrow(new StepCompensationException("transient"))
        .doThrow(new StepCompensationException("transient"))
        .doNothing()
        .when(step0)
        .compensate(any(SagaContext.class));
    List<StepWithPolicy> plan = createPlan(step0);
    ExecutionContext context = createContext();

    // Act
    manager.compensate(plan, context, 0);

    // Assert — 3 attempts total (2 failures + 1 success)
    verify(step0, times(3)).compensate(context);
    verify(store).recordStepEvent(eq("saga-1"), anyInt(), any(StepEvent.class));
  }

  @Test
  void compensate_allRetriesExhausted_appendsFailedEventAndThrows() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    doThrow(new StepCompensationException("persistent"))
        .when(step0)
        .compensate(any(SagaContext.class));
    List<StepWithPolicy> plan = createPlan(step0);
    ExecutionContext context = createContext();

    // Act & Assert
    assertThatThrownBy(() -> manager.compensate(plan, context, 0))
        .isInstanceOf(StepCompensationException.class);

    // Verify all 3 retry attempts were made
    verify(step0, times(3)).compensate(any(SagaContext.class));

    // Verify STEP_COMPENSATION_FAILED event appended
    ArgumentCaptor<StepEvent> eventCaptor = ArgumentCaptor.forClass(StepEvent.class);
    verify(store).recordStepEvent(eq("saga-1"), anyInt(), eventCaptor.capture());
    assertThat(eventCaptor.getValue().getEventType()).isEqualTo(EventType.STEP_COMPENSATION_FAILED);
  }

  @Test
  void compensate_singleStep_compensatesSuccessfully() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    List<StepWithPolicy> plan = createPlan(step0);
    ExecutionContext context = createContext();

    // Act
    manager.compensate(plan, context, 0);

    // Assert
    verify(step0).compensate(context);
    ArgumentCaptor<StepEvent> eventCaptor = ArgumentCaptor.forClass(StepEvent.class);
    verify(store).recordStepEvent(eq("saga-1"), anyInt(), eventCaptor.capture());
    assertThat(eventCaptor.getValue().getEventType()).isEqualTo(EventType.STEP_COMPENSATED);
  }

  @Test
  void compensate_noSteps_completesImmediately() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    List<StepWithPolicy> plan = createPlan(step0);
    ExecutionContext context = createContext();

    // Act — fromStepIndex = -1 means no steps to compensate
    manager.compensate(plan, context, -1);

    // Assert
    verify(step0, never()).compensate(any(SagaContext.class));
    verify(store, never()).recordStepEvent(anyString(), anyInt(), any(StepEvent.class));
  }

  @Test
  void compensate_alreadyCompensatedStep_skipped() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    Step step1 = createStep("step1");
    List<StepWithPolicy> plan = createPlan(step0, step1);
    ExecutionContext context = createContext();
    context.markStepCompensated(1); // step1 already compensated

    // Act
    manager.compensate(plan, context, 1);

    // Assert — step1 skipped, step0 compensated
    verify(step1, never()).compensate(any(SagaContext.class));
    verify(step0).compensate(context);
  }

  @Test
  void compensate_success_appendsStepCompensatedEvents() throws Exception {
    // Arrange
    Step step0 = createStep("step0");
    Step step1 = createStep("step1");
    List<StepWithPolicy> plan = createPlan(step0, step1);
    ExecutionContext context = createContext();

    // Act
    manager.compensate(plan, context, 1);

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
