package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinition;
import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.net.URL;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EmbeddedSagaManagerTest {

  private static final Instant NOW = Instant.parse("2025-01-01T00:00:00Z");

  @Mock private SagaEngine engine;
  @Mock private SagaStore store;
  @Mock private SagaDefinitionRegistry registry;

  private EmbeddedSagaManager manager;

  @BeforeEach
  void setUp() {
    manager = new EmbeddedSagaManager(engine, store, registry, 30_000);
  }

  @AfterEach
  void tearDown() {
    manager.close();
  }

  private static SagaDefinition definition(String name) {
    return SagaDefinition.newBuilder(name, SagaMode.SAGA)
        .step("s1", "com.example.Step1")
        .add()
        .build();
  }

  private static SagaStateSnapshot snapshot(String sagaId, SagaStatus status) {
    return new SagaStateSnapshot(sagaId, "test-saga", status, "owner-1", "1.0", NOW, NOW);
  }

  // =========================================================================
  // register
  // =========================================================================

  @Nested
  class Register {

    @Test
    void register_validDefinitionGiven_buildsPlanAndDelegatesToRegistry() {
      // Arrange
      SagaDefinition def = definition("transfer");

      // Act
      manager.register(def);

      // Assert — build and cache plan eagerly, then persist
      verify(engine).getOrBuildPlan(def);
      verify(registry).register(def);
    }

    @Test
    void register_unresolvableStep_throwsSagaDefinitionException() {
      // Arrange
      SagaDefinition def = definition("transfer");
      org.mockito.Mockito.doThrow(new SagaDefinitionException("Step class not found"))
          .when(engine)
          .getOrBuildPlan(def);

      // Act & Assert
      assertThatThrownBy(() -> manager.register(def)).isInstanceOf(SagaDefinitionException.class);
      // Registry should NOT be called if validation fails
      verify(registry, never()).register(any(SagaDefinition.class));
    }
  }

  // =========================================================================
  // register (Path)
  // =========================================================================

  @Nested
  class RegisterPath {

    @Test
    void register_definitionFileGiven_parsesAndRegisters() throws Exception {
      // Arrange
      URL resource = getClass().getClassLoader().getResource("sagas/transfer.json");
      assertThat(resource).isNotNull();
      Path file = Path.of(resource.toURI());

      // Act
      manager.register(file);

      // Assert
      verify(registry).register(any(SagaDefinition.class));
    }

    @Test
    void register_nonexistentFileGiven_throwsException() {
      // Act & Assert
      assertThatThrownBy(() -> manager.register(Path.of("nonexistent.json")))
          .isInstanceOf(SagaDefinitionException.class);
    }
  }

  // =========================================================================
  // start (synchronous)
  // =========================================================================

  @Nested
  class StartSync {

    @Test
    void start_serverGeneratedId_delegatesToEngineExecute() {
      // Arrange
      SagaDefinition def = definition("transfer");
      when(registry.get("transfer")).thenReturn(def);
      when(engine.execute(eq(def), isNull(), any())).thenReturn("saga-1");

      // Act
      String sagaId = manager.start("transfer", Map.of("amount", 100));

      // Assert
      assertThat(sagaId).isEqualTo("saga-1");
      verify(engine).execute(def, null, Map.of("amount", 100));
    }

    @Test
    void start_clientSuppliedId_delegatesToEngineExecute() {
      // Arrange
      SagaDefinition def = definition("transfer");
      when(registry.get("transfer")).thenReturn(def);
      when(engine.execute(eq(def), eq("my-id"), any())).thenReturn("my-id");

      // Act
      manager.start("my-id", "transfer", Map.of());

      // Assert
      verify(engine).execute(def, "my-id", Map.of());
    }

    @Test
    void start_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      when(registry.get("unknown")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> manager.start("unknown", Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
    }

    @Test
    void start_afterClose_throwsIllegalState() {
      // Arrange
      manager.close();

      // Act & Assert
      assertThatThrownBy(() -> manager.start("transfer", Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void start_clientSuppliedIdAfterClose_throwsIllegalState() {
      // Arrange
      manager.close();

      // Act & Assert
      assertThatThrownBy(() -> manager.start("my-id", "transfer", Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  // =========================================================================
  // startAsync
  // =========================================================================

  @Nested
  class StartAsync {

    @Test
    void startAsync_serverGeneratedId_returnsSagaIdImmediately() {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);

      // Act
      String sagaId = manager.startAsync("transfer", Map.of());

      // Assert — ID returned immediately, execution happens async
      assertThat(sagaId).isEqualTo("saga-1");
      verify(engine).createSaga(def, null, Map.of());
    }

    @Test
    void startAsync_clientSuppliedId_persistsSynchronously() {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("my-id", SagaStatus.RUNNING);
      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), eq("my-id"), any())).thenReturn(saga);

      // Act
      manager.startAsync("my-id", "transfer", Map.of());

      // Assert
      verify(engine).createSaga(def, "my-id", Map.of());
    }

    @Test
    void startAsync_withCallback_dispatchesOnCompleted() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("saga-1", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);

      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(completedSaga));

      // Act
      manager.startAsync("transfer", Map.of(), callback);

      // Assert — callback dispatched asynchronously
      verify(callback, timeout(5000)).onCompleted(completedSaga);
    }

    @Test
    void startAsync_withCallback_dispatchesOnCompensated() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot compensatedSaga = snapshot("saga-1", SagaStatus.COMPENSATED);
      SagaCallback callback = mock(SagaCallback.class);

      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(compensatedSaga));

      // Act
      manager.startAsync("transfer", Map.of(), callback);

      // Assert
      verify(callback, timeout(5000)).onCompensated(compensatedSaga);
    }

    @Test
    void startAsync_withCallback_dispatchesOnEscalated() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot escalatedSaga = snapshot("saga-1", SagaStatus.ESCALATED);
      SagaCallback callback = mock(SagaCallback.class);

      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(escalatedSaga));

      // Act
      manager.startAsync("transfer", Map.of(), callback);

      // Assert
      verify(callback, timeout(5000)).onEscalated(escalatedSaga);
    }

    @Test
    void startAsync_executionFails_stillDispatchesCallback() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot compensatedSaga = snapshot("saga-1", SagaStatus.COMPENSATED);
      SagaCallback callback = mock(SagaCallback.class);

      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      doThrow(new RuntimeException("engine failure"))
          .when(engine)
          .executeSaga(eq(def), any(), any());
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(compensatedSaga));

      // Act
      manager.startAsync("transfer", Map.of(), callback);

      // Assert — callback still dispatched despite engine failure
      verify(callback, timeout(5000)).onCompensated(compensatedSaga);
    }

    @Test
    void startAsync_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      when(registry.get("unknown")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> manager.startAsync("unknown", Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
    }

    @Test
    void startAsync_afterClose_throwsIllegalState() {
      // Arrange
      manager.close();

      // Act & Assert
      assertThatThrownBy(() -> manager.startAsync("transfer", Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void startAsync_executorRejected_logsWarningAndDoesNotThrow() throws InterruptedException {
      // Arrange — simulate race between close() and submit()
      ExecutorService mockExecutor = mock(ExecutorService.class);
      when(mockExecutor.submit(any(Runnable.class)))
          .thenThrow(new java.util.concurrent.RejectedExecutionException("shutting down"));
      when(mockExecutor.awaitTermination(30_000, TimeUnit.MILLISECONDS)).thenReturn(true);
      EmbeddedSagaManager managerWithMockExecutor =
          new EmbeddedSagaManager(engine, store, registry, 30_000, mockExecutor);

      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(registry.get("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);

      // Act — should not throw; saga is already persisted, recovery will handle it
      String sagaId = managerWithMockExecutor.startAsync("transfer", Map.of());

      // Assert
      assertThat(sagaId).isEqualTo("saga-1");
      managerWithMockExecutor.close();
    }
  }

  // =========================================================================
  // resume
  // =========================================================================

  @Nested
  class Resume {

    @Test
    void resume_runningSaga_replaysEventsAndDelegatesToEngine() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaDefinition def = definition("test-saga");
      SagaStateSnapshot completed = snapshot("saga-1", SagaStatus.COMPLETED);
      List<SagaEvent> events =
          List.of(StatusEvent.started("{\"key\":\"val\"}"), StepEvent.completed(0, "s1", null));
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);

      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));
      when(registry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(context);
      when(engine.resumeFrom(eq(def), eq(context), eq(1))).thenReturn(completed);

      // Act
      SagaStateSnapshot result = manager.resume("saga-1");

      // Assert — resumes from step 1 (after last completed step 0)
      verify(engine).resumeFrom(def, context, 1);
      assertThat(result).isSameAs(completed);
    }

    @Test
    void resume_noCompletedSteps_resumesFromStepZero() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaDefinition def = definition("test-saga");
      SagaStateSnapshot completed = snapshot("saga-1", SagaStatus.COMPLETED);
      List<SagaEvent> events = List.of(StatusEvent.started(null));
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);

      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));
      when(registry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(context);
      when(engine.resumeFrom(eq(def), eq(context), eq(0))).thenReturn(completed);

      // Act
      manager.resume("saga-1");

      // Assert
      verify(engine).resumeFrom(def, context, 0);
    }

    @Test
    void resume_compensatingSaga_throwsIllegalState() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPENSATING);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));

      // Act & Assert
      assertThatThrownBy(() -> manager.resume("saga-1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void resume_completedSaga_throwsIllegalState() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPLETED);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));

      // Act & Assert
      assertThatThrownBy(() -> manager.resume("saga-1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void resume_unknownSaga_throwsSagaNotFound() {
      // Arrange
      when(store.getStateSnapshot("unknown")).thenReturn(Optional.empty());

      // Act & Assert
      assertThatThrownBy(() -> manager.resume("unknown")).isInstanceOf(SagaNotFoundException.class);
    }

    @Test
    void resume_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));
      when(registry.resolve("test-saga", "1.0")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> manager.resume("saga-1"))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
    }
  }

  // =========================================================================
  // compensate
  // =========================================================================

  @Nested
  class Compensate {

    @Test
    void compensate_withCompletedSteps_compensatesFromLastCompleted() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPENSATING);
      SagaDefinition def = definition("test-saga");
      List<SagaEvent> events =
          List.of(
              StatusEvent.started(null),
              StepEvent.completed(0, "s1", null),
              StepEvent.completed(1, "s2", null),
              StatusEvent.compensating());
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);

      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));
      when(registry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(context);

      // Act
      manager.compensate("saga-1");

      // Assert — compensate from step 1 (last completed)
      verify(engine).compensateFrom(def, context, 1);
    }

    @Test
    void compensate_partiallyCompensated_resumesCompensation() {
      // Arrange — step 1 compensated, step 0 not yet compensated
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPENSATING);
      SagaDefinition def = definition("test-saga");
      List<SagaEvent> events =
          List.of(
              StatusEvent.started(null),
              StepEvent.completed(0, "s1", null),
              StepEvent.completed(1, "s2", null),
              StatusEvent.compensating(),
              StepEvent.compensated(1, "s2"));
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);

      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));
      when(registry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(context);

      // Act
      manager.compensate("saga-1");

      // Assert — compensate from step 0 (one before the lowest compensated step 1)
      verify(engine).compensateFrom(def, context, 0);
    }

    @Test
    void compensate_noCompletedSteps_transitionsToCompensated() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPENSATING);
      SagaDefinition def = definition("test-saga");
      List<SagaEvent> events = List.of(StatusEvent.started(null), StatusEvent.compensating());
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), saga);

      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));
      when(registry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(context);

      // Act
      manager.compensate("saga-1");

      // Assert — no steps to compensate, but still transitions to COMPENSATED
      verify(engine).compensateFrom(def, context, -1);
    }

    @Test
    void compensate_runningSaga_throwsIllegalState() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));

      // Act & Assert
      assertThatThrownBy(() -> manager.compensate("saga-1"))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void compensate_completedSaga_throwsIllegalState() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPLETED);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));

      // Act & Assert
      assertThatThrownBy(() -> manager.compensate("saga-1"))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void compensate_unknownSaga_throwsSagaNotFound() {
      // Arrange
      when(store.getStateSnapshot("unknown")).thenReturn(Optional.empty());

      // Act & Assert
      assertThatThrownBy(() -> manager.compensate("unknown"))
          .isInstanceOf(SagaNotFoundException.class);
    }
  }

  // =========================================================================
  // getStateSnapshot
  // =========================================================================

  @Nested
  class GetStateSnapshot {

    @Test
    void getStateSnapshot_existingSaga_returnsSnapshot() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPLETED);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));

      // Act
      SagaStateSnapshot result = manager.getStateSnapshot("saga-1");

      // Assert
      assertThat(result).isSameAs(saga);
    }

    @Test
    void getStateSnapshot_unknownSaga_throwsSagaNotFound() {
      // Arrange
      when(store.getStateSnapshot("unknown")).thenReturn(Optional.empty());

      // Act & Assert
      assertThatThrownBy(() -> manager.getStateSnapshot("unknown"))
          .isInstanceOf(SagaNotFoundException.class);
    }
  }

  // =========================================================================
  // completeStep
  // =========================================================================

  @Nested
  class CompleteStep {

    @Test
    void completeStep_always_throwsUnsupported() {
      // Act & Assert
      assertThatThrownBy(() -> manager.completeStep("saga-1", "s1", Map.of()))
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  // =========================================================================
  // startRecovery
  // =========================================================================

  @Nested
  class StartRecovery {

    @Test
    void startRecovery_always_doesNotThrow() {
      // Act — no-op placeholder until SagaRecoveryManager is available
      manager.startRecovery();
    }
  }

  // =========================================================================
  // close
  // =========================================================================

  @Nested
  class Close {

    @Test
    void close_always_shutsDownEngineAndExecutor() throws InterruptedException {
      // Arrange
      ExecutorService mockExecutor = mock(ExecutorService.class);
      when(mockExecutor.awaitTermination(30_000, TimeUnit.MILLISECONDS)).thenReturn(true);
      EmbeddedSagaManager managerWithMockExecutor =
          new EmbeddedSagaManager(engine, store, registry, 30_000, mockExecutor);

      // Act
      managerWithMockExecutor.close();

      // Assert
      verify(mockExecutor).shutdown();
      verify(engine).shutdown();
    }
  }
}
