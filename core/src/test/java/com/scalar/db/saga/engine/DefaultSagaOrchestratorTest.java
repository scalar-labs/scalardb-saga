package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.exception.SagaAlreadyExistsException;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStateAndEvents;
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
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class DefaultSagaOrchestratorTest {

  private static final Instant NOW = Instant.parse("2025-01-01T00:00:00Z");

  @Mock private SagaEngine engine;
  @Mock private SagaStore store;
  @Mock private SagaDefinitionRegistry definitionRegistry;
  @Mock private SagaRecoveryManager recoveryManager;
  @Mock private SagaRetentionManager retentionManager;

  private DefaultSagaOrchestrator orchestrator;

  @BeforeEach
  void setUp() {
    orchestrator =
        new DefaultSagaOrchestrator(
            engine,
            store,
            definitionRegistry,
            recoveryManager,
            retentionManager,
            30_000,
            Integer.MAX_VALUE);
  }

  @AfterEach
  void tearDown() {
    orchestrator.close();
  }

  private static SagaDefinition definition(String name) {
    return SagaDefinition.newBuilder(name).saga().step("s1", "com.example.Step1").add().build();
  }

  private static SagaDefinition definition(String name, String version) {
    return SagaDefinition.newBuilder(name)
        .saga()
        .version(version)
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
      orchestrator.register(def);

      // Assert — build and cache plan eagerly, then persist
      verify(engine).getOrBuildPlan(def);
      verify(definitionRegistry).register(def);
    }

    @Test
    void register_unresolvableStep_throwsSagaDefinitionException() {
      // Arrange
      SagaDefinition def = definition("transfer");
      org.mockito.Mockito.doThrow(
              SagaDefinitionException.stepClassInvalid(
                  "com.example.Foo", "not found on classpath", new ClassNotFoundException()))
          .when(engine)
          .getOrBuildPlan(def);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.register(def))
          .isInstanceOf(SagaDefinitionException.class);
      // Registry should NOT be called if validation fails
      verify(definitionRegistry, never()).register(any(SagaDefinition.class));
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
      orchestrator.register(file);

      // Assert
      verify(definitionRegistry).register(any(SagaDefinition.class));
    }

    @Test
    void register_nonexistentFileGiven_throwsException() {
      // Act & Assert
      assertThatThrownBy(() -> orchestrator.register(Path.of("nonexistent.json")))
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
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.execute(eq(def), isNull(), any())).thenReturn("saga-1");

      // Act
      String sagaId = orchestrator.start("transfer", Map.of("amount", 100));

      // Assert
      assertThat(sagaId).isEqualTo("saga-1");
      verify(definitionRegistry).resolve("transfer");
      verify(engine).execute(def, null, Map.of("amount", 100));
    }

    @Test
    void start_clientSuppliedId_delegatesToEngineExecute() {
      // Arrange
      SagaDefinition def = definition("transfer");
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.execute(eq(def), eq("my-id"), any())).thenReturn("my-id");

      // Act
      orchestrator.start("my-id", "transfer", Map.of());

      // Assert
      verify(definitionRegistry).resolve("transfer");
      verify(engine).execute(def, "my-id", Map.of());
    }

    @Test
    void start_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      when(definitionRegistry.resolve("unknown")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start("unknown", Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
      verify(definitionRegistry).resolve("unknown");
    }

    @Test
    void start_clientSuppliedIdAlreadyExists_propagatesSagaAlreadyExists() {
      // Arrange — the store raises this on the create; assert the orchestrator passes it through
      // untouched, since SagaOrchestrator declares it on the client-supplied-id overloads.
      SagaDefinition def = definition("transfer");
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.execute(eq(def), eq("dup"), any()))
          .thenThrow(new SagaAlreadyExistsException("dup", snapshot("dup", SagaStatus.RUNNING)));

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start("dup", "transfer", Map.of()))
          .isInstanceOf(SagaAlreadyExistsException.class);
    }

    @Test
    void start_malformedClientSuppliedId_propagatesSagaIllegalArgument() {
      // Arrange — validateSagaId rejects it in the store (covered there end to end); this asserts
      // the orchestrator does not wrap or swallow it, as the interface declaration promises.
      SagaDefinition def = definition("transfer");
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.execute(eq(def), eq("bad id!"), any()))
          .thenThrow(new SagaIllegalArgumentException("Invalid saga ID format"));

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start("bad id!", "transfer", Map.of()))
          .isInstanceOf(SagaIllegalArgumentException.class);
    }

    @Test
    void start_afterClose_throwsIllegalState() {
      // Arrange
      orchestrator.close();

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start("transfer", Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void start_clientSuppliedIdAfterClose_throwsIllegalState() {
      // Arrange
      orchestrator.close();

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start("my-id", "transfer", Map.of()))
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
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);

      // Act
      String sagaId = orchestrator.startAsync("transfer", Map.of());

      // Assert — ID returned immediately, execution happens async
      assertThat(sagaId).isEqualTo("saga-1");
      verify(definitionRegistry).resolve("transfer");
      verify(engine).createSaga(def, null, Map.of());
    }

    @Test
    void startAsync_clientSuppliedId_persistsSynchronously() {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("my-id", SagaStatus.RUNNING);
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), eq("my-id"), any())).thenReturn(saga);

      // Act
      orchestrator.startAsync("my-id", "transfer", Map.of());

      // Assert
      verify(definitionRegistry).resolve("transfer");
      verify(engine).createSaga(def, "my-id", Map.of());
    }

    @Test
    void startAsync_withCallback_dispatchesOnCompleted() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("saga-1", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(completedSaga));

      // Act
      orchestrator.startAsync("transfer", Map.of(), callback);

      // Assert — callback dispatched asynchronously
      verify(definitionRegistry).resolve("transfer");
      verify(callback, timeout(5000)).onCompleted(completedSaga);
    }

    @Test
    void startAsync_withCallback_dispatchesOnCompensated() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot compensatedSaga = snapshot("saga-1", SagaStatus.COMPENSATED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(compensatedSaga));

      // Act
      orchestrator.startAsync("transfer", Map.of(), callback);

      // Assert
      verify(definitionRegistry).resolve("transfer");
      verify(callback, timeout(5000)).onCompensated(compensatedSaga);
    }

    @Test
    void startAsync_withCallback_dispatchesOnEscalated() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot escalatedSaga = snapshot("saga-1", SagaStatus.ESCALATED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(escalatedSaga));

      // Act
      orchestrator.startAsync("transfer", Map.of(), callback);

      // Assert
      verify(definitionRegistry).resolve("transfer");
      verify(callback, timeout(5000)).onEscalated(escalatedSaga);
    }

    @Test
    void startAsync_executionReturnsCleanlyButSagaStillRunning_logsAnInvariantViolation()
        throws Exception {
      // Arrange — executeSaga returns normally yet leaves the saga RUNNING. Nothing in the engine's
      // environment explains that, so it is a bug in the engine and must not be logged as if a
      // saga merely failed.
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(runningSaga));

      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        // Act
        orchestrator.startAsync("transfer", Map.of(), callback);

        // Assert — ERROR, and no callback method is invoked: there is no outcome to report.
        await(() -> !logs.list.isEmpty());
        assertThat(logs.list).anySatisfy(e -> assertThat(e.getLevel()).isEqualTo(Level.ERROR));
        verify(callback, never()).onParked(any());
        verify(callback, never()).onCompleted(any());
      } finally {
        orchestratorLogger().detachAppender(logs);
      }
    }

    @Test
    void startAsync_executionThrowsAndLeavesSagaRunning_logsTheAbortNotAnInvariantViolation()
        throws Exception {
      // Arrange — the same end status, reached by a failed execution. The cause is already logged
      // by submitAsync, so this must read as an abort rather than as an engine bug.
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      doThrow(new IllegalStateException("store blew up"))
          .when(engine)
          .executeSaga(eq(def), eq(runningSaga), any());
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(runningSaga));

      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        // Act
        orchestrator.startAsync("transfer", Map.of(), callback);

        // Assert — the dispatch itself reports WARN, not ERROR. (submitAsync separately logs the
        // Throwable at ERROR, so the assertion targets the abort message specifically.)
        await(() -> logs.list.stream().anyMatch(e -> e.getFormattedMessage().contains("reclaim")));
        assertThat(logs.list)
            .filteredOn(e -> e.getFormattedMessage().contains("reclaim"))
            .allSatisfy(e -> assertThat(e.getLevel()).isEqualTo(Level.WARN));
        verify(callback, never()).onParked(any());
      } finally {
        orchestratorLogger().detachAppender(logs);
      }
    }

    @Test
    void startAsync_withCallbackAndSagaParks_dispatchesOnParked() throws Exception {
      // Arrange — execution returns with the saga WAITING, which is what parking on an async step
      // looks like. Before onParked existed this only logged, so a caller waiting on the callback
      // had nothing to wake on and waited out its whole bound.
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot parkedSaga = snapshot("saga-1", SagaStatus.WAITING);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(parkedSaga));

      // Act
      orchestrator.startAsync("transfer", Map.of(), callback);

      // Assert
      verify(callback, timeout(5000)).onParked(parkedSaga);
      verify(callback, never()).onCompleted(any());
      verify(callback, never()).onCompensated(any());
      verify(callback, never()).onEscalated(any());
    }

    @Test
    void startAsync_executionFails_stillDispatchesCallback() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot compensatedSaga = snapshot("saga-1", SagaStatus.COMPENSATED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      doThrow(new RuntimeException("engine failure"))
          .when(engine)
          .executeSaga(eq(def), any(), any());
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(compensatedSaga));

      // Act
      orchestrator.startAsync("transfer", Map.of(), callback);

      // Assert — callback still dispatched despite engine failure
      verify(definitionRegistry).resolve("transfer");
      verify(callback, timeout(5000)).onCompensated(compensatedSaga);
    }

    @Test
    void startAsync_clientSuppliedIdWithCallback_persistsAndDispatchesCallback() throws Exception {
      // Arrange
      SagaDefinition def = definition("transfer");
      SagaStateSnapshot runningSaga = snapshot("my-id", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("my-id", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), eq("my-id"), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("my-id")).thenReturn(Optional.of(completedSaga));

      // Act
      orchestrator.startAsync("my-id", "transfer", Map.of(), callback);

      // Assert
      verify(definitionRegistry).resolve("transfer");
      verify(engine).createSaga(def, "my-id", Map.of());
      verify(callback, timeout(5000)).onCompleted(completedSaga);
    }

    @Test
    void startAsync_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      when(definitionRegistry.resolve("unknown")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.startAsync("unknown", Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
      verify(definitionRegistry).resolve("unknown");
    }

    @Test
    void startAsync_driveThrowsError_swallowedAndCallbackStillDispatched() {
      // Arrange — a mocked executor so we can capture the drive Runnable and run it on the test
      // thread. The detached drive throws an Error; only a catch on Throwable (not Exception)
      // contains it, so running the captured Runnable must not throw, and the finally block must
      // still dispatch the callback.
      ExecutorService mockExecutor = mock(ExecutorService.class);
      DefaultSagaOrchestrator orchestratorWithMockExecutor =
          new DefaultSagaOrchestrator(
              engine,
              store,
              definitionRegistry,
              recoveryManager,
              retentionManager,
              30_000,
              Integer.MAX_VALUE,
              mockExecutor);

      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("saga-1", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);
      doThrow(new Error("drive failed off-thread"))
          .when(engine)
          .executeSaga(eq(def), eq(saga), any());
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(completedSaga));

      // Act — returns immediately; the drive Runnable is captured, not run.
      String sagaId = orchestratorWithMockExecutor.startAsync("transfer", Map.of(), callback);

      // Assert — running the captured drive swallows the Error (proving the catch is on
      // Throwable, not Exception) and still dispatches the callback.
      assertThat(sagaId).isEqualTo("saga-1");
      ArgumentCaptor<Runnable> driveCaptor = ArgumentCaptor.forClass(Runnable.class);
      verify(mockExecutor).execute(driveCaptor.capture());
      assertThatCode(() -> driveCaptor.getValue().run()).doesNotThrowAnyException();
      verify(engine).executeSaga(eq(def), eq(saga), any());
      verify(callback).onCompleted(completedSaga);

      orchestratorWithMockExecutor.close();
    }

    @Test
    void startAsync_callbackDispatchThrowsError_swallowed() {
      // Arrange — the drive succeeds but the user callback throws an Error from the finally
      // block's dispatch. Only a catch on Throwable contains it, so running the captured drive
      // Runnable must not throw.
      ExecutorService mockExecutor = mock(ExecutorService.class);
      DefaultSagaOrchestrator orchestratorWithMockExecutor =
          new DefaultSagaOrchestrator(
              engine,
              store,
              definitionRegistry,
              recoveryManager,
              retentionManager,
              30_000,
              Integer.MAX_VALUE,
              mockExecutor);

      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("saga-1", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(completedSaga));
      doThrow(new Error("callback failed")).when(callback).onCompleted(completedSaga);

      // Act
      String sagaId = orchestratorWithMockExecutor.startAsync("transfer", Map.of(), callback);

      // Assert — the callback's Error is logged, not propagated.
      assertThat(sagaId).isEqualTo("saga-1");
      ArgumentCaptor<Runnable> driveCaptor = ArgumentCaptor.forClass(Runnable.class);
      verify(mockExecutor).execute(driveCaptor.capture());
      assertThatCode(() -> driveCaptor.getValue().run()).doesNotThrowAnyException();
      verify(callback).onCompleted(completedSaga);

      orchestratorWithMockExecutor.close();
    }
  }

  // =========================================================================
  // start (synchronous, versioned)
  // =========================================================================

  @Nested
  class StartSyncVersioned {

    @Test
    void start_serverGeneratedId_delegatesToEngineExecute() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      SagaDefinition def = definition("transfer", "2.0");
      when(definitionRegistry.resolve("transfer", "2.0")).thenReturn(def);
      when(engine.execute(eq(def), isNull(), any())).thenReturn("saga-1");

      // Act
      String sagaId = orchestrator.start(id, Map.of("amount", 100));

      // Assert
      assertThat(sagaId).isEqualTo("saga-1");
      verify(definitionRegistry).resolve("transfer", "2.0");
      verify(engine).execute(def, null, Map.of("amount", 100));
    }

    @Test
    void start_clientSuppliedId_delegatesToEngineExecute() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      SagaDefinition def = definition("transfer", "2.0");
      when(definitionRegistry.resolve("transfer", "2.0")).thenReturn(def);
      when(engine.execute(eq(def), eq("my-id"), any())).thenReturn("my-id");

      // Act
      orchestrator.start("my-id", id, Map.of());

      // Assert
      verify(definitionRegistry).resolve("transfer", "2.0");
      verify(engine).execute(def, "my-id", Map.of());
    }

    @Test
    void start_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("unknown", "1.0");
      when(definitionRegistry.resolve("unknown", "1.0")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.start(id, Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
      verify(definitionRegistry).resolve("unknown", "1.0");
    }
  }

  // =========================================================================
  // startAsync (versioned)
  // =========================================================================

  @Nested
  class StartAsyncVersioned {

    @Test
    void startAsync_serverGeneratedId_returnsSagaIdImmediately() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      SagaDefinition def = definition("transfer", "2.0");
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(definitionRegistry.resolve("transfer", "2.0")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);

      // Act
      String sagaId = orchestrator.startAsync(id, Map.of());

      // Assert
      assertThat(sagaId).isEqualTo("saga-1");
      verify(definitionRegistry).resolve("transfer", "2.0");
      verify(engine).createSaga(def, null, Map.of());
    }

    @Test
    void startAsync_clientSuppliedId_persistsSynchronously() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      SagaDefinition def = definition("transfer", "2.0");
      SagaStateSnapshot saga = snapshot("my-id", SagaStatus.RUNNING);
      when(definitionRegistry.resolve("transfer", "2.0")).thenReturn(def);
      when(engine.createSaga(eq(def), eq("my-id"), any())).thenReturn(saga);

      // Act
      orchestrator.startAsync("my-id", id, Map.of());

      // Assert
      verify(definitionRegistry).resolve("transfer", "2.0");
      verify(engine).createSaga(def, "my-id", Map.of());
    }

    @Test
    void startAsync_withCallback_dispatchesOnCompleted() throws Exception {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      SagaDefinition def = definition("transfer", "2.0");
      SagaStateSnapshot runningSaga = snapshot("saga-1", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("saga-1", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer", "2.0")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(completedSaga));

      // Act
      orchestrator.startAsync(id, Map.of(), callback);

      // Assert
      verify(definitionRegistry).resolve("transfer", "2.0");
      verify(callback, timeout(5000)).onCompleted(completedSaga);
    }

    @Test
    void startAsync_clientSuppliedIdWithCallback_persistsAndDispatchesCallback() throws Exception {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      SagaDefinition def = definition("transfer", "2.0");
      SagaStateSnapshot runningSaga = snapshot("my-id", SagaStatus.RUNNING);
      SagaStateSnapshot completedSaga = snapshot("my-id", SagaStatus.COMPLETED);
      SagaCallback callback = mock(SagaCallback.class);

      when(definitionRegistry.resolve("transfer", "2.0")).thenReturn(def);
      when(engine.createSaga(eq(def), eq("my-id"), any())).thenReturn(runningSaga);
      when(store.getStateSnapshot("my-id")).thenReturn(Optional.of(completedSaga));

      // Act
      orchestrator.startAsync("my-id", id, Map.of(), callback);

      // Assert
      verify(definitionRegistry).resolve("transfer", "2.0");
      verify(engine).createSaga(def, "my-id", Map.of());
      verify(callback, timeout(5000)).onCompleted(completedSaga);
    }

    @Test
    void startAsync_unknownDefinition_throwsDefinitionNotFound() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("unknown", "1.0");
      when(definitionRegistry.resolve("unknown", "1.0")).thenReturn(null);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.startAsync(id, Map.of()))
          .isInstanceOf(SagaDefinitionNotFoundException.class);
      verify(definitionRegistry).resolve("unknown", "1.0");
    }

    @Test
    void startAsync_afterClose_throwsIllegalState() {
      // Arrange
      SagaDefinitionId id = new SagaDefinitionId("transfer", "2.0");
      orchestrator.close();

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.startAsync(id, Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void startAsync_executorRejected_logsWarningAndDoesNotThrow() throws InterruptedException {
      // Arrange — simulate race between close() and execute()
      ExecutorService mockExecutor = mock(ExecutorService.class);
      doThrow(new java.util.concurrent.RejectedExecutionException("shutting down"))
          .when(mockExecutor)
          .execute(any(Runnable.class));
      when(mockExecutor.awaitTermination(anyLong(), any())).thenReturn(true);
      DefaultSagaOrchestrator orchestratorWithMockExecutor =
          new DefaultSagaOrchestrator(
              engine,
              store,
              definitionRegistry,
              recoveryManager,
              retentionManager,
              30_000,
              Integer.MAX_VALUE,
              mockExecutor);

      SagaDefinition def = definition("transfer");
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(definitionRegistry.resolve("transfer")).thenReturn(def);
      when(engine.createSaga(eq(def), isNull(), any())).thenReturn(saga);

      // Act — should not throw; saga is already persisted, recovery will handle it
      String sagaId = orchestratorWithMockExecutor.startAsync("transfer", Map.of());

      // Assert — the ID is returned and the forward drive never ran.
      assertThat(sagaId).isEqualTo("saga-1");
      verify(engine, never()).executeSaga(any(), any(), any());
      orchestratorWithMockExecutor.close();
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
      SagaStateSnapshot result = orchestrator.getStateSnapshot("saga-1");

      // Assert
      assertThat(result).isSameAs(saga);
    }

    @Test
    void getStateSnapshot_unknownSaga_throwsSagaNotFound() {
      // Arrange
      when(store.getStateSnapshot("unknown")).thenReturn(Optional.empty());

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.getStateSnapshot("unknown"))
          .isInstanceOf(SagaNotFoundException.class);
    }

    @Test
    void getSagaDetail_existingSaga_returnsStateAndTimeline() {
      // Arrange — the application read of its own saga's detail, backed by the store's atomic read
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.COMPENSATED);
      when(store.getStateWithEvents("saga-1", Integer.MAX_VALUE))
          .thenReturn(Optional.of(new SagaStateAndEvents(saga, List.of(), false)));

      // Act
      SagaDetail detail = orchestrator.getSagaDetail("saga-1");

      // Assert — the projection itself is covered by SagaDetailReaderTest; here just the wiring
      assertThat(detail.getSnapshot()).isSameAs(saga);
      assertThat(detail.getTimeline()).isEmpty();
      assertThat(detail.isTruncated()).isFalse();
    }

    @Test
    void getSagaDetail_withMaxTimelineEvents_passesBoundToStore() {
      // Arrange — a bounded orchestrator (the daemon path) forwards its bound to the store read
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.ESCALATED);
      when(store.getStateWithEvents("saga-1", 42))
          .thenReturn(Optional.of(new SagaStateAndEvents(saga, List.of(), true)));
      try (DefaultSagaOrchestrator bounded =
          new DefaultSagaOrchestrator(
              engine, store, definitionRegistry, recoveryManager, retentionManager, 30_000, 42)) {

        // Act
        SagaDetail detail = bounded.getSagaDetail("saga-1");

        // Assert
        assertThat(detail.isTruncated()).isTrue();
      }
    }

    @Test
    void getSagaDetail_unknownSaga_throwsSagaNotFound() {
      // Arrange
      when(store.getStateWithEvents("unknown", Integer.MAX_VALUE)).thenReturn(Optional.empty());

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.getSagaDetail("unknown"))
          .isInstanceOf(SagaNotFoundException.class);
    }
  }

  // =========================================================================
  // completeStepAsync
  // =========================================================================

  @Nested
  class CompleteStepAsync {

    @Test
    void completeStepAsync_waitingSaga_returnsRunningAndDrivesAsync() {
      // Arrange — same parked saga as the sync case; here the forward tail runs on asyncExecutor.
      SagaStateSnapshot waiting = snapshot("saga-1", SagaStatus.WAITING);
      SagaDefinition def = definition("test-saga");
      SagaStateSnapshot running = snapshot("saga-1", SagaStatus.RUNNING);
      List<SagaEvent> events =
          List.of(
              StatusEvent.started(null),
              StepEvent.completed(0, "s0", null),
              StepEvent.pending(1, "s1"));
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), running);

      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(waiting));
      when(definitionRegistry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(store.resumeParkedStep(eq(waiting), eq(events.size()), any(StepEvent.class)))
          .thenReturn(running);
      when(engine.replayEvents(eq(running), any())).thenReturn(context);

      // Act
      SagaStateSnapshot result =
          orchestrator.completeStepAsync("saga-1", "s1", Map.of("paymentId", "p1"));

      // Assert — resumes step 1 via a STEP_COMPLETED event, returns the in-flight RUNNING snapshot
      // at once, and dispatches the forward drive (from step 2) off-thread.
      ArgumentCaptor<StepEvent> captor = ArgumentCaptor.forClass(StepEvent.class);
      verify(store).resumeParkedStep(eq(waiting), eq(events.size()), captor.capture());
      assertThat(captor.getValue().getEventType()).isEqualTo(EventType.STEP_COMPLETED);
      assertThat(captor.getValue().getStepIndex()).isEqualTo(1);
      assertThat(captor.getValue().getStepName()).isEqualTo("s1");
      assertThat(result).isSameAs(running);
      verify(engine, timeout(2_000)).resumeFrom(def, context, 2);
    }

    @Test
    void completeStepAsync_nonWaitingSaga_throwsIllegalState() {
      // Arrange — the WAITING check is phase 1 (synchronous), so completeStepAsync still throws
      // before dispatching anything, preserving the daemon's synchronous error mapping.
      SagaStateSnapshot saga = snapshot("saga-1", SagaStatus.RUNNING);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(saga));

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.completeStepAsync("saga-1", "s1", Map.of()))
          .isInstanceOf(IllegalStateException.class);
      verify(engine, never()).resumeFrom(any(), any(), anyInt());
    }

    @Test
    void completeStepAsync_stepNameMismatch_throwsIllegalArgument() {
      // Arrange — saga is parked on "s1" but the callback names a different step
      SagaStateSnapshot waiting = snapshot("saga-1", SagaStatus.WAITING);
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(1, "s1"));
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(waiting));
      when(store.getEvents("saga-1")).thenReturn(events);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.completeStepAsync("saga-1", "other", Map.of()))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void completeStepAsync_noParkedStep_throwsIllegalState() {
      // Arrange — WAITING but no STEP_PENDING marker in history (defensive)
      SagaStateSnapshot waiting = snapshot("saga-1", SagaStatus.WAITING);
      List<SagaEvent> events = List.of(StatusEvent.started(null));
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(waiting));
      when(store.getEvents("saga-1")).thenReturn(events);

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.completeStepAsync("saga-1", "s1", Map.of()))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void completeStepAsync_executorRejected_returnsRunningAndDoesNotThrow() {
      // Arrange — the async executor rejects the forward drive (race between close() and
      // execute()).
      // The step is already durably resumed (RUNNING), so completeStepAsync returns that snapshot
      // and recovery drives the tail.
      ExecutorService mockExecutor = mock(ExecutorService.class);
      org.mockito.Mockito.doThrow(
              new java.util.concurrent.RejectedExecutionException("shutting down"))
          .when(mockExecutor)
          .execute(any(Runnable.class));
      DefaultSagaOrchestrator orchestratorWithMockExecutor =
          new DefaultSagaOrchestrator(
              engine,
              store,
              definitionRegistry,
              recoveryManager,
              retentionManager,
              30_000,
              Integer.MAX_VALUE,
              mockExecutor);

      SagaStateSnapshot waiting = snapshot("saga-1", SagaStatus.WAITING);
      SagaDefinition def = definition("test-saga");
      SagaStateSnapshot running = snapshot("saga-1", SagaStatus.RUNNING);
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(1, "s1"));
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), running);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(waiting));
      when(definitionRegistry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(store.resumeParkedStep(eq(waiting), eq(events.size()), any(StepEvent.class)))
          .thenReturn(running);
      when(engine.replayEvents(eq(running), any())).thenReturn(context);

      // Act — must not throw; the drive was rejected but the step is durably resumed.
      SagaStateSnapshot result =
          orchestratorWithMockExecutor.completeStepAsync("saga-1", "s1", Map.of());

      // Assert — returns the RUNNING snapshot; the forward drive never ran.
      assertThat(result).isSameAs(running);
      verify(engine, never()).resumeFrom(any(), any(), anyInt());

      orchestratorWithMockExecutor.close();
    }

    @Test
    void completeStepAsync_driveThrows_swallowedAndReturnsRunning() {
      // Arrange — same parked saga, but with a mocked executor so we can capture the drive Runnable
      // and run it on the test thread. The detached forward drive throws an Error; only a catch on
      // Throwable (not Exception) contains it, so running the captured Runnable must not throw.
      // completeStepAsync itself returns the RUNNING snapshot — the step is durably resumed and the
      // async failure is logged, not propagated (recovery is the backstop).
      ExecutorService mockExecutor = mock(ExecutorService.class);
      DefaultSagaOrchestrator orchestratorWithMockExecutor =
          new DefaultSagaOrchestrator(
              engine,
              store,
              definitionRegistry,
              recoveryManager,
              retentionManager,
              30_000,
              Integer.MAX_VALUE,
              mockExecutor);

      SagaStateSnapshot waiting = snapshot("saga-1", SagaStatus.WAITING);
      SagaDefinition def = definition("test-saga");
      SagaStateSnapshot running = snapshot("saga-1", SagaStatus.RUNNING);
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(1, "s1"));
      ExecutionContext context = new ExecutionContext("saga-1", Map.of(), running);
      when(store.getStateSnapshot("saga-1")).thenReturn(Optional.of(waiting));
      when(definitionRegistry.resolve("test-saga", "1.0")).thenReturn(def);
      when(store.getEvents("saga-1")).thenReturn(events);
      when(store.resumeParkedStep(eq(waiting), eq(events.size()), any(StepEvent.class)))
          .thenReturn(running);
      when(engine.replayEvents(eq(running), any())).thenReturn(context);
      when(engine.resumeFrom(eq(def), eq(context), eq(2)))
          .thenThrow(new Error("drive failed off-thread"));

      // Act — returns immediately with the RUNNING snapshot; the drive Runnable is captured, not
      // run.
      SagaStateSnapshot result =
          orchestratorWithMockExecutor.completeStepAsync("saga-1", "s1", Map.of());

      // Assert — the resume result is returned, and running the captured drive swallows the Error
      // (proving the catch is on Throwable, not Exception).
      assertThat(result).isSameAs(running);
      ArgumentCaptor<Runnable> driveCaptor = ArgumentCaptor.forClass(Runnable.class);
      verify(mockExecutor).execute(driveCaptor.capture());
      assertThatCode(() -> driveCaptor.getValue().run()).doesNotThrowAnyException();
      verify(engine).resumeFrom(def, context, 2);

      orchestratorWithMockExecutor.close();
    }
  }

  // =========================================================================
  // startBackgroundTasks
  // =========================================================================

  @Nested
  class StartBackgroundTasks {

    @Test
    void startBackgroundTasks_always_startsBothManagers() {
      // Act
      orchestrator.startBackgroundTasks();

      // Assert
      verify(recoveryManager).start();
      verify(retentionManager).start();
    }

    @Test
    void startBackgroundTasks_afterClose_throwsIllegalState() {
      // Arrange
      orchestrator.close();

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.startBackgroundTasks())
          .isInstanceOf(IllegalStateException.class);
    }
  }

  // =========================================================================
  // recover
  // =========================================================================

  @Nested
  class Recover {

    @Test
    void recover_always_delegatesToRecoveryManager() {
      // Act
      orchestrator.recover();

      // Assert
      verify(recoveryManager).recover();
    }

    @Test
    void recover_afterClose_throwsIllegalState() {
      // Arrange
      orchestrator.close();

      // Act & Assert
      assertThatThrownBy(() -> orchestrator.recover()).isInstanceOf(IllegalStateException.class);
    }
  }

  // =========================================================================
  // close
  // =========================================================================

  @Nested
  class Close {

    @Test
    void close_always_stopsBackgroundTasksAndShutsDownEngineAndExecutor()
        throws InterruptedException {
      // Arrange
      ExecutorService mockExecutor = mock(ExecutorService.class);
      when(mockExecutor.awaitTermination(anyLong(), any())).thenReturn(true);
      DefaultSagaOrchestrator orchestratorWithMockExecutor =
          new DefaultSagaOrchestrator(
              engine,
              store,
              definitionRegistry,
              recoveryManager,
              retentionManager,
              30_000,
              Integer.MAX_VALUE,
              mockExecutor);

      // Act
      orchestratorWithMockExecutor.close();

      // Assert
      verify(retentionManager).stop(anyLong());
      verify(recoveryManager).stop(anyLong());
      verify(mockExecutor).shutdown();
      verify(engine).shutdown();
      verify(mockExecutor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
      verify(store).close();
    }
  }

  // Captures the orchestrator's log output so a test can assert the level, not just the text.
  // Callers must detach in a finally: orchestratorLogger().detachAppender(appender).
  private static ListAppender<ILoggingEvent> attachLogCapture() {
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    orchestratorLogger().addAppender(appender);
    return appender;
  }

  private static Logger orchestratorLogger() {
    return (Logger) LoggerFactory.getLogger(DefaultSagaOrchestrator.class);
  }

  /** Polls until the async dispatch has landed, rather than sleeping a fixed interval. */
  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
  }
}
