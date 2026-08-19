package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStore.OverdueParked;
import com.scalar.db.saga.store.SagaStore.Recoverables;
import com.scalar.db.saga.store.SagaStore.ScanCursor;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class SagaRecoveryManagerTest {

  private static final Instant NOW = Instant.parse("2025-01-01T12:00:00Z");
  private static final String OWNER_ID = "recovery-node-1";
  private static final String SAGA_ID = "saga-001";
  private static final String SAGA_NAME = "MoneyTransfer";
  private static final String DEF_VERSION = "1.0";
  private static final Duration GRACE_PERIOD = Duration.ofHours(1);

  @Mock private SagaStore store;
  @Mock private SagaEngine engine;
  @Mock private SagaDefinitionRegistry registry;
  @Mock private ScheduledExecutorService scheduler;

  private RecoveryConfig config;
  private SagaRecoveryManager manager;

  @BeforeEach
  void setUp() {
    config =
        new RecoveryConfig(60_000, 30, GRACE_PERIOD, 1000, 10, Clock.fixed(NOW, ZoneOffset.UTC));
    manager = new SagaRecoveryManager(store, engine, registry, OWNER_ID, config, scheduler);
    // Default: no overdue parked sagas — the recover() staleness-scan tests don't exercise pass 2.
    lenient()
        .when(store.findOverdueParkedSagas(any(), any()))
        .thenReturn(new OverdueParked(List.of(), null));
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(
        SAGA_ID, SAGA_NAME, status, OWNER_ID, DEF_VERSION, NOW.minusSeconds(300), NOW);
  }

  private static SagaDefinition definition() {
    return SagaDefinition.newBuilder(SAGA_NAME)
        .saga()
        .step("debit", "com.example.DebitStep")
        .add()
        .step("credit", "com.example.CreditStep")
        .add()
        .build();
  }

  private void setupSinglePageRecovery(SagaStateSnapshot saga) {
    when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), null));
    when(store.claimForRecovery(saga, OWNER_ID)).thenReturn(Optional.of(saga));
  }

  // Captures the manager's log output so tests can assert an Error was logged, not just contained.
  // Callers must detach the appender in a finally: recoveryLogger().detachAppender(appender).
  private static ListAppender<ILoggingEvent> attachLogCapture() {
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    recoveryLogger().addAppender(appender);
    return appender;
  }

  private static Logger recoveryLogger() {
    return (Logger) LoggerFactory.getLogger(SagaRecoveryManager.class);
  }

  // =========================================================================
  // recover() — cursor pagination
  // =========================================================================

  @Nested
  class Recover {

    @Test
    void recover_noRecoverableSagas_doesNothing() {
      // Arrange
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(), null));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
      verifyNoInteractions(engine);
    }

    @Test
    void recover_multiplePages_processesAllPages() {
      // Arrange
      SagaStateSnapshot saga1 = snapshot(SagaStatus.RUNNING);
      SagaStateSnapshot saga2 =
          new SagaStateSnapshot(
              "saga-002",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              NOW);
      ScanCursor cursor = mock(ScanCursor.class);
      SagaDefinition def = definition();
      ExecutionContext ctx1 = mock(ExecutionContext.class);
      ExecutionContext ctx2 = mock(ExecutionContext.class);

      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(saga1), cursor))
          .thenReturn(new Recoverables(List.of(saga2), null));
      when(store.claimForRecovery(saga1, OWNER_ID)).thenReturn(Optional.of(saga1));
      when(store.claimForRecovery(saga2, OWNER_ID)).thenReturn(Optional.of(saga2));
      when(store.getEvents(saga1.getSagaId())).thenReturn(List.of());
      when(store.getEvents(saga2.getSagaId())).thenReturn(List.of());
      when(engine.replayEvents(saga1, List.of())).thenReturn(ctx1);
      when(engine.replayEvents(saga2, List.of())).thenReturn(ctx2);
      when(ctx1.getCurrentState()).thenReturn(saga1);
      when(ctx2.getCurrentState()).thenReturn(saga2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx1));
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx2));
    }

    @Test
    void recover_claimFails_skipsSaga() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), null));
      when(store.claimForRecovery(saga, OWNER_ID)).thenReturn(Optional.empty());

      // Act
      manager.recover();

      // Assert
      verify(store, never()).getEvents(any());
      verifyNoInteractions(engine);
    }

    @Test
    void recover_batchLimitReached_stopsEarly() {
      // Arrange — batch size of 2
      RecoveryConfig smallBatch =
          new RecoveryConfig(60_000, 30, GRACE_PERIOD, 2, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      SagaRecoveryManager smallManager =
          new SagaRecoveryManager(store, engine, registry, OWNER_ID, smallBatch, scheduler);

      SagaStateSnapshot saga1 = snapshot(SagaStatus.RUNNING);
      SagaStateSnapshot saga2 =
          new SagaStateSnapshot(
              "saga-002",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              NOW);
      SagaStateSnapshot saga3 =
          new SagaStateSnapshot(
              "saga-003",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              NOW);
      ScanCursor cursor = mock(ScanCursor.class);
      SagaDefinition def = definition();
      ExecutionContext ctx1 = mock(ExecutionContext.class);
      ExecutionContext ctx2 = mock(ExecutionContext.class);

      // Page 1 has 2 sagas (hits batch limit), page 2 has 1 more
      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(saga1, saga2), cursor));
      when(store.claimForRecovery(saga1, OWNER_ID)).thenReturn(Optional.of(saga1));
      when(store.claimForRecovery(saga2, OWNER_ID)).thenReturn(Optional.of(saga2));
      when(store.getEvents(saga1.getSagaId())).thenReturn(List.of());
      when(store.getEvents(saga2.getSagaId())).thenReturn(List.of());
      when(engine.replayEvents(saga1, List.of())).thenReturn(ctx1);
      when(engine.replayEvents(saga2, List.of())).thenReturn(ctx2);
      when(ctx1.getCurrentState()).thenReturn(saga1);
      when(ctx2.getCurrentState()).thenReturn(saga2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      smallManager.recover();

      // Assert — processed 2, hit batch limit, did not scan page 2
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx1));
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx2));
      verify(store, never()).claimForRecovery(eq(saga3), any());
      // findRecoverable called only once — batch limit stopped before second page
      verify(store).findRecoverable(any(), any());
    }

    @Test
    void recover_exceptionOnOneSaga_continuesWithNext() {
      // Arrange
      SagaStateSnapshot saga1 = snapshot(SagaStatus.RUNNING);
      SagaStateSnapshot saga2 =
          new SagaStateSnapshot(
              "saga-002",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              NOW);
      SagaDefinition def = definition();
      ExecutionContext ctx2 = mock(ExecutionContext.class);

      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(saga1, saga2), null));
      when(store.claimForRecovery(saga1, OWNER_ID)).thenThrow(new RuntimeException("store error"));
      when(store.claimForRecovery(saga2, OWNER_ID)).thenReturn(Optional.of(saga2));
      when(store.getEvents(saga2.getSagaId())).thenReturn(List.of());
      when(engine.replayEvents(saga2, List.of())).thenReturn(ctx2);
      when(ctx2.getCurrentState()).thenReturn(saga2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — saga1 recovery stopped at claim failure
      verify(store, never()).getEvents(saga1.getSagaId());
      // Assert — saga2 was still recovered despite saga1 failure
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx2));
    }

    @Test
    void recover_errorOnOneSaga_containedAndNextStillRecovered() {
      // Arrange — same shape as the exception case, but with an Error: the per-task catch spans
      // Throwable, so the pass neither throws nor skips the next saga.
      SagaStateSnapshot saga1 = snapshot(SagaStatus.RUNNING);
      SagaStateSnapshot saga2 =
          new SagaStateSnapshot(
              "saga-002",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              NOW);
      SagaDefinition def = definition();
      ExecutionContext ctx2 = mock(ExecutionContext.class);

      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(saga1, saga2), null));
      when(store.claimForRecovery(saga1, OWNER_ID)).thenThrow(new Error("claim blew up"));
      when(store.claimForRecovery(saga2, OWNER_ID)).thenReturn(Optional.of(saga2));
      when(store.getEvents(saga2.getSagaId())).thenReturn(List.of());
      when(engine.replayEvents(saga2, List.of())).thenReturn(ctx2);
      when(ctx2.getCurrentState()).thenReturn(saga2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        assertThatCode(() -> manager.recover()).doesNotThrowAnyException();
      } finally {
        recoveryLogger().detachAppender(logs);
      }

      // Assert — the next saga was still recovered, and the Error was logged with saga context
      // rather than vanishing into the ExecutionException that awaitAll swallows.
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx2));
      assertThat(logs.list)
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                assertThat(event.getFormattedMessage()).contains(SAGA_ID);
                assertThat(event.getThrowableProxy()).isNotNull();
                assertThat(event.getThrowableProxy().getClassName())
                    .isEqualTo(Error.class.getName());
                assertThat(event.getThrowableProxy().getMessage()).isEqualTo("claim blew up");
              });
    }
  }

  // =========================================================================
  // recover() — parked-step timeout sweep
  // =========================================================================

  @Nested
  class ParkedTimeout {

    private void noStaleRecoverables() {
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(), null));
    }

    @Test
    void recover_parkedSweepThrowsError_contained() {
      // Arrange — the parked-timeout task blows up with an Error before doing any work; the
      // per-task catch spans Throwable, so the pass must complete without throwing.
      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenThrow(new Error("read blew up"));

      // Act
      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        assertThatCode(() -> manager.recover()).doesNotThrowAnyException();
      } finally {
        recoveryLogger().detachAppender(logs);
      }

      // Assert — the Error was logged with saga context rather than vanishing into the
      // ExecutionException that awaitAll swallows.
      assertThat(logs.list)
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                assertThat(event.getFormattedMessage()).contains(SAGA_ID);
                assertThat(event.getThrowableProxy()).isNotNull();
                assertThat(event.getThrowableProxy().getClassName())
                    .isEqualTo(Error.class.getName());
                assertThat(event.getThrowableProxy().getMessage()).isEqualTo("read blew up");
              });
    }

    @Test
    void recover_overdueParkedBeforePivot_timesOutToCompensatingAndCompensates() {
      // Arrange — parked at pre-pivot step 0 (BACKWARD saga: pivot = last step)
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      SagaStateSnapshot compensating = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(0, "debit"));
      ExecutionContext ctx = mock(ExecutionContext.class);

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);
      when(store.failParkedStep(
              eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.COMPENSATING)))
          .thenReturn(compensating);
      when(engine.replayEvents(eq(compensating), any())).thenReturn(ctx);

      // Act
      manager.recover();

      // Assert — STEP_FAILED for the parked step, WAITING -> COMPENSATING, then compensate from it
      ArgumentCaptor<StepEvent> failed = ArgumentCaptor.forClass(StepEvent.class);
      verify(store)
          .failParkedStep(eq(waiting), anyInt(), failed.capture(), eq(SagaStatus.COMPENSATING));
      assertThat(failed.getValue().getEventType()).isEqualTo(EventType.STEP_FAILED);
      assertThat(failed.getValue().getStepIndex()).isEqualTo(0);
      verify(engine).compensateFrom(def, ctx, 0);
    }

    @Test
    void recover_overdueParkedAfterPivot_timesOutToEscalated() {
      // Arrange — FORWARD saga: pivot = -1, so the parked step is post-pivot -> escalate
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      SagaDefinition forwardDef =
          SagaDefinition.newBuilder(SAGA_NAME)
              .saga()
              .recoveryStrategy(RecoveryStrategy.FORWARD)
              .step("debit", "com.example.DebitStep")
              .add()
              .build();
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(0, "debit"));

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(forwardDef);

      // Act
      manager.recover();

      // Assert
      verify(store)
          .failParkedStep(eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.ESCALATED));
      verify(engine, never()).compensateFrom(any(), any(), anyInt());
    }

    @Test
    void recover_overdueParkedNoLongerWaiting_skips() {
      // Arrange — a callback already resumed it (now RUNNING); the sweep must not time it out
      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.RUNNING)));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).failParkedStep(any(), anyInt(), any(), any());
    }

    @Test
    void recover_missingDefinitionForParkedSaga_escalatesClearingParkedRow() {
      // Arrange — definition gone: escalate via failParkedStep(ESCALATED) so the parked row
      // clears
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(0, "debit"));

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(null);

      // Act
      manager.recover();

      // Assert — escalated through the parked-clearing op, not a plain recordStatusEvent
      verify(store)
          .failParkedStep(eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.ESCALATED));
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_concurrentCallbackWonRace_swallowsConflict() {
      // Arrange — failParkedStep loses the WAITING-CK race (a callback resumed it first)
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      SagaDefinition def = definition();
      List<SagaEvent> events = List.of(StatusEvent.started(null), StepEvent.pending(0, "debit"));

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);
      when(store.failParkedStep(any(), anyInt(), any(), eq(SagaStatus.COMPENSATING)))
          .thenThrow(mock(SagaConcurrentModificationException.class));

      // Act & Assert — recover() must not propagate the conflict
      manager.recover();
      verify(engine, never()).compensateFrom(any(), any(), anyInt());
    }

    @Test
    void recover_overdueParkedWithinRedriveBounds_redrivesInsteadOfGivingUp() {
      // Arrange — one park attempt, 30 min ago (within the 1h grace, under maxAttempts=3):
      // re-drive.
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition();
      List<SagaEvent> events =
          List.of(
              StatusEvent.started(null),
              StepEvent.pending(0, "debit").withTimestamp(NOW.minusSeconds(1800)));
      ExecutionContext ctx = mock(ExecutionContext.class);

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);
      when(store.redriveParkedStep(eq(waiting), anyInt(), any(StepEvent.class)))
          .thenReturn(running);
      when(engine.replayEvents(eq(running), any())).thenReturn(ctx);

      // Act
      manager.recover();

      // Assert — un-parked (STEP_REISSUING) and re-issued from the parked step; not timed out.
      ArgumentCaptor<StepEvent> redrive = ArgumentCaptor.forClass(StepEvent.class);
      verify(store).redriveParkedStep(eq(waiting), anyInt(), redrive.capture());
      assertThat(redrive.getValue().getEventType()).isEqualTo(EventType.STEP_REISSUING);
      assertThat(redrive.getValue().getStepIndex()).isEqualTo(0);
      verify(engine).resumeFrom(def, ctx, 0);
      verify(store, never()).failParkedStep(any(), anyInt(), any(), any());
    }

    @Test
    void recover_overdueParkedAttemptsExhausted_givesUp() {
      // Arrange — three park attempts (== maxAttempts=3), all recent: the count bound is spent.
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      SagaStateSnapshot compensating = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      StepEvent park = StepEvent.pending(0, "debit").withTimestamp(NOW.minusSeconds(60));
      List<SagaEvent> events = List.of(StatusEvent.started(null), park, park, park);
      ExecutionContext ctx = mock(ExecutionContext.class);

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);
      when(store.failParkedStep(
              eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.COMPENSATING)))
          .thenReturn(compensating);
      when(engine.replayEvents(eq(compensating), any())).thenReturn(ctx);

      // Act
      manager.recover();

      // Assert — gave up (timed out), did not re-drive.
      verify(store)
          .failParkedStep(eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.COMPENSATING));
      verify(store, never()).redriveParkedStep(any(), anyInt(), any());
    }

    @Test
    void recover_overdueParkedGraceExceeded_givesUp() {
      // Arrange — one park attempt (count within bound) but 2h ago (> 1h grace): the grace bound is
      // spent.
      SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
      SagaStateSnapshot compensating = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      List<SagaEvent> events =
          List.of(
              StatusEvent.started(null),
              StepEvent.pending(0, "debit").withTimestamp(NOW.minusSeconds(7200)));
      ExecutionContext ctx = mock(ExecutionContext.class);

      noStaleRecoverables();
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(SAGA_ID), null));
      when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(waiting));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);
      when(store.failParkedStep(
              eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.COMPENSATING)))
          .thenReturn(compensating);
      when(engine.replayEvents(eq(compensating), any())).thenReturn(ctx);

      // Act
      manager.recover();

      // Assert
      verify(store)
          .failParkedStep(eq(waiting), anyInt(), any(StepEvent.class), eq(SagaStatus.COMPENSATING));
      verify(store, never()).redriveParkedStep(any(), anyInt(), any());
    }
  }

  // =========================================================================
  // recoverOne() — RUNNING sagas
  // =========================================================================

  @Nested
  class RecoverRunning {

    @Test
    void recover_runningSagaWithNoEvents_resumesFromStepZero() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(List.of());
      when(engine.replayEvents(saga, List.of())).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert
      verify(engine).recover(new RecoveryAction.Resume(0), def, ctx);
    }

    @Test
    void recover_runningSagaWithCompletedSteps_resumesFromNextStep() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(60)),
              StepEvent.completed(1, "credit", null).withTimestamp(NOW.minusSeconds(30)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — resume from step 2 (after last completed at index 1)
      verify(engine).recover(new RecoveryAction.Resume(2), def, ctx);
    }

    @Test
    void recover_runningSagaStuckBeyondGracePeriod_escalates() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      // Failure event older than grace period (NOW - 2 hours > 1 hour grace)
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(2), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert
      ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(eq(saga), eq(2), captor.capture(), any());
      StatusEvent event = captor.getValue();
      assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
      verify(engine, never()).recover(any(RecoveryAction.Resume.class), any(), any());
    }

    @Test
    void recover_runningSagaWithInDoubtPrePivotFailure_compensatesIncludingFailedStep() {
      // Arrange — step 1's forward failure is in-doubt (null/legacy payload → knownNotCommitted=
      // false): the engine recorded STEP_FAILED(1) and was about to compensate, but crashed before
      // the COMPENSATING transition, so the saga is still RUNNING. Recovery must compensate the
      // possibly-committed step 1, NOT resume forward (resuming would re-run it and, if the re-run
      // then proved non-delivery, skip it — orphaning the original committed side effect).
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition(); // 2-step BACKWARD saga: pivot = last step (index 1)
      ExecutionContext ctx = mock(ExecutionContext.class);

      // Failure within grace period so escalation does not pre-empt compensation.
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(60)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(30)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — compensate including the failed step 1; do not resume forward.
      verify(engine).recover(new RecoveryAction.Compensate(1), def, ctx);
      verify(engine, never()).recover(any(RecoveryAction.Resume.class), any(), any());
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_runningSagaWithKnownNotCommittedPrePivotFailure_compensatesSkippingFailedStep() {
      // Arrange — step 1's forward failure proved non-delivery (knownNotCommitted=true persisted on
      // the STEP_FAILED payload). The engine recorded STEP_FAILED(1) and was about to compensate
      // from step 0 (skipping the un-delivered step 1), but crashed before the COMPENSATING
      // transition. Recovery must compensate from the highest completed step (0), skipping step 1 —
      // not resume forward.
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);

      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(60)),
              StepEvent.failed(1, "credit", "{\"knownNotCommitted\":true}")
                  .withTimestamp(NOW.minusSeconds(30)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — proven-non-delivery step 1 is skipped; compensate from the highest completed (0).
      verify(engine).recover(new RecoveryAction.Compensate(0), def, ctx);
      verify(engine, never()).recover(any(RecoveryAction.Resume.class), any(), any());
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_runningSagaWithPostPivotFailure_resumesForward() {
      // Arrange — a FORWARD-recovery saga has pivot = -1, so every step is post-pivot. A forward
      // failure there is retried by recovery (roll forward), not compensated: there is no
      // crash-before-compensating ambiguity because the engine never compensates past the pivot.
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def =
          SagaDefinition.newBuilder(SAGA_NAME)
              .saga()
              .recoveryStrategy(SagaDefinition.RecoveryStrategy.FORWARD)
              .step("debit", "com.example.DebitStep")
              .add()
              .step("credit", "com.example.CreditStep")
              .add()
              .build();
      ExecutionContext ctx = mock(ExecutionContext.class);

      // Recent failure (within grace) so it is not escalated.
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(60)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(30)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — resume forward at the failed post-pivot step; never compensate.
      verify(engine).recover(new RecoveryAction.Resume(1), def, ctx);
      verify(engine, never()).recover(any(RecoveryAction.Compensate.class), any(), any());
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_runningSagaWithResolvedFailure_resumesInsteadOfEscalating() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);

      // Step 1 failed, then succeeded on retry — failure is resolved.
      // Even though the failure is older than grace period, the saga should NOT be escalated.
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.completed(1, "credit", null).withTimestamp(NOW.minusSeconds(7100)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — failure resolved, resumes from step 2 (after last completed at index 1)
      verify(engine).recover(new RecoveryAction.Resume(2), def, ctx);
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_runningSagaResetWithinGracePeriod_drivesInsteadOfReEscalating() {
      // Arrange — an operator un-escalated this saga 10 minutes ago. The failure it was stuck on is
      // still 2 hours old and always will be, so anchoring on the failure alone would escalate it
      // straight back and undo the intervention without ever driving it.
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaDefinition def = definition();

      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)),
              StatusEvent.escalated("step retry stuck").withTimestamp(NOW.minusSeconds(3600)),
              StatusEvent.reset(SagaStatus.RUNNING, "ops", "downstream fixed")
                  .withTimestamp(NOW.minusSeconds(600)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — driven, and never re-escalated
      verify(engine).recover(any(RecoveryAction.class), eq(def), eq(ctx));
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_runningSagaResetLongerAgoThanGracePeriod_escalates() {
      // Arrange — the operator's reset is itself now older than the grace period, so the saga has
      // had its fresh window and failed to make progress. It escalates again, honestly.
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(28800)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(28800)),
              StatusEvent.reset(SagaStatus.RUNNING, "ops", "downstream fixed")
                  .withTimestamp(NOW.minusSeconds(7200)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(3);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(3), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert
      ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(eq(saga), eq(3), captor.capture(), any());
      assertThat(captor.getValue().getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
      verify(engine, never()).recover(any(), any(), any());
    }

    @Test
    void recover_runningSagaFailingAgainAfterReset_escalatesOnTheNewerFailure() {
      // Arrange — the reset drove the saga, the retry failed again 2 hours ago, and nothing has
      // happened since. The newer failure outlives the intervention, so the clock anchors on the
      // failure again and the saga escalates.
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(28800)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(28800)),
              StatusEvent.reset(SagaStatus.RUNNING, "ops", "downstream fixed")
                  .withTimestamp(NOW.minusSeconds(10800)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(4);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(4), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert
      verify(store).recordStatusEvent(eq(saga), eq(4), any(StatusEvent.class), any());
      verify(engine, never()).recover(any(), any(), any());
    }

    @Test
    void recover_compensatingSagaResetWithinGracePeriod_drivesInsteadOfReEscalating() {
      // Arrange — the compensation-stuck shape: the one an operator most often bulk-resets.
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaDefinition def = definition();

      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StatusEvent.compensating().withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.compensationFailed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StatusEvent.escalated("compensation stuck").withTimestamp(NOW.minusSeconds(3600)),
              StatusEvent.reset(SagaStatus.COMPENSATING, "ops", "downstream fixed")
                  .withTimestamp(NOW.minusSeconds(600)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert
      verify(engine).recover(any(RecoveryAction.Compensate.class), eq(def), eq(ctx));
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_runningSagaRecoveringWithinGracePeriod_stillEscalatesOnTheFailure() {
      // Arrange — recoverSaga only forces a drive the sweep would do anyway, on a saga that never
      // left the recovery cycle, so SAGA_RECOVERING must NOT buy the saga a fresh grace period.
      // Only un-escalating (SAGA_RESET) restarts the clock.
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)),
              StatusEvent.recovering(SagaStatus.RUNNING, "ops", "retry")
                  .withTimestamp(NOW.minusSeconds(600)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(3);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(3), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert
      ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(eq(saga), eq(3), captor.capture(), any());
      assertThat(captor.getValue().getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
      verify(engine, never()).recover(any(), any(), any());
    }

    @Test
    void recover_runningSagaWithMixedResolvedAndUnresolvedFailures_escalatesOnUnresolved() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      // Step 0 failed and was resolved. Step 1 failed and is still unresolved.
      List<SagaEvent> events =
          List.of(
              StepEvent.failed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7100)),
              StepEvent.failed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(3);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(3), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert — step 1 failure is unresolved and beyond grace period → escalate
      verify(store).recordStatusEvent(eq(saga), eq(3), any(StatusEvent.class), any());
      verify(engine, never()).recover(any(RecoveryAction.Resume.class), any(), any());
    }

    @Test
    void recover_crashRecoveryWithNoFailureEvents_resumesWithoutEscalationCheck() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);

      // Only completed events, no failure events — pure crash recovery
      List<SagaEvent> events =
          List.of(StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(60)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert
      verify(engine).recover(new RecoveryAction.Resume(1), def, ctx);
    }
  }

  // =========================================================================
  // recoverOne() — COMPENSATING sagas
  // =========================================================================

  @Nested
  class RecoverCompensating {

    @Test
    void recover_compensatingSagaWithCompensatedSteps_resumesFromPreviousStep() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(120)),
              StepEvent.completed(1, "credit", null).withTimestamp(NOW.minusSeconds(90)),
              StepEvent.compensated(1, "credit").withTimestamp(NOW.minusSeconds(30)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — last compensated was index 1, so resume from 0
      verify(engine).recover(new RecoveryAction.Compensate(0), def, ctx);
    }

    @Test
    void recover_failureNotKnownNotCommittedNoCompensationYet_compensatesIncludingFailedStep() {
      // Arrange — step 2's forward failure does not prove non-delivery (null/legacy payload →
      // knownNotCommitted=false), so it may have committed and must be compensated too.
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(120)),
              StepEvent.completed(1, "credit", null).withTimestamp(NOW.minusSeconds(90)),
              StepEvent.failed(2, "notify", null).withTimestamp(NOW.minusSeconds(60)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — compensation includes the failed step 2, not just the completed steps.
      verify(engine).recover(new RecoveryAction.Compensate(2), def, ctx);
    }

    @Test
    void recover_failureKnownNotCommittedNoCompensationYet_compensatesFromHighestCompleted() {
      // Arrange — step 2's failure is proven non-delivery (knownNotCommitted=true persisted on the
      // STEP_FAILED payload), so it is skipped; compensation starts from the highest completed (1).
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(120)),
              StepEvent.completed(1, "credit", null).withTimestamp(NOW.minusSeconds(90)),
              StepEvent.failed(2, "notify", "{\"knownNotCommitted\":true}")
                  .withTimestamp(NOW.minusSeconds(60)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — the proven-non-delivery failed step is skipped; start from the highest completed.
      verify(engine).recover(new RecoveryAction.Compensate(1), def, ctx);
    }

    @Test
    void recover_compensatingSagaWithResolvedCompensationFailure_resumesInsteadOfEscalating() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);

      // Step 1 compensation failed, then succeeded on retry — failure is resolved.
      // Even though the failure is older than grace period, the saga should NOT be escalated.
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.completed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.compensationFailed(1, "credit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.compensated(1, "credit").withTimestamp(NOW.minusSeconds(7100)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);

      // Act
      manager.recover();

      // Assert — failure resolved, resumes compensation from step 0 (before last compensated at 1)
      verify(engine).recover(new RecoveryAction.Compensate(0), def, ctx);
      verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    }

    @Test
    void recover_compensatingSagaStuckBeyondGracePeriod_escalates() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      // Compensation failure older than grace period
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.compensationFailed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)));

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(2), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert
      ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(eq(saga), eq(2), captor.capture(), any());
      assertThat(captor.getValue().getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
      verify(engine, never()).recover(any(RecoveryAction.Compensate.class), any(), any());
    }
  }

  // =========================================================================
  // recoverOne() — definition resolution
  // =========================================================================

  @Nested
  class DefinitionResolution {

    @Test
    void recover_definitionNotFound_escalates() {
      // Arrange
      SagaStateSnapshot saga = snapshot(SagaStatus.RUNNING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot newState = snapshot(SagaStatus.ESCALATED);

      setupSinglePageRecovery(saga);
      when(store.getEvents(SAGA_ID)).thenReturn(List.of());
      when(engine.replayEvents(saga, List.of())).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(0);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(null);
      when(store.recordStatusEvent(eq(saga), eq(0), any(StatusEvent.class), any()))
          .thenReturn(newState);

      // Act
      manager.recover();

      // Assert
      ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(eq(saga), eq(0), captor.capture(), any());
      StatusEvent event = captor.getValue();
      assertThat(event.getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
      assertThat(event.getPayload()).contains("not found");
      verify(engine, never()).recover(any(RecoveryAction.Resume.class), any(), any());
      verify(engine, never()).recover(any(RecoveryAction.Compensate.class), any(), any());
    }
  }

  // =========================================================================
  // start() / stop()
  // =========================================================================

  @Nested
  class Lifecycle {

    @Test
    void start_schedulesPeriodicRecovery() {
      // Act
      manager.start();

      // Assert
      verify(scheduler)
          .scheduleWithFixedDelay(
              any(Runnable.class), eq(0L), eq(30L), eq(java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    void start_recoveryPassThrowsError_scheduledTaskContainsIt() {
      // Arrange — capture the periodic task and make the pass blow up with an Error. Only a catch
      // on Throwable contains it; a Throwable escaping a scheduleWithFixedDelay task cancels all
      // its future executions, silently stopping recovery for the rest of the process.
      when(store.findRecoverable(any(), any())).thenThrow(new Error("scan blew up"));
      manager.start();
      ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
      verify(scheduler)
          .scheduleWithFixedDelay(task.capture(), eq(0L), eq(30L), eq(TimeUnit.SECONDS));

      // Act & Assert
      assertThatCode(() -> task.getValue().run()).doesNotThrowAnyException();
    }

    @Test
    void stop_shutsDownBothExecutors() throws InterruptedException {
      // Arrange
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);

      // Act
      manager.stop(deadline);

      // Assert
      verify(scheduler).shutdown();
      // shutdownNow is always called in finally as a safety net
      verify(scheduler).shutdownNow();
    }

    @Test
    void stop_forceStopsInFinally() throws InterruptedException {
      // Arrange
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      when(scheduler.awaitTermination(anyLong(), any())).thenReturn(false);

      // Act
      manager.stop(deadline);

      // Assert
      verify(scheduler).shutdown();
      verify(scheduler).shutdownNow();
    }
  }
}
