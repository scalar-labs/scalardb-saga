package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import com.scalar.db.saga.store.SweepScatter;
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
    // Both sweeps start from the owner's scattered cursor; without this stub the mock returns
    // null and the sweeps end before scanning anything.
    lenient().when(store.initialSweepCursor(OWNER_ID)).thenReturn(mock(ScanCursor.class));
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
    void start_schedulesImmediateFirstPassAndDePhasedPeriodicPasses() {
      // Arrange — the periodic train is shifted by the owner's deterministic offset; the startup
      // pass stays immediate so a restart recovers interrupted sagas right away.
      long offset = SweepScatter.offsetSeconds(OWNER_ID, "recovery", 30);

      // Act
      manager.start();

      // Assert
      assertThat(offset).isBetween(0L, 29L);
      verify(scheduler).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.SECONDS));
      verify(scheduler)
          .scheduleWithFixedDelay(
              any(Runnable.class), eq(30L + offset), eq(30L), eq(TimeUnit.SECONDS));
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

  // =========================================================================
  // recover() — success-counted budget, page isolation, cross-pass resume
  // =========================================================================

  @Nested
  class ScatteredSweepBudget {

    /** A manager whose per-sweep budget is {@code batchSize}, on the standard fixed clock. */
    private SagaRecoveryManager managerWithBatchSize(int batchSize) {
      RecoveryConfig config =
          new RecoveryConfig(
              60_000, 30, GRACE_PERIOD, batchSize, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      return new SagaRecoveryManager(store, engine, registry, OWNER_ID, config, scheduler);
    }

    private SagaStateSnapshot namedSnapshot(String sagaId) {
      return new SagaStateSnapshot(
          sagaId, SAGA_NAME, SagaStatus.RUNNING, OWNER_ID, DEF_VERSION, NOW.minusSeconds(300), NOW);
    }

    private void setupSuccessfulRecovery(SagaStateSnapshot saga) {
      SagaDefinition def = definition();
      ExecutionContext ctx = mock(ExecutionContext.class);
      when(store.claimForRecovery(saga, OWNER_ID)).thenReturn(Optional.of(saga));
      when(store.getEvents(saga.getSagaId())).thenReturn(List.of());
      when(engine.replayEvents(saga, List.of())).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      lenient().when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(def);
    }

    @Test
    void recover_claimLost_budgetNotConsumedAndScanContinues() {
      // Arrange — batch size 1: the lost race on page 1 must not spend the budget, so the sweep
      // reaches page 2 and recovers its saga. The attempts-counted budget stopped after page 1.
      SagaRecoveryManager smallManager = managerWithBatchSize(1);

      SagaStateSnapshot lost = namedSnapshot("saga-lost");
      SagaStateSnapshot won = namedSnapshot("saga-won");
      ScanCursor cursor = mock(ScanCursor.class);
      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(lost), cursor))
          .thenReturn(new Recoverables(List.of(won), null));
      when(store.claimForRecovery(lost, OWNER_ID)).thenReturn(Optional.empty());
      setupSuccessfulRecovery(won);

      // Act
      smallManager.recover();

      // Assert
      verify(store, times(2)).findRecoverable(any(), any());
      verify(engine).recover(any(), any(), any());
    }

    @Test
    void recover_driveFailsAfterClaim_budgetConsumed() {
      // Arrange — batch size 1: the claim committed, so the failed drive still spends the budget
      // and the sweep must NOT continue to the next page (that would be a claim spree).
      SagaRecoveryManager smallManager = managerWithBatchSize(1);

      SagaStateSnapshot saga = namedSnapshot("saga-drive-fails");
      ScanCursor cursor = mock(ScanCursor.class);
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), cursor));
      when(store.claimForRecovery(saga, OWNER_ID)).thenReturn(Optional.of(saga));
      when(store.getEvents(saga.getSagaId())).thenThrow(new RuntimeException("store down"));

      // Act
      smallManager.recover();

      // Assert — one page scanned, budget spent by the claimed-but-failed saga
      verify(store, times(1)).findRecoverable(any(), any());
    }

    @Test
    void recover_scanPageFails_skipsBucketAndParkedSweepStillRuns() {
      // Arrange — the first page's scan throws (poison row); the sweep skips past it via
      // advanceSweepCursor and continues, and the parked sweep still runs afterwards.
      ScanCursor afterPoison = mock(ScanCursor.class);
      when(store.findRecoverable(any(), any()))
          .thenThrow(new RuntimeException("row cannot be deserialized"))
          .thenReturn(new Recoverables(List.of(), null));
      when(store.advanceSweepCursor(any())).thenReturn(afterPoison);

      // Act
      manager.recover();

      // Assert
      verify(store).advanceSweepCursor(any());
      verify(store, times(2)).findRecoverable(any(), any());
      verify(store).findOverdueParkedSagas(any(), any());
    }

    @Test
    void recover_budgetStop_nextPassResumesAtSameCursor() {
      // Arrange — batch size 1: pass 1 stops on budget holding the next cursor; pass 2 must
      // resume there instead of restarting at the permutation's first bucket.
      SagaRecoveryManager smallManager = managerWithBatchSize(1);

      ScanCursor initial = mock(ScanCursor.class);
      ScanCursor next = mock(ScanCursor.class);
      when(store.initialSweepCursor(OWNER_ID)).thenReturn(initial);
      SagaStateSnapshot saga = namedSnapshot("saga-budget");
      when(store.findRecoverable(any(), eq(initial)))
          .thenReturn(new Recoverables(List.of(saga), next));
      when(store.findRecoverable(any(), eq(next))).thenReturn(new Recoverables(List.of(), null));
      setupSuccessfulRecovery(saga);

      // Act
      smallManager.recover();
      smallManager.recover();

      // Assert — the initial cursor page was scanned once (pass 1), the resume cursor page once
      // (pass 2)
      verify(store).findRecoverable(any(), eq(initial));
      verify(store).findRecoverable(any(), eq(next));
    }

    @Test
    void recover_claimThrows_budgetConsumedConservatively() {
      // Arrange — batch size 1: a claim that throws may have committed without confirmation, so
      // it must spend the budget; the sweep must NOT continue claiming across the ring.
      SagaRecoveryManager smallManager = managerWithBatchSize(1);

      SagaStateSnapshot saga = namedSnapshot("saga-claim-throws");
      ScanCursor cursor = mock(ScanCursor.class);
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), cursor));
      when(store.claimForRecovery(saga, OWNER_ID))
          .thenThrow(new RuntimeException("commit status unknown and verification failed"));

      // Act
      smallManager.recover();

      // Assert — one page scanned, the error spent the budget, no further scanning
      verify(store, times(1)).findRecoverable(any(), any());
      verify(store, never()).getEvents(any());
    }

    @Test
    void recover_threeConsecutivePageScansFail_sweepStopsAsStoreUnavailable() {
      // Arrange — every staleness page scan fails (store outage, not a poison row)
      when(store.findRecoverable(any(), any())).thenThrow(new RuntimeException("store down"));
      when(store.advanceSweepCursor(any())).thenReturn(mock(ScanCursor.class));

      // Act
      manager.recover();

      // Assert — the sweep gives up after exactly three consecutive failures instead of failing
      // once per bucket through the whole ring; the failing page is not advanced past (the next
      // pass retries it), and the parked sweep still runs independently
      verify(store, times(3)).findRecoverable(any(), any());
      verify(store, times(2)).advanceSweepCursor(any());
      verify(store).findOverdueParkedSagas(any(), any());
    }

    @Test
    void recover_pageFailuresInterleavedWithSuccesses_sweepCompletesTheRevolution() {
      // Arrange — two failures, a success, two failures, then the final page: the consecutive
      // counter resets on each success, so the sweep never trips the store-unavailable stop
      ScanCursor c = mock(ScanCursor.class);
      when(store.findRecoverable(any(), any()))
          .thenThrow(new RuntimeException("blip 1"))
          .thenThrow(new RuntimeException("blip 2"))
          .thenReturn(new Recoverables(List.of(), c))
          .thenThrow(new RuntimeException("blip 3"))
          .thenThrow(new RuntimeException("blip 4"))
          .thenReturn(new Recoverables(List.of(), null));
      when(store.advanceSweepCursor(any())).thenReturn(mock(ScanCursor.class));

      // Act
      manager.recover();

      // Assert — all six pages were attempted; the revolution completed
      verify(store, times(6)).findRecoverable(any(), any());
      verify(store, times(4)).advanceSweepCursor(any());
    }

    @Test
    void recover_slowTaskInFirstBucket_laterBucketAndParkedSweepProceedConcurrently()
        throws Exception {
      // Arrange — the first bucket's saga blocks inside its task; the pass must still scan and
      // process the second bucket AND the parked sweep while it blocks (rounds submit everything
      // before awaiting; a per-page barrier would deadlock the latches below).
      java.util.concurrent.CountDownLatch releaseSlowTask =
          new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.CountDownLatch fastSagaProcessed =
          new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.CountDownLatch parkedProcessed =
          new java.util.concurrent.CountDownLatch(1);
      SagaStateSnapshot slow = namedSnapshot("saga-slow");
      SagaStateSnapshot fast = namedSnapshot("saga-fast");
      ScanCursor cursor = mock(ScanCursor.class);
      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(slow), cursor))
          .thenReturn(new Recoverables(List.of(fast), null));
      when(store.claimForRecovery(slow, OWNER_ID))
          .thenAnswer(
              invocation -> {
                if (!releaseSlowTask.await(5, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("slow task was never released");
                }
                return Optional.empty();
              });
      when(store.claimForRecovery(fast, OWNER_ID))
          .thenAnswer(
              invocation -> {
                fastSagaProcessed.countDown();
                return Optional.empty();
              });
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of("saga-parked"), null));
      when(store.getStateSnapshot("saga-parked"))
          .thenAnswer(
              invocation -> {
                parkedProcessed.countDown();
                return Optional.empty();
              });

      // Act — run the pass on another thread; it cannot finish until the slow task is released
      java.util.concurrent.ExecutorService runner =
          java.util.concurrent.Executors.newSingleThreadExecutor();
      try {
        java.util.concurrent.Future<?> pass = runner.submit(manager::recover);

        // Assert — while the first bucket's task is blocked, the later bucket and the parked
        // sweep both make progress
        assertThat(fastSagaProcessed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(parkedProcessed.await(5, TimeUnit.SECONDS)).isTrue();
        releaseSlowTask.countDown();
        pass.get(5, TimeUnit.SECONDS);
      } finally {
        releaseSlowTask.countDown();
        runner.shutdownNow();
      }
    }

    @Test
    void recover_interruptedMidPass_cancelsInFlightTasksAndRestoresInterruptFlag()
        throws Exception {
      // Arrange — the pass's single task blocks inside its claim; interrupting the pass thread
      // must cancel that task (interrupting it) and charge its outcome. The task observes the
      // interrupt at its next interruptible point, which may be after recover() has returned —
      // cancellation is requested, not joined.
      java.util.concurrent.CountDownLatch claimStarted = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.CountDownLatch taskObservedInterrupt =
          new java.util.concurrent.CountDownLatch(1);
      SagaStateSnapshot saga = namedSnapshot("saga-cancelled-with-pass");
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), null));
      when(store.claimForRecovery(saga, OWNER_ID))
          .thenAnswer(
              invocation -> {
                claimStarted.countDown();
                try {
                  new java.util.concurrent.CountDownLatch(1).await(); // blocks until interrupted
                } catch (InterruptedException e) {
                  taskObservedInterrupt.countDown();
                }
                return Optional.empty();
              });
      java.util.concurrent.atomic.AtomicBoolean interruptFlagRestored =
          new java.util.concurrent.atomic.AtomicBoolean();

      // Act — run the pass, interrupt it mid-await
      Thread passThread =
          new Thread(
              () -> {
                manager.recover();
                interruptFlagRestored.set(Thread.currentThread().isInterrupted());
              });
      passThread.start();
      assertThat(claimStarted.await(5, TimeUnit.SECONDS)).isTrue();
      passThread.interrupt();
      passThread.join(5_000);

      // Assert — the pass returned, its task was interrupted (not abandoned), and the pass
      // thread's interrupt flag was restored
      assertThat(passThread.isAlive()).isFalse();
      assertThat(taskObservedInterrupt.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(interruptFlagRestored).isTrue();
    }

    @Test
    void recover_interruptedWhileBlockedBehindInFlightPass_returnsWithoutRunning()
        throws Exception {
      // Arrange — thread A holds the pass lock (its task blocks on a latch); thread B calls
      // recover(), waits on the lock, and is interrupted: B must return without running a pass.
      java.util.concurrent.CountDownLatch passAStarted = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.CountDownLatch releasePassA = new java.util.concurrent.CountDownLatch(1);
      SagaStateSnapshot saga = namedSnapshot("saga-holding-the-lock");
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), null));
      when(store.claimForRecovery(saga, OWNER_ID))
          .thenAnswer(
              invocation -> {
                passAStarted.countDown();
                if (!releasePassA.await(5, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("pass A was never released");
                }
                return Optional.empty();
              });

      java.util.concurrent.ExecutorService runner =
          java.util.concurrent.Executors.newFixedThreadPool(2);
      try {
        // Act — A enters the pass and blocks; B is interrupted while waiting for the lock
        java.util.concurrent.Future<?> passA = runner.submit(manager::recover);
        assertThat(passAStarted.await(5, TimeUnit.SECONDS)).isTrue();
        java.util.concurrent.atomic.AtomicBoolean interruptedFlagSeen =
            new java.util.concurrent.atomic.AtomicBoolean();
        Thread passBThread =
            new Thread(
                () -> {
                  manager.recover();
                  interruptedFlagSeen.set(Thread.currentThread().isInterrupted());
                });
        passBThread.start();
        passBThread.interrupt();
        passBThread.join(5_000);

        // Assert — B finished promptly (without waiting for A) and re-set the interrupt flag
        assertThat(passBThread.isAlive()).isFalse();
        assertThat(interruptedFlagSeen).isTrue();
        releasePassA.countDown();
        passA.get(5, TimeUnit.SECONDS);

        // Only pass A ever scanned; B ran nothing
        verify(store, times(1)).findRecoverable(any(), any());
      } finally {
        releasePassA.countDown();
        runner.shutdownNow();
      }
    }

    @Test
    void recover_parkedRaceLost_budgetNotConsumedAndScanContinues() {
      // Arrange — batch size 1: the parked saga on page 1 was already resolved by a callback
      // (lost race), so the parked sweep must continue to page 2.
      SagaRecoveryManager smallManager = managerWithBatchSize(1);

      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(), null));
      ScanCursor cursor = mock(ScanCursor.class);
      when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of("saga-resolved"), cursor))
          .thenReturn(new OverdueParked(List.of(), null));
      when(store.getStateSnapshot("saga-resolved")).thenReturn(Optional.empty());

      // Act
      smallManager.recover();

      // Assert
      verify(store, times(2)).findOverdueParkedSagas(any(), any());
    }
  }
}
