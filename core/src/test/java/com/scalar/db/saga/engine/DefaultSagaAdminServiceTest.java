package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.ResetResult;
import com.scalar.db.saga.api.SagaPage;
import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.RecoveryStrategy;
import com.scalar.db.saga.exception.SagaConcurrentModificationException;
import com.scalar.db.saga.exception.SagaDefinitionNotFoundException;
import com.scalar.db.saga.exception.SagaErrorCode;
import com.scalar.db.saga.exception.SagaIllegalArgumentException;
import com.scalar.db.saga.exception.SagaNotFoundException;
import com.scalar.db.saga.exception.SagaPersistenceException;
import com.scalar.db.saga.exception.SagaStatePreconditionException;
import com.scalar.db.saga.store.AdminAuditPayload;
import com.scalar.db.saga.store.EventType;
import com.scalar.db.saga.store.SagaEvent;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultSagaAdminServiceTest {

  private static final String SAGA_ID = "s1";
  private static final String SAGA_NAME = "order-saga";
  private static final String DEF_VERSION = "v1";
  private static final Instant TS = Instant.parse("2026-07-13T10:00:00Z");
  private static final String OPERATOR = "op-alice";

  private SagaStore store;
  private SagaEngine engine;
  private SagaDefinitionRegistry registry;
  private DefaultSagaAdminService service;
  private ExecutorService driveExecutor;

  @BeforeEach
  void setUp() {
    store = mock(SagaStore.class);
    engine = mock(SagaEngine.class);
    registry = mock(SagaDefinitionRegistry.class);
    service = new DefaultSagaAdminService(store, engine, registry, () -> OPERATOR);
    driveExecutor = Executors.newVirtualThreadPerTaskExecutor();
  }

  @AfterEach
  void tearDown() {
    driveExecutor.shutdownNow();
  }

  // A 2-step BACKWARD saga: pivot = the last step (index 1).
  private static SagaDefinition backwardDef() {
    return SagaDefinition.newBuilder(SAGA_NAME)
        .saga()
        .step("debit", "com.example.DebitStep")
        .add()
        .step("credit", "com.example.CreditStep")
        .add()
        .build();
  }

  // A 2-step FORWARD saga: pivot = -1, so every step is post-pivot.
  private static SagaDefinition forwardDef() {
    return SagaDefinition.newBuilder(SAGA_NAME)
        .saga()
        .recoveryStrategy(RecoveryStrategy.FORWARD)
        .step("debit", "com.example.DebitStep")
        .add()
        .step("credit", "com.example.CreditStep")
        .add()
        .build();
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(SAGA_ID, SAGA_NAME, status, "owner", DEF_VERSION, TS, TS);
  }

  private ArgumentCaptor<StatusEvent> stubDrive(SagaStateSnapshot before, List<SagaEvent> events) {
    ExecutionContext ctx = mock(ExecutionContext.class);
    SagaStateSnapshot after = snapshot(SagaStatus.COMPENSATED);
    when(engine.replayEvents(before, events)).thenReturn(ctx);
    when(ctx.getCurrentState()).thenReturn(after);
    ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
    when(store.recordStatusEvent(eq(before), anyInt(), captor.capture(), any()))
        .thenReturn(snapshot(SagaStatus.COMPENSATING));
    return captor;
  }

  // ---------------------------------------------------------------------------
  // recoverSaga — preconditions (transition matrix)
  // ---------------------------------------------------------------------------

  @Test
  void recoverSaga_escalatedSaga_throwsWrongState() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.ESCALATED)));

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaStatePreconditionException.class)
        .extracting(e -> ((SagaStatePreconditionException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
    verify(engine, never()).recover(any(), any(), any());
  }

  @Test
  void recoverSaga_waitingSaga_throwsParked() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.WAITING)));

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaStatePreconditionException.class)
        .extracting(e -> ((SagaStatePreconditionException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.SAGA_PARKED);
  }

  @Test
  void recoverSaga_completedSaga_throwsWrongState() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.COMPLETED)));

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaStatePreconditionException.class);
  }

  @Test
  void recoverSaga_missingSaga_throwsNotFound() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.empty());

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaNotFoundException.class);
  }

  @Test
  void recoverSaga_missingDefinition_throwsDefinitionNotFound() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.RUNNING)));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(null);

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaDefinitionNotFoundException.class);
  }

  // ---------------------------------------------------------------------------
  // recoverSaga — direction (the pivot decides, not the operator)
  // ---------------------------------------------------------------------------

  @Test
  void recoverSaga_runningWithPrePivotFailure_drivesCompensationWithAudit() {
    // Arrange — step 0 done, step 1 failed pre-pivot -> resolve = Compensate(1)
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null));
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(running));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    ArgumentCaptor<StatusEvent> audit = stubDrive(running, events);

    // Act
    service.recoverSaga(SAGA_ID, "downstream broke");

    // Assert — recovering event to COMPENSATING, and the engine compensates from step 1
    assertThat(audit.getValue().getEventType()).isEqualTo(EventType.SAGA_RECOVERING);
    assertThat(audit.getValue().getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(AdminAuditPayload.operator(audit.getValue().getPayload())).isEqualTo(OPERATOR);
    assertThat(AdminAuditPayload.reason(audit.getValue().getPayload()))
        .isEqualTo("downstream broke");
    verify(engine)
        .recover(
            eq(new RecoveryAction.Compensate(1)),
            any(SagaDefinition.class),
            any(ExecutionContext.class));
  }

  @Test
  void recoverSaga_runningCleanCrash_resumesForward() {
    // Arrange — step 0 done, no failure -> resolve = Resume(1)
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null));
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(running));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    ArgumentCaptor<StatusEvent> audit = stubDrive(running, events);

    // Act
    service.recoverSaga(SAGA_ID, "resume it");

    // Assert — recovering event to RUNNING, engine resumes forward from step 1
    assertThat(audit.getValue().getEventType()).isEqualTo(EventType.SAGA_RECOVERING);
    assertThat(audit.getValue().getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    verify(engine)
        .recover(
            eq(new RecoveryAction.Resume(1)),
            any(SagaDefinition.class),
            any(ExecutionContext.class));
  }

  @Test
  void recoverSaga_retiredDefinition_stillRecovers() {
    // Retiring a saga refuses NEW starts. A saga already running under a retired definition still
    // has to be driveable to a conclusion, or retiring one would strand exactly the sagas an
    // operator most needs to finish. Recovery resolves the definition by version and never goes
    // through the start gate, which is what makes that hold.
    // Arrange
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null));
    SagaDefinition retired =
        SagaDefinition.newBuilder(SAGA_NAME)
            .saga()
            .version(DEF_VERSION)
            .disabled(true)
            .step("debit", "com.example.DebitStep")
            .add()
            .step("credit", "com.example.CreditStep")
            .add()
            .build();
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(running));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(retired);
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    stubDrive(running, events);

    // Act
    service.recoverSaga(SAGA_ID, "finish it despite the retirement");

    // Assert
    verify(engine)
        .recover(
            eq(new RecoveryAction.Resume(1)),
            any(SagaDefinition.class),
            any(ExecutionContext.class));
  }

  @Test
  void recoverSaga_compensatingSaga_continuesCompensation() {
    // Arrange — already COMPENSATING, step 1 compensated -> continue from step 0
    SagaStateSnapshot compensating = snapshot(SagaStatus.COMPENSATING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.completed(1, "credit", null),
            StatusEvent.compensating(),
            StepEvent.compensated(1, "credit"));
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(compensating));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    stubDrive(compensating, events);

    // Act
    service.recoverSaga(SAGA_ID, "keep going");

    // Assert
    verify(engine)
        .recover(
            eq(new RecoveryAction.Compensate(0)),
            any(SagaDefinition.class),
            any(ExecutionContext.class));
  }

  // ---------------------------------------------------------------------------
  // forceComplete
  // ---------------------------------------------------------------------------

  @Test
  void forceComplete_escalatedSaga_recordsForceCompletedEvent() {
    // Arrange
    SagaStateSnapshot escalated = snapshot(SagaStatus.ESCALATED);
    SagaStateSnapshot completed = snapshot(SagaStatus.COMPLETED);
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(escalated));
    when(store.getEventCount(SAGA_ID)).thenReturn(5);
    ArgumentCaptor<StatusEvent> audit = ArgumentCaptor.forClass(StatusEvent.class);
    when(store.recordStatusEvent(eq(escalated), eq(5), audit.capture(), any()))
        .thenReturn(completed);

    // Act
    SagaStateSnapshot result = service.forceComplete(SAGA_ID, "confirmed done");

    // Assert
    assertThat(result).isEqualTo(completed);
    assertThat(audit.getValue().getEventType()).isEqualTo(EventType.SAGA_FORCE_COMPLETED);
    assertThat(audit.getValue().getTargetStatus()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(AdminAuditPayload.reason(audit.getValue().getPayload())).isEqualTo("confirmed done");
    verify(engine, never()).recover(any(), any(), any());
  }

  @Test
  void forceComplete_notEscalated_throwsWrongState() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.RUNNING)));

    // Act & Assert
    assertThatThrownBy(() -> service.forceComplete(SAGA_ID, "why"))
        .isInstanceOf(SagaStatePreconditionException.class)
        .extracting(e -> ((SagaStatePreconditionException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.SAGA_WRONG_STATE);
  }

  // ---------------------------------------------------------------------------
  // resetEscalated — single
  // ---------------------------------------------------------------------------

  @Test
  void resetEscalated_compensationStuckEscalation_drivesCompensation() {
    // Arrange — escalated out of COMPENSATING (a SAGA_COMPENSATING event is in the stream)
    SagaStateSnapshot escalated = snapshot(SagaStatus.ESCALATED);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null),
            StatusEvent.compensating());
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(escalated));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    ArgumentCaptor<StatusEvent> audit = stubDrive(escalated, events);

    // Act
    service.resetEscalated(SAGA_ID, "un-escalate");

    // Assert — reset event to COMPENSATING, continues compensating
    assertThat(audit.getValue().getEventType()).isEqualTo(EventType.SAGA_RESET);
    assertThat(audit.getValue().getTargetStatus()).isEqualTo(SagaStatus.COMPENSATING);
    verify(engine)
        .recover(
            eq(new RecoveryAction.Compensate(1)),
            any(SagaDefinition.class),
            any(ExecutionContext.class));
  }

  @Test
  void resetEscalated_postPivotEscalation_resumesForward() {
    // Arrange — FORWARD saga (all steps post-pivot), failed step -> resume forward
    SagaStateSnapshot escalated = snapshot(SagaStatus.ESCALATED);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null));
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(escalated));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(forwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    ArgumentCaptor<StatusEvent> audit = stubDrive(escalated, events);

    // Act
    service.resetEscalated(SAGA_ID, "downstream restored");

    // Assert — reset event to RUNNING, resumes forward from step 1
    assertThat(audit.getValue().getEventType()).isEqualTo(EventType.SAGA_RESET);
    assertThat(audit.getValue().getTargetStatus()).isEqualTo(SagaStatus.RUNNING);
    verify(engine)
        .recover(
            eq(new RecoveryAction.Resume(1)),
            any(SagaDefinition.class),
            any(ExecutionContext.class));
  }

  @Test
  void resetEscalated_notEscalated_throwsWrongState() {
    // Arrange
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.RUNNING)));

    // Act & Assert
    assertThatThrownBy(() -> service.resetEscalated(SAGA_ID, "why"))
        .isInstanceOf(SagaStatePreconditionException.class);
  }

  // ---------------------------------------------------------------------------
  // resetEscalated — bulk
  // ---------------------------------------------------------------------------

  @Test
  void resetEscalated_bulkConflictingStatusFilter_throwsSagaIllegalArgument() {
    // Arrange
    SagaQuery query = SagaQuery.newBuilder().status(SagaStatus.RUNNING).build();

    // Act & Assert
    assertThatThrownBy(() -> service.resetEscalated(query, "why"))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void resetEscalated_bulkMixedOutcomes_countsResetAndSkipped() {
    // Arrange — one resettable saga + one with an unresolvable definition
    SagaStateSnapshot ok =
        new SagaStateSnapshot("ok", SAGA_NAME, SagaStatus.ESCALATED, "o", DEF_VERSION, TS, TS);
    SagaStateSnapshot noDef =
        new SagaStateSnapshot("nodef", "gone", SagaStatus.ESCALATED, "o", "v9", TS, TS);
    when(store.listStateSnapshots(any()))
        .thenReturn(new SagaPage<>(List.of(ok, noDef), "next-token"));
    List<SagaEvent> events =
        List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(registry.resolve("gone", "v9")).thenReturn(null);
    when(store.getEvents("ok")).thenReturn(events);
    when(store.recordStatusEvent(eq(ok), anyInt(), any(), any(), eq(Instant.EPOCH))).thenReturn(ok);

    // Act
    ResetResult result = service.resetEscalated(SagaQuery.newBuilder().build(), "sweep");

    // Assert — the skipped saga is named with its reason (unresolvable definition)
    assertThat(result.getResetCount()).isEqualTo(1);
    assertThat(result.getSkipped())
        .containsExactly(
            new ResetResult.SkippedSaga("nodef", ResetResult.SkipReason.DEFINITION_NOT_FOUND));
    assertThat(result.getNextPageToken()).isEqualTo("next-token");
    // The bulk sweep un-escalates and stamps the row for immediate recovery in one transaction
    // (updated_at = EPOCH), then leaves the drive to the recovery loop rather than driving inline.
    verify(store).recordStatusEvent(eq(ok), anyInt(), any(), any(), eq(Instant.EPOCH));
    verify(store, never()).markForRecovery(any());
    verify(engine, never()).recover(any(), any(), any());
  }

  @Test
  void resetEscalated_bulkLostCasRace_countsAsSkipped() {
    // Arrange — the row changed under us; the co-committed transition throws the CAS-lost exception
    SagaStateSnapshot racing =
        new SagaStateSnapshot("race", SAGA_NAME, SagaStatus.ESCALATED, "o", DEF_VERSION, TS, TS);
    when(store.listStateSnapshots(any())).thenReturn(new SagaPage<>(List.of(racing), null));
    List<SagaEvent> events =
        List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents("race")).thenReturn(events);
    when(store.recordStatusEvent(eq(racing), anyInt(), any(), any(), eq(Instant.EPOCH)))
        .thenThrow(new SagaConcurrentModificationException("race"));

    // Act
    ResetResult result = service.resetEscalated(SagaQuery.newBuilder().build(), "sweep");

    // Assert — the racing saga is named with the CAS-lost reason
    assertThat(result.getResetCount()).isZero();
    assertThat(result.getSkipped())
        .containsExactly(
            new ResetResult.SkippedSaga("race", ResetResult.SkipReason.CONCURRENT_MODIFICATION));
    // The un-escalation and the recovery mark co-commit, so a lost CAS leaves neither behind.
    verify(store, never()).markForRecovery(any());
  }

  @Test
  void resetEscalated_bulkCorruptEventStream_skipsItAndSweepsTheRest() {
    // Arrange — "bad" cannot be read back at all; "ok" is healthy and ordered after it. A reset
    // saga leaves the ESCALATED scan, so aborting on "bad" would strand "ok" on every re-run too.
    SagaStateSnapshot bad =
        new SagaStateSnapshot("bad", SAGA_NAME, SagaStatus.ESCALATED, "o", DEF_VERSION, TS, TS);
    SagaStateSnapshot ok =
        new SagaStateSnapshot("ok", SAGA_NAME, SagaStatus.ESCALATED, "o", DEF_VERSION, TS, TS);
    when(store.listStateSnapshots(any())).thenReturn(new SagaPage<>(List.of(bad, ok), null));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents("bad"))
        .thenThrow(
            SagaPersistenceException.deserializationFailed(new IllegalArgumentException("boom")));
    when(store.getEvents("ok"))
        .thenReturn(List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null)));
    when(store.recordStatusEvent(eq(ok), anyInt(), any(), any(), eq(Instant.EPOCH))).thenReturn(ok);

    // Act
    ResetResult result = service.resetEscalated(SagaQuery.newBuilder().build(), "sweep");

    // Assert — the corrupt saga is reported by its reason code, and the healthy one behind it is
    // still reset. The raw decode message is never returned (it can echo stored bytes); it is
    // logged server-side instead.
    assertThat(result.getResetCount()).isEqualTo(1);
    assertThat(result.getSkipped())
        .containsExactly(
            new ResetResult.SkippedSaga("bad", ResetResult.SkipReason.CORRUPT_EVENT_STREAM));
    assertThat(result.getSkipped().get(0).getDetail()).isNull();
    verify(store).recordStatusEvent(eq(ok), anyInt(), any(), any(), eq(Instant.EPOCH));
    verify(store, never()).markForRecovery(any());
  }

  @Test
  void resetEscalated_bulkCorruptDefinition_skipsAsDefinitionNotFound() {
    // Arrange — the saga's stored definition cannot be decoded, so resolve throws non-retryably.
    // That is not a store outage: the sweep must skip this one saga, not abort on the whole page.
    SagaStateSnapshot bad =
        new SagaStateSnapshot("bad", SAGA_NAME, SagaStatus.ESCALATED, "o", DEF_VERSION, TS, TS);
    when(store.listStateSnapshots(any())).thenReturn(new SagaPage<>(List.of(bad), null));
    when(registry.resolve(SAGA_NAME, DEF_VERSION))
        .thenThrow(
            SagaPersistenceException.deserializationFailed(new IllegalArgumentException("boom")));

    // Act
    ResetResult result = service.resetEscalated(SagaQuery.newBuilder().build(), "sweep");

    // Assert — skipped as unresolvable, never un-escalated or force-driven
    assertThat(result.getResetCount()).isZero();
    assertThat(result.getSkipped())
        .containsExactly(
            new ResetResult.SkippedSaga("bad", ResetResult.SkipReason.DEFINITION_NOT_FOUND));
    verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any(), any());
    verify(store, never()).markForRecovery(any());
  }

  @Test
  void resetEscalated_bulkRetryableStoreFailure_abortsRatherThanSkipping() {
    // Arrange — the store is failing, not this saga. Every remaining row would fail the same way,
    // so the sweep must surface it instead of reporting a page of misleading skips.
    SagaStateSnapshot down =
        new SagaStateSnapshot("down", SAGA_NAME, SagaStatus.ESCALATED, "o", DEF_VERSION, TS, TS);
    when(store.listStateSnapshots(any())).thenReturn(new SagaPage<>(List.of(down), null));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents("down"))
        .thenThrow(
            SagaPersistenceException.storeUnavailable(new IllegalStateException("conn refused")));

    // Act & Assert
    assertThatThrownBy(() -> service.resetEscalated(SagaQuery.newBuilder().build(), "sweep"))
        .isInstanceOf(SagaPersistenceException.class);
  }

  // ---------------------------------------------------------------------------
  // reason + operator validation
  // ---------------------------------------------------------------------------

  @Test
  void recoverSaga_blankReason_throwsSagaIllegalArgument() {
    // Act & Assert — validated before any store interaction. The point of the typed exception is
    // that the embedded caller reaches the same error code a remote caller reconstructs from the
    // wire, so assert the code and not just the type.
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "   "))
        .isInstanceOf(SagaIllegalArgumentException.class)
        .extracting(e -> ((SagaIllegalArgumentException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.INVALID_ARGUMENT);
  }

  @Test
  void recoverSaga_tooLongReason_throwsSagaIllegalArgument() {
    // Arrange
    String longReason = "x".repeat(1025);

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, longReason))
        .isInstanceOf(SagaIllegalArgumentException.class);
  }

  @Test
  void recoverSaga_controlCharsInReason_areReplacedWithSpacesInAudit() {
    // Arrange
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(StatusEvent.started(null), StepEvent.completed(0, "debit", null));
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(running));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    ArgumentCaptor<StatusEvent> audit = stubDrive(running, events);

    // Act — newline (log-forging vector) must not survive into the audit record
    service.recoverSaga(SAGA_ID, "line1\nline2");

    // Assert — the word boundary survives as a space
    assertThat(AdminAuditPayload.reason(audit.getValue().getPayload())).isEqualTo("line1 line2");
  }

  @Test
  void recoverSaga_blankOperator_throwsIllegalState() {
    // Arrange — a misconfigured OperatorContext must fail closed, not write an anonymous record
    DefaultSagaAdminService blankOp =
        new DefaultSagaAdminService(store, engine, registry, () -> "  ");

    // Act & Assert
    assertThatThrownBy(() -> blankOp.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(IllegalStateException.class);
  }

  // ---------------------------------------------------------------------------
  // operator — rejected rather than sanitized, and rejected before anything is written
  // ---------------------------------------------------------------------------

  @Test
  @SuppressWarnings("NullAway") // deliberately returns null from the public-API OperatorContext
  void recoverSaga_nullOperatorGiven_throwsIllegalStateAndWritesNothing() {
    // Arrange — OperatorContext is public API, so an embedded implementation not compiled with
    // NullAway could return null; the service must fail closed, not NPE on isBlank()
    DefaultSagaAdminService service =
        new DefaultSagaAdminService(store, engine, registry, () -> null);

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(IllegalStateException.class);
    verifyNothingWritten();
  }

  @Test
  void recoverSaga_operatorWithControlCharGiven_throwsIllegalStateAndWritesNothing() {
    // Arrange — unlike a reason, a principal is never flattened to fit: a mutated principal is a
    // false audit record
    DefaultSagaAdminService service = serviceWithOperator("op\nalice");

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(IllegalStateException.class);
    verifyNothingWritten();
  }

  @Test
  void recoverSaga_operatorOverMaxLengthGiven_throwsIllegalStateAndWritesNothing() {
    // Arrange — 257 chars, one over the bound
    DefaultSagaAdminService service = serviceWithOperator("a".repeat(257));

    // Act & Assert
    assertThatThrownBy(() -> service.recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(IllegalStateException.class);
    verifyNothingWritten();
  }

  @Test
  void recoverSaga_operatorAtMaxLengthGiven_isAccepted() {
    // Arrange — exactly at the bound; an email is capped at 254 octets, so this must still pass
    String operator = "a".repeat(256);
    ArgumentCaptor<StatusEvent> audit = arrangeDrivableSaga();

    // Act
    serviceWithOperator(operator).recoverSaga(SAGA_ID, "why");

    // Assert
    assertThat(AdminAuditPayload.operator(audit.getValue().getPayload())).isEqualTo(operator);
  }

  @Test
  void recoverSaga_operatorWithSurroundingSpacesGiven_isRecordedUntrimmed() {
    // Arrange — a principal with edge whitespace is a different principal, not a formatting
    // artifact, so it is recorded exactly as the server injected it
    String operator = " op-alice ";
    ArgumentCaptor<StatusEvent> audit = arrangeDrivableSaga();

    // Act
    serviceWithOperator(operator).recoverSaga(SAGA_ID, "why");

    // Assert
    assertThat(AdminAuditPayload.operator(audit.getValue().getPayload())).isEqualTo(operator);
  }

  private DefaultSagaAdminService serviceWithOperator(String operator) {
    return new DefaultSagaAdminService(store, engine, registry, () -> operator);
  }

  /** Asserts the mutation aborted before persisting anything or touching the saga. */
  private void verifyNothingWritten() {
    verify(store, never()).recordStatusEvent(any(), anyInt(), any(), any());
    verify(engine, never()).recover(any(), any(), any());
  }

  /** Arranges a RUNNING saga that resolves to a drive, and returns the audit-event captor. */
  private ArgumentCaptor<StatusEvent> arrangeDrivableSaga() {
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null));
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(running));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    return stubDrive(running, events);
  }

  // ---------------------------------------------------------------------------
  // drive deadline — bounds the drive only, never the durable transition
  // ---------------------------------------------------------------------------

  @Test
  void recoverSaga_withDriveDeadline_driveSettlesInTime_returnsDrivenState() {
    // Arrange
    arrangeDrivableSaga();

    // Act
    SagaStateSnapshot result = boundedService(5_000L).recoverSaga(SAGA_ID, "why");

    // Assert — the drive ran to completion, so the caller sees the terminal state it produced
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    verify(engine).recover(any(), any(), any());
  }

  @Test
  void recoverSaga_withDriveDeadline_driveOverruns_returnsStoredStateWithTransitionDurable()
      throws Exception {
    // Arrange — a drive that outlives the deadline. The second getStateSnapshot is the post-expiry
    // re-read: the abandoned drive still owns the ExecutionContext, so its state cannot be read.
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null));
    when(store.getStateSnapshot(SAGA_ID))
        .thenReturn(Optional.of(running), Optional.of(snapshot(SagaStatus.COMPENSATING)));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    stubDrive(running, events);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              release.await();
              return null;
            })
        .when(engine)
        .recover(any(), any(), any());

    try {
      // Act
      SagaStateSnapshot result = boundedService(50L).recoverSaga(SAGA_ID, "why");

      // Assert — a non-terminal state, which is how the caller tells "still running" from "settled"
      assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
      // The transition is durable even though the drive was abandoned — that is what makes leaving
      // the rest to the recovery loop safe, rather than losing the intervention.
      verify(store).recordStatusEvent(eq(running), anyInt(), any(), any());
    } finally {
      release.countDown();
    }
  }

  @Test
  void recoverSaga_withDriveDeadline_lostCas_throwsRatherThanReportingAcceptance() {
    // Arrange — the CAS that co-commits the audit loses to a concurrent admin/recovery
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(snapshot(SagaStatus.RUNNING)));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(List.of(StatusEvent.started(null)));
    when(store.recordStatusEvent(any(), anyInt(), any(), any()))
        .thenThrow(new SagaConcurrentModificationException(SAGA_ID));

    // Act & Assert — the deadline bounds the drive, never the transition. Were the bound wrapped
    // around the whole call instead, this failure could land after a timeout had already reported
    // the intervention as accepted.
    assertThatThrownBy(() -> boundedService(50L).recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaConcurrentModificationException.class);
    verify(engine, never()).recover(any(), any(), any());
  }

  @Test
  void recoverSaga_withDriveDeadline_driveThrows_surfacesTheDrivesOwnException() {
    // Arrange
    arrangeDrivableSaga();
    doThrow(SagaPersistenceException.storeUnavailable(new RuntimeException("io")))
        .when(engine)
        .recover(any(), any(), any());

    // Act & Assert — bounding the drive must not change which exception a caller sees, so the
    // cause is unwrapped from the ExecutionException the executor wraps it in
    assertThatThrownBy(() -> boundedService(5_000L).recoverSaga(SAGA_ID, "why"))
        .isInstanceOf(SagaPersistenceException.class)
        .extracting(e -> ((SagaPersistenceException) e).getErrorCode())
        .isEqualTo(SagaErrorCode.PERSISTENCE_STORE_UNAVAILABLE);
  }

  @Test
  void recoverSaga_withDriveDeadline_executorShutDown_returnsRecordedStateWithoutDriving() {
    // Arrange — the engine executor is shut down (the orchestrator is closing), so the bounded
    // drive cannot be submitted. The transition still commits.
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null));
    SagaStateSnapshot recorded = snapshot(SagaStatus.COMPENSATING);
    ExecutionContext ctx = mock(ExecutionContext.class);
    when(store.getStateSnapshot(SAGA_ID)).thenReturn(Optional.of(running));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    when(engine.replayEvents(running, events)).thenReturn(ctx);
    when(store.recordStatusEvent(eq(running), anyInt(), any(), any())).thenReturn(recorded);
    when(ctx.getCurrentState()).thenReturn(recorded);
    ExecutorService shutDown = Executors.newVirtualThreadPerTaskExecutor();
    shutDown.shutdownNow();
    when(engine.executor()).thenReturn(shutDown);

    // Act — submitting the drive is rejected; the call must degrade, not throw
    SagaStateSnapshot result =
        new DefaultSagaAdminService(store, engine, registry, () -> OPERATOR, 50L)
            .recoverSaga(SAGA_ID, "why");

    // Assert — the recorded (non-terminal) state comes back, the transition is durable, and the
    // drive never ran (it was rejected at submission), so the recovery loop finishes the rest
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    verify(store).recordStatusEvent(eq(running), anyInt(), any(), any());
    verify(engine, never()).recover(any(), any(), any());
  }

  @Test
  void recoverSaga_withDriveDeadline_driveRejectedMidRun_returnsRereadStateWithoutRethrowing() {
    // Arrange — the outer submit is accepted (executor still up), but the drive itself throws
    // RejectedExecutionException from a nested submit as the executor shuts down mid-drive. The
    // wrapped exception must degrade like the submit-time reject, not surface as an INTERNAL.
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    List<SagaEvent> events =
        List.of(
            StatusEvent.started(null),
            StepEvent.completed(0, "debit", null),
            StepEvent.failed(1, "credit", null));
    when(store.getStateSnapshot(SAGA_ID))
        .thenReturn(Optional.of(running), Optional.of(snapshot(SagaStatus.COMPENSATING)));
    when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(backwardDef());
    when(store.getEvents(SAGA_ID)).thenReturn(events);
    stubDrive(running, events);
    doThrow(new RejectedExecutionException("executor shutting down"))
        .when(engine)
        .recover(any(), any(), any());

    // Act — the drive fails mid-run; the call must degrade rather than throw
    SagaStateSnapshot result = boundedService(5_000L).recoverSaga(SAGA_ID, "why");

    // Assert — the post-transition re-read state (COMPENSATING) comes back, the transition is
    // durable, and the recovery loop finishes the rest
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    verify(store).recordStatusEvent(eq(running), anyInt(), any(), any());
  }

  @Test
  void recoverSaga_withoutDriveDeadline_drivesOnTheCallingThread() {
    // Arrange — the embedded default: no deadline, so no executor is involved at all
    arrangeDrivableSaga();

    // Act
    SagaStateSnapshot result = service.recoverSaga(SAGA_ID, "why");

    // Assert — unchanged from before the deadline existed
    assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    verify(engine, never()).executor();
  }

  private DefaultSagaAdminService boundedService(long deadlineMillis) {
    when(engine.executor()).thenReturn(driveExecutor);
    return new DefaultSagaAdminService(store, engine, registry, () -> OPERATOR, deadlineMillis);
  }

  // ---------------------------------------------------------------------------
  // Reads pass-through
  // ---------------------------------------------------------------------------

  @Test
  void listSagas_delegatesToStore() {
    // Arrange
    SagaQuery query = SagaQuery.newBuilder().status(SagaStatus.ESCALATED).build();
    SagaPage<SagaStateSnapshot> page =
        new SagaPage<>(List.of(snapshot(SagaStatus.ESCALATED)), null);
    when(store.listStateSnapshots(query)).thenReturn(page);

    // Act & Assert
    assertThat(service.listSagas(query)).isSameAs(page);
  }
}
