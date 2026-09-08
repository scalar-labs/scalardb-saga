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
import static org.mockito.Mockito.times;
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
import com.scalar.db.saga.exception.SagaPersistenceException;
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
import java.time.ZoneId;
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
  // findRecoverable only ever returns rows older than the staleness threshold, so a fixture
  // standing in for one of its results must be older too. Before the progress predicate existed
  // nothing compared this field and the fixtures carried NOW, which now reads as "just updated".
  private static final Instant STALE = NOW.minusSeconds(3600);
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
  private MutableClock clock;

  /** Starts at {@link #NOW} and only moves when a test advances it. */
  private static final class MutableClock extends Clock {
    private Instant instant;

    MutableClock(Instant start) {
      this.instant = start;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }

    void advance(Duration amount) {
      instant = instant.plus(amount);
    }
  }

  @BeforeEach
  void setUp() {
    clock = new MutableClock(NOW);
    config = new RecoveryConfig(60_000, 30, GRACE_PERIOD, 1000, 10, clock);
    manager = new SagaRecoveryManager(store, engine, registry, OWNER_ID, config, scheduler);
    // Both sweeps start from the owner's scattered cursor; without this stub the mock returns
    // null and the sweeps end before scanning anything.
    lenient().when(store.initialSweepCursor(OWNER_ID)).thenReturn(mock(ScanCursor.class));
    // Default: no overdue parked sagas — the recover() staleness-scan tests don't exercise pass 2.
    lenient()
        .when(store.findOverdueParkedSagas(any(), any()))
        .thenReturn(new OverdueParked(List.of(), null));
    // Default: the progress probe reports an event older than the staleness window, so a saga that
    // looks stale by its state row is genuinely abandoned and stays claimable. Tests about the
    // guards override this; every other test wants the pre-guard behaviour. An unstubbed mock
    // would return Optional.empty(), which now means "no events at all" — a damaged saga the
    // sweeper deliberately refuses to touch.
    lenient()
        .when(store.getNewestEvent(any()))
        .thenReturn(Optional.of(progressAt(NOW.minusSeconds(3600))));
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    return new SagaStateSnapshot(
        SAGA_ID, SAGA_NAME, status, OWNER_ID, DEF_VERSION, NOW.minusSeconds(300), STALE);
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

  /** The captured WARN lines naming this saga — the hung-drive signal is the only one here. */
  private static List<String> warningsNaming(ListAppender<ILoggingEvent> logs, String sagaId) {
    return logs.list.stream()
        .filter(event -> event.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(message -> message.contains(sagaId))
        .toList();
  }

  /**
   * The stale sweep's counters from the pass summary, so a test can name the outcome it expects
   * rather than settle for what the store was not asked to do.
   */
  private static String staleCounters(ListAppender<ILoggingEvent> logs) {
    String summary =
        logs.list.stream()
            .map(ILoggingEvent::getFormattedMessage)
            .filter(message -> message.startsWith("Recovery pass:"))
            .reduce((first, second) -> second)
            .orElseThrow(() -> new AssertionError("no recovery pass summary was logged"));
    return summary.substring(summary.indexOf("stale[") + "stale[".length(), summary.indexOf(']'));
  }

  /** A newest-event stub standing for ordinary progress: a step that completed at this instant. */
  private static SagaStore.NewestEvent progressAt(Instant createdAt) {
    return new SagaStore.NewestEvent(EventType.STEP_COMPLETED, createdAt);
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
              STALE);
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
      // The guards ask the engine whether this instance is driving the saga, so "untouched" now
      // means never asked to replay or recover — not zero interactions.
      verify(engine, never()).replayEvents(any(), any());
      verify(engine, never()).recover(any(), any(), any());
    }

    @Test
    void recover_sweepBudgetReached_stopsEarly() {
      // Arrange — sweep budget of 2
      RecoveryConfig smallBudget =
          new RecoveryConfig(60_000, 30, GRACE_PERIOD, 2, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      SagaRecoveryManager smallManager =
          new SagaRecoveryManager(store, engine, registry, OWNER_ID, smallBudget, scheduler);

      SagaStateSnapshot saga1 = snapshot(SagaStatus.RUNNING);
      SagaStateSnapshot saga2 =
          new SagaStateSnapshot(
              "saga-002",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              STALE);
      SagaStateSnapshot saga3 =
          new SagaStateSnapshot(
              "saga-003",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              STALE);
      ScanCursor cursor = mock(ScanCursor.class);
      SagaDefinition def = definition();
      ExecutionContext ctx1 = mock(ExecutionContext.class);
      ExecutionContext ctx2 = mock(ExecutionContext.class);

      // Page 1 has 2 sagas (hits the sweep budget), page 2 has 1 more
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

      // Assert — processed 2, hit the sweep budget, did not scan page 2
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx1));
      verify(engine).recover(eq(new RecoveryAction.Resume(0)), eq(def), eq(ctx2));
      verify(store, never()).claimForRecovery(eq(saga3), any());
      // findRecoverable called only once — the sweep budget stopped it before the second page
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
              STALE);
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
              STALE);
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
      // rather than surfacing in awaitOutcomes as a context-free ExecutionException.
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
  // recover() — claim guards (progress-based staleness)
  // =========================================================================

  /**
   * A stale {@code updated_at} does not mean abandoned: step events never touch the state row, so a
   * saga in a long step is indistinguishable from a dead one by that field alone. These pin the two
   * guards that tell them apart, and the cases where each must yield.
   */
  @Nested
  class ClaimGuards {

    private SagaStateSnapshot staleSaga() {
      return snapshot(SagaStatus.RUNNING);
    }

    /** Only the scan: a skipped saga never reaches the claim, so stubbing it would be unused. */
    private void scanReturns(SagaStateSnapshot saga) {
      when(store.findRecoverable(any(), any())).thenReturn(new Recoverables(List.of(saga), null));
    }

    private SagaStateSnapshot handoffSaga() {
      return new SagaStateSnapshot(
          SAGA_ID,
          SAGA_NAME,
          SagaStatus.RUNNING,
          OWNER_ID,
          DEF_VERSION,
          NOW.minusSeconds(300),
          Instant.EPOCH);
    }

    @Test
    void recover_sagaDrivenByThisInstance_isNotClaimed() {
      // Arrange — the drive is alive here; its state row is stale only because steps do not touch
      // it. Claiming would rewrite the clustering key and kill the very drive that is progressing.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(engine.isLocallyActive(SAGA_ID)).thenReturn(true);

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_locallyActiveHandoffRow_isNotClaimed() {
      // Arrange — an EPOCH row whose drive is still running locally. shutdown() leaves exactly
      // this when its drain times out, marking every saga still in the active set; an operator
      // reset can too. The local check must win over the hand-off carve-out, or the claim would
      // kill that live drive.
      SagaStateSnapshot saga = handoffSaga();
      scanReturns(saga);
      when(engine.isLocallyActive(SAGA_ID)).thenReturn(true);

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_recentEventFromAnotherInstance_isNotClaimed() {
      // Arrange — nothing running here, but the event stream shows a step outcome inside the
      // window: someone is driving it.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID)).thenReturn(Optional.of(progressAt(NOW.minusSeconds(5))));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_recentStatusEvent_isNotClaimed() {
      // Arrange — only a give-up marker is excluded; every other type counts as progress, including
      // a status event. A saga that has only just transitioned has no step outcome yet, so
      // narrowing the check to step outcomes would falsely claim it — as it would a saga still in
      // its first step.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(
              Optional.of(
                  new SagaStore.NewestEvent(EventType.SAGA_COMPENSATING, NOW.minusSeconds(1))));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_eventArrivesWhileWaitingForAPermit_isNotClaimed() {
      // Screening happens before a recovery permit is taken, and that wait is unbounded — permits
      // are held for whole drives. If the decision rested on the screening read, another replica
      // could finish a step in the gap and be claimed out from under: the claim matches the scanned
      // row's clustering key, which step events never touch, so nothing else fences it. The reading
      // taken under the permit is the one that decides.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(Optional.of(progressAt(NOW.minusSeconds(600)))) // screening: looks abandoned
          .thenReturn(Optional.of(progressAt(NOW.minusSeconds(1)))); // under the permit: alive

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_driveStartsWhileWaitingForAPermit_isNotClaimed() {
      // The mirror of the event-probe race above, for the other guard. Screening for a local drive
      // also happens before a permit is taken, so a drive can start on this instance during the
      // wait; the claim would then rewrite the token of a drive that is one step old. The screen
      // says no drive, the re-check under the permit says there is one, and the re-check decides.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(engine.isLocallyActive(SAGA_ID)).thenReturn(false, true);
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(Optional.of(progressAt(NOW.minusSeconds(600))));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_newestEventIsACompensationGiveUp_isClaimedDespiteBeingRecent() {
      // A compensation failure is written by a drive that then stops and hands the saga back to
      // recovery. Reading it as liveness would make the give-up postpone the very retry it asks
      // for, so the saga would sit idle for a whole timeout with nobody driving it.
      SagaStateSnapshot saga = staleSaga();
      setupSinglePageRecovery(saga);
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(
              Optional.of(
                  new SagaStore.NewestEvent(
                      EventType.STEP_COMPENSATION_FAILED, NOW.minusSeconds(1))));

      // Act
      manager.recover();

      // Assert — claimed straight away, even though the event is one second old
      verify(store).claimForRecovery(saga, OWNER_ID);
    }

    @Test
    void recover_lastEventOlderThanTheWindow_isClaimed() {
      // Arrange — no local drive and no recent event: genuinely abandoned.
      SagaStateSnapshot saga = staleSaga();
      setupSinglePageRecovery(saga);
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(Optional.of(progressAt(NOW.minusSeconds(600))));

      // Act
      manager.recover();

      // Assert
      verify(store).claimForRecovery(saga, OWNER_ID);
    }

    @Test
    void recover_handoffRowWithRecentEvents_isClaimedImmediately() {
      // Arrange — an operator or a dying drive stamped EPOCH to hand the saga over. Honouring the
      // probe here would delay a deliberate hand-off by a whole timeout.
      SagaStateSnapshot saga = handoffSaga();
      setupSinglePageRecovery(saga);
      when(store.getNewestEvent(SAGA_ID)).thenReturn(Optional.of(progressAt(NOW.minusSeconds(1))));

      // Act
      manager.recover();

      // Assert
      verify(store).claimForRecovery(saga, OWNER_ID);
    }

    @Test
    void recover_stateRowWithNoEvents_isNotClaimedAndReportsAnError() {
      // Arrange — createSaga writes SAGA_STARTED in the same transaction as the state row, so this
      // cannot come from the engine. Claiming would replay an empty history and restart the saga
      // from step 0 with no input, since SAGA_STARTED is what carries it.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID)).thenReturn(Optional.empty());
      ListAppender<ILoggingEvent> logs = attachLogCapture();

      try {
        // Act
        manager.recover();

        // Assert — the outcome is ERROR, not SKIPPED. Both leave the claim unmade, so the counter
        // is what separates them, and the difference matters: an error spends sweep budget, which
        // is how a pass winds down against a damaged store, while a skip is free.
        verify(store, never()).claimForRecovery(any(), any());
        assertThat(staleCounters(logs)).contains("errors=1");
      } finally {
        recoveryLogger().detachAppender(logs);
      }
    }

    @Test
    void recover_handoffRowWithNoEvents_isNotClaimedEitherAndReportsAnError() {
      // Arrange — the carve-out skips the progress comparison but not the emptiness check: a
      // hand-off is one of the likelier ways a damaged row reaches the sweeper, and no caller can
      // vouch for a history that is not there.
      SagaStateSnapshot saga = handoffSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID)).thenReturn(Optional.empty());
      ListAppender<ILoggingEvent> logs = attachLogCapture();

      try {
        // Act
        manager.recover();

        // Assert — an error, as for any other damaged row; the carve-out changes nothing here.
        verify(store, never()).claimForRecovery(any(), any());
        assertThat(staleCounters(logs)).contains("errors=1");
      } finally {
        recoveryLogger().detachAppender(logs);
      }
    }

    @Test
    void recover_probeFails_leavesTheSagaUntouched() {
      // Arrange — a failed read is not evidence that the saga stopped progressing. Claiming on it
      // would rewrite the token of a drive that may be alive, and the drive that follows a claim
      // reads the same events table, so it would fail too: a live saga killed and nothing
      // recovered.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID))
          .thenThrow(SagaPersistenceException.storeUnavailable(new RuntimeException("boom")));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_eventStampInTheFuture_isNotClaimed() {
      // Arrange — a writer whose clock runs ahead of ours. The stamp is "recent" by any comparison,
      // so the saga is left alone rather than claimed out from under whoever is driving it.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(Optional.of(progressAt(NOW.plusSeconds(3600))));

      // Act
      manager.recover();

      // Assert
      verify(store, never()).claimForRecovery(any(), any());
    }

    @Test
    void recover_locallyActivePastTenTimeouts_namesTheSagaOnceInAWarning() {
      // Arrange — a drive that never releases its saga is skipped silently on every pass, so
      // without this warning a wedged drive is invisible. How long it has been driving comes from
      // the engine, which recorded it when the drive registered.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(engine.isLocallyActive(SAGA_ID)).thenReturn(true);
      when(engine.activeSince(SAGA_ID)).thenReturn(Optional.of(NOW));
      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        // Act — a drive that started just now is not yet suspicious.
        manager.recover();
        assertThat(warningsNaming(logs, SAGA_ID)).isEmpty();

        // Act — the same drive, still holding the saga past ten timeouts
        clock.advance(Duration.ofMinutes(11));
        manager.recover();

        // Assert — named once
        assertThat(warningsNaming(logs, SAGA_ID)).hasSize(1);

        // Act — and not again every pass afterwards, which would drown the signal
        clock.advance(Duration.ofMinutes(11));
        manager.recover();

        // Assert
        assertThat(warningsNaming(logs, SAGA_ID)).hasSize(1);
      } finally {
        recoveryLogger().detachAppender(logs);
      }
    }

    @Test
    void recover_sagaFinishesAndRunsLongAgain_warnsAfresh() {
      // Arrange — a saga that completes and later runs long again must be reported again rather
      // than being silenced forever by the first warning. The engine reports a fresh start time for
      // the second episode, so the elapsed time never folds in the first one or the gap between.
      SagaStateSnapshot saga = staleSaga();
      scanReturns(saga);
      when(engine.isLocallyActive(SAGA_ID)).thenReturn(true);
      when(engine.activeSince(SAGA_ID)).thenReturn(Optional.of(NOW));
      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        manager.recover();
        clock.advance(Duration.ofMinutes(11));
        manager.recover();
        assertThat(warningsNaming(logs, SAGA_ID)).hasSize(1);

        // Act — the drive ends, so the next pass forgets the saga
        when(engine.isLocallyActive(SAGA_ID)).thenReturn(false);
        when(engine.activeSince(SAGA_ID)).thenReturn(Optional.empty());
        lenient()
            .when(store.getNewestEvent(SAGA_ID))
            .thenReturn(Optional.of(progressAt(NOW.minusSeconds(3600))));
        lenient().when(store.claimForRecovery(any(), any())).thenReturn(Optional.empty());
        manager.recover();

        // Act — a second, separate episode starts now and runs long in its turn
        Instant secondEpisodeStart = clock.instant();
        when(engine.isLocallyActive(SAGA_ID)).thenReturn(true);
        when(engine.activeSince(SAGA_ID)).thenReturn(Optional.of(secondEpisodeStart));
        manager.recover();
        clock.advance(Duration.ofMinutes(11));
        manager.recover();

        // Assert — warned again, because the first stint was forgotten when the drive ended
        assertThat(warningsNaming(logs, SAGA_ID)).hasSize(2);
      } finally {
        recoveryLogger().detachAppender(logs);
      }
    }

    @Test
    void recover_skippedSagas_consumeNoBudgetAndDoNotStopTheScan() {
      // Arrange — a budget of 1 and two pages, the first holding a saga that is skipped. Charging
      // skips would let live sagas exhaust the budget and starve recovery of the dead ones.
      config =
          new RecoveryConfig(60_000, 30, GRACE_PERIOD, 1, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      manager = new SagaRecoveryManager(store, engine, registry, OWNER_ID, config, scheduler);
      lenient()
          .when(store.findOverdueParkedSagas(any(), any()))
          .thenReturn(new OverdueParked(List.of(), null));

      SagaStateSnapshot skipped = snapshot(SagaStatus.RUNNING);
      SagaStateSnapshot claimable =
          new SagaStateSnapshot(
              "saga-002",
              SAGA_NAME,
              SagaStatus.RUNNING,
              OWNER_ID,
              DEF_VERSION,
              NOW.minusSeconds(300),
              STALE);
      // The sweep starts from the owner's scattered cursor, not null, so page one is keyed on it.
      ScanCursor first = mock(ScanCursor.class);
      ScanCursor next = mock(ScanCursor.class);
      when(store.initialSweepCursor(OWNER_ID)).thenReturn(first);
      when(store.findRecoverable(any(), eq(first)))
          .thenReturn(new Recoverables(List.of(skipped), next));
      when(store.findRecoverable(any(), eq(next)))
          .thenReturn(new Recoverables(List.of(claimable), null));
      when(engine.isLocallyActive(SAGA_ID)).thenReturn(true);
      lenient()
          .when(store.getNewestEvent(any()))
          .thenReturn(Optional.of(progressAt(NOW.minusSeconds(600))));
      when(store.claimForRecovery(claimable, OWNER_ID)).thenReturn(Optional.empty());

      // Act
      manager.recover();

      // Assert — the skip did not spend the single unit of budget, so the scan reached page two
      verify(store).claimForRecovery(claimable, OWNER_ID);
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

      // Assert — the Error was logged with saga context rather than surfacing in awaitOutcomes as
      // a context-free ExecutionException.
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
    void recover_stuckPastGraceWithStaleEvents_isStillClaimedAndEscalates() {
      // Escalation runs only after a committed claim, so both new guards now sit in front of it.
      // The other escalation tests reach it through the shared lenient stub, which states no
      // intent; this one stubs the probe explicitly so the guard interaction is what is pinned.
      SagaStateSnapshot saga = snapshot(SagaStatus.COMPENSATING);
      ExecutionContext ctx = mock(ExecutionContext.class);
      SagaStateSnapshot escalated = snapshot(SagaStatus.ESCALATED);
      List<SagaEvent> events =
          List.of(
              StepEvent.completed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)),
              StepEvent.compensationFailed(0, "debit", null).withTimestamp(NOW.minusSeconds(7200)));

      setupSinglePageRecovery(saga);
      // Nothing has written an event for two hours: the saga is genuinely abandoned, not slow.
      when(store.getNewestEvent(SAGA_ID))
          .thenReturn(Optional.of(progressAt(NOW.minusSeconds(7200))));
      when(store.getEvents(SAGA_ID)).thenReturn(events);
      when(engine.replayEvents(saga, events)).thenReturn(ctx);
      when(ctx.getCurrentState()).thenReturn(saga);
      when(ctx.nextSequence()).thenReturn(2);
      when(registry.resolve(SAGA_NAME, DEF_VERSION)).thenReturn(definition());
      when(store.recordStatusEvent(eq(saga), eq(2), any(StatusEvent.class), any()))
          .thenReturn(escalated);

      // Act
      manager.recover();

      // Assert — the guards let it through and it still escalates
      verify(store).claimForRecovery(saga, OWNER_ID);
      ArgumentCaptor<StatusEvent> captor = ArgumentCaptor.forClass(StatusEvent.class);
      verify(store).recordStatusEvent(eq(saga), eq(2), captor.capture(), any());
      assertThat(captor.getValue().getTargetStatus()).isEqualTo(SagaStatus.ESCALATED);
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
    void start_recoveryPassThrowsError_scheduledTaskContainsIt() {
      // Arrange — capture the periodic task and make the pass blow up with an Error. Only a catch
      // on Throwable contains it; a Throwable escaping a scheduleWithFixedDelay task cancels all
      // its future executions, silently stopping recovery for the rest of the process.
      when(store.findRecoverable(any(), any())).thenThrow(new Error("scan blew up"));
      manager.start();
      long offset = SweepScatter.offsetSeconds(OWNER_ID, "recovery", 30);
      ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
      verify(scheduler)
          .scheduleWithFixedDelay(task.capture(), eq(30L + offset), eq(30L), eq(TimeUnit.SECONDS));

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

  // =========================================================================
  // start() — budget vs page-size floor
  // =========================================================================

  /**
   * The budget floor is documented for operators in three files but neither config can check it
   * alone: only the store knows its page size, only the engine knows the budget. So the check lives
   * at startup, where both are in hand. It warns rather than rejects, because every value below the
   * floor was legal before the check existed and failing startup on one would turn an upgrade into
   * an outage.
   *
   * <p>These assert the message text, not just that something was logged: the operator
   * documentation tells people to grep their logs for "throttled", so the word is part of the
   * contract rather than an implementation detail.
   */
  @Nested
  class StartupBudgetWarning {

    @Test
    void start_budgetBelowOnePage_warnsTrailingStatusIsThrottled() {
      // Arrange
      when(store.recoveryPageSize()).thenReturn(200);
      ListAppender<ILoggingEvent> appender = attachLogCapture();

      try {
        // Act
        managerWithBudget(150).start();

        // Assert — the floor it names is the page size, the value the check actually accepts, and
        // the remedy carries both the field an embedded caller sets and the daemon operator's key.
        assertThat(appender.list)
            .filteredOn(e -> e.getLevel() == Level.WARN)
            .extracting(ILoggingEvent::getFormattedMessage)
            .anySatisfy(
                m ->
                    assertThat(m)
                        .contains("throttled")
                        .contains("Raise RecoveryConfig.maxRecoveriesPerSweep to at least 200")
                        .contains("scalar.db.saga.server.recovery.max_recoveries_per_sweep"));
      } finally {
        recoveryLogger().detachAppender(appender);
      }
    }

    /**
     * Suppression is {@code budget >= pageSize}, so a budget of exactly one page is enough and must
     * stay silent. This is the boundary the message's remedy is worded against: it advises raising
     * the budget <em>to</em> the page size, which only helps if that value is accepted.
     */
    @Test
    void start_budgetExactlyOnePage_warnsNothing() {
      // Arrange
      when(store.recoveryPageSize()).thenReturn(200);
      ListAppender<ILoggingEvent> appender = attachLogCapture();

      try {
        // Act
        managerWithBudget(200).start();

        // Assert
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.WARN).isEmpty();
      } finally {
        recoveryLogger().detachAppender(appender);
      }
    }

    @Test
    void start_budgetAboveOnePage_warnsNothing() {
      // Arrange
      when(store.recoveryPageSize()).thenReturn(200);
      ListAppender<ILoggingEvent> appender = attachLogCapture();

      try {
        // Act
        managerWithBudget(250).start();

        // Assert
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.WARN).isEmpty();
      } finally {
        recoveryLogger().detachAppender(appender);
      }
    }

    /**
     * A store that does not page recovery scans reports 0, and must not be warned about. This pins
     * the outcome, not the branch that produces it: the {@code pageSize <= 0} term reads as what
     * guards this, but since a budget is validated positive at construction, {@code budget >=
     * pageSize} already returns for any non-positive page size. Deleting the term changes nothing
     * this test can see. It is kept as a statement of intent, not because it is reachable.
     */
    @Test
    void start_storeDoesNotPage_warnsNothing() {
      // Arrange — a budget of 1 would trip the comparison against any real page size
      when(store.recoveryPageSize()).thenReturn(0);
      ListAppender<ILoggingEvent> appender = attachLogCapture();

      try {
        // Act
        managerWithBudget(1).start();

        // Assert
        assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.WARN).isEmpty();
      } finally {
        recoveryLogger().detachAppender(appender);
      }
    }

    private SagaRecoveryManager managerWithBudget(int budget) {
      RecoveryConfig config =
          new RecoveryConfig(
              60_000, 30, GRACE_PERIOD, budget, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      return new SagaRecoveryManager(store, engine, registry, OWNER_ID, config, scheduler);
    }
  }

  // =========================================================================
  // recover() — success-counted budget, page isolation, cross-pass resume
  // =========================================================================

  @Nested
  class ScatteredSweepBudget {

    /**
     * A page holds every recoverable status one after another, RUNNING before COMPENSATING, and the
     * sweep submits at most its remaining budget before advancing the bucket. A budget no larger
     * than one status scan therefore never reaches COMPENSATING — the failure the documented budget
     * floor exists to prevent, and the one nothing verified before this test.
     */
    @Test
    void recover_budgetCoversOnlyTheLeadingStatus_neverReachesCompensating() {
      // Arrange — a page of 2 RUNNING then 2 COMPENSATING, against a budget of 2
      SagaRecoveryManager manager = managerWithMaxRecoveriesPerSweep(2);
      SagaStateSnapshot running1 = snapshotWithId("run-1", SagaStatus.RUNNING);
      SagaStateSnapshot running2 = snapshotWithId("run-2", SagaStatus.RUNNING);
      SagaStateSnapshot comp1 = snapshotWithId("comp-1", SagaStatus.COMPENSATING);
      SagaStateSnapshot comp2 = snapshotWithId("comp-2", SagaStatus.COMPENSATING);
      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(running1, running2, comp1, comp2), null));
      when(store.claimForRecovery(any(), eq(OWNER_ID))).thenReturn(Optional.empty());

      // Act
      manager.recover();

      // Assert — the budget is spent on the leading status; the trailing one is never claimed
      verify(store).claimForRecovery(eq(running1), eq(OWNER_ID));
      verify(store).claimForRecovery(eq(running2), eq(OWNER_ID));
      verify(store, never()).claimForRecovery(eq(comp1), any());
      verify(store, never()).claimForRecovery(eq(comp2), any());
    }

    /** With a budget covering the whole page, the trailing status is reached. */
    @Test
    void recover_budgetCoversTheWholePage_reachesCompensating() {
      // Arrange — same page, budget of 4
      SagaRecoveryManager manager = managerWithMaxRecoveriesPerSweep(4);
      SagaStateSnapshot running1 = snapshotWithId("run-1", SagaStatus.RUNNING);
      SagaStateSnapshot running2 = snapshotWithId("run-2", SagaStatus.RUNNING);
      SagaStateSnapshot comp1 = snapshotWithId("comp-1", SagaStatus.COMPENSATING);
      SagaStateSnapshot comp2 = snapshotWithId("comp-2", SagaStatus.COMPENSATING);
      when(store.findRecoverable(any(), any()))
          .thenReturn(new Recoverables(List.of(running1, running2, comp1, comp2), null));
      when(store.claimForRecovery(any(), eq(OWNER_ID))).thenReturn(Optional.empty());

      // Act
      manager.recover();

      // Assert
      verify(store).claimForRecovery(eq(comp1), eq(OWNER_ID));
      verify(store).claimForRecovery(eq(comp2), eq(OWNER_ID));
    }

    private SagaStateSnapshot snapshotWithId(String sagaId, SagaStatus status) {
      return new SagaStateSnapshot(
          sagaId, SAGA_NAME, status, OWNER_ID, DEF_VERSION, NOW.minusSeconds(300), NOW);
    }

    /**
     * A manager whose per-sweep budget is {@code maxRecoveriesPerSweep}, on the standard fixed
     * clock.
     */
    private SagaRecoveryManager managerWithMaxRecoveriesPerSweep(int maxRecoveriesPerSweep) {
      RecoveryConfig config =
          new RecoveryConfig(
              60_000,
              30,
              GRACE_PERIOD,
              maxRecoveriesPerSweep,
              10,
              Clock.fixed(NOW, ZoneOffset.UTC));
      return new SagaRecoveryManager(store, engine, registry, OWNER_ID, config, scheduler);
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
      // Arrange — sweep budget 1: the lost race on page 1 must not spend the budget, so the sweep
      // reaches page 2 and recovers its saga. The attempts-counted budget stopped after page 1.
      SagaRecoveryManager smallManager = managerWithMaxRecoveriesPerSweep(1);

      SagaStateSnapshot lost = snapshotWithId("saga-lost", SagaStatus.RUNNING);
      SagaStateSnapshot won = snapshotWithId("saga-won", SagaStatus.RUNNING);
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
      // Arrange — sweep budget 1: the claim committed, so the failed drive still spends the budget
      // and the sweep must NOT continue to the next page (that would be a claim spree).
      SagaRecoveryManager smallManager = managerWithMaxRecoveriesPerSweep(1);

      SagaStateSnapshot saga = snapshotWithId("saga-drive-fails", SagaStatus.RUNNING);
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
      // Arrange — sweep budget 1: pass 1 stops on budget holding the next cursor; pass 2 must
      // resume there instead of restarting at the permutation's first bucket.
      SagaRecoveryManager smallManager = managerWithMaxRecoveriesPerSweep(1);

      ScanCursor initial = mock(ScanCursor.class);
      ScanCursor next = mock(ScanCursor.class);
      when(store.initialSweepCursor(OWNER_ID)).thenReturn(initial);
      SagaStateSnapshot saga = snapshotWithId("saga-budget", SagaStatus.RUNNING);
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
      // Arrange — sweep budget 1: a claim that throws may have committed without confirmation, so
      // it must spend the budget; the sweep must NOT continue claiming across the ring.
      SagaRecoveryManager smallManager = managerWithMaxRecoveriesPerSweep(1);

      SagaStateSnapshot saga = snapshotWithId("saga-claim-throws", SagaStatus.RUNNING);
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
      SagaStateSnapshot slow = snapshotWithId("saga-slow", SagaStatus.RUNNING);
      SagaStateSnapshot fast = snapshotWithId("saga-fast", SagaStatus.RUNNING);
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
      SagaStateSnapshot saga = snapshotWithId("saga-cancelled-with-pass", SagaStatus.RUNNING);
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
      SagaStateSnapshot saga = snapshotWithId("saga-holding-the-lock", SagaStatus.RUNNING);
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
      // Arrange — sweep budget 1: the parked saga on page 1 was already resolved by a callback
      // (lost race), so the parked sweep must continue to page 2.
      SagaRecoveryManager smallManager = managerWithMaxRecoveriesPerSweep(1);

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
