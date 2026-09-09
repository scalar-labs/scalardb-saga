package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SweepScatter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
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
class SagaRetentionManagerTest {

  private static final Instant NOW = Instant.parse("2025-01-08T12:00:00Z");
  private static final Duration RETENTION_PERIOD = Duration.ofDays(7);
  private static final Instant THRESHOLD = NOW.minus(RETENTION_PERIOD);
  private static final String OWNER_ID = "retention-owner";

  @Mock private SagaStore store;
  @Mock private ScheduledExecutorService scheduler;

  private RetentionConfig config;
  private SagaRetentionManager manager;

  @BeforeEach
  void setUp() {
    config = new RetentionConfig(RETENTION_PERIOD, 3600, 100, 10, Clock.fixed(NOW, ZoneOffset.UTC));
    manager = new SagaRetentionManager(store, OWNER_ID, config, scheduler);
  }

  private static SagaStateSnapshot snapshot(String sagaId, SagaStatus status) {
    return new SagaStateSnapshot(
        sagaId,
        "test-saga",
        status,
        "owner-1",
        "1.0",
        Instant.parse("2024-12-25T00:00:00Z"),
        Instant.parse("2024-12-25T00:00:00Z"));
  }

  // Captures the manager's log output so tests can assert an Error was logged, not just contained.
  // Callers must detach the appender in a finally: retentionLogger().detachAppender(appender).
  private static ListAppender<ILoggingEvent> attachLogCapture() {
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    retentionLogger().addAppender(appender);
    return appender;
  }

  private static Logger retentionLogger() {
    return (Logger) LoggerFactory.getLogger(SagaRetentionManager.class);
  }

  // =========================================================================
  // cleanup()
  // =========================================================================

  @Nested
  class Cleanup {

    @Test
    void cleanup_noExpiredSagas_deletesNothing() {
      // Arrange
      when(store.findByStatusOlderThan(any(), any(), anyInt(), any(), anyInt()))
          .thenReturn(List.of());

      // Act
      manager.cleanup();

      // Assert
      verify(store, never()).deleteSaga(any());
    }

    @Test
    void cleanup_expiredCompletedSagas_purgesThem() {
      // Arrange
      SagaStateSnapshot saga1 = snapshot("saga-001", SagaStatus.COMPLETED);
      SagaStateSnapshot saga2 = snapshot("saga-002", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(saga1, saga2));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(98), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.deleteSaga(any())).thenReturn(true);

      // Act
      manager.cleanup();

      // Assert
      verify(store).deleteSaga("saga-001");
      verify(store).deleteSaga("saga-002");
    }

    @Test
    void cleanup_expiredCompensatedSagas_purgesThem() {
      // Arrange
      SagaStateSnapshot saga = snapshot("saga-003", SagaStatus.COMPENSATED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(saga));

      // Act
      manager.cleanup();

      // Assert
      verify(store).deleteSaga("saga-003");
    }

    @Test
    void cleanup_mixOfCompletedAndCompensated_purgesBoth() {
      // Arrange
      SagaStateSnapshot completed = snapshot("saga-c1", SagaStatus.COMPLETED);
      SagaStateSnapshot compensated = snapshot("saga-c2", SagaStatus.COMPENSATED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(completed));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(compensated));
      when(store.deleteSaga("saga-c1")).thenReturn(true);
      when(store.deleteSaga("saga-c2")).thenReturn(true);

      // Act
      manager.cleanup();

      // Assert
      verify(store).deleteSaga("saga-c1");
      verify(store).deleteSaga("saga-c2");
    }

    @Test
    void cleanup_passBudgetReached_stopsEarly() {
      // Arrange
      RetentionConfig smallBudget =
          new RetentionConfig(RETENTION_PERIOD, 3600, 2, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      SagaRetentionManager smallManager =
          new SagaRetentionManager(store, OWNER_ID, smallBudget, scheduler);

      SagaStateSnapshot saga1 = snapshot("saga-001", SagaStatus.COMPLETED);
      SagaStateSnapshot saga2 = snapshot("saga-002", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(2), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(saga1, saga2));
      when(store.deleteSaga(any())).thenReturn(true);

      // Act
      smallManager.cleanup();

      // Assert — purged 2, hit the pass budget, did not scan COMPENSATED
      verify(store).deleteSaga("saga-001");
      verify(store).deleteSaga("saga-002");
      verify(store, never())
          .findByStatusOlderThan(eq(SagaStatus.COMPENSATED), any(), anyInt(), any(), anyInt());
    }

    @Test
    void cleanup_deleteFailsForOneSaga_continuesWithOthers() {
      // Arrange
      SagaStateSnapshot saga1 = snapshot("saga-fail", SagaStatus.COMPLETED);
      SagaStateSnapshot saga2 = snapshot("saga-ok", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(saga1, saga2));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      doThrow(new RuntimeException("delete failed")).when(store).deleteSaga("saga-fail");
      when(store.deleteSaga("saga-ok")).thenReturn(true);

      // Act
      manager.cleanup();

      // Assert — saga-ok is still deleted despite saga-fail failure
      verify(store).deleteSaga("saga-fail");
      verify(store).deleteSaga("saga-ok");
    }

    @Test
    void cleanup_deleteThrowsErrorForOneSaga_containedAndContinues() {
      // Arrange — same shape as the RuntimeException case, but with an Error: purgeOneSafely's
      // catch spans Throwable, so the pass neither throws nor counts the failed purge.
      SagaStateSnapshot saga1 = snapshot("saga-fail", SagaStatus.COMPLETED);
      SagaStateSnapshot saga2 = snapshot("saga-ok", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(saga1, saga2));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      doThrow(new Error("delete blew up")).when(store).deleteSaga("saga-fail");
      when(store.deleteSaga("saga-ok")).thenReturn(true);

      // Act
      ListAppender<ILoggingEvent> logs = attachLogCapture();
      try {
        assertThatCode(() -> manager.cleanup()).doesNotThrowAnyException();
      } finally {
        retentionLogger().detachAppender(logs);
      }

      // Assert — the COMPENSATED budget of 99 pins that the errored purge refunded its budget
      // (only saga-ok's success consumed any), and the Error was logged with saga context instead
      // of surfacing at the await as a context-free ExecutionException.
      verify(store).deleteSaga("saga-ok");
      verify(store, atLeastOnce())
          .findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt());
      assertThat(logs.list)
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("saga-fail");
                assertThat(event.getThrowableProxy()).isNotNull();
                assertThat(event.getThrowableProxy().getClassName())
                    .isEqualTo(Error.class.getName());
                assertThat(event.getThrowableProxy().getMessage()).isEqualTo("delete blew up");
              });
    }

    @Test
    void cleanup_alreadyPurgedSaga_doesNotConsumeBudget() {
      // Arrange — one delete is a no-op (another replica already purged it): only the real purge
      // counts, so the COMPENSATED scan still gets a remaining budget of 99.
      SagaStateSnapshot real = snapshot("saga-real", SagaStatus.COMPLETED);
      SagaStateSnapshot alreadyPurged = snapshot("saga-noop", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(real, alreadyPurged));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.deleteSaga("saga-real")).thenReturn(true);
      when(store.deleteSaga("saga-noop")).thenReturn(false);

      // Act
      manager.cleanup();

      // Assert — the refund keeps the remaining budget at 99, in the first round and again in the
      // second round the pass runs before concluding no further progress is possible
      verify(store, atLeastOnce())
          .findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt());
      verify(store, never())
          .findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(98), eq(OWNER_ID), anyInt());
    }

    @Test
    void cleanup_successivePasses_rotateTheSweepStart() {
      // Arrange — the rotation handed to the store increments once per pass, so successive
      // passes start the bucket sweep at successive positions and a sustained backlog cannot
      // starve the same tail buckets forever
      when(store.findByStatusOlderThan(any(), any(), anyInt(), any(), anyInt()))
          .thenReturn(List.of());

      // Act
      manager.cleanup();
      manager.cleanup();

      // Assert
      verify(store)
          .findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), eq(0));
      verify(store)
          .findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), eq(1));
    }

    @Test
    void cleanup_escalatedSagasNotQueried() {
      // Arrange
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());

      // Act
      manager.cleanup();

      // Assert — ESCALATED is never queried
      verify(store, never())
          .findByStatusOlderThan(eq(SagaStatus.ESCALATED), any(), anyInt(), any(), anyInt());
      verify(store, never())
          .findByStatusOlderThan(eq(SagaStatus.RUNNING), any(), anyInt(), any(), anyInt());
      verify(store, never())
          .findByStatusOlderThan(eq(SagaStatus.COMPENSATING), any(), anyInt(), any(), anyInt());
    }

    @Test
    void cleanup_remainingBudgetPassedCorrectlyToSecondStatus() {
      // Arrange
      SagaStateSnapshot completed1 = snapshot("saga-c1", SagaStatus.COMPLETED);
      SagaStateSnapshot completed2 = snapshot("saga-c2", SagaStatus.COMPLETED);
      SagaStateSnapshot completed3 = snapshot("saga-c3", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(completed1, completed2, completed3));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(97), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.deleteSaga(any())).thenReturn(true);

      // Act
      manager.cleanup();

      // Assert — remaining budget for COMPENSATED is 100 - 3 = 97
      verify(store, atLeastOnce())
          .findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(97), eq(OWNER_ID), anyInt());
    }

    @Test
    void cleanup_lostRaceRefundsBudget_nextRoundRescansAndSpendsIt() {
      // Arrange — pass budget 2. The first round scans two candidates but loses one to a racing
      // replica (deleteSaga returns false), refunding its budget; the purged rows are physically
      // gone, so the second round's re-scan surfaces a fresh candidate and the refunded budget is
      // spent on it instead of being silently dropped.
      RetentionConfig smallBudget =
          new RetentionConfig(RETENTION_PERIOD, 3600, 2, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      SagaRetentionManager smallManager =
          new SagaRetentionManager(store, OWNER_ID, smallBudget, scheduler);
      SagaStateSnapshot lostRace = snapshot("saga-lost", SagaStatus.COMPLETED);
      SagaStateSnapshot won = snapshot("saga-won", SagaStatus.COMPLETED);
      SagaStateSnapshot fresh = snapshot("saga-fresh", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(2), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(lostRace, won));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(1), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(fresh));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(1), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.deleteSaga("saga-lost")).thenReturn(false);
      when(store.deleteSaga("saga-won")).thenReturn(true);
      when(store.deleteSaga("saga-fresh")).thenReturn(true);

      // Act
      smallManager.cleanup();

      // Assert — the second round purged the fresh candidate with the refunded budget
      verify(store).deleteSaga("saga-fresh");
    }

    @Test
    void cleanup_roundPurgesNothing_endsPassWithoutRescanning() {
      // Arrange — every candidate is lost to racing replicas: the round makes no progress, so the
      // pass must end instead of re-scanning (a round of failing deletes would refetch the same
      // rows forever).
      SagaStateSnapshot lost1 = snapshot("saga-lost-1", SagaStatus.COMPLETED);
      SagaStateSnapshot lost2 = snapshot("saga-lost-2", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(lost1, lost2));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      when(store.deleteSaga(any())).thenReturn(false);

      // Act
      manager.cleanup();

      // Assert — each status was scanned exactly once: no second round
      verify(store, times(1))
          .findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt());
      verify(store, times(1))
          .findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt());
    }

    @Test
    void cleanup_purgeTaskThrowsError_otherPurgesStillCounted() {
      // Arrange — an Error thrown by a delete is contained inside purgeOneSafely (its catch spans
      // Throwable): the pass must not throw, and the surviving purges must still be attempted and
      // counted.
      SagaStateSnapshot bad = snapshot("saga-bad", SagaStatus.COMPLETED);
      SagaStateSnapshot ok = snapshot("saga-ok", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(bad, ok));
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of());
      doThrow(new AssertionError("simulated Error escaping the purge task"))
          .when(store)
          .deleteSaga("saga-bad");
      when(store.deleteSaga("saga-ok")).thenReturn(true);

      // Act — must not throw
      manager.cleanup();

      // Assert — the surviving purge was still attempted and counted (COMPENSATED saw budget 99)
      verify(store).deleteSaga("saga-ok");
      verify(store, atLeastOnce())
          .findByStatusOlderThan(
              eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99), eq(OWNER_ID), anyInt());
    }

    @Test
    void cleanup_interruptedMidAwait_cancelsRemainingPurgeTasks() throws Exception {
      // Arrange — two purge tasks block inside their deletes; interrupting the pass thread must
      // cancel both (interrupting them) and drain their outcomes instead of leaving them to run
      // past the pass, and the interrupt flag must be restored.
      java.util.concurrent.CountDownLatch bothStarted = new java.util.concurrent.CountDownLatch(2);
      java.util.concurrent.CountDownLatch bothInterrupted =
          new java.util.concurrent.CountDownLatch(2);
      SagaStateSnapshot s1 = snapshot("saga-block-1", SagaStatus.COMPLETED);
      SagaStateSnapshot s2 = snapshot("saga-block-2", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(
              eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100), eq(OWNER_ID), anyInt()))
          .thenReturn(List.of(s1, s2));
      when(store.deleteSaga(any()))
          .thenAnswer(
              invocation -> {
                bothStarted.countDown();
                try {
                  new java.util.concurrent.CountDownLatch(1).await(); // blocks until interrupted
                } catch (InterruptedException e) {
                  bothInterrupted.countDown();
                }
                return false;
              });
      java.util.concurrent.atomic.AtomicBoolean interruptFlagRestored =
          new java.util.concurrent.atomic.AtomicBoolean();

      // Act — run the pass, interrupt it mid-await
      Thread passThread =
          new Thread(
              () -> {
                manager.cleanup();
                interruptFlagRestored.set(Thread.currentThread().isInterrupted());
              });
      passThread.start();
      assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
      passThread.interrupt();
      passThread.join(5_000);

      // Assert — the pass returned, both tasks were interrupted (not abandoned), and the pass
      // thread's interrupt flag was restored
      assertThat(passThread.isAlive()).isFalse();
      assertThat(bothInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(interruptFlagRestored).isTrue();
    }
  }

  // =========================================================================
  // start() / stop()
  // =========================================================================

  @Nested
  class Lifecycle {

    @Test
    void start_schedulesDelayedCleanupWithOwnerOffset() {
      // Arrange — the first run waits at least one interval, then the owner's deterministic
      // de-phasing offset on top.
      long offset = SweepScatter.offsetSeconds(OWNER_ID, "retention", 3600);

      // Act
      manager.start();

      // Assert
      assertThat(offset).isBetween(0L, 3599L);
      verify(scheduler)
          .scheduleWithFixedDelay(
              any(Runnable.class), eq(3600L + offset), eq(3600L), eq(TimeUnit.SECONDS));
    }

    @Test
    void start_cleanupPassThrowsError_scheduledTaskContainsIt() {
      // Arrange — capture the periodic task and make the pass blow up with an Error. Only a catch
      // on Throwable contains it; a Throwable escaping a scheduleWithFixedDelay task cancels all
      // its future executions, silently stopping retention cleanup for the rest of the process.
      when(store.findByStatusOlderThan(any(), any(), anyInt(), any(), anyInt()))
          .thenThrow(new Error("scan blew up"));
      manager.start();
      long offset = SweepScatter.offsetSeconds(OWNER_ID, "retention", 3600);
      ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
      verify(scheduler)
          .scheduleWithFixedDelay(
              task.capture(), eq(3600L + offset), eq(3600L), eq(TimeUnit.SECONDS));

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
