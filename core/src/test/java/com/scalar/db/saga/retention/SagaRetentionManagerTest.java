package com.scalar.db.saga.retention;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.RetentionConfig;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.store.SagaStore;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SagaRetentionManagerTest {

  private static final Instant NOW = Instant.parse("2025-01-08T12:00:00Z");
  private static final Duration RETENTION_PERIOD = Duration.ofDays(7);
  private static final Instant THRESHOLD = NOW.minus(RETENTION_PERIOD);

  @Mock private SagaStore store;
  @Mock private ScheduledExecutorService scheduler;

  private RetentionConfig config;
  private SagaRetentionManager manager;

  @BeforeEach
  void setUp() {
    config = new RetentionConfig(RETENTION_PERIOD, 3600, 100, 10, Clock.fixed(NOW, ZoneOffset.UTC));
    manager = new SagaRetentionManager(store, config, scheduler);
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

  // =========================================================================
  // cleanup()
  // =========================================================================

  @Nested
  class Cleanup {

    @Test
    void cleanup_noExpiredSagas_deletesNothing() {
      // Arrange
      when(store.findByStatusOlderThan(any(), any(), anyInt())).thenReturn(List.of());

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
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of(saga1, saga2));
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(98)))
          .thenReturn(List.of());

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
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of());
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(100)))
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
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of(completed));
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99)))
          .thenReturn(List.of(compensated));

      // Act
      manager.cleanup();

      // Assert
      verify(store).deleteSaga("saga-c1");
      verify(store).deleteSaga("saga-c2");
    }

    @Test
    void cleanup_batchLimitReached_stopsEarly() {
      // Arrange
      RetentionConfig smallBatch =
          new RetentionConfig(RETENTION_PERIOD, 3600, 2, 10, Clock.fixed(NOW, ZoneOffset.UTC));
      SagaRetentionManager smallManager = new SagaRetentionManager(store, smallBatch, scheduler);

      SagaStateSnapshot saga1 = snapshot("saga-001", SagaStatus.COMPLETED);
      SagaStateSnapshot saga2 = snapshot("saga-002", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(2)))
          .thenReturn(List.of(saga1, saga2));

      // Act
      smallManager.cleanup();

      // Assert — purged 2, hit batch limit, did not scan COMPENSATED
      verify(store).deleteSaga("saga-001");
      verify(store).deleteSaga("saga-002");
      verify(store, never()).findByStatusOlderThan(eq(SagaStatus.COMPENSATED), any(), anyInt());
    }

    @Test
    void cleanup_deleteFailsForOneSaga_continuesWithOthers() {
      // Arrange
      SagaStateSnapshot saga1 = snapshot("saga-fail", SagaStatus.COMPLETED);
      SagaStateSnapshot saga2 = snapshot("saga-ok", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of(saga1, saga2));
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(99)))
          .thenReturn(List.of());
      doThrow(new RuntimeException("delete failed")).when(store).deleteSaga("saga-fail");
      doNothing().when(store).deleteSaga("saga-ok");

      // Act
      manager.cleanup();

      // Assert — saga-ok is still deleted despite saga-fail failure
      verify(store).deleteSaga("saga-fail");
      verify(store).deleteSaga("saga-ok");
    }

    @Test
    void cleanup_escalatedSagasNotQueried() {
      // Arrange
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of());
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of());

      // Act
      manager.cleanup();

      // Assert — ESCALATED is never queried
      verify(store, never()).findByStatusOlderThan(eq(SagaStatus.ESCALATED), any(), anyInt());
      verify(store, never()).findByStatusOlderThan(eq(SagaStatus.RUNNING), any(), anyInt());
      verify(store, never()).findByStatusOlderThan(eq(SagaStatus.COMPENSATING), any(), anyInt());
    }

    @Test
    void cleanup_remainingBudgetPassedCorrectlyToSecondStatus() {
      // Arrange
      SagaStateSnapshot completed1 = snapshot("saga-c1", SagaStatus.COMPLETED);
      SagaStateSnapshot completed2 = snapshot("saga-c2", SagaStatus.COMPLETED);
      SagaStateSnapshot completed3 = snapshot("saga-c3", SagaStatus.COMPLETED);
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPLETED), eq(THRESHOLD), eq(100)))
          .thenReturn(List.of(completed1, completed2, completed3));
      when(store.findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(97)))
          .thenReturn(List.of());

      // Act
      manager.cleanup();

      // Assert — remaining budget for COMPENSATED is 100 - 3 = 97
      verify(store).findByStatusOlderThan(eq(SagaStatus.COMPENSATED), eq(THRESHOLD), eq(97));
    }
  }

  // =========================================================================
  // start() / stop()
  // =========================================================================

  @Nested
  class Lifecycle {

    @Test
    void start_schedulesDelayedCleanup() {
      // Act
      manager.start();

      // Assert — first run is delayed (not immediate like recovery)
      verify(scheduler)
          .scheduleWithFixedDelay(any(Runnable.class), eq(3600L), eq(3600L), eq(TimeUnit.SECONDS));
    }

    @Test
    void stop_shutsDownScheduler() throws InterruptedException {
      // Arrange
      when(scheduler.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);

      // Act
      manager.stop();

      // Assert
      verify(scheduler).shutdown();
    }

    @Test
    void stop_forcesShutdownOnTimeout() throws InterruptedException {
      // Arrange
      when(scheduler.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(false);

      // Act
      manager.stop();

      // Assert
      verify(scheduler).shutdown();
      verify(scheduler).shutdownNow();
    }
  }
}
