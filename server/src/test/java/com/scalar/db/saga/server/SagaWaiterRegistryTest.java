package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** Covers the registry that lets any drive on this process wake a waiting request. */
class SagaWaiterRegistryTest {

  private static final String SAGA_ID = "s1";

  private final SagaWaiterRegistry registry = new SagaWaiterRegistry();

  private static SagaStateSnapshot snapshot(String sagaId, SagaStatus status) {
    SagaStateSnapshot snapshot = mock(SagaStateSnapshot.class);
    when(snapshot.getSagaId()).thenReturn(sagaId);
    when(snapshot.getStatus()).thenReturn(status);
    return snapshot;
  }

  @Test
  void onSagaSettled_withARegisteredWaiter_completesIt() {
    // Arrange
    SagaStateSnapshot settledSnapshot = snapshot(SAGA_ID, SagaStatus.COMPLETED);
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();

    try (SagaWaiterRegistry.Waiter waiter = registry.register(SAGA_ID, settled)) {
      // Act
      registry.onSagaSettled(settledSnapshot);

      // Assert
      assertThat(settled).isCompletedWithValue(settledSnapshot);
      assertThat(waiter).isNotNull();
    }
  }

  @Test
  void onSagaSettled_twoWaitersOnOneSaga_completesBoth() {
    // Arrange — two clients awaiting the same saga is legal, so neither may be dropped.
    SagaStateSnapshot settledSnapshot = snapshot(SAGA_ID, SagaStatus.COMPLETED);
    CompletableFuture<SagaStateSnapshot> first = new CompletableFuture<>();
    CompletableFuture<SagaStateSnapshot> second = new CompletableFuture<>();

    try (SagaWaiterRegistry.Waiter firstWaiter = registry.register(SAGA_ID, first);
        SagaWaiterRegistry.Waiter secondWaiter = registry.register(SAGA_ID, second)) {
      // Act
      registry.onSagaSettled(settledSnapshot);

      // Assert
      assertThat(first).isCompletedWithValue(settledSnapshot);
      assertThat(second).isCompletedWithValue(settledSnapshot);
      assertThat(firstWaiter).isNotSameAs(secondWaiter);
    }
  }

  @Test
  void onSagaSettled_forAnUnwatchedSaga_doesNothing() {
    // Arrange & Act — the common case: nobody is waiting, so settling must cost nothing.
    registry.onSagaSettled(snapshot("other", SagaStatus.COMPLETED));

    // Assert
    assertThat(registry.isWatching("other")).isFalse();
    assertThat(registry.watchedSagaCount()).isZero();
  }

  @Test
  void isWatching_whileRegistered_returnsTrue() {
    // Arrange
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();

    // Act & Assert
    try (SagaWaiterRegistry.Waiter waiter = registry.register(SAGA_ID, settled)) {
      assertThat(registry.isWatching(SAGA_ID)).isTrue();
      assertThat(settled).isNotDone();
      assertThat(waiter).isNotNull();
    }
  }

  @Test
  void close_lastWaiterLeaves_dropsTheSagaEntirely() {
    // Arrange
    SagaWaiterRegistry.Waiter waiter = registry.register(SAGA_ID, new CompletableFuture<>());

    // Act
    waiter.close();

    // Assert — the entry is removed, not left empty, so the map cannot grow without bound.
    assertThat(registry.isWatching(SAGA_ID)).isFalse();
    assertThat(registry.watchedSagaCount()).isZero();
  }

  @Test
  void close_oneOfTwoWaitersLeaves_keepsWatchingForTheOther() {
    // Arrange
    CompletableFuture<SagaStateSnapshot> leavingFuture = new CompletableFuture<>();
    CompletableFuture<SagaStateSnapshot> stayingFuture = new CompletableFuture<>();
    SagaWaiterRegistry.Waiter leaving = registry.register(SAGA_ID, leavingFuture);
    SagaStateSnapshot settledSnapshot = snapshot(SAGA_ID, SagaStatus.COMPENSATED);

    try (SagaWaiterRegistry.Waiter staying = registry.register(SAGA_ID, stayingFuture)) {
      // Act
      leaving.close();

      // Assert
      assertThat(registry.isWatching(SAGA_ID)).isTrue();
      registry.onSagaSettled(settledSnapshot);
      assertThat(stayingFuture).isCompletedWithValue(settledSnapshot);
      assertThat(leavingFuture).isNotDone();
      assertThat(staying).isNotNull();
    }
  }

  @Test
  void close_calledTwice_isIdempotent() {
    // Arrange — try-with-resources plus an explicit close, or a retry after a throw, must not
    // corrupt another waiter's registration.
    SagaWaiterRegistry.Waiter waiter = registry.register(SAGA_ID, new CompletableFuture<>());
    waiter.close();

    // Act
    waiter.close();

    // Assert
    assertThat(registry.watchedSagaCount()).isZero();
  }

  @Test
  void register_afterAPreviousWaiterLeft_watchesAgain() {
    // Arrange — dropping the entry on the last close must not stop a later request registering.
    registry.register(SAGA_ID, new CompletableFuture<>()).close();
    SagaStateSnapshot settledSnapshot = snapshot(SAGA_ID, SagaStatus.ESCALATED);
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();

    // Act
    try (SagaWaiterRegistry.Waiter waiter = registry.register(SAGA_ID, settled)) {
      registry.onSagaSettled(settledSnapshot);

      // Assert
      assertThat(settled).isCompletedWithValue(settledSnapshot);
      assertThat(waiter).isNotNull();
    }
  }

  @Test
  void onSagaSettled_calledTwice_keepsTheFirstOutcome() {
    // Arrange — a racing drive must not overwrite the answer already handed to the waiter.
    SagaStateSnapshot first = snapshot(SAGA_ID, SagaStatus.COMPLETED);
    SagaStateSnapshot second = snapshot(SAGA_ID, SagaStatus.COMPENSATED);
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();

    try (SagaWaiterRegistry.Waiter waiter = registry.register(SAGA_ID, settled)) {
      // Act
      registry.onSagaSettled(first);
      registry.onSagaSettled(second);

      // Assert
      assertThat(settled).isCompletedWithValue(first);
      assertThat(waiter).isNotNull();
    }
  }
}
