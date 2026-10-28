package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.api.SagaCallback;
import com.scalar.db.saga.api.SagaDefinitionId;
import com.scalar.db.saga.api.SagaDetail;
import com.scalar.db.saga.api.SagaOrchestrator;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;

/**
 * In-memory {@link SagaOrchestrator} for runner tests: configurable failure injection, snapshot
 * polling behavior, and an optional latch that makes {@code start} block (to exercise the stall
 * watchdog). Only the overloads the runner uses are implemented.
 */
final class FakeSagaOrchestrator implements SagaOrchestrator {

  private final AtomicLong ids = new AtomicLong();
  private final AtomicLong startCalls = new AtomicLong();
  private final ConcurrentHashMap<String, AtomicInteger> pollsUntilTerminal =
      new ConcurrentHashMap<>();
  private final int runningPolls;
  private final int failEveryNth;
  private final @Nullable CountDownLatch startBlocker;

  private FakeSagaOrchestrator(
      int runningPolls, int failEveryNth, @Nullable CountDownLatch startBlocker) {
    this.runningPolls = runningPolls;
    this.failEveryNth = failEveryNth;
    this.startBlocker = startBlocker;
  }

  /** Starts succeed and the first snapshot read is already terminal. */
  static FakeSagaOrchestrator completing() {
    return new FakeSagaOrchestrator(0, 0, null);
  }

  /** Snapshot reads return {@code RUNNING} for the first {@code polls} reads per saga. */
  static FakeSagaOrchestrator completingAfterPolls(int polls) {
    return new FakeSagaOrchestrator(polls, 0, null);
  }

  /** Every {@code n}-th start call (1-based) throws {@link IllegalStateException}. */
  static FakeSagaOrchestrator failingEveryNth(int n) {
    return new FakeSagaOrchestrator(0, n, null);
  }

  /** Every start blocks on {@code latch} (interruption surfaces as a runtime failure). */
  static FakeSagaOrchestrator blockingOn(CountDownLatch latch) {
    return new FakeSagaOrchestrator(0, 0, latch);
  }

  private String doStart() {
    long call = startCalls.incrementAndGet();
    if (startBlocker != null) {
      try {
        startBlocker.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("start interrupted", e);
      }
    }
    if (failEveryNth > 0 && call % failEveryNth == 0) {
      throw new IllegalStateException("injected start failure");
    }
    String sagaId = "saga-" + ids.incrementAndGet();
    pollsUntilTerminal.put(sagaId, new AtomicInteger(runningPolls));
    return sagaId;
  }

  @Override
  public String start(String sagaName, Map<String, Object> input) {
    return doStart();
  }

  @Override
  public String startAsync(String sagaName, Map<String, Object> input) {
    return doStart();
  }

  @Override
  public SagaStateSnapshot getStateSnapshot(String sagaId) {
    AtomicInteger remaining = pollsUntilTerminal.get(sagaId);
    SagaStatus status =
        remaining != null && remaining.getAndDecrement() > 0
            ? SagaStatus.RUNNING
            : SagaStatus.COMPLETED;
    return new SagaStateSnapshot(
        sagaId, "bench", status, "fake", "1", Instant.EPOCH, Instant.EPOCH);
  }

  @Override
  public void close() {}

  // --- overloads the runner never calls -------------------------------------------------------

  @Override
  public void start(String sagaId, String sagaName, Map<String, Object> input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String start(SagaDefinitionId id, Map<String, Object> input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void start(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String startAsync(String sagaName, Map<String, Object> input, SagaCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startAsync(String sagaId, String sagaName, Map<String, Object> input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startAsync(
      String sagaId, String sagaName, Map<String, Object> input, SagaCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String startAsync(SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startAsync(String sagaId, SagaDefinitionId id, Map<String, Object> input) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startAsync(
      String sagaId, SagaDefinitionId id, Map<String, Object> input, SagaCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SagaDetail getSagaDetail(String sagaId) {
    throw new UnsupportedOperationException();
  }
}
