package com.scalar.db.saga.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Covers the shared bounded wait: the poll interval derived from the bound, and the wait itself.
 *
 * <p>The interval governs notice latency alone — the registry covers a local drive and the read at
 * bound expiry covers everything else — so what matters is that it stays proportional to the bound
 * between a floor and a ceiling.
 *
 * <p>{@code awaitWithin} is tested here rather than only through the transports because its failure
 * paths are unreachable from them: a caller cannot interrupt a request handler's thread, or make a
 * shutdown signal fail. Driving the futures directly also pins the two-phase polling machine in
 * seconds rather than through a running server.
 */
class BoundedWaitTest {

  @Test
  void pollIntervalMillis_defaultBoundGiven_returnsOneSixthOfIt() {
    // Act / Assert — the 60s default yields 10s: six polls per window.
    assertThat(BoundedWait.pollIntervalMillis(60_000L)).isEqualTo(10_000L);
  }

  @Test
  void pollIntervalMillis_boundsBelowTheFloorGiven_returnsTheFloor() {
    // Act / Assert — 5s would be 833ms proportionally, tight enough to spin. Zero reaches this from
    // a bound already spent, where a non-positive interval would spin without waiting at all.
    assertThat(BoundedWait.pollIntervalMillis(5_000L)).isEqualTo(1_000L);
    assertThat(BoundedWait.pollIntervalMillis(0L)).isEqualTo(1_000L);
  }

  @Test
  void pollIntervalMillis_ceilingBoundaryBoundGiven_returnsTheCeiling() {
    // Act / Assert — 180s is exactly six 30s polls, the largest bound still purely proportional.
    assertThat(BoundedWait.pollIntervalMillis(180_000L)).isEqualTo(30_000L);
  }

  @Test
  void pollIntervalMillis_boundsAboveTheCeilingGiven_returnsTheCeiling() {
    // Act / Assert — proportionality alone would give 100s at a 600s bound, which is the case the
    // ceiling exists for: a short saga running under a long bound should not wait that long to be
    // noticed. It stays clamped however far the bound is raised.
    assertThat(BoundedWait.pollIntervalMillis(600_000L)).isEqualTo(30_000L);
    assertThat(BoundedWait.pollIntervalMillis(1_800_000L)).isEqualTo(30_000L);
  }

  @Test
  void awaitWithin_parkedSagaNeverSettles_retainsNothingOnTheAbortSignal()
      throws InterruptedException {
    // Arrange — `abort` stands in for the shutdown signal REST passes straight through, which lives
    // as long as the server. A parked saga that never settles is the case with no push left to
    // fire: the callback died at the park and the registry deregisters at the end of the wait, so
    // without a signal for the wait itself the anyOf node stays attached to `abort` for the life of
    // the process. Nothing here completes, exactly as in that case.
    CompletableFuture<Void> abort = new CompletableFuture<>();
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();
    CompletableFuture<Void> parked = new CompletableFuture<>();
    parked.complete(null);
    SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
    WeakReference<CompletableFuture<SagaStateSnapshot>> ref = new WeakReference<>(settled);

    // Act — the bound elapses with the saga still parked.
    SagaStateSnapshot answer = BoundedWait.awaitWithin(settled, abort, parked, 50L, () -> waiting);
    settled = null;
    parked = null;

    // Assert — the wait still answers from the read, and leaves the request's futures collectable.
    assertThat(answer).isEqualTo(waiting);
    assertThat(abort.isDone()).isFalse();
    assertThat(collectable(ref)).isTrue();
  }

  @Test
  void awaitWithin_sagaSettlesLocallyDuringASlowPollRead_answersWithTheSettledSnapshot()
      throws InterruptedException {
    // Arrange — the last poll tick before the bound, against a store slow enough that the read
    // outlives the deadline. A drive on this process settles the saga while that read is in
    // flight, so the store's answer is stale the moment it arrives and the terminal snapshot is
    // already in memory. Polling from the start, as AwaitSaga does, so no park signal is needed.
    CompletableFuture<Void> abort = new CompletableFuture<>();
    CompletableFuture<SagaStateSnapshot> settled = new CompletableFuture<>();
    SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
    SagaStateSnapshot completed = snapshot(SagaStatus.COMPLETED);

    // Act
    SagaStateSnapshot answer =
        BoundedWait.awaitWithin(
            settled,
            abort,
            null,
            1_200L,
            () -> {
              // The registry completes the waiter mid-read, which is what the read cannot see.
              settled.complete(completed);
              sleep(400L);
              return waiting;
            });

    // Assert — the outcome the caller waited for, not the stale read that raced it.
    assertThat(answer).isEqualTo(completed);
  }

  @Test
  void awaitWithin_boundAlreadyElapsedGiven_readsOnceWithoutPolling() {
    // Arrange — a per-call deadline can tighten the bound to nothing before the wait even starts.
    AtomicInteger reads = new AtomicInteger();
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);

    // Act
    SagaStateSnapshot answer =
        BoundedWait.awaitWithin(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            null,
            0L,
            () -> {
              reads.incrementAndGet();
              return running;
            });

    // Assert — straight to the one read that decides the answer; no tick, no spin.
    assertThat(answer).isEqualTo(running);
    assertThat(reads).hasValue(1);
  }

  @Test
  void awaitWithin_abortCompletesExceptionally_answersFromTheReadWithoutThrowing() {
    // Arrange — anyOf propagates an exceptional source, so the wait sees ExecutionException rather
    // than a value. The saga's own state is still the answer, and the caller must not see the
    // failure of a signal that only ever meant "stop waiting".
    CompletableFuture<Void> abort = new CompletableFuture<>();
    abort.completeExceptionally(new IllegalStateException("shutdown signal failed"));
    AtomicInteger reads = new AtomicInteger();
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);

    // Act
    SagaStateSnapshot answer =
        BoundedWait.awaitWithin(
            new CompletableFuture<>(),
            abort,
            null,
            30_000L,
            () -> {
              reads.incrementAndGet();
              return running;
            });

    // Assert
    assertThat(answer).isEqualTo(running);
    assertThat(reads).hasValue(1);
  }

  @Test
  void awaitWithin_abortCompletes_endsTheWaitWellInsideTheBound() {
    // Arrange — the shutdown short-circuit. Already complete on entry, so the first slice returns
    // at once rather than running the bound down.
    CompletableFuture<Void> abort = new CompletableFuture<>();
    abort.complete(null);
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);

    // Act
    long startNanos = System.nanoTime();
    SagaStateSnapshot answer =
        BoundedWait.awaitWithin(new CompletableFuture<>(), abort, null, 30_000L, () -> running);
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    // Assert — the bound is a maximum, not a promise to wait.
    assertThat(answer).isEqualTo(running);
    assertThat(elapsedMillis).isLessThan(5_000L);
  }

  @Test
  void awaitWithin_waitingThreadIsInterrupted_restoresTheFlagAndAnswersFromTheRead() {
    // Arrange — interrupting before the call is equivalent to interrupting during it: the first
    // timed get observes the flag and throws immediately. Swallowing the interrupt silently would
    // strand it, so the wait has to hand it back to the caller.
    SagaStateSnapshot running = snapshot(SagaStatus.RUNNING);
    AtomicInteger reads = new AtomicInteger();
    Thread.currentThread().interrupt();

    try {
      // Act
      SagaStateSnapshot answer =
          BoundedWait.awaitWithin(
              new CompletableFuture<>(),
              new CompletableFuture<>(),
              null,
              30_000L,
              () -> {
                reads.incrementAndGet();
                return running;
              });

      // Assert — answered rather than propagating, with the flag left for the caller to see.
      assertThat(answer).isEqualTo(running);
      assertThat(reads).hasValue(1);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Clear it so the flag cannot leak into whatever this thread runs next.
      Thread.interrupted();
    }
  }

  @Test
  void awaitWithin_sagaParksMidWait_startsPollingForTheRestOfTheBound() {
    // Arrange — the two-phase machine. Before the park the drive is local, so the wait sits in one
    // slice and reads nothing; the park is the first moment a resume could land somewhere no push
    // reaches, so the rest of the bound is polled. A 2.5s bound gives a 1s interval, and the park
    // lands ~200ms in, leaving room for two ticks and the final read.
    CompletableFuture<Void> parked = new CompletableFuture<>();
    AtomicInteger reads = new AtomicInteger();
    SagaStateSnapshot waiting = snapshot(SagaStatus.WAITING);
    CompletableFuture<Void> park =
        CompletableFuture.runAsync(
            () -> parked.complete(null),
            CompletableFuture.delayedExecutor(200L, TimeUnit.MILLISECONDS));

    // Act
    SagaStateSnapshot answer =
        BoundedWait.awaitWithin(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            parked,
            2_500L,
            () -> {
              reads.incrementAndGet();
              return waiting;
            });
    // Long since done, but joining keeps a failure to schedule the park visible rather than
    // leaving it to surface as an unexplained read count.
    park.join();

    // Assert — polling began. Without the flip the whole bound is one slice and the store is read
    // exactly once, at the end; the range absorbs a tick either way without admitting that case.
    assertThat(answer).isEqualTo(waiting);
    assertThat(reads).hasValueBetween(2, 4);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /**
   * Whether the referent became unreachable. Retries rather than trusting one {@code System.gc()},
   * which is a hint; a reachable referent never becomes collectable, so this only ever slows a
   * failing run down, never turns one green.
   */
  private static boolean collectable(WeakReference<?> ref) throws InterruptedException {
    for (int attempt = 0; attempt < 50 && ref.get() != null; attempt++) {
      System.gc();
      Thread.sleep(20L);
    }
    return ref.get() == null;
  }

  private static SagaStateSnapshot snapshot(SagaStatus status) {
    Instant now = Instant.parse("2026-09-08T00:00:00Z");
    return new SagaStateSnapshot("saga-1", "transfer", status, "v1", now, now);
  }
}
