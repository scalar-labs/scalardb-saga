package com.scalar.db.saga.server;

import com.scalar.db.saga.api.SagaStateSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * The wait shared by every bounded synchronous path: the REST and gRPC saga starts, and gRPC's
 * {@code AwaitSaga} long-poll.
 *
 * <p>One implementation rather than three, because the rule has three sources of truth to reconcile
 * and hand-kept copies drift. It ends on whichever comes first: the saga settles on this process
 * (the per-start callback, or the {@link SagaWaiterRegistry} for a drive that carries none), a poll
 * tick observes it settled elsewhere, the caller aborts, or the bound elapses. Whatever ends it,
 * the answer is the freshest state available — which is why a wait that is never notified is not
 * wasted.
 */
public final class BoundedWait {

  /**
   * Lower bound on the poll interval, so a pathologically small wait bound cannot produce a tight
   * spin.
   */
  private static final long MIN_POLL_INTERVAL_MILLIS = 1_000L;

  /**
   * Upper bound on the poll interval.
   *
   * <p>Without it the interval is purely proportional, which is wrong for a <em>short</em> saga
   * running under a <em>long</em> bound: at a 600s bound a saga finishing in 30s would go unnoticed
   * for 100s. Mixed saga durations under one bound are normal, so proportionality alone does not
   * hold. 20 scans across a 600s window is 0.03/s, cheap enough to buy back that latency.
   */
  private static final long MAX_POLL_INTERVAL_MILLIS = 30_000L;

  /** Divisor: six polls per window at any bound below the ceiling. */
  private static final long POLLS_PER_WINDOW = 6L;

  private BoundedWait() {}

  /**
   * The interval between polls, derived from the effective bound rather than fixed.
   *
   * <p>Polling no longer <em>discovers</em> a completion — the registry covers a local drive and
   * the read at bound expiry covers everything else — so this governs notice latency alone. Every
   * saga that reaches this path has already outlived its bound's timescale, so the right precision
   * is relative to that, not absolute: a fixed value is as disproportionate at one bound as it is
   * tight at another.
   *
   * @param boundMillis the effective wait bound
   * @return the poll interval in milliseconds, within {@value #MIN_POLL_INTERVAL_MILLIS} and
   *     {@value #MAX_POLL_INTERVAL_MILLIS}
   */
  public static long pollIntervalMillis(long boundMillis) {
    long derived = boundMillis / POLLS_PER_WINDOW;
    return Math.min(Math.max(derived, MIN_POLL_INTERVAL_MILLIS), MAX_POLL_INTERVAL_MILLIS);
  }

  /**
   * Waits for the saga to settle, up to {@code boundMillis}, and returns the freshest state known
   * when the wait ends.
   *
   * <p>The caller registers with the {@link SagaWaiterRegistry} and closes that registration
   * itself, because only the caller knows whether a read must happen before the wait: a start has
   * just created the saga and needs none, while a long-poll on an existing saga may find it already
   * terminal.
   *
   * @param settledLocally the future registered with the registry, completed if a drive on this
   *     process settles the saga
   * @param outcome the per-start callback's future, or {@code null} when there is no start callback
   *     (the long-poll path). It is kept alongside the registry because a start with a
   *     server-generated id cannot register before {@code startAsync} returns the id, by which time
   *     the saga may already have settled
   * @param abort completes when the caller should stop waiting early — server shutdown, or a
   *     cancelled call
   * @param boundMillis the effective bound, already tightened by any per-call deadline
   * @param read reads the saga's current state; called on each poll tick and once at the end
   * @return the settled snapshot if the saga settled, otherwise the state as read when the wait
   *     ended
   */
  public static SagaStateSnapshot awaitWithin(
      CompletableFuture<SagaStateSnapshot> settledLocally,
      @Nullable CompletableFuture<SagaStateSnapshot> outcome,
      CompletableFuture<?> abort,
      @Nullable CompletableFuture<?> pollFrom,
      long boundMillis,
      Supplier<SagaStateSnapshot> read) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(boundMillis);
    long intervalMillis = pollIntervalMillis(boundMillis);
    boolean polling = pollFrom == null || pollFrom.isDone();
    CompletableFuture<?> wakeUp = wakeUp(settledLocally, outcome, abort, polling ? null : pollFrom);

    while (true) {
      long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
      if (remainingMillis <= 0) {
        break;
      }
      // Until the saga parks, it is being driven on this process, so a push reaches us and a poll
      // can only find what the push would have delivered sooner. Wait out the whole remainder in
      // one slice and read nothing.
      long sliceMillis = polling ? Math.min(intervalMillis, remainingMillis) : remainingMillis;
      try {
        wakeUp.get(sliceMillis, TimeUnit.MILLISECONDS);
        if (!polling && settledSnapshot(settledLocally, outcome) == null && !abort.isDone()) {
          // Only the park fired. From here the saga can be resumed on another replica, where no
          // push reaches us, so start polling for what is left of the bound.
          polling = true;
          wakeUp = wakeUp(settledLocally, outcome, abort, null);
          continue;
        }
        break;
      } catch (TimeoutException e) {
        // The bound cut this slice short, so there is no time left to poll for: fall through to the
        // read below rather than reading twice in succession.
        if (sliceMillis == remainingMillis) {
          break;
        }
        // A poll tick. The saga may have settled on another replica, where nothing can notify us.
        SagaStateSnapshot polled = read.get();
        if (polled.getStatus().isTerminal()) {
          return polled;
        }
        // The read can itself outlive the deadline on a slow store. It is then already the freshest
        // answer there is, and falling through would read again in the same breath — two
        // transactions for one answer, on exactly the store this is meant to spare. Only this case:
        // a tick that finished well before the deadline goes stale by the time the deadline
        // arrives, and the read below is what makes the bound-expiry answer worth having.
        if (System.nanoTime() - deadlineNanos >= 0) {
          return polled;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        // The abort signal completed exceptionally; the state below is the answer either way.
        break;
      }
    }

    SagaStateSnapshot settled = settledSnapshot(settledLocally, outcome);
    // The read is what makes an un-notified wait worth having: a saga resumed and settled on
    // another replica is invisible to both futures above, and visible here.
    return settled != null ? settled : read.get();
  }

  /**
   * The signals that end a slice. Rebuilt once if polling begins mid-wait, because {@code anyOf}
   * stays completed: reusing one that the park signal already completed would spin the loop.
   */
  private static CompletableFuture<?> wakeUp(
      CompletableFuture<SagaStateSnapshot> settledLocally,
      @Nullable CompletableFuture<SagaStateSnapshot> outcome,
      CompletableFuture<?> abort,
      @Nullable CompletableFuture<?> pollFrom) {
    List<CompletableFuture<?>> signals = new ArrayList<>(4);
    signals.add(settledLocally);
    signals.add(abort);
    if (outcome != null) {
      signals.add(outcome);
    }
    if (pollFrom != null) {
      signals.add(pollFrom);
    }
    return CompletableFuture.anyOf(signals.toArray(new CompletableFuture<?>[0]));
  }

  /** The saga's settled snapshot if either push mechanism delivered one, else {@code null}. */
  private static @Nullable SagaStateSnapshot settledSnapshot(
      CompletableFuture<SagaStateSnapshot> settledLocally,
      @Nullable CompletableFuture<SagaStateSnapshot> outcome) {
    SagaStateSnapshot fromCallback = outcome != null ? outcome.getNow(null) : null;
    return fromCallback != null ? fromCallback : settledLocally.getNow(null);
  }
}
