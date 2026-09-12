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
 * and hand-kept copies drift. It ends on whichever comes first: the saga settles on this process, a
 * poll tick observes it settled elsewhere, the caller aborts, or the bound elapses. Whatever ends
 * it, the answer is the freshest state available — which is why a wait that is never notified is
 * not wasted.
 *
 * <p>Two mechanisms report a local settle — the {@link SagaWaiterRegistry} for any drive, and the
 * start callback for the window before a server-generated id exists to register under — but they
 * share one future rather than taking one each. They cannot disagree: a single dispatch hands both
 * the same snapshot, and only one of them can carry a terminal state for a given saga, since the
 * callback belonging to the first drive dies when that drive parks.
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
  static long pollIntervalMillis(long boundMillis) {
    long derived = boundMillis / POLLS_PER_WINDOW;
    return Math.min(Math.max(derived, MIN_POLL_INTERVAL_MILLIS), MAX_POLL_INTERVAL_MILLIS);
  }

  /**
   * Waits for the saga to settle, up to {@code boundMillis}, and returns the freshest state known
   * when the wait ends.
   *
   * <p>The caller registers with the {@link SagaWaiterRegistry} and closes that registration
   * itself. A long-poll reads before it registers, since the saga it names may already be terminal.
   * A start does not, having just created the saga; instead this wait reads for itself where
   * polling begins, because a start registers only once it has a saga id and a saga that parks and
   * is resumed in that window settles with nothing listening.
   *
   * @param settled completed with the saga's terminal snapshot by whichever mechanism on this
   *     process sees it settle: the registry, or the start callback for a start whose
   *     server-generated id does not exist to register under until {@code startAsync} returns. Both
   *     deliver the same snapshot from the same dispatch, and {@code complete} is first-wins, so
   *     one future serves both
   * @param abort completes when the caller should stop waiting early — server shutdown, or a
   *     cancelled call
   * @param pollFrom completes when the saga parks, which is the first moment a resume can land
   *     where no push reaches this process; polling starts then. {@code null} polls from the
   *     outset, for a long-poll on a saga that may already be being driven anywhere
   * @param boundMillis the effective bound, already tightened by any per-call deadline
   * @param read reads the saga's current state; called where polling begins, on each poll tick, and
   *     once at the end
   * @return the settled snapshot if the saga settled, otherwise the state as read when the wait
   *     ended
   */
  public static SagaStateSnapshot awaitWithin(
      CompletableFuture<SagaStateSnapshot> settled,
      CompletableFuture<?> abort,
      @Nullable CompletableFuture<?> pollFrom,
      long boundMillis,
      Supplier<SagaStateSnapshot> read) {
    // Every other signal here describes the saga; this one describes the wait, and only this one is
    // guaranteed to fire. A wait that ends for a reason the saga knows nothing about, because a
    // poll tick found the answer or the bound elapsed, would otherwise leave its `anyOf` node on
    // the caller's abort future with no source that can ever complete it. That matters because REST
    // passes the server-lifetime shutdown signal as `abort`, so such a node is retained until the
    // process exits. Completed in the finally, which every exit runs, including the two returns
    // from inside the loop.
    CompletableFuture<Void> finished = new CompletableFuture<>();
    try {
      long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(boundMillis);
      long intervalMillis = pollIntervalMillis(boundMillis);
      boolean polling = pollFrom == null || pollFrom.isDone();
      CompletableFuture<?> wakeUp = wakeUp(settled, abort, polling ? null : pollFrom, finished);

      // The saga had already parked when this wait began, so its resume may have settled it before
      // the caller could register. That settle reached nobody, and the first tick is a whole
      // interval away. Read once now rather than waiting that out. Only a start reaches this; a
      // long-poll passes no park signal and has read already.
      if (polling && pollFrom != null) {
        SagaStateSnapshot alreadySettled = settledAnswer(settled, read, deadlineNanos);
        if (alreadySettled != null) {
          return alreadySettled;
        }
      }

      while (true) {
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (remainingMillis <= 0) {
          break;
        }
        // Until the saga parks it is being driven on this process, so in the ordinary case a push
        // reaches us and a poll could only find what the push delivers sooner. Wait out the whole
        // remainder in one slice and read nothing.
        //
        // One case gets no push: a drive that dies before reaching a verdict leaves the saga
        // RUNNING, and the engine logs that rather than reporting it, so neither the callback nor
        // the park signal fires. Such a wait runs to its bound and answers from the read below,
        // which is what it did before polling existed. Recovery reclaims the saga on its own
        // schedule, and at the default bound that schedule starts no earlier than the bound
        // expires, so polling here would find nothing. It is only worth revisiting for a bound set
        // well above the recovery staleness threshold; see todos/096.
        long sliceMillis = polling ? Math.min(intervalMillis, remainingMillis) : remainingMillis;
        try {
          wakeUp.get(sliceMillis, TimeUnit.MILLISECONDS);
          if (!polling && settled.getNow(null) == null && !abort.isDone()) {
            // Only the park fired. From here the saga can be resumed on another replica, where no
            // push reaches us, so start polling for what is left of the bound.
            polling = true;
            wakeUp = wakeUp(settled, abort, null, finished);
            // The resume can equally have landed before the caller registered, which is the one
            // way a settle on this process reaches no one. Same read, same reason as on entry.
            SagaStateSnapshot alreadySettled = settledAnswer(settled, read, deadlineNanos);
            if (alreadySettled != null) {
              return alreadySettled;
            }
            continue;
          }
          break;
        } catch (TimeoutException e) {
          // The bound cut this slice short, so there is no time left to poll for: fall through to
          // the read below rather than reading twice in succession.
          if (sliceMillis == remainingMillis) {
            break;
          }
          // A poll tick. The saga may have settled on another replica, where nothing can notify us.
          SagaStateSnapshot polled = read.get();
          // Answer now if the poll found a terminal state, or if the read itself outlived the
          // deadline. The second case exists so that a slow read is not followed by the bound
          // expiry read in the same breath; two transactions for one answer, on exactly the store
          // this is meant to spare. It is only that case: a tick that finished well before the
          // deadline goes stale by the time the deadline arrives, and the read below is what makes
          // the bound expiry answer worth having.
          if (polled.getStatus().isTerminal() || System.nanoTime() - deadlineNanos >= 0) {
            // A drive on this process may have settled the saga while that read was in flight,
            // which the read cannot see but the futures already hold. Consulting them costs no
            // I/O, and it keeps every exit from this method answering from the same place; without
            // it a slow read returns its stale non-terminal state and the caller is told the saga
            // is still running when its outcome was in hand.
            SagaStateSnapshot pushed = settled.getNow(null);
            return pushed != null ? pushed : polled;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (ExecutionException e) {
          // The abort signal completed exceptionally; the state below is the answer either way.
          break;
        }
      }

      SagaStateSnapshot pushed = settled.getNow(null);
      // The read is what makes an un-notified wait worth having: a saga resumed and settled on
      // another replica is invisible to the future above, and visible here.
      return pushed != null ? pushed : read.get();
    } finally {
      // Retires every `anyOf` built above, whichever exit brought us here. Completing a source of
      // an `anyOf` is what makes the JDK unlink its node from the other sources' stacks.
      finished.complete(null);
    }
  }

  /**
   * The answer when the saga has already settled, or {@code null} when it has not.
   *
   * <p>Consulted where polling begins, which is the first moment a settle can have happened without
   * reaching this wait. A start registers only once it has a saga id, so a saga that parks and is
   * resumed in that window settles with nothing listening: the callback belonging to the first
   * drive died at the park, and the registration had not happened yet. Neither reports it, and
   * without this read the wait would sit until a tick found what was already decided.
   *
   * <p>The push is consulted first, so a wait that was notified pays no read.
   *
   * @param settled the future a local push completes
   * @param read reads the saga's current state
   * @param deadlineNanos when the bound expires, so a read that outlives it answers rather than
   *     being followed by a second one
   * @return the settled snapshot, or {@code null} when the saga has not settled
   */
  private static @Nullable SagaStateSnapshot settledAnswer(
      CompletableFuture<SagaStateSnapshot> settled,
      Supplier<SagaStateSnapshot> read,
      long deadlineNanos) {
    SagaStateSnapshot pushed = settled.getNow(null);
    if (pushed != null) {
      return pushed;
    }
    SagaStateSnapshot current = read.get();
    // Terminal, or this read itself outlived the bound. The second case is the poll tick's rule,
    // for the same reason: a slow read followed by the bound expiry read would spend two
    // transactions on one answer, on exactly the store this is meant to spare.
    if (current.getStatus().isTerminal() || System.nanoTime() - deadlineNanos >= 0) {
      // A drive may have settled the saga while that read was in flight, which the read cannot see
      // but the future already holds.
      SagaStateSnapshot raced = settled.getNow(null);
      return raced != null ? raced : current;
    }
    return null;
  }

  /**
   * The signals that end a slice. Rebuilt once if polling begins mid-wait, because {@code anyOf}
   * stays completed: reusing one that the park signal already completed would spin the loop.
   *
   * <p>{@code finished} belongs in every set this builds, the rebuilt one included. It is what
   * retires the resulting node once the wait is over; the rebuilt set is the one that would
   * otherwise be retained, since the park has already fired and cleared the first.
   */
  private static CompletableFuture<?> wakeUp(
      CompletableFuture<SagaStateSnapshot> settled,
      CompletableFuture<?> abort,
      @Nullable CompletableFuture<?> pollFrom,
      CompletableFuture<?> finished) {
    List<CompletableFuture<?>> signals = new ArrayList<>(4);
    signals.add(settled);
    signals.add(abort);
    signals.add(finished);
    if (pollFrom != null) {
      signals.add(pollFrom);
    }
    return CompletableFuture.anyOf(signals.toArray(new CompletableFuture<?>[0]));
  }
}
