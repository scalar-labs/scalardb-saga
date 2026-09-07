package com.scalar.db.saga.engine;

import com.scalar.db.saga.api.SagaStateSnapshot;

/**
 * A hook a front end installs to learn that a saga settled <em>on this process</em>, whichever
 * drive settled it.
 *
 * <p>It exists because {@link com.scalar.db.saga.api.SagaCallback} cannot serve that purpose. A
 * callback belongs to one {@code startAsync} call and one drive: when a saga parks and later
 * resumes, the resume is a separate drive that carries no callback, so the original one is
 * structurally incapable of firing again. A listener is installed per process instead of per start,
 * so the resumed drive reaches it.
 *
 * <p>The intended implementation is a registry of in-flight waiters keyed by saga id — a daemon's
 * bounded synchronous start, or its long-poll — which is why {@link #isWatching} exists: settling a
 * saga nobody is waiting for must cost nothing, and in particular must not provoke the store read
 * that {@code dispatchOutcome} falls back on when a drive dies before reaching a verdict.
 *
 * <p><b>Best-effort, never a correctness mechanism.</b> It fires only for a drive on this process,
 * so a saga resumed on another replica settles without any local notification. Every caller must
 * therefore keep a fallback that does not depend on being notified. It is an optimisation that
 * removes latency, not a delivery guarantee.
 *
 * <p><b>Recovery drives are excluded.</b> A saga settled by the recovery manager does not reach
 * this listener even when recovery ran on this process, so "whichever drive" above means a start or
 * a resume, not every drive. Deliberate: every recovery timescale sits at or above a synchronous
 * wait bound — sixty seconds of staleness, parked deadlines in minutes to hours, a four-hour
 * compensation grace — so a waiter is almost never still present when recovery settles a saga, and
 * wiring it would mean carrying this listener to three more drive sites for a case the fallback
 * already covers.
 *
 * <p>Implementations must be thread-safe: drives run concurrently on the async executor. They must
 * also be quick and must not block — this runs on the drive's own thread, and a slow listener
 * delays the callback dispatch behind it.
 */
public interface SettlementListener {

  /** A listener that watches nothing, for an orchestrator with no front end installed. */
  SettlementListener NO_OP =
      new SettlementListener() {
        @Override
        public boolean isWatching(String sagaId) {
          return false;
        }

        @Override
        public void onSagaSettled(SagaStateSnapshot saga) {}
      };

  /**
   * Whether anything on this process is waiting for the given saga. Called on every drive's
   * completion, so it must be cheap — a map lookup, not a store read.
   *
   * @param sagaId the saga that is settling
   * @return {@code true} if {@link #onSagaSettled} should be called for it
   */
  boolean isWatching(String sagaId);

  /**
   * Called when a saga reaches a terminal state on this process and {@link #isWatching} returned
   * {@code true} for it. Never called for a non-terminal outcome: a parked saga is still live, and
   * a waiter has nothing to act on until it settles.
   *
   * @param saga the settled saga's snapshot, in a terminal status
   */
  void onSagaSettled(SagaStateSnapshot saga);
}
