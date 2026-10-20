package com.scalar.db.saga.api;

/**
 * Callback interface for asynchronous saga completion notifications. Passed to {@link
 * SagaOrchestrator#startAsync} to receive notifications when the saga reaches a terminal state, or
 * parks short of one.
 *
 * <p>The two implementations differ on a saga that parks. The embedded orchestrator reports the
 * outcome of the drive the start began, so a park ends it with {@link #onParked}. The client SDK
 * watches the saga on the server until it is terminal, so it never calls {@link #onParked} and
 * reports the terminal state instead, {@link #onEscalated} included.
 *
 * <p>Neither guarantees a call. The client SDK calls nothing once it is closed, once the saga is
 * purged, or after a failure that retrying cannot fix, and neither calls anything if its process
 * exits. Code that waits for a callback should wait under a bound and fall back to {@link
 * SagaOrchestrator#getStateSnapshot}.
 */
public interface SagaCallback {

  /**
   * Called when the saga completes successfully (all steps executed and confirmed).
   *
   * @param saga the saga's final state
   */
  void onCompleted(SagaStateSnapshot saga);

  /**
   * Called when the saga is fully compensated (all compensations succeeded).
   *
   * @param saga the saga's final state
   */
  void onCompensated(SagaStateSnapshot saga);

  /**
   * Called when the saga is escalated ({@link SagaStatus#ESCALATED}): stuck beyond its grace period
   * and needing manual intervention.
   *
   * <p><b>From the embedded orchestrator, this does not fire in the normal course.</b> A drive's
   * own verdict is never {@code ESCALATED}; escalation is set only by recovery, which runs on its
   * own schedule, possibly on another replica, with no access to the callback the start supplied.
   * Reaching here at all takes a drive that fails before producing a verdict, so that the outcome
   * must be read back from the store, plus recovery escalating the same saga in the window before
   * that read. That path is real, but it is not something to design against: to observe escalation,
   * poll {@link SagaOrchestrator#getStateSnapshot}.
   *
   * <p>The client SDK does call it: it watches the saga on the server, where recovery's escalation
   * is visible.
   *
   * <p>Defaults to doing nothing, so no implementation has to write a method that all but never
   * runs embedded. It stays on the interface rather than being removed so that the narrow path
   * above still reaches the caller that asked to hear about it.
   *
   * @param saga the saga's state at escalation
   */
  default void onEscalated(SagaStateSnapshot saga) {}

  /**
   * Called when the saga stops without reaching a terminal state: it parked on an asynchronous step
   * and is waiting for that step's callback or its deadline ({@link SagaStatus#WAITING}). The saga
   * is still live and still owned by the engine, and resumes when the step reports back.
   *
   * <p>Called by the embedded orchestrator only. The client SDK keeps watching a parked saga and
   * reports its terminal state instead.
   *
   * <p><b>This is the last thing this callback will hear about the saga.</b> Not "might be" — the
   * resume path carries no {@code SagaCallback}, so once {@code onParked} has fired, {@link
   * #onCompleted}, {@link #onCompensated} and {@link #onEscalated} will never fire for it. Poll
   * {@link SagaOrchestrator#getStateSnapshot} or {@link SagaOrchestrator#getSagaDetail} to observe
   * the eventual outcome; an implementation that waits for a terminal method instead waits forever.
   *
   * <p>Parking is a normal outcome, not a failure. This method exists because it is the only notice
   * that no terminal callback is coming. An implementation that signals completion from {@link
   * #onCompleted} and its siblings — counting down a latch, completing a future — must therefore
   * not go on waiting for one of those to fire, or it waits forever on a saga that is alive and
   * still progressing. Two shapes satisfy that. Release the caller here and let it poll {@link
   * SagaOrchestrator#getStateSnapshot} for the outcome; or keep waiting, but under a bound, and
   * poll from this point rather than depending on a callback that cannot arrive. The bounded shape
   * is what this project's own daemon does, because a saga that parks may still finish while the
   * caller is willing to wait, and answering at the park would discard that outcome.
   *
   * <p>Defaults to doing nothing because observing a park is optional: a saga with no asynchronous
   * step never parks, and such a caller should not be made to write an empty method for it.
   * Override it whenever a saga may contain an asynchronous step.
   *
   * @param saga the saga's state at the park
   */
  default void onParked(SagaStateSnapshot saga) {}
}
