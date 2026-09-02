package com.scalar.db.saga.api;

/**
 * Callback interface for asynchronous saga completion notifications. Passed to {@link
 * SagaOrchestrator#startAsync} to receive notifications when the saga reaches a terminal state, or
 * parks short of one.
 */
public interface SagaCallback {

  /** Called when the saga completes successfully (all steps executed and confirmed). */
  void onCompleted(SagaStateSnapshot saga);

  /** Called when the saga is fully compensated (all compensations succeeded). */
  void onCompensated(SagaStateSnapshot saga);

  /**
   * Called when the saga is escalated (stuck beyond grace period, needs manual intervention).
   *
   * <p>Note: Currently, escalation only occurs during recovery, which runs asynchronously on a
   * separate thread without access to the original callback. This method is provided for future
   * use. To detect escalations, poll {@link SagaOrchestrator#getStateSnapshot}.
   */
  void onEscalated(SagaStateSnapshot saga);

  /**
   * Called when the saga stops without reaching a terminal state: it parked on an asynchronous step
   * and is waiting for that step's callback or its deadline ({@link SagaStatus#WAITING}). The saga
   * is still live and still owned by the engine, and resumes when the step reports back.
   *
   * <p><b>This is the last thing this callback will hear about the saga.</b> Not "might be" — the
   * resume path carries no {@code SagaCallback}, so once {@code onParked} has fired, {@link
   * #onCompleted}, {@link #onCompensated} and {@link #onEscalated} will never fire for it. Poll
   * {@link SagaOrchestrator#getStateSnapshot} or {@link SagaOrchestrator#getSagaDetail} to observe
   * the eventual outcome; an implementation that waits for a terminal method instead waits forever.
   *
   * <p>Parking is a normal outcome, not a failure. It is reported because a caller waiting for the
   * saga needs to know the wait is over: without it, a bounded synchronous start has nothing to
   * wake on and waits out its entire bound for a saga that stopped progressing in milliseconds.
   *
   * <p>Defaults to doing nothing, so an existing implementation keeps compiling and keeps its
   * previous behaviour. Override it when the caller must be released as soon as the saga parks.
   */
  default void onParked(SagaStateSnapshot saga) {}
}
