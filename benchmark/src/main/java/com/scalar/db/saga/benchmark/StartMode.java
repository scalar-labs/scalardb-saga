package com.scalar.db.saga.benchmark;

/** How each benchmark operation starts its saga and observes the outcome. */
public enum StartMode {
  /**
   * {@code start(...)}: blocks until the saga is terminal. Embedded, this executes the saga on the
   * worker thread; over gRPC it long-polls {@code AwaitSaga} — both mirror what a synchronous
   * client experiences, including the request-thread pressure the sync path puts on a server.
   */
  SYNC,

  /**
   * {@code startAsync(...)} then poll {@code getStateSnapshot} until terminal or the per-operation
   * timeout. Separates accept latency (the start call) from end-to-end latency (to terminal).
   */
  ASYNC_POLL,

  /**
   * {@code startAsync(...)} only; outcomes are resolved in the drain phase after the workers stop.
   * Measures pure admission throughput and lets the engine's backlog behavior show up as sagas
   * still pending at drain end.
   */
  ASYNC_FIRE
}
