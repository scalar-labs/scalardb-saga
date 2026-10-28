package com.scalar.db.saga.benchmark;

import com.scalar.db.saga.api.SagaOrchestrator;

/**
 * Owns one fully-wired {@link SagaOrchestrator} under test plus whatever infrastructure the mode
 * needs (a store, an in-process daemon, a fake participant). The runner sees only the interface;
 * switching harnesses switches the implementation and nothing else.
 */
interface BenchHarness extends AutoCloseable {

  /** The orchestrator under test. Owned by the harness — released by {@link #close()}. */
  SagaOrchestrator orchestrator();

  /** One-line description of what is being benchmarked, for the report header. */
  String description();

  /**
   * Step executions beyond the first per saga observed by this harness's instrumentation, or {@code
   * -1} when the mode cannot observe them (an external server's participants are not ours).
   */
  long duplicateStepExecutions();

  @Override
  void close();
}
