/**
 * Load-generation harness for ScalarDB Saga. Drives the {@link
 * com.scalar.db.saga.api.SagaOrchestrator} interface so the identical workload runs against the
 * embedded engine ({@code DefaultSagaOrchestrator}), an in-process daemon over real gRPC ({@code
 * GrpcSagaOrchestratorClient} against a {@code SagaServer} on ephemeral ports), or an external
 * daemon. Built to make concurrency collapses observable: it reports per-interval progress, stall
 * warnings, terminal-status and error breakdowns, and duplicate step executions.
 */
@NullMarked
package com.scalar.db.saga.benchmark;

import org.jspecify.annotations.NullMarked;
