/**
 * The saga engine and the embedded-mode entry point.
 *
 * <p>{@link DefaultSagaOrchestrator} is the public embedded entry point (build via {@code
 * newBuilder()}, then register definitions, start sagas, and run background tasks). The rest of the
 * package is internal to the engine: the execution loop and context, TCC adapters, retry/timeout
 * policy, the definition registry, the crash-recovery and retention managers, engine configuration
 * ({@code RecoveryConfig} / {@code RetentionConfig} / {@code ShutdownMode}), and step resolution.
 *
 * <p>{@link SettlementListener} is the one other type meant to be used from outside: a front end
 * implements it and installs it through the builder to learn that a saga settled, whichever drive
 * did it. The daemon's waiter registry is the implementation this project ships.
 */
@NullMarked
package com.scalar.db.saga.engine;

import org.jspecify.annotations.NullMarked;
