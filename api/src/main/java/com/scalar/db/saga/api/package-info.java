/**
 * The public API of ScalarDB Saga: {@link SagaOrchestrator}, which starts sagas and reads their
 * state; {@link SagaAdminService}, which lists and intervenes in them; the {@link Step} and {@link
 * TccStep} contracts a code step implements; and the immutable value types they exchange.
 *
 * <p>Application code depends on this package and on {@code com.scalar.db.saga.exception} only,
 * whether it runs the engine in-process or talks to a saga server through the Java client SDK: both
 * implement the same interfaces. Everything here is Java 8 compatible.
 */
@NullMarked
package com.scalar.db.saga.api;

import org.jspecify.annotations.NullMarked;
