/**
 * Saga definitions: what a saga is made of, built in code or parsed from JSON or YAML.
 *
 * <p>{@link SagaDefinition} is the immutable, validated description of one saga: its steps, mode,
 * recovery strategy and timeouts. Build it with {@code SagaDefinition.newBuilder(name)}, which
 * hands out a builder typed to the chosen mode, or read it from a document with {@link
 * SagaDefinitionParser}. A step is either a class step, backed by an application's {@code Step} or
 * {@code TccStep} implementation, or a declarative service step whose {@link CallSpec} per phase
 * says how to call a registered remote service with no Java code; {@link HttpCall} is the HTTP call
 * spec. {@link RetryPolicy} bounds the retries of a step's forward action and of its compensation.
 *
 * <p>Application code building sagas in embedded mode uses this package directly; the saga server
 * reads the same definitions from its configured directory.
 */
@NullMarked
package com.scalar.db.saga.definition;

import org.jspecify.annotations.NullMarked;
