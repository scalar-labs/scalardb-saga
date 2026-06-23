/**
 * HTTP transport internals: the shared HTTP engine ({@code HttpExchange} + {@code
 * OutboundHttpPolicy}) plus the front-ends that ride it — the declarative transport adapter and the
 * code-step {@code SagaHttpClient} ({@link com.scalar.db.saga.transport.SagaHttpClientImpl},
 * exposed to users as {@link com.scalar.db.saga.api.SagaHttpClient}). Built-in HTTP support
 * propagates the saga correlation headers, classifies status codes, and enforces the outbound HTTP
 * policy.
 *
 * <p>Classes here are internal; the only user-facing types are the {@code api} interfaces they
 * implement.
 */
@NullMarked
package com.scalar.db.saga.transport;

import org.jspecify.annotations.NullMarked;
