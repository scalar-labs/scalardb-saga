/**
 * HTTP transport internals: the shared HTTP engine ({@code HttpExchange} + {@code
 * OutboundHttpPolicy}) plus the front-ends that ride it — the declarative transport adapter and the
 * code-step {@code SagaHttpClient} ({@link com.scalar.db.saga.transport.SagaHttpClientImpl},
 * exposed to users as {@link com.scalar.db.saga.api.SagaHttpClient}). Built-in HTTP support
 * propagates the saga correlation headers, classifies status codes, and enforces the outbound HTTP
 * policy.
 *
 * <p>Classes here are internal — reachable by users only through the {@code api} interfaces they
 * implement — with two exceptions that form the user-facing configuration hot-reload seam: {@link
 * com.scalar.db.saga.transport.HttpEndpointRegistrar} (obtained via the orchestrator's {@code
 * httpEndpointRegistrar()}) and {@link com.scalar.db.saga.transport.HttpServiceConfig} (the value a
 * swap caller constructs).
 */
@NullMarked
package com.scalar.db.saga.transport;

import org.jspecify.annotations.NullMarked;
