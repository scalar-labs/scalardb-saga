/**
 * The daemon's gRPC transport: the wire rendering of the saga lifecycle and admin APIs.
 *
 * <p>{@link SagaServiceImpl} serves the lifecycle RPCs (start, await, get, detail) and {@link
 * AdminServiceImpl} the operator ones (list, recover, force-complete, reset), both over the same
 * orchestrator instance the REST routes use. Every privileged service is wrapped in {@link
 * SagaSecurityInterceptor}, which authenticates the call and checks the role its {@link
 * com.scalar.db.saga.server.security.SagaOperation} requires, and, when rate limiting is enabled,
 * in {@link SagaRateLimitInterceptor}, which charges the caller's budget for the operations {@code
 * SagaOperation} marks rate-limited and refuses to wave through a method it cannot map; {@link
 * GrpcOperations} maps method names to operations so the two transports share one policy. Every
 * error and every interceptor refusal is composed through {@link GrpcErrorMapper}, so each carries
 * its {@link com.scalar.db.saga.exception.SagaErrorCode} in an {@code ErrorInfo} detail.
 */
@NullMarked
package com.scalar.db.saga.server.grpc;

import org.jspecify.annotations.NullMarked;
