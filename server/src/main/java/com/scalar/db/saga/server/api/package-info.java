/**
 * The REST transport: Javalin routes over the orchestrator and the admin service, and the JSON
 * shapes they exchange.
 *
 * <p>{@link SagaResource} serves saga starts, reads and listing; {@link SagaAdminResource} the
 * operator interventions; {@link CallbackResource} the signed async-step callbacks whose URLs
 * {@link HmacCallbackUrlProvider} mints; and {@link HealthResource} the probes. The request and
 * response records ({@link StartSagaRequest}, {@link SagaDetailResponse} and their siblings) are
 * the JSON bodies as the wire sees them, kept apart from the api value types so the REST contract
 * stays fixed while those evolve. {@link ErrorMapper} turns every exception into the one JSON error
 * body; {@link RateLimitHandler} charges the same {@link RateLimiter} the gRPC interceptor charges,
 * so a caller's start budget spans both transports; and {@link RequestParsing} is the edge parsing
 * both transports share.
 */
@NullMarked
package com.scalar.db.saga.server.api;

import org.jspecify.annotations.NullMarked;
