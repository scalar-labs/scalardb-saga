/**
 * The daemon's authentication SPI and access policy, shared by the REST and gRPC transports.
 *
 * <p>A {@link SagaSecurityProvider} authenticates a transport-neutral {@link SagaAuthRequest} and
 * resolves it to a {@link SagaIdentity}: a principal for the audit trail and the {@link SagaRole}s
 * the caller holds. Authorization is then the same for every provider and transport: each {@link
 * SagaOperation} names the minimum role it requires, and the identity must hold a role that implies
 * it. The REST before-handler, {@link SagaSecurityHandler}, and the gRPC interceptor consult the
 * one configured provider and the one policy, so an operation cannot be exposed under a different
 * rule on one transport than on the other.
 *
 * <p>Built-in providers: {@link NoopSecurityProvider} (the default, which authenticates every
 * request as a full-access identity), {@link JwtSecurityProvider} and {@link
 * ApiKeySecurityProvider}. A provider rejects a credential with {@link
 * SagaAuthenticationException}, reports its own outage with {@link SagaAuthUnavailableException},
 * and the RBAC check refuses a known caller with {@link SagaAuthorizationException}; the transports
 * map the three to {@code 401}, {@code 503} and {@code 403} and their gRPC counterparts.
 */
@NullMarked
package com.scalar.db.saga.server.security;

import org.jspecify.annotations.NullMarked;
