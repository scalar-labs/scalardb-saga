/**
 * The standalone saga server: the daemon that hosts an embedded engine and serves it over REST and
 * gRPC.
 *
 * <p>{@link SagaServerCommand} is the entry point: it parses the command line, loads the properties
 * file into a {@link SagaServerConfig}, and runs a {@link SagaServer}, which builds the engine,
 * binds the enabled transports, and drains them on shutdown. The rest of the package is the
 * daemon's own machinery: the configuration pass that validates and registers service files and
 * saga definitions and re-runs it on an interval ({@code ConfigReconciler}, {@code
 * SagaConfigReloadManager}), the resolution of {@code ${file:...}} and {@code ${env:...}} secret
 * references, TLS material loading and reload, the security-provider factory, and the registry that
 * wakes a synchronous start when its saga settles.
 *
 * <p>The REST and gRPC transports live in the {@code api} and {@code grpc} subpackages; {@code
 * security} holds the authentication SPI and the access policy both share.
 */
@NullMarked
package com.scalar.db.saga.server;

import org.jspecify.annotations.NullMarked;
